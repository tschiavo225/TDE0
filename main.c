#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <math.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CITY_COUNT 2
#define CITY_CAXIAS 0
#define CITY_BENTO 1
#define MAX_TIME_HITS 8
#define DISPLAY_TZ_OFFSET_SECONDS (-3 * 3600)

static const char *CITY_NAMES[CITY_COUNT] = {
    "Caxias do Sul",
    "Bento Gonçalves"
};

typedef struct {
    int city;

    int has_temp;
    double temp;
    time_t temp_ts;

    int has_humidity;
    double humidity;
    time_t humidity_ts;

    int has_pressure;
    double pressure;
    time_t pressure_ts;

    int has_battery;
    double battery;
    time_t battery_ts;

    int has_sf;
    int sf;
    time_t sf_ts;

    int source_file_index;
    time_t record_ts;
} SensorRecord;

typedef struct {
    SensorRecord *items;
    size_t size;
    size_t capacity;
} RecordVector;

typedef struct {
    unsigned long long *items;
    size_t capacity;
    size_t size;
} HashSet;

typedef struct {
    char filename[256];
    long processed;
    long duplicates;
    time_t min_ts;
    time_t max_ts;
    int has_period;
} FileSummary;

typedef struct LogNode {
    char *text;
    struct LogNode *next;
} LogNode;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    LogNode *head;
    LogNode *tail;
    int finished;
} LogQueue;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond_reading_done;
    int reading_done;

    RecordVector records;
    FileSummary *files;
    int file_count;
} SharedData;

typedef struct {
    int count;
    double min;
    double max;
    double sum;

    time_t min_times[MAX_TIME_HITS];
    int min_time_count;
    int min_time_extra;

    time_t max_times[MAX_TIME_HITS];
    int max_time_count;
    int max_time_extra;
} MetricStats;

typedef struct {
    MetricStats temperature;
    MetricStats humidity;
    MetricStats pressure;

    int has_initial_battery;
    double initial_battery;
    time_t initial_battery_ts;

    int has_final_battery;
    double final_battery;
    time_t final_battery_ts;

    int sf_used[256];
} CityStats;

typedef struct {
    CityStats city[CITY_COUNT];
} StatsResult;

typedef struct {
    SharedData *shared;
    LogQueue *log_queue;
    char **input_files;
    int input_file_count;
} ReaderArgs;

typedef struct {
    SharedData *shared;
    LogQueue *log_queue;
    StatsResult *result;
} StatsArgs;

static LogQueue g_log_queue;
static SharedData g_shared;
static StatsResult g_stats;
static int g_verbose_log = 0;

static void die(const char *message) {
    fprintf(stderr, "Erro: %s\n", message);
    exit(EXIT_FAILURE);
}

static char *xstrdup(const char *s) {
    size_t len = strlen(s);
    char *copy = malloc(len + 1);
    if (!copy) die("memória insuficiente");
    memcpy(copy, s, len + 1);
    return copy;
}

static void log_queue_init(LogQueue *q) {
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->head = NULL;
    q->tail = NULL;
    q->finished = 0;
}

static void log_queue_finish(LogQueue *q) {
    pthread_mutex_lock(&q->mutex);
    q->finished = 1;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

static void log_msg(LogQueue *q, const char *fmt, ...) {
    char buffer[1024];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);

    LogNode *node = malloc(sizeof(LogNode));
    if (!node) return;
    node->text = xstrdup(buffer);
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail) {
        q->tail->next = node;
    } else {
        q->head = node;
    }
    q->tail = node;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

static void *logger_thread(void *arg) {
    LogQueue *q = (LogQueue *)arg;
    FILE *log_file = fopen("processamento.log", "w");
    if (!log_file) {
        fprintf(stderr, "Não foi possível criar processamento.log: %s\n", strerror(errno));
        return NULL;
    }

    time_t logger_start = time(NULL);
    struct tm logger_tm;
    localtime_r(&logger_start, &logger_tm);
    char logger_stamp[32];
    strftime(logger_stamp, sizeof(logger_stamp), "%Y-%m-%d %H:%M:%S", &logger_tm);
    fprintf(log_file, "[%s] Thread de logs iniciada. Arquivo processamento.log aberto para auditoria detalhada.\n", logger_stamp);

    for (;;) {
        pthread_mutex_lock(&q->mutex);
        while (!q->head && !q->finished) {
            pthread_cond_wait(&q->cond, &q->mutex);
        }

        if (!q->head && q->finished) {
            pthread_mutex_unlock(&q->mutex);
            break;
        }

        LogNode *node = q->head;
        q->head = node->next;
        if (!q->head) q->tail = NULL;
        pthread_mutex_unlock(&q->mutex);

        time_t now = time(NULL);
        struct tm tm_now;
        localtime_r(&now, &tm_now);
        char stamp[32];
        strftime(stamp, sizeof(stamp), "%Y-%m-%d %H:%M:%S", &tm_now);

        fprintf(log_file, "[%s] %s\n", stamp, node->text);
        free(node->text);
        free(node);
    }

    time_t logger_end = time(NULL);
    struct tm logger_end_tm;
    localtime_r(&logger_end, &logger_end_tm);
    char logger_end_stamp[32];
    strftime(logger_end_stamp, sizeof(logger_end_stamp), "%Y-%m-%d %H:%M:%S", &logger_end_tm);
    fprintf(log_file, "[%s] Thread de logs finalizada. Todas as mensagens pendentes foram gravadas.\n", logger_end_stamp);

    fclose(log_file);
    return NULL;
}

static void record_vector_init(RecordVector *v) {
    v->items = NULL;
    v->size = 0;
    v->capacity = 0;
}

static void record_vector_push(RecordVector *v, SensorRecord rec) {
    if (v->size == v->capacity) {
        size_t new_capacity = v->capacity == 0 ? 1024 : v->capacity * 2;
        SensorRecord *new_items = realloc(v->items, new_capacity * sizeof(SensorRecord));
        if (!new_items) die("memória insuficiente ao armazenar registros");
        v->items = new_items;
        v->capacity = new_capacity;
    }
    v->items[v->size++] = rec;
}

static unsigned long long fnv1a_hash(const char *s) {
    unsigned long long h = 1469598103934665603ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 1099511628211ULL;
    }
    return h ? h : 1ULL;
}

static void hashset_init(HashSet *set, size_t initial_capacity) {
    set->capacity = initial_capacity;
    set->size = 0;
    set->items = calloc(set->capacity, sizeof(unsigned long long));
    if (!set->items) die("memória insuficiente no hashset");
}

static void hashset_free(HashSet *set) {
    free(set->items);
    set->items = NULL;
    set->capacity = 0;
    set->size = 0;
}

static void hashset_rehash(HashSet *set) {
    HashSet new_set;
    hashset_init(&new_set, set->capacity * 2);

    for (size_t i = 0; i < set->capacity; i++) {
        unsigned long long value = set->items[i];
        if (!value) continue;

        size_t pos = value % new_set.capacity;
        while (new_set.items[pos]) {
            pos = (pos + 1) % new_set.capacity;
        }
        new_set.items[pos] = value;
        new_set.size++;
    }

    free(set->items);
    *set = new_set;
}

static int hashset_insert(HashSet *set, unsigned long long value) {
    if ((set->size + 1) * 10 > set->capacity * 7) {
        hashset_rehash(set);
    }

    size_t pos = value % set->capacity;
    while (set->items[pos]) {
        if (set->items[pos] == value) return 0;
        pos = (pos + 1) % set->capacity;
    }

    set->items[pos] = value;
    set->size++;
    return 1;
}

static char *read_entire_file(const char *path, size_t *size_out) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "Não foi possível abrir %s: %s\n", path, strerror(errno));
        return NULL;
    }

    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }

    long size = ftell(f);
    if (size < 0) {
        fclose(f);
        return NULL;
    }
    rewind(f);

    char *buffer = malloc((size_t)size + 1);
    if (!buffer) {
        fclose(f);
        die("memória insuficiente ao ler arquivo");
    }

    size_t read_size = fread(buffer, 1, (size_t)size, f);
    fclose(f);
    buffer[read_size] = '\0';
    if (size_out) *size_out = read_size;
    return buffer;
}

static const char *basename_simple(const char *path) {
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static const char *city_name_or_unknown(int city) {
    if (city >= 0 && city < CITY_COUNT) return CITY_NAMES[city];
    return "Cidade desconhecida";
}

static char *unescape_json_string(const char *start, const char **end_out) {
    size_t capacity = 4096;
    size_t size = 0;
    char *out = malloc(capacity);
    if (!out) die("memória insuficiente ao converter JSON interno");

    const char *p = start;
    while (*p) {
        char c = *p++;
        if (c == '"') {
            break;
        }

        if (c == '\\') {
            char esc = *p++;
            switch (esc) {
                case '"': c = '"'; break;
                case '\\': c = '\\'; break;
                case '/': c = '/'; break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case 'u':
                    /* Para este trabalho não precisamos decodificar unicode. */
                    c = '?';
                    for (int i = 0; i < 4 && isxdigit((unsigned char)*p); i++) p++;
                    break;
                default:
                    c = esc;
                    break;
            }
        }

        if (size + 2 > capacity) {
            capacity *= 2;
            char *new_out = realloc(out, capacity);
            if (!new_out) die("memória insuficiente ao expandir JSON interno");
            out = new_out;
        }
        out[size++] = c;
    }

    out[size] = '\0';
    if (end_out) *end_out = p;
    return out;
}

static int extract_string_field(const char *json, const char *field, char *out, size_t out_size) {
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\":\"", field);
    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p += strlen(pattern);

    size_t i = 0;
    while (*p && *p != '"') {
        if (i + 1 < out_size) out[i++] = *p;
        p++;
    }
    out[i] = '\0';
    return 1;
}

static int determine_city(const char *device_name) {
    if (strstr(device_name, "Caxias")) return CITY_CAXIAS;
    if (strstr(device_name, "Bento")) return CITY_BENTO;
    return -1;
}

static time_t parse_iso_datetime(const char *s) {
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    if (sscanf(s, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6) {
        return (time_t)0;
    }

    struct tm tm_value;
    memset(&tm_value, 0, sizeof(tm_value));
    tm_value.tm_year = year - 1900;
    tm_value.tm_mon = month - 1;
    tm_value.tm_mday = day;
    tm_value.tm_hour = hour;
    tm_value.tm_min = minute;
    tm_value.tm_sec = second;
    tm_value.tm_isdst = 0;

    time_t epoch = timegm(&tm_value);

    const char *tz = strchr(s, 'T');
    if (tz) {
        tz = tz + 1;
        tz = strchr(tz, '+') ? strchr(tz, '+') : strchr(tz, '-');
    }

    if (tz && strlen(tz) >= 6) {
        int tz_hour = 0, tz_min = 0;
        if (sscanf(tz + 1, "%2d:%2d", &tz_hour, &tz_min) == 2) {
            int offset = (tz_hour * 3600) + (tz_min * 60);
            if (*tz == '-') offset = -offset;
            epoch -= offset;
        }
    }

    return epoch;
}

static void format_datetime_br(time_t epoch, char *out, size_t out_size) {
    if (!epoch) {
        snprintf(out, out_size, "N/D");
        return;
    }

    time_t display_epoch = epoch + DISPLAY_TZ_OFFSET_SECONDS;
    struct tm tm_value;
    gmtime_r(&display_epoch, &tm_value);
    strftime(out, out_size, "%d/%m/%Y %H:%M:%S", &tm_value);
}

static void format_date_br(time_t epoch, char *out, size_t out_size) {
    if (!epoch) {
        snprintf(out, out_size, "N/D");
        return;
    }

    time_t display_epoch = epoch + DISPLAY_TZ_OFFSET_SECONDS;
    struct tm tm_value;
    gmtime_r(&display_epoch, &tm_value);
    strftime(out, out_size, "%d/%m/%Y", &tm_value);
}

static void format_measurement_detail(char *out, size_t out_size, int has_value, double value,
                                      time_t ts, const char *unit) {
    if (!has_value) {
        snprintf(out, out_size, "N/D");
        return;
    }

    char time_text[64];
    format_datetime_br(ts, time_text, sizeof(time_text));
    snprintf(out, out_size, "%.2f%s em %s", value, unit, time_text);
}

static void format_sf_detail(char *out, size_t out_size, int has_sf, int sf, time_t ts) {
    if (!has_sf) {
        snprintf(out, out_size, "N/D");
        return;
    }

    char time_text[64];
    format_datetime_br(ts, time_text, sizeof(time_text));
    snprintf(out, out_size, "SF%d em %s", sf, time_text);
}

static void log_record_detail(LogQueue *log_queue, const char *filename, long sequence, const char *source_field,
                              const SensorRecord *rec) {
    if (!g_verbose_log) return;
    char base_time[64];
    char temp[96];
    char humidity[96];
    char pressure[96];
    char battery[96];
    char sf[96];

    format_datetime_br(rec->record_ts, base_time, sizeof(base_time));
    format_measurement_detail(temp, sizeof(temp), rec->has_temp, rec->temp, rec->temp_ts, " °C");
    format_measurement_detail(humidity, sizeof(humidity), rec->has_humidity, rec->humidity, rec->humidity_ts, " %");
    format_measurement_detail(pressure, sizeof(pressure), rec->has_pressure, rec->pressure, rec->pressure_ts, " hPa");
    format_measurement_detail(battery, sizeof(battery), rec->has_battery, rec->battery, rec->battery_ts, " V");
    format_sf_detail(sf, sizeof(sf), rec->has_sf, rec->sf, rec->sf_ts);

    log_msg(log_queue,
            "Registro aceito #%ld | arquivo=%s | campo=%s | cidade=%s | data_base=%s | temperatura=%s | umidade=%s | pressao=%s | bateria=%s | spreading_factor=%s",
            sequence, filename, source_field, city_name_or_unknown(rec->city), base_time, temp, humidity, pressure, battery, sf);
}

static int extract_object_value_number(const char *from, const char *object_end, double *value_out) {
    const char *p = strstr(from, "\"value\":");
    if (!p || (object_end && p > object_end)) return 0;
    p += strlen("\"value\":");
    while (*p && isspace((unsigned char)*p)) p++;

    if (*p == '"') return 0;

    char *endptr = NULL;
    double value = strtod(p, &endptr);
    if (endptr == p) return 0;
    *value_out = value;
    return 1;
}

static int extract_object_value_string(const char *from, const char *object_end, char *out, size_t out_size) {
    const char *p = strstr(from, "\"value\":");
    if (!p || (object_end && p > object_end)) return 0;
    p += strlen("\"value\":");
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return 0;
    p++;

    size_t i = 0;
    while (*p && *p != '"' && (!object_end || p < object_end)) {
        if (i + 1 < out_size) out[i++] = *p;
        p++;
    }
    out[i] = '\0';
    return 1;
}

static int extract_object_time(const char *from, const char *object_end, time_t *ts_out) {
    const char *p = strstr(from, "\"time\":\"");
    if (!p || (object_end && p > object_end)) return 0;
    p += strlen("\"time\":\"");

    char time_text[64];
    size_t i = 0;
    while (*p && *p != '"' && (!object_end || p < object_end)) {
        if (i + 1 < sizeof(time_text)) time_text[i++] = *p;
        p++;
    }
    time_text[i] = '\0';

    *ts_out = parse_iso_datetime(time_text);
    return *ts_out != 0;
}

static void update_record_ts(SensorRecord *rec, time_t ts) {
    if (!ts) return;
    if (!rec->record_ts || ts < rec->record_ts) rec->record_ts = ts;
}

static int parse_inner_payload(const char *inner_json, SensorRecord *rec) {
    memset(rec, 0, sizeof(*rec));

    char device_name[256];
    if (!extract_string_field(inner_json, "device_name", device_name, sizeof(device_name))) {
        return 0;
    }

    int city = determine_city(device_name);
    if (city < 0) return 0;
    rec->city = city;

    const char *p = inner_json;
    while ((p = strstr(p, "\"variable\":\"")) != NULL) {
        p += strlen("\"variable\":\"");

        char variable[128];
        size_t vi = 0;
        while (*p && *p != '"') {
            if (vi + 1 < sizeof(variable)) variable[vi++] = *p;
            p++;
        }
        variable[vi] = '\0';

        const char *object_end = strchr(p, '}');
        double numeric_value = 0.0;
        time_t ts = 0;
        int has_number = extract_object_value_number(p, object_end, &numeric_value);
        int has_time = extract_object_time(p, object_end, &ts);

        if (strcmp(variable, "temperature") == 0 && has_number && has_time) {
            rec->has_temp = 1;
            rec->temp = numeric_value;
            rec->temp_ts = ts;
            update_record_ts(rec, ts);
        } else if (strcmp(variable, "humidity") == 0 && has_number && has_time) {
            rec->has_humidity = 1;
            rec->humidity = numeric_value;
            rec->humidity_ts = ts;
            update_record_ts(rec, ts);
        } else if (strcmp(variable, "airpressure") == 0 && has_number && has_time) {
            rec->has_pressure = 1;
            rec->pressure = numeric_value;
            rec->pressure_ts = ts;
            update_record_ts(rec, ts);
        } else if (strcmp(variable, "batterylevel") == 0 && has_number && has_time) {
            rec->has_battery = 1;
            rec->battery = numeric_value;
            rec->battery_ts = ts;
            update_record_ts(rec, ts);
        } else if ((strcmp(variable, "lora_spreading_factor") == 0 ||
                    strcmp(variable, "spreading_factor") == 0 ||
                    strcmp(variable, "sf") == 0) && has_number) {
            rec->has_sf = 1;
            rec->sf = (int)numeric_value;
            rec->sf_ts = has_time ? ts : rec->record_ts;
        } else if (strcmp(variable, "datarate") == 0) {
            char text_value[64];
            if (extract_object_value_string(p, object_end, text_value, sizeof(text_value))) {
                char *sf = strstr(text_value, "SF");
                if (sf && isdigit((unsigned char)sf[2])) {
                    rec->has_sf = 1;
                    rec->sf = atoi(sf + 2);
                    rec->sf_ts = has_time ? ts : rec->record_ts;
                }
            }
        }

        p = object_end ? object_end + 1 : p;
    }

    return rec->has_temp || rec->has_humidity || rec->has_pressure || rec->has_battery || rec->has_sf;
}

static const char *find_next_payload_field(const char *p, const char **value_start_out, const char **field_name_out) {
    /* Busca em varredura linear para evitar O(n^2) em arquivos grandes. */
    const char *payload_pattern = "\"payload\":\"";
    const char *brute_pattern = "\"brute_data\":\"";
    const size_t payload_len = strlen(payload_pattern);
    const size_t brute_len = strlen(brute_pattern);

    while (*p) {
        if (*p == '"') {
            if (strncmp(p, payload_pattern, payload_len) == 0) {
                *value_start_out = p + payload_len;
                *field_name_out = "payload";
                return p;
            }
            if (strncmp(p, brute_pattern, brute_len) == 0) {
                *value_start_out = p + brute_len;
                *field_name_out = "brute_data";
                return p;
            }
        }
        p++;
    }

    return NULL;
}

static void update_file_summary_period(FileSummary *summary, time_t ts) {
    if (!ts) return;
    if (!summary->has_period) {
        summary->min_ts = ts;
        summary->max_ts = ts;
        summary->has_period = 1;
    } else {
        if (ts < summary->min_ts) summary->min_ts = ts;
        if (ts > summary->max_ts) summary->max_ts = ts;
    }
}

static void process_file(const char *path, int file_index, SharedData *shared, LogQueue *log_queue, HashSet *dedup) {
    size_t file_size = 0;
    char *content = read_entire_file(path, &file_size);
    if (!content) {
        log_msg(log_queue, "Falha ao ler arquivo %s", path);
        return;
    }

    FileSummary *summary = &shared->files[file_index];
    snprintf(summary->filename, sizeof(summary->filename), "%s", basename_simple(path));

    log_msg(log_queue, "Iniciando leitura do arquivo %s (%zu bytes)", path, file_size);
    log_msg(log_queue, "Arquivo %s: varredura preparada. Campos JSON aceitos: payload e brute_data", summary->filename);
    log_msg(log_queue, "Arquivo %s: eliminação de duplicatas ativa usando hash FNV-1a do JSON interno", summary->filename);

    const char *p = content;
    const char *value_start = NULL;
    const char *field_name = NULL;
    long found_payloads = 0;
    long ignored_payloads = 0;
    long accepted_by_city[CITY_COUNT] = {0};
    long metric_presence[CITY_COUNT][5] = {{0}}; /* temp, umidade, pressão, bateria, sf */

    while (find_next_payload_field(p, &value_start, &field_name)) {
        const char *after_string = NULL;
        char *inner = unescape_json_string(value_start, &after_string);
        found_payloads++;

        if (found_payloads == 1 || found_payloads % 5000 == 0) {
            log_msg(log_queue,
                    "Arquivo %s: %ld payloads/brute_data encontrados até agora. Último campo lido: %s",
                    summary->filename, found_payloads, field_name);
        }

        unsigned long long hash = fnv1a_hash(inner);
        if (!hashset_insert(dedup, hash)) {
            summary->duplicates++;
            log_msg(log_queue,
                    "Duplicata eliminada no arquivo %s | ocorrência=%ld | campo=%s | hash=%llu",
                    summary->filename, found_payloads, field_name, hash);
            free(inner);
            p = after_string;
            continue;
        }

        SensorRecord rec;
        if (parse_inner_payload(inner, &rec)) {
            rec.source_file_index = file_index;

            pthread_mutex_lock(&shared->mutex);
            record_vector_push(&shared->records, rec);
            pthread_mutex_unlock(&shared->mutex);

            summary->processed++;
            update_file_summary_period(summary, rec.record_ts);

            if (rec.city >= 0 && rec.city < CITY_COUNT) {
                accepted_by_city[rec.city]++;
                if (rec.has_temp) metric_presence[rec.city][0]++;
                if (rec.has_humidity) metric_presence[rec.city][1]++;
                if (rec.has_pressure) metric_presence[rec.city][2]++;
                if (rec.has_battery) metric_presence[rec.city][3]++;
                if (rec.has_sf) metric_presence[rec.city][4]++;
            }

            log_record_detail(log_queue, summary->filename, summary->processed, field_name, &rec);
        } else {
            ignored_payloads++;
            if (ignored_payloads <= 20 || ignored_payloads % 1000 == 0) {
                log_msg(log_queue,
                        "Payload ignorado no arquivo %s | ocorrência=%ld | campo=%s | motivo=sem cidade reconhecida ou sem medições úteis",
                        summary->filename, found_payloads, field_name);
            }
        }

        free(inner);
        p = after_string;
    }

    for (int city = 0; city < CITY_COUNT; city++) {
        log_msg(log_queue,
                "Resumo parcial do arquivo %s para %s | registros=%ld | temperatura=%ld | umidade=%ld | pressao=%ld | bateria=%ld | SF=%ld",
                summary->filename,
                CITY_NAMES[city],
                accepted_by_city[city],
                metric_presence[city][0],
                metric_presence[city][1],
                metric_presence[city][2],
                metric_presence[city][3],
                metric_presence[city][4]);
    }

    char start_date[32], end_date[32];
    format_date_br(summary->min_ts, start_date, sizeof(start_date));
    format_date_br(summary->max_ts, end_date, sizeof(end_date));

    log_msg(log_queue,
            "Arquivo %s finalizado. Payloads encontrados: %ld, registros úteis: %ld, ignorados: %ld, duplicatas: %ld, período: %s a %s",
            path, found_payloads, summary->processed, ignored_payloads, summary->duplicates, start_date, end_date);

    free(content);
}

static void *reader_thread(void *arg) {
    ReaderArgs *args = (ReaderArgs *)arg;
    HashSet dedup;
    hashset_init(&dedup, 65536);

    log_msg(args->log_queue, "Thread de leitura iniciada");
    log_msg(args->log_queue, "Thread de leitura: %d arquivo(s) recebido(s) para processamento", args->input_file_count);

    for (int i = 0; i < args->input_file_count; i++) {
        log_msg(args->log_queue, "Thread de leitura: iniciando arquivo %d de %d: %s",
                i + 1, args->input_file_count, args->input_files[i]);
        process_file(args->input_files[i], i, args->shared, args->log_queue, &dedup);
        log_msg(args->log_queue, "Thread de leitura: arquivo %d de %d concluído: %s",
                i + 1, args->input_file_count, args->input_files[i]);
    }

    log_msg(args->log_queue, "Thread de leitura: deduplicação finalizada com %zu hash(es) únicos armazenados", dedup.size);
    hashset_free(&dedup);

    pthread_mutex_lock(&args->shared->mutex);
    args->shared->reading_done = 1;
    pthread_cond_signal(&args->shared->cond_reading_done);
    pthread_mutex_unlock(&args->shared->mutex);

    log_msg(args->log_queue, "Thread de leitura finalizada");
    return NULL;
}

static void metric_init(MetricStats *m) {
    memset(m, 0, sizeof(*m));
    m->min = DBL_MAX;
    m->max = -DBL_MAX;
}

static void metric_add_time(time_t *times, int *count, int *extra, time_t ts) {
    if (*count < MAX_TIME_HITS) {
        times[*count] = ts;
        (*count)++;
    } else {
        (*extra)++;
    }
}

static void metric_add(MetricStats *m, double value, time_t ts) {
    const double EPS = 0.000001;

    if (m->count == 0 || value < m->min - EPS) {
        m->min = value;
        m->min_time_count = 0;
        m->min_time_extra = 0;
        metric_add_time(m->min_times, &m->min_time_count, &m->min_time_extra, ts);
    } else if (fabs(value - m->min) <= EPS) {
        metric_add_time(m->min_times, &m->min_time_count, &m->min_time_extra, ts);
    }

    if (m->count == 0 || value > m->max + EPS) {
        m->max = value;
        m->max_time_count = 0;
        m->max_time_extra = 0;
        metric_add_time(m->max_times, &m->max_time_count, &m->max_time_extra, ts);
    } else if (fabs(value - m->max) <= EPS) {
        metric_add_time(m->max_times, &m->max_time_count, &m->max_time_extra, ts);
    }

    m->sum += value;
    m->count++;
}

static void city_stats_init(CityStats *s) {
    metric_init(&s->temperature);
    metric_init(&s->humidity);
    metric_init(&s->pressure);
    s->has_initial_battery = 0;
    s->has_final_battery = 0;
    memset(s->sf_used, 0, sizeof(s->sf_used));
}

static void stats_result_init(StatsResult *result) {
    for (int i = 0; i < CITY_COUNT; i++) {
        city_stats_init(&result->city[i]);
    }
}

static void metric_detail_for_log(MetricStats *m, char *out, size_t out_size, const char *unit) {
    if (m->count == 0) {
        snprintf(out, out_size, "sem dados");
        return;
    }

    char min_time[64];
    char max_time[64];
    format_datetime_br(m->min_times[0], min_time, sizeof(min_time));
    format_datetime_br(m->max_times[0], max_time, sizeof(max_time));

    snprintf(out, out_size,
             "qtd=%d, min=%.2f%s em %s, max=%.2f%s em %s, media=%.2f%s",
             m->count,
             m->min, unit, min_time,
             m->max, unit, max_time,
             m->sum / m->count, unit);
}

static void sf_list_for_log(CityStats *city, char *out, size_t out_size) {
    out[0] = '\0';
    int first = 1;
    for (int sf = 0; sf < 256; sf++) {
        if (!city->sf_used[sf]) continue;
        char item[16];
        snprintf(item, sizeof(item), "%sSF%d", first ? "" : ", ", sf);
        strncat(out, item, out_size - strlen(out) - 1);
        first = 0;
    }
    if (first) snprintf(out, out_size, "N/D");
}

static void log_city_stats_summary(LogQueue *log_queue, int city_index, CityStats *city) {
    char temp[256];
    char humidity[256];
    char pressure[256];
    char sf[256];
    metric_detail_for_log(&city->temperature, temp, sizeof(temp), " °C");
    metric_detail_for_log(&city->humidity, humidity, sizeof(humidity), " %");
    metric_detail_for_log(&city->pressure, pressure, sizeof(pressure), " hPa");
    sf_list_for_log(city, sf, sizeof(sf));

    if (city->has_initial_battery && city->has_final_battery) {
        char initial_time[64];
        char final_time[64];
        format_datetime_br(city->initial_battery_ts, initial_time, sizeof(initial_time));
        format_datetime_br(city->final_battery_ts, final_time, sizeof(final_time));
        log_msg(log_queue,
                "Resumo estatístico de %s | temperatura: %s | umidade: %s | pressao: %s | bateria: inicial=%.2f V em %s, final=%.2f V em %s, consumo=%.2f V | SFs=%s",
                CITY_NAMES[city_index], temp, humidity, pressure,
                city->initial_battery, initial_time,
                city->final_battery, final_time,
                city->initial_battery - city->final_battery,
                sf);
    } else {
        log_msg(log_queue,
                "Resumo estatístico de %s | temperatura: %s | umidade: %s | pressao: %s | bateria: sem dados suficientes | SFs=%s",
                CITY_NAMES[city_index], temp, humidity, pressure, sf);
    }
}

static void update_battery(CityStats *stats, double value, time_t ts) {
    if (!stats->has_initial_battery || ts < stats->initial_battery_ts) {
        stats->has_initial_battery = 1;
        stats->initial_battery = value;
        stats->initial_battery_ts = ts;
    }

    if (!stats->has_final_battery || ts > stats->final_battery_ts) {
        stats->has_final_battery = 1;
        stats->final_battery = value;
        stats->final_battery_ts = ts;
    }
}

static void *stats_thread(void *arg) {
    StatsArgs *args = (StatsArgs *)arg;
    log_msg(args->log_queue, "Thread de estatísticas aguardando leitura");

    pthread_mutex_lock(&args->shared->mutex);
    while (!args->shared->reading_done) {
        pthread_cond_wait(&args->shared->cond_reading_done, &args->shared->mutex);
    }

    size_t count = args->shared->records.size;
    SensorRecord *snapshot = malloc(count * sizeof(SensorRecord));
    if (!snapshot && count > 0) die("memória insuficiente para snapshot estatístico");
    memcpy(snapshot, args->shared->records.items, count * sizeof(SensorRecord));
    pthread_mutex_unlock(&args->shared->mutex);

    stats_result_init(args->result);
    log_msg(args->log_queue, "Thread de estatísticas iniciou processamento de %zu registros", count);

    for (size_t i = 0; i < count; i++) {
        SensorRecord *rec = &snapshot[i];
        if (rec->city < 0 || rec->city >= CITY_COUNT) {
            log_msg(args->log_queue, "Thread de estatísticas: registro %zu ignorado por cidade inválida", i + 1);
            continue;
        }

        if ((i + 1) == 1 || (i + 1) % 5000 == 0 || (i + 1) == count) {
            log_msg(args->log_queue, "Thread de estatísticas: processando registro %zu de %zu (%s)",
                    i + 1, count, CITY_NAMES[rec->city]);
        }

        CityStats *city = &args->result->city[rec->city];
        if (rec->has_temp) metric_add(&city->temperature, rec->temp, rec->temp_ts);
        if (rec->has_humidity) metric_add(&city->humidity, rec->humidity, rec->humidity_ts);
        if (rec->has_pressure) metric_add(&city->pressure, rec->pressure, rec->pressure_ts);
        if (rec->has_battery) update_battery(city, rec->battery, rec->battery_ts);
        if (rec->has_sf && rec->sf >= 0 && rec->sf < 256) city->sf_used[rec->sf] = 1;
    }

    for (int city = 0; city < CITY_COUNT; city++) {
        log_city_stats_summary(args->log_queue, city, &args->result->city[city]);
    }

    free(snapshot);
    log_msg(args->log_queue, "Thread de estatísticas finalizada");
    return NULL;
}

static double elapsed_seconds(struct timespec start, struct timespec end) {
    return (double)(end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000.0;
}

static void print_metric_row(const char *city_name, MetricStats *m) {
    if (m->count == 0) {
        printf("%-18s | %-7s | %-32s | %-7s | %-32s | %-7s\n",
               city_name, "N/D", "N/D", "N/D", "N/D", "N/D");
        return;
    }

    char min_time[64] = "";
    char max_time[64] = "";

    format_datetime_br(m->min_times[0], min_time, sizeof(min_time));
    int min_extra_total = (m->min_time_count - 1) + m->min_time_extra;
    if (min_extra_total > 0) {
        char extra[16];
        snprintf(extra, sizeof(extra), " (+%d)", min_extra_total);
        strncat(min_time, extra, sizeof(min_time) - strlen(min_time) - 1);
    }

    format_datetime_br(m->max_times[0], max_time, sizeof(max_time));
    int max_extra_total = (m->max_time_count - 1) + m->max_time_extra;
    if (max_extra_total > 0) {
        char extra[16];
        snprintf(extra, sizeof(extra), " (+%d)", max_extra_total);
        strncat(max_time, extra, sizeof(max_time) - strlen(max_time) - 1);
    }

    printf("%-18s | %7.2f | %-32.32s | %7.2f | %-32.32s | %7.2f\n",
           city_name, m->min, min_time, m->max, max_time, m->sum / m->count);
}

static void print_metric_section(const char *title, const char *unit, MetricStats metrics[CITY_COUNT]) {
    printf("\n\n------------------------------------------------------------\n");
    printf("%s (%s)\n", title, unit);
    printf("------------------------------------------------------------\n");
    printf("%-18s | %-7s | %-32s | %-7s | %-32s | %-7s\n",
           "Cidade", "Mínima", "Data/Hora", "Máxima", "Data/Hora", "Média");
    printf("------------------------------------------------------------------------------------------------------------\n");

    for (int i = 0; i < CITY_COUNT; i++) {
        print_metric_row(CITY_NAMES[i], &metrics[i]);
    }
}

static void print_battery_section(StatsResult *stats) {
    printf("\n\n------------------------------------------------------------\n");
    printf("BATERIA\n");
    printf("------------------------------------------------------------\n");
    printf("%-18s | %-11s | %-9s | %-10s\n", "Cidade", "Inicial (V)", "Final (V)", "Consumo (V)");
    printf("------------------------------------------------------------\n");

    for (int i = 0; i < CITY_COUNT; i++) {
        CityStats *c = &stats->city[i];
        if (c->has_initial_battery && c->has_final_battery) {
            printf("%-18s | %11.2f | %9.2f | %10.2f\n",
                   CITY_NAMES[i], c->initial_battery, c->final_battery,
                   c->initial_battery - c->final_battery);
        } else {
            printf("%-18s | %-11s | %-9s | %-10s\n", CITY_NAMES[i], "N/D", "N/D", "N/D");
        }
    }
}

static void print_sf_section(StatsResult *stats) {
    printf("\n\n------------------------------------------------------------\n");
    printf("SPREADING FACTORS UTILIZADOS\n");
    printf("------------------------------------------------------------\n");
    printf("%-18s | %s\n", "Cidade", "SF utilizados");
    printf("------------------------------------------------------------\n");

    for (int i = 0; i < CITY_COUNT; i++) {
        char buffer[256] = "";
        int first = 1;
        for (int sf = 0; sf < 256; sf++) {
            if (!stats->city[i].sf_used[sf]) continue;
            char item[16];
            snprintf(item, sizeof(item), "%sSF%d", first ? "" : ", ", sf);
            strncat(buffer, item, sizeof(buffer) - strlen(buffer) - 1);
            first = 0;
        }
        if (first) snprintf(buffer, sizeof(buffer), "N/D");
        printf("%-18s | %s\n", CITY_NAMES[i], buffer);
    }
}

static void print_file_summaries(SharedData *shared) {
    for (int i = 0; i < shared->file_count; i++) {
        FileSummary *f = &shared->files[i];
        char start[32], end[32];
        format_date_br(f->min_ts, start, sizeof(start));
        format_date_br(f->max_ts, end, sizeof(end));

        printf("\nArquivo analisado: %s\n", f->filename[0] ? f->filename : "N/D");
        printf("Total de registros processados: %ld\n", f->processed);
        printf("Duplicatas eliminadas: %ld\n", f->duplicates);
        printf("Período analisado: %s a %s\n", start, end);
    }
}

static void print_report(SharedData *shared, StatsResult *stats, double total_seconds) {
    MetricStats temp_metrics[CITY_COUNT];
    MetricStats hum_metrics[CITY_COUNT];
    MetricStats press_metrics[CITY_COUNT];

    for (int i = 0; i < CITY_COUNT; i++) {
        temp_metrics[i] = stats->city[i].temperature;
        hum_metrics[i] = stats->city[i].humidity;
        press_metrics[i] = stats->city[i].pressure;
    }

    printf("============================================================\n");
    printf("ANÁLISE DE DADOS DOS SENSORES - CityLivingLab\n");
    printf("Processamento utilizando pthreads\n");
    printf("============================================================\n");

    print_file_summaries(shared);
    print_metric_section("TEMPERATURA", "°C", temp_metrics);
    print_metric_section("UMIDADE", "%", hum_metrics);
    print_metric_section("PRESSÃO ATMOSFÉRICA", "hPa", press_metrics);
    print_battery_section(stats);
    print_sf_section(stats);

    printf("\n\n------------------------------------------------------------\n");
    printf("DESEMPENHO\n");
    printf("------------------------------------------------------------\n");
    printf("Tempo total de execução: %.4f segundos\n", total_seconds);
    printf("Threads utilizadas: 3\n");
    printf(" - Thread 1: leitura dos dados e eliminação de duplicatas\n");
    printf(" - Thread 2: cálculo das estatísticas\n");
    printf(" - Thread 3: registro de logs\n");
    printf("\nArquivo de log gerado: processamento.log\n");
    printf("\n============================================================\n");
    printf("Processamento finalizado com sucesso.\n");
    printf("============================================================\n");
}

static void shared_init(SharedData *shared, int file_count) {
    pthread_mutex_init(&shared->mutex, NULL);
    pthread_cond_init(&shared->cond_reading_done, NULL);
    shared->reading_done = 0;
    record_vector_init(&shared->records);
    shared->file_count = file_count;
    shared->files = calloc((size_t)file_count, sizeof(FileSummary));
    if (!shared->files) die("memória insuficiente para resumo dos arquivos");
}

static void shared_free(SharedData *shared) {
    free(shared->records.items);
    free(shared->files);
    pthread_mutex_destroy(&shared->mutex);
    pthread_cond_destroy(&shared->cond_reading_done);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Uso: %s [1] arquivo1.json [arquivo2.json ...]\n", argv[0]);
        fprintf(stderr, "     O argumento opcional '1' como primeiro parâmetro ativa o log detalhado por registro.\n");
        fprintf(stderr, "Exemplo: %s data/mqtt_senzemo_cx_bg.json data/senzemo_cx_bg.json\n", argv[0]);
        fprintf(stderr, "Exemplo: %s 1 data/mqtt_senzemo_cx_bg.json data/senzemo_cx_bg.json\n", argv[0]);
        return EXIT_FAILURE;
    }

    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    int first_file_arg = 1;
    if (argc >= 3 && strcmp(argv[1], "1") == 0) {
        g_verbose_log = 1;
        first_file_arg = 2;
    }

    int input_file_count = argc - first_file_arg;
    char **input_files = &argv[first_file_arg];

    log_queue_init(&g_log_queue);
    shared_init(&g_shared, input_file_count);
    stats_result_init(&g_stats);

    pthread_t log_tid, reader_tid, stats_tid;

    ReaderArgs reader_args = {
        .shared = &g_shared,
        .log_queue = &g_log_queue,
        .input_files = input_files,
        .input_file_count = input_file_count
    };

    StatsArgs stats_args = {
        .shared = &g_shared,
        .log_queue = &g_log_queue,
        .result = &g_stats
    };

    if (pthread_create(&log_tid, NULL, logger_thread, &g_log_queue) != 0) {
        die("falha ao criar thread de logs");
    }

    log_msg(&g_log_queue, "Programa iniciado. Log detalhado por registro: %s. Arquivos informados: %d",
            g_verbose_log ? "ativado" : "desativado", input_file_count);
    for (int i = 0; i < input_file_count; i++) {
        log_msg(&g_log_queue, "Argumento de entrada %d: %s", i + 1, input_files[i]);
    }

    if (pthread_create(&stats_tid, NULL, stats_thread, &stats_args) != 0) {
        die("falha ao criar thread de estatísticas");
    }
    log_msg(&g_log_queue, "Thread de estatísticas criada com sucesso");

    if (pthread_create(&reader_tid, NULL, reader_thread, &reader_args) != 0) {
        die("falha ao criar thread de leitura");
    }
    log_msg(&g_log_queue, "Thread de leitura criada com sucesso");

    pthread_join(reader_tid, NULL);
    log_msg(&g_log_queue, "Thread de leitura sincronizada no main");

    pthread_join(stats_tid, NULL);
    log_msg(&g_log_queue, "Thread de estatísticas sincronizada no main");

    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double total = elapsed_seconds(start_time, end_time);

    log_msg(&g_log_queue, "Processamento finalizado em %.4f segundos", total);
    log_queue_finish(&g_log_queue);
    pthread_join(log_tid, NULL);

    print_report(&g_shared, &g_stats, total);

    shared_free(&g_shared);
    pthread_mutex_destroy(&g_log_queue.mutex);
    pthread_cond_destroy(&g_log_queue.cond);

    return EXIT_SUCCESS;
}