# Análise de Dados dos Sensores - CityLivingLab

## Descrição

Este projeto contém um programa desenvolvido em linguagem C para processar arquivos JSON com medições coletadas por sensores do projeto CityLivingLab.

O programa lê dados das cidades de **Caxias do Sul** e **Bento Gonçalves**, processa as medições dos sensores e apresenta um relatório final no terminal com estatísticas de temperatura, umidade, pressão atmosférica, bateria e Spreading Factors utilizados nas transmissões.

O processamento utiliza **pthreads**, separando as etapas principais em múltiplas threads.

## Funcionalidades

O programa realiza as seguintes operações:

- leitura de arquivos JSON contendo dados dos sensores;
- identificação das cidades de Caxias do Sul e Bento Gonçalves;
- eliminação de registros duplicados;
- cálculo da menor, maior e média de temperatura;
- cálculo da menor, maior e média de umidade;
- cálculo da menor, maior e média de pressão atmosférica;
- identificação da data e horário em que ocorreram os valores mínimos e máximos;
- cálculo do consumo de bateria por cidade;
- identificação dos Spreading Factors utilizados, quando disponíveis;
- geração de arquivo de log detalhado;
- cálculo do tempo total de execução;
- exibição dos resultados no formato solicitado pelo enunciado.

## Estrutura do projeto

Uma estrutura esperada para o projeto é:

```text
citylivinglab_pthreads/
├── main.c
├── mqtt_senzemo_cx_bg.json
├── senzemo_cx_bg.json
└── README.md
```

Após a execução, também será gerado o arquivo:

```text
processamento.log
```

Esse arquivo contém os logs detalhados do processamento.

## Requisitos para execução

Para compilar e executar o projeto, é necessário ter:

- sistema Linux ou VM da disciplina;
- compilador GCC instalado;
- biblioteca pthread disponível;
- arquivos JSON de entrada dentro da pasta `data`.

## Compilação

Para compilar o programa, execute o comando abaixo na raiz do projeto:

```bash
gcc main.c -o citylivinglab
```

Esse comando gera o executável chamado:

```text
citylivinglab
```

## Execução

Para executar o programa com os arquivos JSON fornecidos, utilize:

```bash
./citylivinglab mqtt_senzemo_cx_bg.json senzemo_cx_bg.json
```

Também é possível informar o número 1 antes do nome dos arquivos para listar nos logs todas as linhas lidas:

```bash
./citylivinglab 1 mqtt_senzemo_cx_bg.json senzemo_cx_bg.json
```

## Funcionamento geral

O programa trabalha com três threads principais.

### Thread 1: leitura dos dados

Responsável por:

- abrir os arquivos JSON;
- ler o conteúdo dos arquivos;
- localizar os campos `payload` e `brute_data`;
- converter o JSON interno;
- identificar a cidade do sensor;
- eliminar registros duplicados;
- armazenar os registros válidos para processamento.

### Thread 2: cálculo das estatísticas

Responsável por:

- aguardar o término da leitura;
- percorrer os registros válidos;
- calcular valores mínimos, máximos e médias;
- calcular consumo de bateria;
- identificar os Spreading Factors utilizados;
- preparar os dados para o relatório final.

### Thread 3: registro dos logs

Responsável por:

- receber mensagens de log das outras etapas;
- gravar o arquivo `processamento.log`;
- registrar informações de auditoria sobre a execução do programa.

## Dados processados

O programa considera os seguintes tipos de medição:

| Variável no JSON | Informação processada |
|---|---|
| `temperature` | Temperatura |
| `humidity` | Umidade |
| `airpressure` | Pressão atmosférica |
| `batterylevel` | Nível da bateria |
| `lora_spreading_factor` | Spreading Factor |
| `spreading_factor` | Spreading Factor |
| `sf` | Spreading Factor |
| `datarate` | Spreading Factor, quando contém valor como SF7, SF8 etc. |

As cidades são identificadas pelo nome do dispositivo:

| Nome no dispositivo | Cidade considerada |
|---|---|
| `Caxias` | Caxias do Sul |
| `Bento` | Bento Gonçalves |

## Saída do programa

Ao final da execução, o programa apresenta no terminal um relatório contendo:

- arquivos analisados;
- total de registros processados;
- quantidade de duplicatas eliminadas;
- período analisado;
- estatísticas de temperatura;
- estatísticas de umidade;
- estatísticas de pressão atmosférica;
- consumo de bateria;
- Spreading Factors utilizados;
- tempo total de execução;
- threads utilizadas;
- nome do arquivo de log gerado.

Exemplo de seções exibidas:

```text
TEMPERATURA (°C)
UMIDADE (%)
PRESSÃO ATMOSFÉRICA (hPa)
BATERIA
SPREADING FACTORS UTILIZADOS
DESEMPENHO
```

## Arquivo de log

Durante a execução, o programa gera o arquivo:

```text
processamento.log
```

Esse arquivo registra detalhes como:

- início e fim das threads;
- arquivos lidos;
- quantidade de dados encontrados;
- registros úteis processados;
- duplicatas eliminadas;
- dados aceitos por cidade;
- progresso do cálculo estatístico;
- tempo total de execução.

O objetivo do log é permitir auditoria e análise do comportamento do programa.

## Exemplo de execução

```bash
gcc main.c -o citylivinglab
./citylivinglab mqtt_senzemo_cx_bg.json senzemo_cx_bg.json
```

Após executar, o terminal exibirá o relatório final e será criado o arquivo `processamento.log`.

## Observações

- Os horários dos registros são convertidos para o fuso horário UTC-3 no momento da apresentação.
- Caso existam múltiplas ocorrências do mesmo valor mínimo ou máximo, o programa mantém a primeira ocorrência exibida e contabiliza ocorrências adicionais.
- Registros sem medições válidas são ignorados.
- Arquivos que não possam ser abertos são registrados no log e não interrompem todo o processamento.
- O programa foi desenvolvido para trabalhar com os formatos JSON utilizados nos arquivos fornecidos no trabalho.
