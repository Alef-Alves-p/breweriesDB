# Brewery ETL Pipeline com Apache Airflow

Este projeto implementa um pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow para processar dados de cervejarias. O pipeline organiza os dados em três camadas: **bronze**, **silver** e **gold**. A extração de dados é feita a partir da camada bronze, a transformação ocorre para a camada silver, e a carga final é realizada na camada gold.

O projeto está dividido em várias partes: o Airflow orquestra o fluxo de dados entre as etapas de extração, transformação e carga, além de permitir a realização de testes para garantir a qualidade dos dados.

## Estrutura do Projeto

A estrutura de diretórios do projeto é a seguinte:

breweriesDB/
├── dags/                      # Arquivos do pipeline Airflow
│   ├── brewery_pipeline.py            # Definição do DAG no Airflow
├── data/                      # Data Lake local
│   ├── bronze/                # Camada bronze: dados brutos
│   ├── silver/                # Camada prata: dados transformados
│   ├── gold/                  # Camada ouro: dados agregados
├── logs
│   ├──dag_processor_manager
│   ├──scheduler
├── scripts/                   # Scripts auxiliares
│   ├── extract.py             # Código para consumir a API
│   ├── transform.py           # Código para transformação dos dados
│   ├── load.py                # Código para salvar os dados
│   ├── test_functions.py      # Casos de teste
├──tests
|   ├── test_aggregate.py
│   ├── test_verify_data.py
│   ├── verify_data_bronze.py
│   ├── verify_data_silver.py
│   ├── verify_data_gold.py
├── requirements.txt           # Dependências do projeto
├── README.md                  # Documentação do projeto
├── airflow.cfg
├── airflow.db
├── webserver_config.py
└── venv                       # Configurações sensíveis (não versionadas)


## Pré-requisitos

Antes de começar, você precisará garantir que os seguintes pré-requisitos estão instalados:

- Python 3.x
- Apache Airflow 2.x
- Bibliotecas necessárias (detalhadas abaixo)

## Instalação

### Passo 1: Criação do ambiente virtual

Primeiro, crie um ambiente virtual para isolar as dependências do projeto:

python3 -m venv venv


### Passo 2: Ativar o ambiente virtual

No Linux ou macOS:
source venv/bin/activate

No Windows:
.\venv\Scripts\activate


### Passo 3: Instalar dependências
Com o ambiente virtual ativado, instale as dependências do projeto:

pip install -r requirements.txt

conteudo arquivo requirements.txt(
apache-airflow==2.7.0
pandas==1.5.3
requests==2.28.2
pyarrow==18.1.0
fastparquet==0.8.3
pytest==7.4.2
pyspark==3.4.0
)


### Passo 4: Configurar o Apache Airflow
Agora, inicialize o banco de dados do Airflow, que armazenará os metadados e logs das tarefas:

airflow db init


### Passo 5: Criar um usuário no Airflow
Caso ainda não tenha criado um usuário nmo Airflow, execute o seguinte comando para criar um usuário administrador:

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --role Admin \
    --password admin


### Passo 6: Iniciar o Airflow
Tendo tudo configurado, inicie o servidor web e o scheduler do Airflow:

airflow webserver -p 8080
airflow scheduler
acesse o painel do Airflow em http://localhost:8080 com o usuário e senha criados.



### Pipeline ETL
Camadas de Dados
O pipeline é estruturado em três camadas:

1 - Camada Bronze (Raw Data):
    Dados brutos extraídos de uma fonte externa e armazenados no formato Parquet.

2 - Camada Silver (Transformação):
    Dados transformados (limpeza, formatação, etc.) são armazenados para análises preliminares.

3 - Camada Gold (Agregado):
    Dados finais, agregados e preparados para consumo final, prontos para análises e relatórios.


### DAG do Airflow
A DAG (Directed Acyclic Graph) principal do Airflow é chamada brewery_pipeline e é composta por três tarefas principais:

1 - Tarefa de Extração (extract_data): Responsável por extrair os dados da camada bronze.
2 - Tarefa de Transformação (transform_data): Realiza a transformação dos dados para a camada silver.
3 - Tarefa de Carga (load_aggregated_data): Carrega os dados transformados para a camada gold.


### Exemplo de Código da DAG
Abaixo está o exemplo de código que define a DAG e as tarefas do pipeline:

*****************************************************************************
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_aggregated_data

# Argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 9),
    'retries': 1,
}

# Definindo a DAG
dag = DAG(
    'brewery_pipeline',
    default_args=default_args,
    description='ETL pipeline for breweries data',
    schedule_interval=None,
    catchup=False,
)

# Definindo as tarefas
t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=['/data/bronze/'],
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=['/data/bronze/', '/data/silver/'],
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_aggregated_data,
    op_args=['/data/silver/', '/data/gold/'],
    dag=dag,
)

# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3

*****************************************************************************

### Tarefas de cada etapa:
 - Extração (extract_data): Lê os dados brutos da camada bronze (ex: arquivos CSV ou Parquet) e os
 prepara para transformação.
 - Transformação (transform_data): Limpa e formata os dados extraídos, aplicando as transformações necessárias para     preparar os dados para análise.
 - Carga (load_aggregated_data): Carrega os dados transformados para a camada gold e realiza agregações, como cálculos de médias ou totais.


### Testes Automatizados
Para garantir a qualidade do pipeline, testes automatizados são realizados com pytest. Esses testes validam as diferentes etapas do pipeline.

Testes de Validação de Dados
A validação verifica se os dados extraídos, transformados e carregados estão corretos. O arquivo tests/verify_data.py contém funções que garantem que os dados estejam no formato esperado em cada camada.

### Rodando os Testes
Execute os testes com o comando abaixo:

pytest tests/test_brewery_pipeline.py


### Verificando a Validação dos Dados
Use o script verify_data.py para validar os dados extraídos, transformados e carregados em cada etapa do pipeline.

### Executando o Pipeline
Para executar o pipeline, siga as etapas abaixo:

 - Acesse o painel do Airflow em http://localhost:8080.
 - Localize a DAG brewery_pipeline e clique em "Trigger DAG" para iniciar a execução.
 - O Airflow irá orquestrar a execução das tarefas de extração, transformação e carga de dados.
 - Você pode acompanhar o progresso de cada tarefa diretamente no painel do Airflow.# breweriesDB
# breweriesDB
