import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
# Verifica se o caminho do script existe antes de adicionar
script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
if os.path.isdir(script_path):
    sys.path.append(script_path)
else:
    raise FileNotFoundError(f'O diretório de scripts não foi encontrado: {script_path}')

# Importa as funções diretamente
from extract import extract_data
from transform import transform_data
from load import load_aggregated_data

# Definindo os argumentos padrão para a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 9),
    'retries': 1,
}

# Criando a DAG
dag = DAG(
    'brewery_pipeline',  # O id da DAG
    default_args=default_args,
    description='ETL pipeline for breweries data',
    schedule_interval='@daily',  # Definindo um intervalo diário
    catchup=False,
)

# Caminhos dos dados
API_URL = "https://api.openbrewerydb.org/breweries"
BRONZE_PATH = Path(__file__).resolve().parent.parent / 'data/bronze/breweries.parquet'
SILVER_PATH = Path(__file__).resolve().parent.parent / 'data/silver'
GOLD_PATH = Path(__file__).resolve().parent.parent / 'data/gold'

# Verificação para garantir que o arquivo bronze existe
if not BRONZE_PATH.exists():
    raise FileNotFoundError(f'O arquivo de dados não foi encontrado: {BRONZE_PATH}')

# Tarefa de extração de dados
t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=[API_URL, BRONZE_PATH],
    dag=dag,
)

# Tarefa de transformação de dados
t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[BRONZE_PATH, SILVER_PATH],
    dag=dag,
)

# Tarefa de carregamento de dados
t3 = PythonOperator(
    task_id='load_aggregated_data',
    python_callable=load_aggregated_data,
    op_args=[SILVER_PATH, GOLD_PATH],
    dag=dag,
)

# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3
