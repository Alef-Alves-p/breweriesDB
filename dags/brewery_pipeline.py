import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))


# Importa as funções diretamente
from extract import extract_data
from scripts.transform import transform_data
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
    schedule_interval=None,  # Pode ser configurado para uma execução periódica
    catchup=False,
)

# Caminhos dos dados
BRONZE_PATH = Path(__file__).resolve().parent.parent / 'data/bronze/breweries.parquet'
SILVER_PATH = Path(__file__).resolve().parent.parent / 'data/silver'
GOLD_PATH = Path(__file__).resolve().parent.parent / 'data/gold'

# Tarefa de extração de dados
t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=[BRONZE_PATH],
    dag=dag,
)

# Tarefa de transformação de dados
t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[BRONZE_PATH, SILVER_PATH],  # Adiciona os argumentos necessários
    dag=dag,
)

# Tarefa de carregamento de dados
t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_aggregated_data,
    op_args=[SILVER_PATH, GOLD_PATH],  # Adiciona os argumentos necessários
    dag=dag,
)

# Definindo a ordem de execução das tarefas
t1 >> t2 >> t3
