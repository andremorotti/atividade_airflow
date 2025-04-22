from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Argumentos padrão
default_args = {
    'owner': 'André Morotti',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
etl_path = os.path.join(base_path, 'etl')
bronze_path = os.path.join(etl_path, 'bronze')
silver_path = os.path.join(etl_path, 'silver')
gold_script = os.path.join(etl_path, 'gold', 'dataset_gold.py')

with DAG(
    dag_id='pipeline_atividade',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Atividade de pipeline e lakehouse com Airflow',
) as dag:
    
    customers_parquet = BashOperator(
        task_id='customers_parquet',
        bash_command=f'spark-submit {os.path.join(bronze_path, 'customers_parquet.py')}'
    )

    orders_parquet = BashOperator(
        task_id='orders_parquet',
        bash_command=f'spark-submit {os.path.join(bronze_path, 'orders_parquet.py')}'
    )

    order_item_parquet = BashOperator(
        task_id='order_item_parquet',
        bash_command=f'spark-submit {os.path.join(bronze_path, 'order_item_parquet.py')}'
    )

    customers_rename = BashOperator(
        task_id='customers_rename',
        bash_command=f'spark-submit {os.path.join(silver_path, 'customers_rename.py')}'
    )

    orders_rename = BashOperator(
        task_id='orders_rename',
        bash_command=f'spark-submit {os.path.join(silver_path, 'orders_rename.py')}'
    )

    order_item_rename = BashOperator(
        task_id='order_item_rename',
        bash_command=f'spark-submit {os.path.join(silver_path, 'order_item_rename.py')}'
    )

    gold_task = BashOperator(
        task_id='generate_gold_dataset',
        bash_command=f'spark-submit {gold_script}'
    )

    customers_parquet >> customers_rename
    orders_parquet >> orders_rename
    order_item_parquet >> order_item_rename
    [customers_rename, orders_rename, order_item_rename] >> gold_task
