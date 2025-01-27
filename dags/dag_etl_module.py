from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from app.etl_module import run_etl_process

default_args = {
    'owner': 'dkisai',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# DAG que se ejecuta cada 30 segundos (experimental en Airflow >=2.2)
with DAG(
    dag_id='etl_module_dag',
    default_args=default_args,
    schedule_interval=timedelta(seconds=30),
    catchup=False
) as dag:

    run_etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl_process,
        op_kwargs={'config_path': 'metrobus_config.ini'},
    )

    run_etl_task