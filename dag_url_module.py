# dag_url_module.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# Importamos nuestra función principal del url_module
from url_module import run_url_process

default_args = {
    'owner': 'dkisai',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG que se ejecuta cada 10 horas
with DAG(
    dag_id='url_module_dag',
    default_args=default_args,
    schedule_interval='0 */10 * * *',  # cada 10 horas (cron)
    catchup=False
) as dag:
    
    run_url_task = PythonOperator(
        task_id='run_url_task',
        python_callable=run_url_process,
        op_kwargs={'config_path': 'metrobus_config.ini'},  # Pasamos la ruta del .ini si queremos
    )

    run_url_task