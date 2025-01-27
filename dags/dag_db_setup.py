from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import configparser
from app.db_setup_module import ensure_alcaldias_and_stops_tables

default_args = {
    'owner': 'dkisai',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

def setup_db_task():
    cfg = configparser.ConfigParser()
    cfg.read("metrobus_config.ini")
    db_config = {
        "host": cfg["DATABASE"]["host"],
        "port": cfg["DATABASE"]["port"],
        "dbname": cfg["DATABASE"]["dbname"],
        "user": cfg["DATABASE"]["user"],
        "password": cfg["DATABASE"]["password"],
    }
    ensure_alcaldias_and_stops_tables(db_config)

with DAG(
    dag_id="db_setup_dag",
    default_args=default_args,
    schedule_interval='@once',  # Corre una sola vez
    catchup=False
) as dag:
    
    run_setup_db = PythonOperator(
        task_id="run_setup_db",
        python_callable=setup_db_task
    )

    run_setup_db