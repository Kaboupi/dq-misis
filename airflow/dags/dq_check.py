from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from config.config import DQ_CONFIG_DIR
from dqengine.dqengine import DQEngine
        

def perform_dq():
    engine = DQEngine(DQ_CONFIG_DIR)
    engine.parse_config_dir()
    

with DAG(
    dag_id="dq_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dq"],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )
    
    perform_dq_task = PythonOperator(
        task_id='perform_dq',
        python_callable=perform_dq,
    )
    
    end_task = EmptyOperator(
        task_id='end',
    )

    start_task >> perform_dq_task >> end_task