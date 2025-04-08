from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from config.config import CLICKHOUSE_CONN_ID
import logging

SCHEMA_MARTS = "misis_project"
SCHEMA_ODS = f'{SCHEMA_MARTS}_ods'
TABLE_NAME = "sales_data"


def clickhouse_build_mart(**kwargs):
    from clickhouse_connect import get_client
    from functions.func import clickhouse_query_builder
    
    # Connect to Clickhouse
    logging.info('Connecting to Clickhouse Database')
    clickhouse_conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
    client = get_client(
        host=clickhouse_conn.host,
        port=clickhouse_conn.port,
        user=clickhouse_conn.login,
        password=clickhouse_conn.password,
        database=clickhouse_conn.schema or 'default',
    )
    
    # Get unique batch from XCom args
    batch_uuid = kwargs['ti'].xcom_pull(key='batch_uuid')
    logging.info(f'batch_uuid: {batch_uuid}')

    # Set all queries
    query_header = clickhouse_query_builder(
        schema=SCHEMA_MARTS,
        table=TABLE_NAME,
        order_by=['toYYYYMM(valid_from_dttm)'],
        partition_by=['toYYYYMM(valid_from_dttm)'],
    )
    
    multiquery = query_header + f"""
        select
            toStartOfMonth(`Date`) as valid_from_dttm
            , 1 as agg_id
            , Country country
            , State state
            , Product_Category "type"
            , Sub_Category subtype
            , ROUND(SUM(Cost), 0) cost
            , ROUND(SUM(Revenue), 0) revenue
        from `{SCHEMA_ODS}`.`{TABLE_NAME}`
        group by 1,2,3,4,5,6
        union all
        select
            toStartOfWeek(`Date` - INTERVAL '1 DAY') + INTERVAL '1 DAY'
            -- toStartOfWeek(`Date`) as valid_from_dttm
            , 2 as agg_id
            , Country country
            , State state
            , Product_Category "type"
            , Sub_Category subtype
            , ROUND(SUM(Cost), 0) cost
            , ROUND(SUM(Revenue), 0) revenue
        from `{SCHEMA_ODS}`.`{TABLE_NAME}`
        group by 1,2,3,4,5,6
    """
    
    for query in multiquery.split(';'):
        logging.info(f'Execute query: {query}')
        client.query(query)
    
    logging.info('Success!')
        

with DAG(
    dag_id="clickhouse_build_mart",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["transform"],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    clickhouse_build_mart_task = PythonOperator(
        task_id="clickhouse_build_mart",
        python_callable=clickhouse_build_mart,
    )
    
    trigger_dq_dag_task = TriggerDagRunOperator(
        task_id='trigger_dq_dag',
        trigger_dag_id='dq_dag',
        wait_for_completion=False,
    )
    
    end_task = EmptyOperator(
        task_id='end',
    )


    (
        start_task
        >> clickhouse_build_mart_task
        >> trigger_dq_dag_task
        >> end_task
    )