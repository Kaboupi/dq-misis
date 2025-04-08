from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from config.config import POSTGRES_CONN_ID, CLICKHOUSE_CONN_ID, MINIO_CONN
from datetime import datetime
from functions.func import get_s3_config
import logging

SCHEMA_MARTS = "misis_project"
SCHEMA_ODS = f'{SCHEMA_MARTS}_ods'
TABLE_NAME = "sales_data"

S3_BUCKET_NAME = "postgres-data"
S3_CONFIG = get_s3_config(MINIO_CONN)

def transfer_postgres_to_s3(**kwargs):
    from io import BytesIO
    import boto3
    import uuid
    
    # Connect to Postgres
    logging.info('Connecting to PostgreSQL Database')
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Select data from table
    query = f"SELECT * FROM {SCHEMA_MARTS}.{TABLE_NAME};"
    logging.info(f'Running query:\n{query}')
    df = postgres_hook.get_pandas_df(query)

    # Save data to buffer
    logging.info('Saving data to buffer')
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)
    
    # Generate unique batch via uuid
    batch_uuid = str(uuid.uuid4())
    kwargs['ti'].xcom_push(key='batch_uuid', value=batch_uuid)
    logging.info(f'batch_uuid: {batch_uuid}')

    # Set full path to bucket
    s3_full_path = f'{TABLE_NAME}/{batch_uuid}/{TABLE_NAME}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.parquet'
    logging.info(f'Loading data to minio to s3://{S3_BUCKET_NAME}/{s3_full_path}')
    
    # Transfer data to s3
    s3_client = boto3.client("s3", **S3_CONFIG)
    s3_client.upload_fileobj(parquet_buffer, S3_BUCKET_NAME, s3_full_path)

    logging.info('Success!')


def transfer_s3_to_clickhouse(**kwargs):
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
        schema=SCHEMA_ODS, 
        table=TABLE_NAME,
        order_by=['`Date`'],
    )
    
    multiquery = query_header + f"""
        SELECT *
        FROM s3(
            '{S3_CONFIG["endpoint_url"]}/{S3_BUCKET_NAME}/{TABLE_NAME}/{batch_uuid}/*.parquet',
            '{S3_CONFIG["aws_access_key_id"]}',
            '{S3_CONFIG["aws_secret_access_key"]}',
            'Parquet',
            '`Date` Date, `Day` UInt8, `Month` LowCardinality(String), `Year` UInt16, `Customer_Age` UInt8, `Age_Group` LowCardinality(String), `Customer_Gender` LowCardinality(String), `Country` LowCardinality(String), `State` LowCardinality(String), `Product_Category` LowCardinality(String), `Sub_Category` LowCardinality(String), `Product` String, `Order_Quantity` UInt16, `Unit_Cost` Decimal(10, 2), `Unit_Price` Decimal(10, 2), `Profit` Decimal(10, 2), `Cost` Decimal(10, 2), `Revenue` Decimal(10, 2)'
        )
    """
    
    for query in multiquery.split(';'):
        logging.info(f'Execute query: {query}')
        client.query(query)
    
    logging.info('Success!')
        

with DAG(
    dag_id="postgres_s3_clickhouse",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["transfer"],
) as dag:
    
    start_task = EmptyOperator(
        task_id='start',
    )

    transfer_postgres_to_s3_task = PythonOperator(
        task_id="transfer_postgres_to_s3",
        python_callable=transfer_postgres_to_s3,
    )
    
    transfer_s3_to_clickhouse_task = PythonOperator(
        task_id="transfer_s3_to_clickhouse",
        python_callable=transfer_s3_to_clickhouse,
    )
    
    trigger_clickhouse_build_mart_task = TriggerDagRunOperator(
        task_id='trigger_clickhouse_build_mart',
        trigger_dag_id='clickhouse_build_mart',
        wait_for_completion=False,
    )
    
    end_task = EmptyOperator(
        task_id='end',
    )


    (
        start_task
        >> transfer_postgres_to_s3_task
        >> transfer_s3_to_clickhouse_task
        >> trigger_clickhouse_build_mart_task
        >> end_task
    )