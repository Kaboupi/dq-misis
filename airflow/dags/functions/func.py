"""Sample script with functions"""

def get_s3_config(conn_id: str = 's3'):
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    return {
        "aws_access_key_id": conn.login,
        "aws_secret_access_key": conn.password,
        "endpoint_url": f"http://{conn.host}:{conn.port}",
    }
    

def clickhouse_query_builder(
        schema: str,
        table: str,
        order_by: list[str] = None,
        partition_by: list[str] = None,
        engine: str = 'MergeTree()'
        ) -> str:
    
    # Raise an exception cause of MergeTree issues
    if not order_by and engine == 'MergeTree()':
        raise ValueError(f'Error! `order_by` argument is not defined! Can\'t be used with the {engine} engine!')
    
    order_by_clause = f'ORDER BY ({', '.join(order_by)})'
    partition_by_clause = f'\nPARTITION BY ({', '.join(partition_by)})' if partition_by else ''
    
    return f'''
        CREATE DATABASE IF NOT EXISTS `{schema}`;
        DROP TABLE IF EXISTS `{schema}`.`{table}`;
        CREATE TABLE `{schema}`.`{table}`
        ENGINE = {engine}
        {order_by_clause}{partition_by_clause}
        AS
    '''