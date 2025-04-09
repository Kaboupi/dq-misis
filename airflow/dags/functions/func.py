"""Sample script with functions"""

def get_s3_config(conn_id: str = 's3'):
    """### get_s3_config
    Retrieves the connection parameters from Airflow connection to MinIO service.

    Args:
        conn_id (str, optional) : Airflow connection string. Defaults to 's3'.

    Returns:
        dict : Dict with keys to MinIO service.
    """
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
    """## Description
    Constructs header for DDL Clickhouse query in the following format:

    ```
    CREATE DATABASE IF NOT EXISTS `schema`;
    DROP TABLE IF EXISTS `schema`.`table`;
    CREATE TABLE `schema`.`table`
    ENGINE = [ENGINE]
    [ORDER BY() | PARTITION BY()]
    AS
    ```

    Args:
        schema (str): Schema to create.
        table (str): Table to create.
        order_by (list[str], optional): `ORDER BY` clause for Clickhouse. Nessessary with `MergeTree()`. Defaults to `None`.
        partition_by (list[str], optional): `PARTITION BY` clause for Clickhouse. Defaults to `None`.
        engine (str, optional): Engine for Clickhouse table. Defaults to `'MergeTree()'`.

    Raises:
        **ValueError**: If engine is `'MergeTree()'`, but `order_by: list[str]` is not set.

    Returns:
        str: Header for DDL Clickhouse query.
    """
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