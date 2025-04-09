from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_connect import get_client
from sqlalchemy import create_engine
from typing import List, Dict
import logging
import pandas as pd
import psycopg2 as pg
import yaml

class DQEngine:
    """## Description
    The DQEngine class is designed to perform data quality checks (DQ)
    by parsing configuration files, executing queries on supported databases
    (PostgreSQL, MySQL, ClickHouse), and exporting the results
    to a centralized PostgreSQL table for tracking.
    """
    __allowed_connectors: List[str] = ['postgres', 'mysql', 'clickhouse']
    __schema: str = 'data_quality_checks'
    __dqp_conn_id: str
    
    def __init__(self, dqp_conn_id: str) -> None:
        self.__dqp_conn_id = dqp_conn_id
        logging.info('Initialized DQEngine instance')

    def parse_config_dir(self, configs_path: str) -> None:
        import os
        for config_name in os.listdir(configs_path):
            logging.info(f'Start to parse config: {config_name}')
            self.parse_config(f'{configs_path}/{config_name}')
        
    def parse_config(self, config_path: str) -> None:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            self._validate_config_version(config)
            conf_metadata = config['metadata']
            conf_tasks = config['tasks']
        
        self._log_metadata(conf_metadata)
        table_name = f"{conf_metadata['project']}__{conf_metadata['name']}"
        logging.info(f'Table name: {table_name}')
            
        for task in conf_tasks:
            self._process_task(task, conf_tasks[task], table_name)
    
    def _validate_config_version(self, config: Dict) -> None:
        """Checks configuration version"""
        if config.get('version') != 'v1':
            raise ValueError(f'Unsupported config version: {config.get("version")}')
        
    def _log_metadata(self, metadata: Dict) -> None:
        annotations = metadata['annotations']
        logging.info(f"Config's owner: {annotations['owner']}. Description: {annotations['description']}")
        
    def _process_task(self, task_name: str, task_spec: Dict, table_name: str) -> None:
        """Обрабатывает задачу"""
        asset = task_spec['spec']['asset']
        params = task_spec['spec']['params']
        
        conn_type, conn_id = asset['type'], asset['connection']
        if conn_type not in self.__allowed_connectors:
            raise ValueError(f'Unsupported connector: {conn_type}')
        
        query = params['query']
        logging.info(f'Executing query [{conn_type}]: {query}')

        df = self._execute_query(conn_type, conn_id, query)
        df = self._transform_dataframe(df, params['fields']['dimDate'], asset['key'], task_name)
        self._export_dataframe(df, table_name, asset['key'], task_name)
        
    def _execute_query(self, conn_type: str, conn_id: str, query: str) -> pd.DataFrame:
        if conn_type == 'postgres':
            return PostgresHook(postgres_conn_id=conn_id).get_pandas_df(query)
        elif conn_type in ['mysql', 'clickhouse']:
            from airflow.hooks.base import BaseHook
            connection = BaseHook.get_connection(conn_id)
            client = get_client(
                host=connection.host,
                port=connection.port,
                user=connection.login,
                password=connection.password,
                database=connection.schema or 'default',
            )
            result = client.query(query)
            return pd.DataFrame(result.result_rows, columns=result.column_names)
        else:
            raise ValueError(f'Unsupported connector: {conn_type}')
        
    def _transform_dataframe(self, df: pd.DataFrame, index_field: str, key: str, task_name: str) -> pd.DataFrame:
        df = df.set_index(index_field)
        df['key'] = key
        df['task'] = task_name
        df['_updated_dttm'] = pd.Timestamp.now()
        return df
    
    def _export_dataframe(self, df: pd.DataFrame, table_name: str, key: str, task_name: str) -> None:
        from sqlalchemy import create_engine
        
        conn_uri = PostgresHook(postgres_conn_id=self.__dqp_conn_id).get_uri()
        engine = create_engine(conn_uri)
        
        try:
            with pg.connect(conn_uri) as conn:
                with conn.cursor() as cursor:
                    delete_query = f"""
                        DELETE FROM {self.__schema}.{table_name}
                        WHERE key = '{key}'
                            AND task = '{task_name}';
                    """
                    cursor.execute(delete_query)
                    conn.commit()
        except Exception as e:
            logging.error('Error during export: {e}')
            raise
        
        df.to_sql(
            name=table_name,
            schema=self.__schema,
            con=engine,
            index=True,
            if_exists='append',
            method='multi',
            chunksize=1000,
        )
        engine.dispose()
        