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
    to a centralized DQP table for tracking.
    """
    __allowed_connectors: List[str] = ['postgres', 'mysql', 'clickhouse']
    __schema: str = 'data_quality_checks'
    __dqp_conn_id: str
    
    def __init__(self, dqp_conn_id: str) -> None:
        """## Description
        Initializes the DQEngine instance with the provided connection ID.

        Args:
            dqp_conn_id (str): The Airflow connection ID for the centralized PostgreSQL database.
        """
        self.__dqp_conn_id = dqp_conn_id
        logging.info('Initialized DQEngine instance')

    def parse_config_dir(self, configs_path: str) -> None:
        """## Description
        Scans the specified directory for configuration files and processes each file using the `parse_config` method.

        ## Details
        - Iterates through all files in the `configs_path` directory;
        - Logs the name of each configuration file before processing;
        - Calls the `parse_config` method for each configuration file.

        Args:
            configs_path (str): Path to the directory containing configuration files.
        """
        import os
        for config_name in os.listdir(configs_path):
            logging.info(f'Start to parse config: {config_name}')
            self.parse_config(f'{configs_path}/{config_name}')
        
    def parse_config(self, config_path: str) -> None:
        """## Description
        Reads and parses the YAML file via `pyyaml`.

        Args:
            config_path (str): The path to the configuration file to process.

        Raises:
            **ValueError**: If the configuration version is not "v1"
            **ValueError**: If the connection type is not supported.

        ## Details
        Constructs a table name for storing DQ check results as `f"{conf_metadata['project']}__{conf_metadata["name"]}"`.
        - Iterates through each task in configs;
        - Parses the task's specifications (e.g., query, connection details);
        - Validates the connection type (must be one of postgres, mysql, or clickhouse);
        - Transforms the resulting DataFrame by adding necessary columns (key and task);
        - Exports the transformed DataFrame to the centralized PostgreSQL Database.
        """
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
        """## Description
        Validates the version of the configuration file.

        Args:
            config (Dict): The parsed configuration dictionary.

        Raises:
            **ValueError**: If the configuration version is not "v1".
        """
        if config.get('version') != 'v1':
            raise ValueError(f'Unsupported config version: {config.get("version")}')
        
    def _log_metadata(self, metadata: Dict) -> None:
        """## Description
        Logs the metadata information from the configuration file.

        Args:
            metadata (Dict): The metadata section of the configuration file.
        """
        annotations = metadata['annotations']
        logging.info(f"Config's owner: {annotations['owner']}. Description: {annotations['description']}")
        
    def _process_task(self, task_name: str, task_spec: Dict, table_name: str) -> None:
        """## Description
        Processes a single task from the configuration file.

        Args:
            task_name (str): The name of the task.
            task_spec (Dict): The specification of the task.
            table_name (str): The name of the table to store DQ check results.

        ## Details
        - Parses the task's specifications (e.g., query, connection details);
        - Validates the connection type;
        - Executes the query and retrieves the result as a DataFrame;
        - Transforms the DataFrame by adding necessary columns;
        - Exports the transformed DataFrame to the centralized PostgreSQL Database.
        """
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
        """## Description
        Executes a SQL query on the specified database and returns the result as a Pandas DataFrame.

        Args:
            conn_type (str): The type of database (e.g., postgres, mysql, clickhouse).
            conn_id (str): The connection ID for the database.
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The result of the query as a Pandas DataFrame.

        Raises:
            **UnsupportedConnectorError**: If the connector type is not supported.
        """
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
        
    def _transform_dataframe(self, df: pd.DataFrame, index: str, key: str, task_name: str) -> pd.DataFrame:
        """## Description
        Transforms the DataFrame by adding necessary columns (e.g., key, task, timestamp).

        Args:
            df (pd.DataFrame): The input DataFrame.
            index (str): The field to use as the pd.DataFrame index.
            key (str): The key value to add as a column.
            task_name (str): The task name to add as a column.

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        df = df.set_index(index)
        df['key'] = key
        df['task'] = task_name
        df['_updated_dttm'] = pd.Timestamp.now()
        return df
    
    def _export_dataframe(self, df: pd.DataFrame, table_name: str, key: str, task_name: str) -> None:
        """## Description
        Exports the transformed DataFrame to a centralized DQP Database.

        Args:
            df (pd.DataFrame): The DataFrame to export.
            table_name (str): The name of the table to store the data.
            key (str): The key value used for filtering existing rows.
            task_name (str): The task name used for filtering existing rows.

        ## Details
        - Deletes existing rows in the target table that match the current key and task to avoid duplicates;
        - Writes the transformed DataFrame to the target table using `pandas.DataFrame.to_sql()` with batch insertion (`method='multi'` and `chunksize=1000`);
        - Closes the database connection and disposes of the SQLAlchemy engine after the operation.

        Raises:
            **Exception**: If there's an issue during the export process.
        """
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
        