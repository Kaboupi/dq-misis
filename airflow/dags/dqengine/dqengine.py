from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from config.config import DQP_CONN_ID
import pandas as pd
import logging

class DQEngine:
    """### DQEngine class
    The DQEngine class is designed to perform data quality checks (DQ)
    by parsing configuration files, executing queries on supported databases
    (PostgreSQL, MySQL, ClickHouse), and exporting the results
    to a centralized PostgreSQL table for tracking.
    """
    __allowed_connectors : list[str] = ['postgres', 'mysql', 'clickhouse']
    __schema : str = 'data_quality_checks'
    
    def __init__(self, configs_path: str):
        """### Creates DQEngine instance
        Initializes the DQEngine instance with the directory path
        containing configuration files for data quality checks.

        Args:
            configs_path (str): The directory path where DQ configuration files are stored.
        """
        logging.info(f'Directory to scan for DQ configs: {configs_path}')
        self.configs_path = configs_path
        
        
    def parse_config_dir(self,):
        """### parse_config_dir()
        Scans the specified directory for configuration files
        and processes each file using the `__perform_dq_check` method.
        
        ### Details:
        - Iterates through all files in the `configs_path` directory;
        - Logs the name of each configuration file before processing;
        - Calls the `__perform_dq_check` method for each configuration file.
        """
        import os
        
        for config_name in os.listdir(self.configs_path):
            logging.info(f'Start to parse config: {config_name}')
            self.__perform_dq_check(config_name)
        
        
    def __perform_dq_check(self, config_name: str):
        """### perform_dq_check()

        ### Args:
            config_name (str): The name of the configuration file to process.
            
        ### Details:
        - Reads and parses the YAML file via `pyyaml`.
        - Extracts metadata and tasks from the configuration.
        - Constructs a table name for storing DQ check results as `f"{conf_metadata['project']}__{conf_metadata["name"]}"`.
        - - Iterates through each task in configs:
        - - Parses the task's specifications (e.g., query, connection details);
        - - Validates the connection type (must be one of postgres, mysql, or clickhouse);
        - - Transforms the resulting DataFrame by adding necessary columns (key and task);
        - - Exports the transformed DataFrame to the centralized PostgreSQL Database.

        ### Raises:
        - **ValueError** : If the configuration version is not "v1"
        - **ValueError** : If the connection type is not supported.
        """
        import yaml
        
        # Parse yaml config
        logging.info(f'Start read config: {self.configs_path}')
        with open(f'{self.configs_path}/{config_name}', 'r') as file:
            config = yaml.safe_load(file)
            if config['version'] != 'v1':
                raise ValueError(f'Error at {self.configs_path}! DQ supports only "v1" configs!')
            conf_metadata = config['metadata']
            conf_tasks = config['tasks']
            
        conf_annotations = conf_metadata['annotations']
        logging.info(f"Config\'s owner: {conf_annotations['owner']}.\nDescription: {conf_annotations['description']}")
        # Build table name for DQ checks
        self.table_name = f"{conf_metadata['project']}__{conf_metadata["name"]}"
        logging.info(f'Table name: {self.table_name}')
        
        # Go through all the tasks in yaml config
        logging.info('Start parse tasks in config')
        for task in conf_tasks:
            self.task = task
            conf_spec = conf_tasks[task]['spec']
            logging.info(f'Task: {self.task}')
            
            # Start to parse spec
            conf_asset, conf_params = conf_spec['asset'], conf_spec['params']
            self.conf_index = conf_params['fields']['dimDate']
            self.conf_key = conf_asset['key']
            self.conf_query = conf_params['query']
            logging.info(f'Domain: {conf_asset['domain']}\nDescription: {conf_asset['description']}')
            
            # Connection type exception
            self.conn_type, self.conn_id = conf_asset['type'], conf_asset['connection']
            if self.conn_type not in self.__allowed_connectors:
                raise ValueError(f'Connection param {self.conn_type} in {self.configs_path} must be in {self.__allowed_connectors}')
            
            logging.info(f'Query to run [{self.conn_type}]: {self.conf_query}')
            
            # Check connections
            if self.conn_type == 'postgres':
                self.__get_postgres_df()
                
            if self.conn_type in ['mysql', 'clickhouse']:
                self.__get_clickhouse_df()
                
            # Add nessessary cols via pandas
            logging.info(f'Transform dataframe step with index as {self.conf_index}')
            self.comparison_df.set_index(self.conf_index, inplace=True)
            self.comparison_df['key'] = self.conf_key
            self.comparison_df['task'] = task
        
            # Start export
            self.__export_df()
            
            logging.info('Success!')
        
    def __get_postgres_df(self,):
        """### __get_postgres_df()
        Executes a SQL query on a PostgreSQL database and loads the result into a Pandas DataFrame.

        ### Details:
        - Uses the `PostgresHook` to execute the query and retrieve the result as a Pandas DataFrame;
        - Stores the resulting DataFrame in the `self.comparison_df` instance variable.
        """
        self.comparison_df = PostgresHook(postgres_conn_id=self.conn_id).get_pandas_df(self.conf_query)

    
    def __get_clickhouse_df(self,):
        """### __get_clickhouse_df()
        Executes a SQL query on a ClickHouse database and loads the result into a Pandas DataFrame.

        ### Details:
        - Uses the `BaseHook` to execute the query and retrieve the result as a Pandas DataFrame;
        - Connects to the ClickHouse database using `clickhouse-connect`;
        - Stores the resulting DataFrame in the `self.comparison_df` instance variable.
        """
        from clickhouse_connect import get_client
        from airflow.hooks.base import BaseHook
        
        logging.info('Connecting to Clickhouse')
        clickhouse_conn = BaseHook.get_connection(self.conn_id)
        client = get_client(
            host=clickhouse_conn.host,
            port=clickhouse_conn.port,
            user=clickhouse_conn.login,
            password=clickhouse_conn.password,
            database=clickhouse_conn.schema or 'default',
        )
        result_clickhouse = client.query(self.conf_query)
        self.comparison_df = pd.DataFrame(result_clickhouse.result_rows, columns=result_clickhouse.column_names)
        
    
    def __export_df(self, if_exists: str = 'append') -> None:
        """### __export_df()
        Exports the transformed DataFrame `comparison_df` to a centralized PostgreSQL Database to analyze check results.

        ### Args:
            if_exists (str, optional): Specifies the behavior if the table already exists. Defaults to `'append'`.

        ### Details:
        - Establishes a connection to the centralized PostgreSQL Database using `sqlalchemy` and `psycopg2-binary`;
        - Deletes existing rows in the target table that match the current key and task to avoid duplicates;
        - Writes the transformed DataFrame to the target table using `pandas.DataFrame.to_sql()` with batch insertion (`method='multi'` and `chunksize=1000`);
        - Closes the database connection and disposes of the SQLAlchemy engine after the operation.

        ### Raises:
            **ValueError** : If there's an issue with the DQP connection
        """
        from sqlalchemy import create_engine
        import psycopg2 as pg
        
        conn_uri_dqp = PostgresHook(postgres_conn_id=DQP_CONN_ID).get_uri()
        engine_dqp = create_engine(conn_uri_dqp)
        conn_dqp = pg.connect(conn_uri_dqp)
        cursor_dqp = conn_dqp.cursor()
        
        # Delete old rows for each DQ check if exist
        try:
            dqp_query = f"""
                DELETE FROM {self.__schema}.{self.table_name}
                WHERE key = '{self.conf_key}'
                    AND task = '{self.task}';
            """
            logging.info(f'Running query for DQP: {dqp_query}')
            
            cursor_dqp.execute(dqp_query)
            conn_dqp.commit()

        except Exception as e:
            logging.error(f'Found error: {e}')
            conn_dqp.rollback()
            
        finally:
            cursor_dqp.close()
            conn_dqp.close()
        
        self.comparison_df.to_sql(
            name=self.table_name,
            schema=self.__schema,
            con=engine_dqp,
            index=True,
            if_exists=if_exists,
            method="multi",
            chunksize=1000,
        )
        engine_dqp.dispose()