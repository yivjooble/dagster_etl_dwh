import trino
from contextlib import contextmanager
import psycopg2
import psycopg
from psycopg.rows import dict_row
from sqlalchemy import create_engine
from utility_hub.data_collections import db_credential_keys
from utility_hub.core_tools import get_creds_from_vault
from dagster import Failure
from clickhouse_driver import Client
from functools import lru_cache
from psycopg_pool import ConnectionPool


class DatabaseConnectionManager:
    """
    Manages database connections across multiple databases and credentials.

    It supports dynamic host and database configurations for different credentials,
    with special handling for the 'dwh' credential, which uses default host and database settings.
    """

    def __init__(self):
        # Initialize default host, database, and db_type for 'dwh' credentials
        self.default_host = get_creds_from_vault("DWH_HOST")
        self.default_database = get_creds_from_vault("DWH_DB")
        self.default_db_type = "postgresql"

        # Define credentials for all supported database connections
        self.db_credentials = {
            key: {
                "user": get_creds_from_vault(credential["user"]),
                "password": get_creds_from_vault(credential["password"])
            } for key, credential in db_credential_keys.items()
        }

        # Define database types and their connection parameters
        self.db_type_config = {
            "mssql": {
                "driver": "ODBC Driver 17 for SQL Server",
                "port": 1433,
                "url_template": "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}"
            },
            "postgresql": {
                "url_template": "postgresql+psycopg2://{user}:{password}@{host}/{database}"
            },
            "mariadb": {
                "port": 3306,
                "url_template": "mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}"
            },
            # Additional database types can be added here
        }

    @lru_cache(maxsize=256)
    def get_sqlalchemy_engine(self, credential_key="dwh", host=None, database=None, db_type=None):
        """
        Retrieves or creates an SQLAlchemy engine based on the provided credentials,
        host, database, and database type. Special handling for 'dwh' to use default host and database.

        Parameters:
        - credential_key: The key to identify the set of credentials to use.
        - host: The host of the database (optional for 'dwh').
        - database: The name of the database (optional for 'dwh').
        - db_type: The type of the database ('mssql', 'mariadb', etc.).

        Returns:
        - SQLAlchemy engine object.
        """
        if credential_key == "dwh":
            host = self.default_host
            database = self.default_database
            db_type = self.default_db_type
        else:
            if not host or not database or not db_type:
                raise ValueError(f"Host, database, and db_type must be provided for {credential_key} credentials")

        config = self.db_type_config.get(db_type)
        if not config:
            raise ValueError(f"Unsupported db_type: {db_type}")

        user = self.db_credentials[credential_key]["user"]
        password = self.db_credentials[credential_key]["password"]

        connection_params = {
            "user": user,
            "password": password,
            "host": host,
            "database": database
        }

        if "port" in config:
            connection_params["port"] = config["port"]
        if "driver" in config:
            connection_params["driver"] = config["driver"]

        connection_url = config["url_template"].format(**connection_params)

        engine = create_engine(
            connection_url,
            connect_args={"connect_timeout": 14400} if db_type in ["mssql", "mariadb"] else {},
            pool_pre_ping=True,
            echo=False
        )

        return engine

    @contextmanager
    def get_psycopg2_connection(self, credential_key="dwh", host=None, database=None):
        """
        Yields a psycopg2 connection for the specified credentials, automatically handling
        connection closure.

        Parameters:
        - credential_key: The key to identify the set of credentials to use.
        - host: The host of the database. For 'dwh' credentials, defaults to the class-defined host.
        - database: The name of the database. For 'dwh' credentials, defaults to the class-defined database.

        Yields:
        - A psycopg2 connection object.
        """
        if credential_key == "dwh":
            host = self.default_host
            database = self.default_database
        elif not host or not database:
            raise ValueError(f"Host and database must be provided for {credential_key} credentials")

        user = self.db_credentials[credential_key]["user"]
        password = self.db_credentials[credential_key]["password"]

        connection = psycopg2.connect(
            host=host,
            dbname=database,
            user=user,
            password=password,
        )
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def get_psycopg_connection(self, credential_key="dwh", host=None, database=None):
        """
        Yields a psycopg connection for the specified credentials, automatically handling
        connection closure.

        Parameters:
        - credential_key: The key to identify the set of credentials to use.
        - host: The host of the database. For 'dwh' credentials, defaults to the class-defined host.
        - database: The name of the database. For 'dwh' credentials, defaults to the class-defined database.

        Yields:
        - A psycopg connection object.
        """
        if credential_key == "dwh":
            host = self.default_host
            database = self.default_database
        elif not host or not database:
            raise ValueError(f"Host and database must be provided for {credential_key} credentials")

        user = self.db_credentials[credential_key]["user"]
        password = self.db_credentials[credential_key]["password"]

        connection = psycopg.connect(
            autocommit=True,
            host=host,
            dbname=database,
            user=user,
            password=password,
            row_factory=dict_row
        )
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def get_clickhouse_driver_client(database):
        """
        Connect to Clickhouse over clickhouse_driver
        :return: client
        """
        try:
            client = Client(host=get_creds_from_vault("CLICKHOUSE_HOST"),
                            port=get_creds_from_vault("CLICKHOUSE_PORT_2"),
                            user=get_creds_from_vault("CLICKHOUSE_USER"),
                            password=get_creds_from_vault("CLICKHOUSE_PASSWORD"),
                            database=database,
                            settings={'use_numpy': True}
                            )
            return client
        except Exception as exc:
            raise Failure(f"Error while connecting to Clickhouse: {exc}")

    @staticmethod
    def get_trino_connection(catalog: str):
        try:
            conn = trino.dbapi.connect(
                host='10.0.3.252',
                port=8080,
                user=get_creds_from_vault('TRINO_USER'),
                catalog=catalog,
                session_properties={
                    'query_max_stage_count': '250'
                }
            )
            return conn
        except Exception as e:
            raise Failure(f"Failed to connect to Trino:\n{e}")

    @lru_cache(maxsize=32)
    def get_connection_pool(self, credential_key="dwh", host=None, database=None, min_size=1, max_size=12):
        """
        Creates or returns a cached connection pool for the specified database.

        Parameters:
            credential_key (str): The key to identify the set of credentials to use
            host (str): Database host. For 'dwh' credentials, defaults to the class-defined host
            database (str): Database name. For 'dwh' credentials, defaults to the class-defined database
            min_size (int): Minimum number of connections in the pool
            max_size (int): Maximum number of connections in the pool

        Returns:
            ConnectionPool: A connection pool instance

        Example:
            >>> db_manager = DatabaseConnectionManager()
            >>> with db_manager.get_connection_pool(database='my_db').connection() as conn:
            ...     with conn.cursor() as cur:
            ...         cur.execute("SELECT * FROM my_table")
        """
        if credential_key == "dwh":
            host = self.default_host
            database = self.default_database
        elif not host or not database:
            raise ValueError(f"Host and database must be provided for {credential_key} credentials")

        user = self.db_credentials[credential_key]["user"]
        password = self.db_credentials[credential_key]["password"]

        # Build connection string
        conninfo = (
            f"host={host} "
            f"dbname={database} "
            f"user={user} "
            f"password={password}"
        )

        # Create pool with settings
        pool = ConnectionPool(
            conninfo=conninfo,
            min_size=min_size,
            max_size=max_size,
            open=True,  # Open pool immediately
            kwargs={"row_factory": dict_row},  # Use dict_row
            # Additional settings
            timeout=30.0,  # Maximum time to wait for a connection
            max_lifetime=7200.0,  # Maximum connection lifetime (2 hours)
            max_idle=300.0  # Maximum idle time (5 minutes)
        )

        return pool
