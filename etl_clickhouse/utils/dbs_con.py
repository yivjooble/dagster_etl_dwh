import os
import sys
import traceback
import clickhouse_connect
import psycopg2

from clickhouse_driver import Client
from dotenv import load_dotenv
from sqlalchemy import create_engine
from dagster import (
    Failure
)

# load environment variables
load_dotenv()


def get_clickhouse_client(context, db) -> clickhouse_connect:
    """
    Connect to Clickhouse over clickhouse_connect
    :return: client
    """
    try:
        client = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST"),
                                               port=os.getenv("CLICKHOUSE_PORT_1"),
                                               username=os.getenv("CLICKHOUSE_USER"),
                                               password=os.getenv("CLICKHOUSE_PASSWORD"),
                                               database=db)
        return client
    except Exception as e:
        print(f"Error while connecting to Clickhouse: {e}")
        context.log.error(f"Error while connecting to Clickhouse: {e}\n traceback: {traceback.format_exc()}")
        sys.exit(1)


def get_clickhouse_driver_client(context, input_country):
    """
    Connect to Clickhouse over clickhouse_driver
    :return: client
    """
    try:
        client = Client(host=os.getenv("CLICKHOUSE_HOST"),
                        port=os.getenv("CLICKHOUSE_PORT_2"),
                        user=os.getenv("CLICKHOUSE_USER"),
                        password=os.getenv("CLICKHOUSE_PASSWORD"),
                        database=input_country,
                        settings={'use_numpy': True})
        return client
    except Exception as e:
        print(f"Error while connecting to Clickhouse: {e}")
        context.log.error(f"Error while connecting to Clickhouse: {e}\n traceback: {traceback.format_exc()}")
        sys.exit(1)


def get_history_conn_psycopg2(host, db):
    """
    Connect to the history database using psycopg2
    :return: connection object
    """
    conn = psycopg2.connect(
        host=host,
        database=db,
        user=os.environ.get('HISTORY_USER'),
        password=os.environ.get('HISTORY_PASSWORD'))
    return conn


def get_history_conn_sqlalchemy(host, db):
    user = os.environ.get('HISTORY_USER')
    password = os.environ.get('HISTORY_PASSWORD')
    host = host,
    database = db,
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{database}")


def get_dwh_conn_psycopg2():
    conn = psycopg2.connect(
        host=os.environ.get('DWH_HOST'),
        database=os.environ.get('DWH_DB'),
        user=os.environ.get('DWH_USER'),
        password=os.environ.get('DWH_PASSWORD'))
    return conn


def get_dwh_conn_sqlalchemy():
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    host = os.environ.get('DWH_HOST')
    dbname = os.environ.get('DWH_DB')
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")


def get_prod_conn_sqlalchemy(host, database):
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    port = 1433
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},
        pool_pre_ping=True)


def get_conn_to_rpl_sqlalchemy(host, dbname):
    try:
        user = os.environ.get('REPLICA_USER')
        password = os.environ.get('REPLICA_PASSWORD')
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")
    except Exception as e:
        raise Failure(f"Error connecting to replica: {e}")


def get_str_conn_to_rpl_sqlalchemy(host, dbname) -> str:
    """
    For Dask.
    """
    try:
        user = os.environ.get('REPLICA_USER')
        password = os.environ.get('REPLICA_PASSWORD')
        return f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
    except Exception as e:
        raise Failure(f"Error connecting to replica: {e}")


def get_conn_to_rpl_psycopg2(host, dbname):
    try:
        user = os.environ.get('REPLICA_USER')
        password = os.environ.get('REPLICA_PASSWORD')
        return psycopg2.connect(
            host=host,
            database=dbname,
            user=user,
            password=password
        )
    except Exception as e:
        raise Failure(f"Error connecting to replica: {e}")