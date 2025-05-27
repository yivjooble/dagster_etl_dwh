import psycopg2
import os

from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def connect_to_pg_db(host, dbname):
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('INTERNAL_USER')} password={os.environ.get('INTERNAL_PASSWORD')}"
    )


def conn_conversion_us_db_postgres(host, dbname):
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('CONVERSION_USER')} "
        f"password={os.environ.get('CONVERSION_PASSWORD')}"
    )


def conn_employer_sqlalchemy_mariadb(host, database):
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('INTERNAL_USER')
    password = os.environ.get('INTERNAL_PASSWORD')
    port = 3306
    # driver = 'ODBC Driver 17 for SQL Server'

    return create_engine(
        f"mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{database}",
        connect_args={'connect_timeout': 14400},)


def conn_mssql_db(host, database):
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('INTERNAL_USER')
    password = os.environ.get('INTERNAL_PASSWORD')
    port = 1433
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},)
    
    
def conn_mssql_soska_db(host, database):
    user = os.environ.get('S_USER')
    password = os.environ.get('S_PASSWORD')
    port = 1433
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},)
    

def conn_mssql_seo_server_db(host, database):
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('GBC_USER')
    password = os.environ.get('GBC_PASSWORD')
    port = 1433
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},)
