import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from .prod_sqlinstances import SqlInstanceList

load_dotenv()


def get_prod_db_host(db_name: str) -> str:
    # iterate over sql instances
    for sql_instance in SqlInstanceList:
        for country_db in sql_instance['CountryList']:
            if str(country_db).strip().upper() == str(db_name).strip().upper():
                return sql_instance['LocalAddress']


def prod_conn_sqlalchemy(host, database):
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    port = 1433
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},)