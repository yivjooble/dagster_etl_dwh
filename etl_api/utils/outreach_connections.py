import psycopg2
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def ltf_conn_sqlalchemy():
    # DEFINE THE DATABASE CREDENTIALS
    user = os.environ.get('LTF_DB_USERNAME')
    password = os.environ.get('LTF_DB_PASS')
    host = os.environ.get('LTF_DB_HOST')
    port = 1433
    database = os.environ.get('LTF_DB_NAME')
    driver = 'ODBC Driver 17 for SQL Server'
    return create_engine(
        f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver={driver}",
        connect_args={'connect_timeout': 14400},)