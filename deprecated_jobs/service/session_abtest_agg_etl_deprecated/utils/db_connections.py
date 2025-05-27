import psycopg2
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

def dwh_conn_psycopg2():
    conn = psycopg2.connect(
        host=os.environ.get('DWH_HOST'),
        database=os.environ.get('DWH_DB'),
        user=os.environ.get('DWH_USER'),
        password=os.environ.get('DWH_PASSWORD'))
    return conn


def dwh_conn_sqlalchemy():
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    host = os.environ.get('DWH_HOST')
    dbname = os.environ.get('DWH_DB')
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")


# Create a SQLAlchemy session factory using the DWH engine
Session = sessionmaker(bind=dwh_conn_sqlalchemy())