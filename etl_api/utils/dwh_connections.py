import psycopg2
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()


def dwh_conn_psycopg2():
    conn = psycopg2.connect(
        host=os.environ.get('DWH_HOST'),
        database=os.environ.get('DWH_DB'),
        user=os.environ.get('DWH_USER'),
        password=os.environ.get('DWH_PASSWORD'))
    return conn


def cloudberry_conn_psycopg2():
    conn = psycopg2.connect(
        host="an-dwh.jooble.com",
        database="an_dwh",
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
