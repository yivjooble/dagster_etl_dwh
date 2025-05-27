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


def dwh_conn_sqlalchemy():
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    host = os.environ.get('DWH_HOST')
    dbname = os.environ.get('DWH_DB')
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")


# Create a SQLAlchemy session factory using the DWH engine
Session = sessionmaker(bind=dwh_conn_sqlalchemy())


def save_to_dwh(df, table_name, schema):
    df.to_sql(
        table_name,
        con=dwh_conn_sqlalchemy(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=10000
    )


def truncate_dwh_table(table_name, schema):
    conn = dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
    conn.commit()
    cur.close()
    conn.close()


def execute_on_dwh(sql):
    conn = dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
