import os
import psycopg2

from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()


def connect_psycopg2(host, dbname):
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('DWH_USER')} password={os.environ.get('DWH_PASSWORD')}")

def connect_sqlalchemy(host, dbname):
    return create_engine(
        f"postgresql+psycopg2://{os.environ.get('DWH_USER')}:{os.environ.get('DWH_PASSWORD')}@{host}/{dbname}")
        

# Create a SQLAlchemy session factory using the DWH engine
Session = sessionmaker(bind=connect_sqlalchemy("10.0.1.61", "postgres"))

def get_sql(table_name):
    with open(f'sql/{table_name}.sql', 'r') as sql_file:
        sql_query = sql_file.read()
    return sql_query