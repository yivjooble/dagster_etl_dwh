import psycopg2
import os

from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def connect_to_replica(host, dbname):
    con = psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('REPLICA_USER')} password={os.environ.get('REPLICA_PASSWORD')}"
    )
    return con


def conn_to_repstat_sqlalchemy(host, dbname):
    user = os.environ.get('REPLICA_USER')
    password = os.environ.get('REPLICA_PASSWORD')
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")
