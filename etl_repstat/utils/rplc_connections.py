import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def connect_to_replica(host, dbname):
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('REPLICA_USER')} "
        f"password={os.environ.get('REPLICA_PASSWORD')}"
    )
