import json
from contextlib import closing

import psycopg2
import pandas as pd

from pathlib import Path
from sqlalchemy import create_engine


CREDS_DIR = Path(__file__).parent / 'credentials'

with open(f'{CREDS_DIR}/dwh_cred.json') as json_file:
    dwh_cred = json.load(json_file)

USER = dwh_cred['user']
PASSWORD = dwh_cred['password']
HOST = dwh_cred['host']
PORT = dwh_cred['port']
DB = dwh_cred['database']


def select_to_dwh(query, engine):
    return pd.read_sql_query(
        query,
        con=engine
    )


def truncate_target_table(table_name):
    with closing(psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'delete from dc.{table_name}')
            conn.commit()


def write_to_dwh(df, table_name, logger=None):
    truncate_target_table(table_name)
    df.drop_duplicates().to_sql(
        table_name,
        con=get_dwh_engine(),
        schema='dc',
        if_exists='append',
        index=False
    )
    if logger is not None:
        logger.info(f'{table_name} truncated')
        logger.info(f'{df.shape[0]} row are inserted into {table_name}')


def get_dwh_engine():
    return create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')
