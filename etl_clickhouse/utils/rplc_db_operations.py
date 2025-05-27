import pandas as pd
import gzip
import pickle

from dagster import (
    Failure
)
from .dbs_con import get_conn_to_rpl_sqlalchemy, get_conn_to_rpl_psycopg2


def generate_params_dynamically(context, sql_instance_country_query) -> tuple:
    params = tuple(value for key, value in sql_instance_country_query.items() if key.startswith('to_sqlcode'))
    return params


def start_query_on_rplc_db(context, sql_instance_country_query: dict):
    try:
        conn = get_conn_to_rpl_psycopg2(sql_instance_country_query['sql_instance_host'],
                                        sql_instance_country_query['country_db'])
        cursor = conn.cursor()

        params = generate_params_dynamically(context, sql_instance_country_query)

        # launch procedure on replica's db
        cursor.execute(sql_instance_country_query['query'],
                       params, )
        conn.commit()

        context.log.info(f"success for {sql_instance_country_query['country_db']}, {params}")
        cursor.close()
        conn.close()
        return 'success'
    except Exception as e:
        context.log.error(f"error for: {sql_instance_country_query['country_db']}\n{e}")
        raise e


def create_rplc_df(context, PATH_TO_DATA, sql_instance_country_query: dict):
    try:
        date_int = sql_instance_country_query['to_sqlcode_date_or_datediff_start']
        df = pd.read_sql_query(sql=sql_instance_country_query['query'],
                               con=get_conn_to_rpl_sqlalchemy(sql_instance_country_query['sql_instance_host'],
                                                              sql_instance_country_query['country_db']),
                               params={'datediff': date_int})

        file_path = f"{PATH_TO_DATA}/{sql_instance_country_query['country_db']}_DB_{date_int}.pkl"
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(df, f)

        context.log.info(
            f"Data loaded for: {sql_instance_country_query['country_db']}, {sql_instance_country_query['to_sqlcode_date_or_datediff_start']}, {df.shape[0]},")
        return file_path
    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise e


def truncate_rpl_table(host: str, db_name: str, table_name: str, schema: str):
    try:
        conn = get_conn_to_rpl_psycopg2(host, db_name)
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        raise Failure(f"truncate error: {e}")


def save_to_rpl_table(df, host: str, db_name: str, table_name: str, schema: str):
    try:
        df.to_sql(
            table_name,
            con=get_conn_to_rpl_sqlalchemy(host, db_name),
            schema=schema,
            if_exists='append',
            index=False
        )
    except Exception as e:
        raise Failure(f"saving to rpl error: {e}")


def execute_on_rpl(sql, host, dbname):
    conn = get_conn_to_rpl_psycopg2(host, dbname)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
