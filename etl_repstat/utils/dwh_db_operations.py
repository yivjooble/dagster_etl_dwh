from .dwh_connections import (
    dwh_conn_psycopg2,
    dwh_conn_sqlalchemy
)
from .date_format_settings import date_diff_to_date
from contextlib import contextmanager
import pandas as pd


@contextmanager
def connect_to_database():
    """Context manager to handle database connections."""
    connection = dwh_conn_psycopg2()
    try:
        yield connection
    finally:
        connection.close()


def delete_data_from_dwh_table(context,
                               SCHEMA,
                               TABLE_NAME,
                               DELETE_COUNTRY_COLUMN,
                               DELETE_DATE_DIFF_COLUMN,
                               launch_countries,
                               launch_datediff_start,
                               id_list=None,
                               launch_datediff_end=None):
    try:
        launch_datediff_end = launch_datediff_end if launch_datediff_end else launch_datediff_start
        # log_date = date_diff_to_date(launch_datediff_end) if isinstance(launch_datediff_end, int) \
        #     else launch_datediff_end
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""delete from {SCHEMA}.{TABLE_NAME} 
                                   where {DELETE_COUNTRY_COLUMN} in ({id_list})
                                   and {DELETE_DATE_DIFF_COLUMN} between '{launch_datediff_start}' and '{launch_datediff_end}';""")
                conn.commit()
                cursor.close()
        context.log.info(f'Deleted data from [{SCHEMA}.{TABLE_NAME}]\n'
                         f'countries: {id_list}\n'
                         f'datediff between [{launch_datediff_start}] and [{launch_datediff_end}]')
    except Exception as e:
        context.log.error(f'deleting error: {e}')
        raise e
    

def save_to_dwh(df, table_name, schema):
    df.to_sql(
        table_name,
        con=dwh_conn_sqlalchemy(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=200000
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


def check_if_db_table_exists(schema_name: str, table_name: str) -> bool:
    dwh_con = connect_to_database()
    sql = f'''select * from information_schema.tables where table_schema = '{schema_name}' and table_name = '{table_name}';'''
    with dwh_con as conn:
        df = pd.read_sql_query(sql=sql, con=conn)

        if not df.empty:
            return True
        else:
            return False
