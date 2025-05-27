import pandas as pd
from .dwh_connections import (
    dwh_conn_psycopg2,
    dwh_conn_sqlalchemy,
    citus_conn_sqlalchemy
)


def delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_COUNTRY_COLUMN, DELETE_DATE_DIFF_COLUMN,
                               launch_countries, launch_datediff_start, id_list=None, delete_sign='='):
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""delete from {SCHEMA}.{TABLE_NAME}
                                   where {DELETE_COUNTRY_COLUMN} in ({id_list})
                                   and {DELETE_DATE_DIFF_COLUMN} {delete_sign} {launch_datediff_start};""")
                conn.commit()
                cursor.close()
        context.log.info(f'Deleted data from [{SCHEMA}.{TABLE_NAME}]\n'
                         f'countries: {id_list}\n'
                         f'datediff {delete_sign} [{context.resources.globals["reload_date_start"]}]')
    except Exception as e:
        context.log.error(f'deleting error: {e}')
        raise e


def save_to_citus(df, table_name, schema):
    df.to_sql(
        table_name,
        con=citus_conn_sqlalchemy(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=50000
    )


def save_to_dwh(df, table_name, schema):
    df.to_sql(
        table_name,
        con=dwh_conn_sqlalchemy(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=50000
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
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def select_from_dwh(sql):
    conn = dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(sql)
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    conn.commit()
    cur.close()
    conn.close()
    return df
