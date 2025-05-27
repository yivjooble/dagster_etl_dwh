from .dwh_connections import (
    dwh_conn_psycopg2,
    dwh_conn_sqlalchemy
)


def delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_DATE_DIFF_COLUMN, date_start, date_end):
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""delete from {SCHEMA}.{TABLE_NAME} 
                                   where {DELETE_DATE_DIFF_COLUMN} between '{date_start}' and '{date_end}';""")
                conn.commit()
                cursor.close()
        context.log.info(f'Deleted data from [{SCHEMA}.{TABLE_NAME}]\n'
                         f'datediff: {date_start}/-/{date_end}')
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
        chunksize=50000
    )


def truncate_dwh_table(context, table_name, schema):
    conn = dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(f"truncate {schema}.{table_name};")
    context.log.info(f"Truncated: {schema}.{table_name}")
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