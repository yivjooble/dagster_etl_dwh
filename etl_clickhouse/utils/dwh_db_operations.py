from .dbs_con import (
    get_dwh_conn_psycopg2,
    get_dwh_conn_sqlalchemy
)

def delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_COUNTRY_COLUMN, DELETE_DATE_DIFF_COLUMN, launch_countries, launch_datediff_start, launch_datediff_end, id_list):
    try:
        with get_dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""delete from {SCHEMA}.{TABLE_NAME} 
                                   where {DELETE_COUNTRY_COLUMN} in ({id_list})
                                   and {DELETE_DATE_DIFF_COLUMN} between '{launch_datediff_start}' and '{launch_datediff_end}';""")
                conn.commit()
                cursor.close()
        context.log.info(f'Deleted data from [{SCHEMA}.{TABLE_NAME}]\n'
                         f'countries: {launch_countries}\n'
                         f'[between {launch_datediff_start} and {launch_datediff_end}]')
    except Exception as e:
        context.log.error(f'deleting error: {e}')
        raise e
    

def save_to_dwh(df, table_name, schema):
    df.to_sql(
        table_name,
        con=get_dwh_conn_sqlalchemy(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=200000
    )

def truncate_dwh_table(table_name, schema):
    conn = get_dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
    conn.commit()
    cur.close()
    conn.close()

def execute_on_dwh(sql):
    conn = get_dwh_conn_psycopg2()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()