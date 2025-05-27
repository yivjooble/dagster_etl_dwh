from .dwh_connections import dwh_conn_psycopg2, dwh_conn_sqlalchemy


def delete_data_from_dwh_table(
    context,
    schema,
    table_name,
    delete_country_column,
    delete_date_date_diff_column,
    date_start,
    date_end=None,
    id_list=None,
):
    date_end = date_end if date_end else date_start
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""delete from {schema}.{table_name}
                        where {delete_country_column} in ({id_list})
                        and {delete_date_date_diff_column} between '{date_start}' and '{date_end}';"""
                )
                conn.commit()
                cursor.close()
        context.log.info(
            f"Deleted data from [{schema}.{table_name}]\n"
            f"countries: {id_list}\n"
            f'date_diff: [{date_start}]'
        )
    except Exception as e:
        context.log.error(f"deleting error: {e}")
        raise e


def delete_data_from_dwh_table_wo_countries(
    context,
    schema,
    table_name,
    delete_date_column,
    date_start,
    date_end=None,
):
    date_end = date_end if date_end else date_start
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""delete from {schema}.{table_name}
                        where {delete_date_column} between '{date_start}' and '{date_end}';"""
                )
                conn.commit()
                cursor.close()
        context.log.info(
            f"Deleted data from [{schema}.{table_name}]\n"
            f'date: [{date_start}]/-/[{date_end}]'
        )
    except Exception as e:
        context.log.error(f"deleting error: {e}")
        raise e


def generate_params_dynamically(context, params) -> tuple:
    params = tuple(
        value for key, value in params.items() if key.startswith("to_sqlcode")
    )
    context.log.info(f"{params}")
    return params


def start_query_on_dwh_db(context, params: dict):
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                param = generate_params_dynamically(context, params)

                cursor.execute(
                    params["query"],
                    param,
                )
                conn.commit()
                cursor.close()
                context.log.info("success")
    except Exception as e:
        context.log.error(f"error:\n{e}")
        raise e


def save_to_dwh(df, table_name, schema):
    df.to_sql(
        table_name,
        con=dwh_conn_sqlalchemy(),
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=50000,
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
