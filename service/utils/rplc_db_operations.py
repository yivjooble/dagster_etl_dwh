import pandas as pd
import gzip
import pickle

from .rplc_connections import connect_to_replica


def generate_params_dynamically(context, sql_instance_country_query) -> tuple:
    params = tuple(value for key, value in sql_instance_country_query.items() if key.startswith('to_sqlcode'))
    return params


def start_query_on_rplc_db(context, sql_instance_country_query: dict):
    conn = connect_to_replica(sql_instance_country_query['sql_instance_host'], sql_instance_country_query['country_db'])
    conn.autocommit = True
    cursor = conn.cursor()

    run_date = sql_instance_country_query['run_date'] if 'run_date' in sql_instance_country_query\
        else None
    params = generate_params_dynamically(context, sql_instance_country_query)

    try:
        # launch procedure on replica's db
        cursor.execute(sql_instance_country_query['query'], params,)
        conn.commit()

        context.log.info(f"success for {sql_instance_country_query['country_db']}" + f", {run_date}" if run_date else '')
        conn.close()
        return 'success'
    except Exception as e:
        context.log.error(f"error for: {sql_instance_country_query['country_db']}\n{e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def create_rplc_df(context, PATH_TO_DATA, sql_instance_country_query: dict):
    try:
        date_int = sql_instance_country_query['to_sqlcode_date_or_datediff_start']
        df = pd.read_sql_query(sql=sql_instance_country_query['query'],
                               con=connect_to_replica(sql_instance_country_query['sql_instance_host'],
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


def run_ddl_replica(context, params: dict):
    try:
        db = params['country_db']
        db_host = params['sql_instance_host']
        ddl_query = params['ddl_query']

        conn = connect_to_replica(db_host, db)
        cursor = conn.cursor()

        # launch procedure on replica's db
        cursor.execute(ddl_query, )
        conn.commit()

        # close connection
        cursor.close()
        conn.close()
    except Exception as e:
        context.log.error(f"error for: {db}\n{e}")
        raise e
