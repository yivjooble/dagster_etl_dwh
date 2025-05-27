import os
import pandas as pd
import gzip
import pickle
from contextlib import contextmanager
from typing import Dict
from .rplc_connections import connect_to_replica
from .date_format_settings import date_diff_to_date


@contextmanager
def connect_to_database(host, db_name):
    """Context manager to handle database connections."""
    connection = connect_to_replica(host, db_name)
    try:
        yield connection
    finally:
        connection.close()


def run_ddl_replica(context, params: dict):
    try:
        db = params['country_db']
        db_host = params['sql_instance_host']
        ddl_query = params['ddl_query']

        with connect_to_database(db_host, db) as conn:
            cursor = conn.cursor()

            # launch procedure on replica's db
            cursor.execute(ddl_query, )
            conn.commit()

    except Exception as e:
        context.log.error(f"error for: {db}\n{e}")
        raise e


def generate_params_dynamically(context, sql_instance_country_query) -> tuple:
    params = tuple(value for key, value in sql_instance_country_query.items() if key.startswith('to_sqlcode'))
    return params


def start_query_on_rplc_db(context, sql_instance_country_query: Dict[str, any]):
    try:
        # Ensure all required keys are present
        required_keys = {'sql_instance_host', 'country_db', 'query'}
        if not required_keys.issubset(sql_instance_country_query.keys()):
            missing_keys = required_keys - sql_instance_country_query.keys()
            raise ValueError(f"Missing keys in sql_instance_country_query: {missing_keys}")

        context.log.debug(f"Connecting to DB with host: {sql_instance_country_query['sql_instance_host']} "
                          f"and DB: {sql_instance_country_query['country_db']}")

        # Establish connection
        with connect_to_database(sql_instance_country_query['sql_instance_host'],
                                 sql_instance_country_query['country_db']) as conn:
            cursor = conn.cursor()

            # Generate dynamic parameters for SQL execution
            params = generate_params_dynamically(context, sql_instance_country_query)
            query = sql_instance_country_query['query']

            # Log the query and parameters
            context.log.debug(f"Executing query: {query} with params: {params}")

            # Execute the query
            cursor.execute(query, params)
            conn.commit()

            context.log.info(f"Success for {sql_instance_country_query['country_db']}, Params: {params}")

            return 'success'
    except Exception as e:
        context.log.error(f"Error for: {sql_instance_country_query['country_db']}\nException: {e}")
        raise e


def create_rplc_df(context, path_to_data, sql_instance_country_query: dict):
    try:
        query = sql_instance_country_query.get('query')
        date_int = sql_instance_country_query.get('to_sqlcode_date_or_datediff_start')
        params = {'datediff': date_int}

        # Convert date_diff to date, if applicable, else use as is
        log_date = date_diff_to_date(date_int) if isinstance(date_int, int) else date_int

        # Connect to the database and execute the query
        with connect_to_database(sql_instance_country_query['sql_instance_host'],
                                 sql_instance_country_query['country_db']) as conn:
            df = pd.read_sql_query(sql=query, con=conn, params=params)

        # Save dataframe to a compressed pickle file
        file_path = os.path.join(path_to_data, f"{sql_instance_country_query['country_db']}_DB_{date_int}.pkl")
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(df, f)

        context.log.info(
            f"Data loaded for: {sql_instance_country_query['country_db']}, {log_date}, {df.shape[0]}")
        return file_path
    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise e


def create_df_sql_rpl(context, path_to_data, sql_instance_country_query: dict):
    try:
        date_start = sql_instance_country_query['to_sql_date_start'] \
            if 'to_sql_date_start' in sql_instance_country_query else None
        params = {'date_start': date_start}
        log_date = date_diff_to_date(date_start) if isinstance(date_start, int) else date_start
        df = pd.read_sql_query(sql=sql_instance_country_query['query'],
                               con=connect_to_replica(sql_instance_country_query['sql_instance_host'],
                                                      sql_instance_country_query['country_db']),
                               params=params)

        file_path = f"{path_to_data}/{sql_instance_country_query['country_db']}_DB_{date_start}.pkl"
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(df, f)

        context.log.info(
            f"Data loaded for: {sql_instance_country_query['country_db']}, date: {log_date}, rows_cnt: {df.shape[0]},")
        return file_path
    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise e
