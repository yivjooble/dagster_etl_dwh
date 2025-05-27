import gzip
import pickle

import pandas as pd
from dagster import (
    Failure,
)
from sqlalchemy import text

from etl_jooble_internal.utils.db_connections import (
    connect_to_pg_db,
    conn_employer_sqlalchemy_mariadb,
    conn_mssql_db,
    conn_mssql_soska_db,
    conn_mssql_seo_server_db,
    conn_conversion_us_db_postgres,
)
from etl_jooble_internal.utils.dwh_connections import dwh_conn_sqlalchemy


def save_pkl_files(context, df, path_do_data, sql_instance_country_query: dict, i=0, cluster='cluster'):
    today_date = pd.Timestamp.now().strftime("%Y_%m_%d")
    file_path = f"{path_do_data}/{sql_instance_country_query['db_name']}_DB_{today_date}_{i}_{cluster}.pkl"
    with gzip.open(file_path, "wb") as f:
        pickle.dump(df, f)

    return file_path


def save_pkl_files_mssql_seo_server_db(context, df, path_do_data, sql_instance_country_query: dict, i=0):
    today_date = pd.Timestamp.now().strftime("%Y_%m_%d")
    country_code = sql_instance_country_query['country_code'] if 'country_code' in sql_instance_country_query else 'none'
    db_name = sql_instance_country_query['db_name'] if 'db_name' in sql_instance_country_query else 'none'
    file_path = f"{path_do_data}/{country_code}_{db_name}_DB_{today_date}_{i}.pkl"
    with gzip.open(file_path, "wb") as f:
        pickle.dump(df, f)

    return file_path


def start_query_on_db(context, path_do_data, sql_instance_country_query: dict):
    try:
        cluster = sql_instance_country_query.get("cluster")
        context.log.info(f"Starting query on {cluster} db")

        if cluster == "us":
            conn = conn_conversion_us_db_postgres(
                sql_instance_country_query["sql_instance_host"],
                sql_instance_country_query["db_name"],
            )
            context.log.info(f"Connected to {cluster} db")
        else:
            conn = connect_to_pg_db(
                sql_instance_country_query["sql_instance_host"],
                sql_instance_country_query["db_name"],
            )

        chunks = pd.read_sql_query(
            sql=sql_instance_country_query["query"],
            con=conn,
            chunksize=100000,
        )

        file_paths = []
        for i, chunk in enumerate(chunks):
            if cluster in ('us', 'nl'):
                chunk['cluster'] = 'nl' if cluster == 'nl' else 'us'
            file_path = save_pkl_files(context, chunk, path_do_data, sql_instance_country_query, i, cluster)
            file_paths.append(file_path)
            context.log.info(f"chunk {i} saved to pkl: {file_path}")

        return file_paths

    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_on_mssql_seo_server_db(context, path_do_data, sql_instance_country_query: dict) -> list:
    try:
        q = sql_instance_country_query["formatted_query"] if "formatted_query" in sql_instance_country_query \
            else sql_instance_country_query["query"]
        params = {key: val for key, val in sql_instance_country_query.items() if key.startswith("to_sql")}
        chunks = pd.read_sql_query(
            sql=text(q),
            con=conn_mssql_seo_server_db(
                sql_instance_country_query["sql_instance_host"],
                sql_instance_country_query["db_name"],
            ),
            chunksize=100000,
            params=params,
        )

        file_paths = []
        for i, chunk in enumerate(chunks):
            file_path = save_pkl_files_mssql_seo_server_db(context, chunk, path_do_data, sql_instance_country_query, i)
            file_paths.append(file_path)
            context.log.info(f"chunk {i} saved to pkl: {file_path}")

        return file_paths

    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_on_history_db(context, path_do_data, sql_instance_country_query: dict):
    try:
        df = pd.read_sql_query(
            sql=sql_instance_country_query["query"],
            con=connect_to_pg_db(
                sql_instance_country_query["sql_instance_host"],
                "History_Job_" + str(sql_instance_country_query["db_name"]).upper(),
            ),
            params={
                "previous_month_start": sql_instance_country_query["previous_month_start"],
                "previous_month_end": sql_instance_country_query["previous_month_end"],
                "abroad_region": sql_instance_country_query["abroad_region"],
                "db_name": sql_instance_country_query["db_name"],
                "country_id": sql_instance_country_query["country_id"],
            },
        )

        file_path = save_pkl_files(
            context, df, path_do_data, sql_instance_country_query
        )
        return file_path
    except Exception as e:
        context.log.error(f"ERROR: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_maria_db(
        context, path_do_data, table_name, sql_instance_country_query: dict
):
    try:
        engine = conn_employer_sqlalchemy_mariadb(
            sql_instance_country_query["sql_instance_host"],
            sql_instance_country_query["db_name"],
        )
        df = pd.read_sql(
            sql=text(sql_instance_country_query["query"]),
            con=engine,
            params={
                key: val
                for key, val in sql_instance_country_query.items()
                if key.startswith("to_sqlcode")
            },
        )

        file_path = save_pkl_files(
            context, df, path_do_data, sql_instance_country_query
        )

        return file_path
    except Exception as e:
        context.log.error(f"query execution error: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_mssql_db(
        context,
        path_do_data,
        table_name,
        sql_instance_country_query: dict,
        host_name: str = None,
):
    try:
        # choose host name to connect to needed mssql db
        if host_name == "seo_server":
            engine = conn_mssql_seo_server_db(host_name, sql_instance_country_query["db_name"])
        else:
            engine = conn_mssql_db(
                sql_instance_country_query["sql_instance_host"],
                sql_instance_country_query["db_name"],
            )

        df = pd.read_sql(
            sql=text(sql_instance_country_query["query"]),
            con=engine,
            params={
                key: val
                for key, val in sql_instance_country_query.items()
                if key.startswith("to_sqlcode")
            },
        )

        file_path = save_pkl_files(
            context, df, path_do_data, sql_instance_country_query
        )

        return file_path
    except Exception as e:
        context.log.error(f"query execution error: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_mssql_soska_db(
        context, path_do_data, table_name, sql_instance_country_query: dict
):
    try:
        engine = conn_mssql_soska_db(
            sql_instance_country_query["sql_instance_host"],
            sql_instance_country_query["db_name"],
        )
        df = pd.read_sql(
            sql=text(sql_instance_country_query["query"]),
            con=engine,
            params={
                key: val
                for key, val in sql_instance_country_query.items()
                if key.startswith("to_sqlcode")
            },
        )

        file_path = save_pkl_files(
            context, df, path_do_data, sql_instance_country_query
        )

        return file_path
    except Exception as e:
        context.log.error(f"query execution error: {e}")
        raise Failure(f"ERROR: {e}")


def start_query_mssql_db_large_size(
        context, schema, table_name, sql_instance_country_query: dict, host_name: str = None
):
    try:
        # choose host name to connect to needed mssql db
        if host_name == "seo_server":
            engine = conn_mssql_seo_server_db(host_name, sql_instance_country_query["db_name"])
        else:
            engine = conn_mssql_db(
                sql_instance_country_query["sql_instance_host"],
                sql_instance_country_query["db_name"],
            )

        chunk_iter = pd.read_sql(
            sql=text(sql_instance_country_query["query"]),
            con=engine,
            params={
                key: val
                for key, val in sql_instance_country_query.items()
                if key.startswith("to_sqlcode")
            },
            chunksize=100000,
        )

        for chunk in chunk_iter:
            chunk.to_sql(
                name=table_name,
                con=dwh_conn_sqlalchemy(),
                schema=schema,
                if_exists="append",
                index=False,
            )
            context.log.info(f"chunk saved to dwh: {schema}.{table_name}")
    except Exception as e:
        context.log.error(f"query execution error: {e}")
        raise Failure(f"ERROR: {e}")
