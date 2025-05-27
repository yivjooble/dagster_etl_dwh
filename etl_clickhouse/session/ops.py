import gzip
import os
import pickle
from datetime import datetime, timedelta

import pandas as pd
from dagster import (
    op,
    job,
    Field,
    make_values_resource,
    fs_io_manager,
    DynamicOut,
    DynamicOutput,
    Failure
)

from etl_clickhouse.utils.date_format_settings import get_datediff
from etl_clickhouse.utils.ch_config import ch_countries, job_config, retry_policy
from etl_clickhouse.utils.dbs_con import get_conn_to_rpl_sqlalchemy, get_clickhouse_client
from etl_clickhouse.utils.rplc_config import clusters
from etl_clickhouse.utils.io_manager_path import get_io_manager_path
from etl_clickhouse.utils.utils import delete_pkl_files, job_prefix
from etl_clickhouse.utils.ch_operations import save_to_clickhouse

# Define dates
DATE_START = (datetime.now().date() - timedelta(2)).strftime('%Y-%m-%d')
DATE_END = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

# Define setting
STORAGE_TABLE_NAME = "session"  # name of table in Clickhouse
DELETE_DATE_COLUMN = "date_diff"  # timestamp, cast to date
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def session_create_dict_params(context):
    """
    Generate dict with params for each country.
    """
    context.log.info('===== Get data from database')

    # delete pkl files
    delete_pkl_files(context, PATH_TO_DATA)

    # get params from globals
    launch_countries = context.resources.globals["reload_countries"]
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]),
                               pd.to_datetime(context.resources.globals["reload_date_end"]))

    # get sql code
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 os.path.join("sql", f"{STORAGE_TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()

    # iterate over sql instances
    for cluster in clusters.values():
        for country_name in cluster['dbs']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(country_name).lower() in str(launch_country).lower():
                    # create dict with needed params
                    yield DynamicOutput(
                        value={'country': country_name,
                               'host': cluster['host'],
                               'query': q,
                               'date_range': date_range,
                               },
                        mapping_key='history_' + country_name
                    )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def session_save_data_as_pkl(context, dict_params: dict):
    """
    Save data as pkl to /data.
    """
    context.log.info('===== Save data from database as pkl to /data')
    # get params from dict
    country_name = dict_params['country']
    host = dict_params['host']
    query = dict_params['query']
    date_range = dict_params['date_range']

    # create connection
    conn = get_conn_to_rpl_sqlalchemy(host=host, dbname=country_name)

    # execute
    for raw_date in date_range:
        date = raw_date.strftime('%Y-%m-%d')
        date_diff = get_datediff(raw_date.strftime('%Y-%m-%d'))
        custom_date_format = raw_date.strftime('%Y_%m_%d')
        try:
            df = pd.read_sql_query(sql=query,
                                   con=conn,
                                   params={'date': date_diff})

            file_path = f"{PATH_TO_DATA}/{country_name}_DB_{custom_date_format}.pkl"
            with gzip.open(file_path, 'wb') as f:
                pickle.dump(df, f)

            context.log.info(f"> Data loaded for: {country_name.upper()}, {date}, {df.shape[0]}")

            # insert data to Clickhouse table
            if file_path.endswith(".pkl"):
                # load .pkl file
                with gzip.open(file_path, 'rb') as f:
                    df = pickle.load(f)

                    if not df.empty:

                        # delete data from Clickhouse table
                        client = get_clickhouse_client(context, country_name)
                        client.command(
                            cmd=f"alter table {country_name}.{STORAGE_TABLE_NAME} "
                                f"delete where {DELETE_DATE_COLUMN} between {date_diff} and {date_diff}"
                        )
                        context.log.info(f'Data deleted for: {STORAGE_TABLE_NAME}, {country_name.upper()}, '
                                         f'{date}, {date}')

                        # insert data to Clickhouse table
                        save_to_clickhouse(context, country_name, df, STORAGE_TABLE_NAME)
                        context.log.info('---------------------------------')
                    else:
                        context.log.info(f'Empty df for: {country_name}, {date}')

        except Exception as e:
            Failure(f"Error while loading: {country_name}, {date}\n {e}")
            raise e


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=ch_countries),
                                                    reload_date_start=Field(str, default_value=DATE_START),
                                                    reload_date_end=Field(str, default_value=DATE_END),
                                                    is_datediff=Field(bool, default_value=False)
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + STORAGE_TABLE_NAME)
def session_job():
    """
    Description: Load session table from replica database to Clickhouse
    Connects to: replica database, Clickhouse
    Settings: TABLE_NAME, DELETE_DATE_COLUMN, PATH_TO_DATA, DATE_START, DATE_END
    """
    # get data from history database
    create_dict_params = session_create_dict_params()

    # save data from history database as pkl to /data
    create_dict_params.map(session_save_data_as_pkl).collect()
