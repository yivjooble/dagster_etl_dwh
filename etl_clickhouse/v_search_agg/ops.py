import gzip
import os
import pickle
from datetime import datetime, timedelta
from sqlalchemy import create_engine

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
from etl_clickhouse.utils.ch_config import job_config, retry_policy
from etl_clickhouse.utils.dbs_con import get_dwh_conn_sqlalchemy, get_clickhouse_client
from etl_clickhouse.utils.io_manager_path import get_io_manager_path
from etl_clickhouse.utils.utils import delete_pkl_files, job_prefix
from etl_clickhouse.utils.ch_operations import save_to_clickhouse
from utility_hub import DwhOperations
from utility_hub.core_tools import fetch_gitlab_data

# Define dates
DATE_START = (datetime.now().date() - timedelta(7)).strftime('%Y-%m-%d')
DATE_END = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

# Define setting
SCHEMA = "dwh"
TABLE_NAME = "v_search_agg"  # name of table in Clickhouse
DELETE_DATE_COLUMN = "load_datediff"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()

# Define gitlab links
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key='clickhouse',
    dir_name=TABLE_NAME,
    file_name='f_' + TABLE_NAME,
    ref='main'
)
GITLAB_SELECT_Q, GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key='clickhouse',
    dir_name=TABLE_NAME,
    file_name=TABLE_NAME,
    ref='main'
)


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def v_search_agg_create_dict_params(context):
    """
    Generate dict with params for each country.
    """
    context.log.info('===== Get data from database')

    # delete pkl files
    delete_pkl_files(context, PATH_TO_DATA)

    # Create or replace function on dwh
    DwhOperations.execute_on_dwh(
        context=context,
        ddl_query=GITLAB_DDL_Q,
        destination_db="cloudberry"
    )
    context.log.info(f"Gitlab DDL-query link:\n{GITLAB_DDL_URL}\n"
                     f"Re-created function on DWH")

    # Define select query from dwh
    context.log.info(f"Gitlab SELECT-query link:\n{GITLAB_SELECT_Q_URL}")
    query = GITLAB_SELECT_Q

    # get params from globals
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]),
                               pd.to_datetime(context.resources.globals["reload_date_end"]))

    # create dict with needed params
    yield DynamicOutput(
        value={
            'date_range': date_range,
            'query': query
        },
        mapping_key='dwh'
    )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def v_search_agg_save_data_as_pkl(context, dict_params: dict):
    """
    Save data as pkl to /data.
    """

    # Create connection to cloudberry
    def _get_dwh_conn_sqlalchemy():
        user = os.environ.get('DWH_USER')
        password = os.environ.get('DWH_PASSWORD')
        host = "an-dwh.jooble.com"
        dbname = "an_dwh"
        return create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")

    # get params from dict
    date_range = dict_params['date_range']
    query = dict_params['query']

    dwh_conn = _get_dwh_conn_sqlalchemy()
    try:
        for raw_date in date_range:
            date = raw_date.strftime('%Y-%m-%d')
            date_diff = get_datediff(date)
            custom_date_format = raw_date.strftime('%Y_%m_%d')

            context.log.info(f"Load data from DWH for date: {date}")

            df = pd.read_sql_query(sql=query,
                                   con=dwh_conn,
                                   params={'date': date})

            file_path = f"{PATH_TO_DATA}/dwh_db_{custom_date_format}.pkl"
            with gzip.open(file_path, 'wb') as f:
                pickle.dump(df, f)

            context.log.info(f"> Data loaded for: {date}, {df.shape[0]}")

            if file_path.endswith(".pkl"):
                with gzip.open(file_path, 'rb') as f:
                    df = pickle.load(f)

                    if not df.empty:
                        # delete data from Clickhouse table
                        client = get_clickhouse_client(context, SCHEMA)
                        client.command(
                            cmd=f"alter table {SCHEMA}.{TABLE_NAME} delete "
                                f"where {DELETE_DATE_COLUMN} between {date_diff} and {date_diff}"
                        )
                        context.log.info(f'Data deleted from: {SCHEMA}.{TABLE_NAME}, {date}, {date}')

                        # insert data to Clickhouse table
                        save_to_clickhouse(context=context,
                                           db_name=SCHEMA,
                                           df=df,
                                           table_name=TABLE_NAME)
                        context.log.info('---------------------------------')

                    else:
                        context.log.info(f'Empty df for: {SCHEMA}.{TABLE_NAME}, {date}')

    except Exception as e:
        Failure(f"Error while loading: {SCHEMA}.{TABLE_NAME}\n {e}")
        raise e
    finally:
        delete_pkl_files(context, PATH_TO_DATA)


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=DATE_START),
                                                    reload_date_end=Field(str, default_value=DATE_END),
                                                    is_datediff=Field(bool, default_value=False)
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     metadata={
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "gitlab_select_url": f"{GITLAB_SELECT_Q_URL}",
     },
     )
def v_search_agg_job():
    """
    Description: Load jdp_away_clicks_agg table from replica database to Clickhouse
    Connects to: replica database, Clickhouse
    Settings: TABLE_NAME, DELETE_DATE_COLUMN, PATH_TO_DATA, DATE_START, DATE_END
    """
    # get data from history database
    create_dict_params = v_search_agg_create_dict_params()
    # save data from history database as pkl to /data
    create_dict_params.map(v_search_agg_save_data_as_pkl).collect()
