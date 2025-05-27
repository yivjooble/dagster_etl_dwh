import sys
import gzip
import os
import pickle
from datetime import datetime
from typing import List

import pandas as pd
from dagster import (
    op,
    job,
    In,
    Out,
    Field,
    make_values_resource,
    fs_io_manager,
    DynamicOut,
    DynamicOutput,
    Failure
)

from etl_clickhouse.utils.rplc_config import clusters
from etl_clickhouse.utils.ch_config import ch_countries, job_config, retry_policy
from etl_clickhouse.utils.dbs_con import get_clickhouse_client, get_conn_to_rpl_sqlalchemy
from etl_clickhouse.utils.io_manager_path import get_io_manager_path
from etl_clickhouse.utils.utils import delete_pkl_files, job_prefix
from etl_clickhouse.utils.ch_operations import save_to_clickhouse

# Define setting
STORAGE_TABLE_NAME = "info_region"  # name of table in Clickhouse
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def info_region_create_dict_params(context):
    """
    Generate dict with params for each country.
    """
    context.log.info('===== Get data from database')

    # delete pkl files
    delete_pkl_files(context, PATH_TO_DATA)

    # get params from globals
    launch_countries = context.resources.globals["reload_countries"]

    # get sql code
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{STORAGE_TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()

    # iterate over sql instances
    for cluster in clusters.values():
        for rpl_country in cluster['dbs']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(rpl_country).lower() in str(launch_country).lower():
                    # create dict with needed params
                    yield DynamicOutput(
                        value={'country': rpl_country,
                               'host': cluster['host'],
                               'query': q,
                               },
                        mapping_key='history_' + rpl_country
                    )


@op(out=Out(str),
    retry_policy=retry_policy)
def info_region_save_data_as_pkl(context, dict_params: dict) -> str:
    """
    Save data as pkl to /data.
    """
    context.log.info('===== Save data from database as pkl to /data')
    # get params from dict
    country = dict_params['country']
    host = dict_params['host']
    query = dict_params['query']

    # get connection
    conn = get_conn_to_rpl_sqlalchemy(host=host, dbname=country)

    # execute query
    custom_date_format = datetime.now().date().strftime('%Y_%m_%d')
    try:
        df = pd.read_sql_query(sql=query,
                               con=conn)

        file_path = f"{PATH_TO_DATA}/{country}_DB_{custom_date_format}.pkl"
        with gzip.open(file_path, 'wb') as f:
            pickle.dump(df, f)

        context.log.info(
            f"Data loaded for: {country}, {custom_date_format}, {df.shape[0]},")

        return file_path
    except Exception as e:
        Failure(f"Error while loading data from database: {country}, {custom_date_format}\n {e}")
        raise e


@op(ins={"file_paths": In(List[str])},
    required_resource_keys={'globals'})
def info_region_ch_truncate_table(context, **kwargs):
    """
    Truncate table.
    """
    context.log.info('===== Delete data from table in Clickhouse')
    # get params from globals
    launch_countries = context.resources.globals["reload_countries"]

    # delete data from Clickhouse table
    for ch_country in ch_countries:
        # filter if custom countries
        for launch_country in launch_countries:
            if str(ch_country).lower() in str(launch_country).lower():

                # delete data from Clickhouse table
                try:
                    client = get_clickhouse_client(context, ch_country)
                    client.command(
                        cmd=f"alter table {ch_country}.{STORAGE_TABLE_NAME} delete where 1=1"
                    )
                    context.log.info(f"Data deleted from: {ch_country}.{STORAGE_TABLE_NAME}")

                except Exception as e:
                    Failure(f"Error while deleting data from Clickhouse: {ch_country}\n {e}")
                    sys.exit(1)


@op(required_resource_keys={'globals'})
def info_region_ch_insert_date(context, delete, file_paths):
    """
    Insert data.
    """
    context.log.info('===== Insert data to Clickhouse table')
    # get params from globals
    launch_countries = context.resources.globals["reload_countries"]

    try:
        for file_path in file_paths:
            if file_path.endswith(".pkl"):

                # check country by .pkl file name
                pkl_file_country = os.path.basename(file_path).split('_')[0]

                # filter if custom countries
                for input_country in launch_countries:
                    if str(input_country).lower() in str(pkl_file_country).lower():

                        # load .pkl file
                        with gzip.open(file_path, 'rb') as f:
                            df = pickle.load(f)

                            # insert data to Clickhouse table
                            save_to_clickhouse(context, input_country, df, STORAGE_TABLE_NAME)
    except Exception as e:
        Failure(f"Error while inserting data to Clickhouse: {e}")
        sys.exit(1)


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=ch_countries),
                                                    is_datediff=Field(bool, default_value=False)
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + STORAGE_TABLE_NAME)
def info_region_job():
    """
    Description: Load info_region table from replica database to Clickhouse
    Connections: replica database, Clickhouse
    Settings: TABLE_NAME, DELETE_DATE_COLUMN, PATH_TO_DATA
    """
    # get data from history database
    create_dict_params = info_region_create_dict_params()

    # save data from history database as pkl to /data
    pkl_files_path = create_dict_params.map(info_region_save_data_as_pkl).collect()

    # delete data from Clickhouse table
    ch_delete = info_region_ch_truncate_table(pkl_files_path)

    # insert data to Clickhouse table
    info_region_ch_insert_date(ch_delete, pkl_files_path)
