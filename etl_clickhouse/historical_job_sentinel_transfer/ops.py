import os
import glob
import dask.dataframe as dd

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

from etl_clickhouse.utils.ch_config import job_config
from etl_clickhouse.utils.dbs_con import get_str_conn_to_rpl_sqlalchemy
from etl_clickhouse.utils.rplc_db_operations import execute_on_rpl
from etl_clickhouse.utils.io_manager_path import get_io_manager_path
from etl_clickhouse.utils.utils import job_prefix
from etl_clickhouse.utils.ch_operations import save_to_clickhouse

# Define setting
RPL_HOST = '192.168.1.207'
RPL_DB = 'us'
RPL_SCHEMA = 'an'
RPL_TABLE_NAME = 'job_sentinel_historical_data'
RPL_PROCEDURE = 'call an.prc_job_sentinel_historical_data();'
CH_SCHEMA = 'us'
CH_TABLE_NAME = "historical_job_sentinel_transfer"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
CUSTOM_COUNTRIES_LIST = ['us']


def delete_parquet_files(context, path_to_data):
    files = glob.glob(path_to_data + '/*')
    for f in files:
        os.remove(f)
    context.log.info('deleted .parquet files')


@op(required_resource_keys={'globals'})
def historical_job_sentinel_transfer_save_data_as_parquet(context):
    """
    Save data.
    """
    from dask.distributed import Client
    # Set workers and threads
    n_workers = context.resources.globals["n_workers"]
    threads_per_worker = context.resources.globals["threads_per_worker"]
    npartitions = context.resources.globals["npartitions"]
    Client(n_workers=n_workers, threads_per_worker=threads_per_worker)

    try:
        execute_on_rpl(
            sql=RPL_PROCEDURE,
            host=RPL_HOST,
            dbname=RPL_DB
        )
        context.log.info(f'Procedure: {RPL_PROCEDURE}\nDone!')

        con = get_str_conn_to_rpl_sqlalchemy(host=RPL_HOST, dbname=RPL_DB)

        # Dask parallel processing
        df = dd.read_sql_table(
            schema=RPL_SCHEMA,
            table_name=RPL_TABLE_NAME,
            con=con,
            index_col='id_job',
            npartitions=npartitions,
            bytes_per_chunk="256 MiB",
        )

        df = df.reset_index()
        df.to_parquet(PATH_TO_DATA)

    except Exception as e:
        Failure(f"Error while loading: {RPL_HOST}\n {e}")
        raise e


@op(out=DynamicOut())
def historical_job_sentinel_transfer_read_parquet(context, parquets):
    for filename in os.listdir(PATH_TO_DATA):
        if filename.endswith('.parquet'):
            yield DynamicOutput(
                value={'filename': filename},
                mapping_key='ch_' + filename.replace('.', '_')
            )


@op
def historical_job_sentinel_transfer_save_parquet_to_ch(context, params: dict):
    filename = params['filename']
    file_path = os.path.join(PATH_TO_DATA, filename)

    # Size of each parquet file
    context.log.info(f"Size of {filename}: {os.path.getsize(file_path) / 1024 / 1024:.2f} MB")

    df = pd.read_parquet(file_path)

    # Cast all columns to str
    df = df.astype(str)

    # insert data to Clickhouse table
    save_to_clickhouse(context,
                       db_name=CH_SCHEMA,
                       table_name=CH_TABLE_NAME,
                       df=df)


@op
def historical_job_sentinel_transfer_drop_useless(context, save_parquet):
    delete_parquet_files(context, PATH_TO_DATA)

    # Drop replica table
    execute_on_rpl(
        sql="drop table if exists an.job_sentinel_historical_data;",
        host=RPL_HOST,
        dbname=RPL_DB
    )
    context.log.debug('Replica physical table was dropped.')


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRIES_LIST),
                                                    n_workers=Field(int, default_value=12),
                                                    threads_per_worker=Field(int, default_value=6),
                                                    npartitions=Field(int, default_value=1000)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + CH_TABLE_NAME,
     description='Copy data from postgres_replica to clickhouse db.')
def historical_job_sentinel_transfer_job():
    """
    Description: Load historical job_sentinel table from replica database to Clickhouse
    Connects to: replica database, Clickhouse
    """
    # get data from history database
    to_parquet = historical_job_sentinel_transfer_save_data_as_parquet()
    # save data from history database as parquet
    read_parquet = historical_job_sentinel_transfer_read_parquet(to_parquet)
    save_parquet = read_parquet.map(historical_job_sentinel_transfer_save_parquet_to_ch).collect()
    historical_job_sentinel_transfer_drop_useless(save_parquet)
