import os
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

from etl_clickhouse.utils.ch_config import job_config, retry_policy, ch_countries
from etl_clickhouse.utils.dbs_con import get_clickhouse_client
from etl_clickhouse.utils.rplc_config import clusters
from etl_clickhouse.utils.io_manager_path import get_io_manager_path
from etl_clickhouse.utils.utils import delete_pkl_files, job_prefix
from etl_clickhouse.pr_project.pr_tmp_raw.sql.pr_tmp_raw import generate_aggregation_query_to_run

# Define dates
DATE_START = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
DATE_END = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

# Define setting
STORAGE_TABLE_NAME = "pr_tmp_raw"  # name of table in Clickhouse
DELETE_DATE_COLUMN = "date_created"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def pr_tmp_raw_create_dict_params(context):
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
                               'date_range': date_range,
                               },
                        mapping_key='history_' + country_name
                    )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def pr_tmp_raw_save_data_as_pkl(context, dict_params: dict):
    """
    Save data as pkl to /data.
    """
    # get params from dict
    country_name = dict_params['country']
    date_range = dict_params['date_range']

    # create connection
    client = get_clickhouse_client(context, country_name)

    # execute
    for raw_date in date_range:
        date = raw_date.strftime('%Y-%m-%d')
        try:
            client.command(
                cmd=f"alter table {country_name}.{STORAGE_TABLE_NAME} delete "
                    f"where {DELETE_DATE_COLUMN} between toDate('{date}') and toDate('{date}')"
            )
            context.log.info(f'Data deleted for: {country_name}.{STORAGE_TABLE_NAME}, {date}, {date}')

            query_to_insert = generate_aggregation_query_to_run(date_from=date,
                                                                date_to=date,
                                                                db=country_name)
            # execute query
            client.command(cmd=query_to_insert)
        except Exception as e:
            Failure(f"Error while loading: {country_name}.{STORAGE_TABLE_NAME}, {date}\n {e}")
            raise e


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=DATE_START),
                                                    reload_date_end=Field(str, default_value=DATE_END),
                                                    reload_countries=Field(list, default_value=ch_countries),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + "job_cnt_pre_agg")
def pr_tmp_raw_job():
    """
    Description: Aggregate raw data for pr-project.
    Connects to: Clickhouse
    """
    create_dict_params = pr_tmp_raw_create_dict_params()
    create_dict_params.map(pr_tmp_raw_save_data_as_pkl)
