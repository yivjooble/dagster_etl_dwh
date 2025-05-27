import os
import pandas as pd

from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput,
)

from utility_hub.core_tools import fetch_gitlab_data
# project import
from ..utils.io_manager_path import get_io_manager_path
# module import
from ..utils.rplc_job_config import retry_policy, job_config
from ..utils.rplc_config import clusters, map_country_code_to_id, all_countries_list
from ..utils.rplc_db_operations import start_query_on_rplc_db, run_ddl_replica
from ..utils.date_format_settings import get_datediff
from ..utils.utils import delete_pkl_files, job_prefix

TABLE_NAME = "prc_analyse_partitions"
SCHEMA = "an"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
CURRENT_DATE = datetime.now().date().strftime('%Y-%m-%d')
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
PROCEDURE_CALL = "call an.prc_analyse_partitions(%s);"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split()[-1].strip('an.')
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    root_dir="dwh_team",
    file_name=PROC_NAME_PARSED,
)


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def analyse_partitions_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on targed db using Dagster multitasting

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    delete_pkl_files(context, PATH_TO_DATA)
    launch_countries = context.resources.globals["reload_countries"]
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]),
                               pd.to_datetime(context.resources.globals["reload_date_end"]))

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: [{date_range}]\n'
                     f"DDL run on replica:\n{GITLAB_DDL_URL}")

    # iterate over sql instances
    for cluster_info in clusters.values():
        for country in cluster_info['dbs']:
            for launch_country in launch_countries:
                if str(country).lower() in str(launch_country).strip('_').lower():
                    for country_name, country_id in map_country_code_to_id.items():
                        if str(launch_country).strip('_').lower() in country_name:
                            yield DynamicOutput(
                                value={'sql_instance_host': cluster_info['host'],
                                       'country_db': str(country).lower().strip(),
                                       'query': PROCEDURE_CALL,
                                       'date_range': date_range,
                                       'ddl_query': GITLAB_DDL_Q,
                                       'country_id': country_id
                                       },
                                mapping_key='procedure_' + country
                            )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def analyse_partitions_launch_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    params = {
        'country_db': sql_instance_country_query['country_db'],
        'sql_instance_host': sql_instance_country_query['sql_instance_host'],
        'ddl_query': sql_instance_country_query['ddl_query'],
    }
    run_ddl_replica(context, params)
    context.log.info(f'DDL run: {sql_instance_country_query["country_db"]}')

    for date in sql_instance_country_query['date_range']:
        operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))

        sql_instance_country_query['to_sqlcode_date_int'] = operation_date_diff
        sql_instance_country_query['run_date'] = date.strftime('%Y-%m-%d')
        start_query_on_rplc_db(context, sql_instance_country_query)


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{YESTERDAY_DATE}",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "rpl",
         "procedure_name": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def analyse_partitions_job():
    # start procedure on replica
    replica_instances = analyse_partitions_get_sqlinstance()
    replica_instances.map(analyse_partitions_launch_query_on_db).collect()


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=CURRENT_DATE),
                                                    reload_date_end=Field(str, default_value=CURRENT_DATE),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME + '_current_date',
     description=f'{SCHEMA}.{TABLE_NAME}',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{CURRENT_DATE}",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "rpl",
         "procedure_name": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def analyse_partitions_job_current_date():
    # start procedure on replica
    replica_instances = analyse_partitions_get_sqlinstance()
    replica_instances.map(analyse_partitions_launch_query_on_db).collect()
