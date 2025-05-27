import os

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
from ..utils.utils import delete_pkl_files, job_prefix

TABLE_NAME = "rpl_analyze_schema"
SCHEMA = "an"

PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
PROCEDURE_CALL = "call an.analyze_schema(%s);"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split()[-1].strip('an.')
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    root_dir="dwh_team",
    file_name='analyze_schema',
)


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def analyse_full_partitions_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on targed db using Dagster multitasting

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    delete_pkl_files(context, PATH_TO_DATA)
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
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
                                       'ddl_query': GITLAB_DDL_Q,
                                       'country_id': country_id,
                                       },
                                mapping_key='procedure_' + country
                            )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def analyse_full_partitions_launch_query_on_db(context, sql_instance_country_query: dict):
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

    schema = context.resources.globals["schema"]


    sql_instance_country_query['to_sqlcode_schema_name'] = schema
    start_query_on_rplc_db(context, sql_instance_country_query)
    context.log.info(f'Analyzed: {schema}\n--------------')


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    schema=Field(str, default_value='public'),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME + '_public',
     description=f'{SCHEMA}.{TABLE_NAME}',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "rpl",
         "procedure_name": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def analyse_public_schema():
    # start procedure on replica
    replica_instances = analyse_full_partitions_get_sqlinstance()
    replica_instances.map(analyse_full_partitions_launch_query_on_db).collect()


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    schema=Field(str, default_value='an'),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME + '_an',
     description=f'{SCHEMA}.{TABLE_NAME}',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "rpl",
         "procedure_name": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def analyse_an_schema():
    # start procedure on replica
    replica_instances = analyse_full_partitions_get_sqlinstance()
    replica_instances.map(analyse_full_partitions_launch_query_on_db).collect()

