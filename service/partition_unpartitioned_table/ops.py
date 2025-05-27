from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field,
    Failure
)

from utility_hub import (
    Operations,
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)

# project import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


PROCEDURE_NAME = "partition_unpartitioned_table"
SCHEMA = "an"
PROCEDURE_CALL = "call an.partition_unpartitioned_table(%s);"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split('.')[1]
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    file_name=PROC_NAME_PARSED,
    root_dir="dwh_team",
)


US_COUNTRIES = [
    'sv', 'kr', 'mx', 'sg', 'ph', 'nz', 'my', 'za'
]
TABLES = [
    'public.session_impression',
    'public.session_impression_on_screen',
    'public.session_impression_recommend',
    'public.session_impression_recommend_on_screen',
    'public.session_account',
    'public.session_action',
    'public.session_adsense_version',
    'public.session_alertview',
    'public.session_alertview_action',
    'public.session_alertview_message',
    'public.session_apply',
    'public.session_apply_action',
    'public.session_auth',
    'public.session_away',
    'public.session_away_message',
    'public.session_banner_action',
    'public.session_external',
    'public.session_feature',
    'public.session_feature_action',
    'public.session_filter_action',
    'public.session_jdp',
    'public.session_jdp_action',
    'public.session_jdp_message',
    'public.session_recommend',
    'public.session_search',
    'public.session_search_message',
    'public.session_test',
    'public.session_test_agg',
    'public.session_utm'
]
job_config_us = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 7,
            }
        }
    }
}


@op(out=DynamicOut(),  required_resource_keys={'globals'})
def partition_unpartitioned_table_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on target db using Dagster multitasking

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f"DDL run on replica:\n{GITLAB_DDL_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def partition_unpartitioned_table_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    DbOperations.create_procedure(context, sql_instance_country_query)
    try:
        tables_to_partition = context.resources.globals["tables_to_partition"]

        for table in tables_to_partition:
            sql_instance_country_query['to_sqlcode_table_name'] = table
            DbOperations.call_procedure(context, sql_instance_country_query)
            context.log.info(f'Partitioned: {table}\n--------------')
    except Exception:
        raise Failure()


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   tables_to_partition=Field(list, default_value=['public.session'])),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(PROCEDURE_NAME),
    tags={"service": "partition_unpartitioned_table"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "rpl",
    },
    description=f'Partition un-partitioned tables on replica.'
)
def partition_unpartitioned_table_job():
    # start procedure on replica
    instances = partition_unpartitioned_table_get_sqlinstance()
    instances.map(partition_unpartitioned_table_query_on_db)


@job(
    config=job_config_us,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=US_COUNTRIES),
                                                   tables_to_partition=Field(list, default_value=TABLES)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(PROCEDURE_NAME, "_us_cluster"),
    tags={"service": "partition_unpartitioned_table"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "rpl",
    },
    description=f'Partition un-partitioned tables on replica. US cluster.'
)
def partition_unpartitioned_table_us_job():
    # start procedure on replica
    instances = partition_unpartitioned_table_get_sqlinstance()
    instances.map(partition_unpartitioned_table_query_on_db)
