from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field,
)

# module import
from utility_hub import (
    Operations,
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "apply_conversion_service"
SCHEMA = "an"

PROCEDURE_CALL = "call an.prc_apply_conversion_service();"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split('.')[1]

GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=PROC_NAME_PARSED,
    file_name=PROC_NAME_PARSED,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def apply_conversion_service_get_sqlinstance(context):
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


@op(retry_policy=retry_policy)
def apply_conversion_service_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    DbOperations.call_procedure(context, sql_instance_country_query)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME, '_to_replica'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "rpl",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True",
        "triggered_by_job": "internal__apply_conversion_service"
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def apply_conversion_service_job():
    instances = apply_conversion_service_get_sqlinstance()
    instances.map(apply_conversion_service_query_on_db)
