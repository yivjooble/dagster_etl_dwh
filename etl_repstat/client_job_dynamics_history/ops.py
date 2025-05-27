from datetime import datetime, timedelta

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
    DwhOperations,
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import (
    fetch_gitlab_data,
    generate_job_name,
)
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "client_job_dynamics"
SCHEMA = "traffic"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country_id"

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = "call an.prc_client_job_dynamics_h();"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split('.')[1]

GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name='prc_client_job_dynamics',
    file_name='prc_client_job_dynamics_h',
)
GITLAB_SELECT_Q, GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name='prc_client_job_dynamics',
    file_name='create_rpl_df',
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def client_job_dynamics_h_get_sql_instance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    '''
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f"DDL run on replica:\n{GITLAB_DDL_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            select_query=GITLAB_SELECT_Q,):
        yield sql_instance


@op(retry_policy=retry_policy)
def client_job_dynamics_h_query_on_db(context, sql_instance_country_query: dict):
    """
    Start procedure on rpl with input data.

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    DbOperations.call_procedure(context, sql_instance_country_query)

    # Generator for retrieving chunks
    chunk_generator = DbOperations.execute_query_and_return_chunks(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
        country_column=COUNTRY_COLUMN
    )

    # Check for the presence of data
    first_chunk = next(chunk_generator, None)
    if first_chunk is None:
        return

    # Save the first chunk
    DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

    # Save the remaining chunks
    for chunk in chunk_generator:
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(table_name=TABLE_NAME, additional_suffix="_h"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def client_job_dynamics_h_job():
    instances = client_job_dynamics_h_get_sql_instance()
    instances.map(client_job_dynamics_h_query_on_db)
