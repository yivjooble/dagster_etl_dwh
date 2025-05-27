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
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "registration_email_funnel"
SCHEMA = "aggregation"

PROCEDURE_CALL = "call an.prc_registration_email_funnel(%s);"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split('.')[1]

GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=PROC_NAME_PARSED,
    file_name=PROC_NAME_PARSED,
)
GITLAB_SELECT_Q, GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=PROC_NAME_PARSED,
    file_name='create_rpl_df',
)

CUSTOM_COUNTRY_LIST = ['de', 'uk', 'fr', 'ca', 'us', 'pl', 'es', 'at', 'be', 'ch', 'nl', 'it', 'se']


@op(out=DynamicOut(), required_resource_keys={'globals'})
def registration_email_funnel_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on targed db using Dagster multitasting

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
            ddl_query=GITLAB_DDL_Q,
            select_query=GITLAB_SELECT_Q,):
        yield sql_instance


@op(retry_policy=retry_policy)
def registration_email_funnel_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    country_id = sql_instance_country_query['country_id']
    country_code = sql_instance_country_query['db_name']

    # Get max session_datediff for country
    max_datediff_query = """SELECT MAX(session_datediff) AS session_datediff
                            FROM aggregation.registration_email_funnel
                            WHERE is_complete_data = 1
                            AND country_id = %s;"""

    operation_date_diff = DwhOperations.execute_on_dwh(
        context=context,
        query=max_datediff_query,
        params=(country_id,),
        fetch_results=True
    )

    context.log.info(f"--> Starting sql-script on: {country_code}")

    sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff[0]['session_datediff']

    DbOperations.call_procedure(context, sql_instance_country_query)

    # Generator for retrieving chunks
    chunk_generator = DbOperations.execute_query_and_return_chunks(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
    )

    # Check for the presence of data
    first_chunk = next(chunk_generator, None)
    if first_chunk is None:
        return

    # Delete data from dwh table
    deletion_query = """
        DELETE FROM aggregation.registration_email_funnel
        WHERE is_complete_data = 0 AND country_id = %s;
    """
    DwhOperations.execute_on_dwh(context=context, query=deletion_query, params=(country_id,))
    context.log.debug(f'Data deleted successfully. Country: {country_code}')

    # Save the first chunk
    DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

    # Save the remaining chunks
    for chunk in chunk_generator:
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRY_LIST),),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def registration_email_funnel_job():
    instances = registration_email_funnel_get_sqlinstance()
    instances.map(registration_email_funnel_query_on_db)
