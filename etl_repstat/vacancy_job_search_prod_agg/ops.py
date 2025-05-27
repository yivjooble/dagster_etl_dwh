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
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "vacancy_job_search_prod_agg"
SCHEMA = "aggregation"
DATE_COLUMN = "year_month"
COUNTRY_COLUMN = "country_id"

FIRST_DAY_OF_MONTH = ((datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d"))
LAST_DAY_OF_MONTH = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
YEAR_MONTH = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y%m")

PROCEDURE_CALL = "call an.prc_vacancy_job_search_prod_agg(%s, %s, %s, %s, %s);"
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

CUSTOM_COUNTRY_LIST = ['uk', 'fr', 'at', 'de', 'us', 'pl', 'ch', 'be', 'nl', 'ro', 'hu', 'ca', 'rs']


@op(out=DynamicOut(), required_resource_keys={'globals'})
def vacancy_job_search_prod_agg_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    '''
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                     f"DDL run on replica:\n{GITLAB_DDL_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            select_query=GITLAB_SELECT_Q,):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def vacancy_job_search_prod_agg_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    try:
        country_id = sql_instance_country_query['country_id']
        destination_db = context.resources.globals["destination_db"]
        operation_datediff_start = get_datediff(context.resources.globals["reload_date_start"])
        operation_datediff_end = get_datediff(context.resources.globals["reload_date_end"])
        operation_year_month = context.resources.globals["reload_year_month"]

        sql_instance_country_query['to_sqlcode_date_start'] = context.resources.globals["reload_date_start"]
        sql_instance_country_query['to_sqlcode_date_end'] = context.resources.globals["reload_date_end"]
        sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_datediff_start
        sql_instance_country_query['to_sqlcode_date_or_datediff_end'] = operation_datediff_end
        sql_instance_country_query['to_sqlcode_year_month'] = operation_year_month

        context.log.info(f"--> Starting sql-script on: {operation_year_month}")

        DbOperations.call_procedure(context, sql_instance_country_query)

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query,
            country_column=COUNTRY_COLUMN,
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        DwhOperations.delete_data_from_dwh_table(context=context,
                                                 schema=SCHEMA,
                                                 table_name=TABLE_NAME,
                                                 country_column=COUNTRY_COLUMN,
                                                 date_column=DATE_COLUMN,
                                                 date_start=operation_year_month,
                                                 country=country_id,
                                                 destination_db=destination_db)

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk, destination_db=destination_db)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk, destination_db=destination_db)

    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRY_LIST),
                                                   reload_date_start=Field(str, default_value=FIRST_DAY_OF_MONTH),
                                                   reload_date_end=Field(str, default_value=LAST_DAY_OF_MONTH),
                                                   reload_year_month=Field(str, default_value=YEAR_MONTH),
                                                   destination_db=Field(str, default_value="both"),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{FIRST_DAY_OF_MONTH} - {LAST_DAY_OF_MONTH}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def vacancy_job_search_prod_agg_job():
    instances = vacancy_job_search_prod_agg_get_sqlinstance()
    instances.map(vacancy_job_search_prod_agg_query_on_db)
