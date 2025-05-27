from copy import deepcopy
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
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "project_conversions_daily"
SCHEMA = "aggregation"
DATE_DIFF_COLUMN = "session_date"
COUNTRY_COLUMN = "country_id"

# reload -2-1
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime("%Y-%m-%d")
BEFORE_YESTERDAY_DATE = (datetime.now().date() - timedelta(2)).strftime("%Y-%m-%d")
# reload -7-3
THIRD_DATE = (datetime.now().date() - timedelta(3)).strftime("%Y-%m-%d")
SEVENTH_DATE = (datetime.now().date() - timedelta(7)).strftime("%Y-%m-%d")

PROCEDURE_CALL = "call an.prc_project_conversions_daily(%s);"
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


@op(out=DynamicOut(), required_resource_keys={'globals'})
def project_conversions_daily_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.

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


@op(retry_policy=retry_policy)
def project_conversions_daily_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    country_id = sql_instance_country_query['country_id']

    for date in sql_instance_country_query['date_range']:
        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)

        operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
        operation_date = date.strftime('%Y-%m-%d')
        context.log.info(f"--> Starting sql-script on: {date.strftime('%Y-%m-%d')}")

        local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff

        DbOperations.call_procedure(context, local_sql_instance_country_query)

        local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query,
            country_column=COUNTRY_COLUMN,
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            continue

        DwhOperations.delete_data_from_dwh_table(context=context,
                                                 schema=SCHEMA,
                                                 table_name=TABLE_NAME,
                                                 country_column=COUNTRY_COLUMN,
                                                 date_column=DATE_DIFF_COLUMN,
                                                 date_start=operation_date,
                                                 country=country_id)

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)


@job(
    config=repstat_job_config,
    resource_defs={
        "globals": make_values_resource(
            reload_countries=Field(list, default_value=all_countries_list),
            reload_date_end=Field(str, default_value=YESTERDAY_DATE),
            reload_date_start=Field(str, default_value=BEFORE_YESTERDAY_DATE),
            is_datediff=Field(bool, default_value=False),
        ),
        "io_manager": fs_io_manager.configured(
            {"base_dir": f"{get_io_manager_path()}"}
        ),
    },
    name=generate_job_name(TABLE_NAME, '_last_two_days'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{BEFORE_YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def project_conversions_daily_reload_two_days_job():
    instances = project_conversions_daily_get_sqlinstance()
    instances.map(project_conversions_daily_query_on_db).collect()


@job(
    config=repstat_job_config,
    resource_defs={
        "globals": make_values_resource(
            reload_countries=Field(list, default_value=all_countries_list),
            reload_date_end=Field(str, default_value=THIRD_DATE),
            reload_date_start=Field(str, default_value=SEVENTH_DATE),
            is_datediff=Field(bool, default_value=False),
        ),
        "io_manager": fs_io_manager.configured(
            {"base_dir": f"{get_io_manager_path()}"}
        ),
    },
    name=generate_job_name(TABLE_NAME, '_last_five_days'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{SEVENTH_DATE} - {THIRD_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def project_conversions_daily_reload_five_days_job():
    instances = project_conversions_daily_get_sqlinstance()
    instances.map(project_conversions_daily_query_on_db).collect()
