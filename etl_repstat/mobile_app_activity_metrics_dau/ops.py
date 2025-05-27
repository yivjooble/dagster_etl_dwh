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


# Define constants
TABLE_NAME = "activity_metrics"
SCHEMA = "mobile_app"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country_id"

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = "call an.prc_mobile_app_activity_metrics_dau(%s);"
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
def activity_metrics_dau_get_sql_instance(context):
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
def activity_metrics_dau_query_on_db(context, sql_instance_country_query: dict):
    """
    Start procedure on rpl with input data.

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

        operation_date = date.strftime('%Y-%m-%d')
        operation_date_diff = get_datediff(operation_date)
        context.log.info(f"--> Starting sql-script on: {operation_date}")

        local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff

        DbOperations.call_procedure(context, local_sql_instance_country_query)

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            continue

        query = """DELETE
                   FROM mobile_app.activity_metrics
                   WHERE date = %s
                   AND country_id = %s
                   AND metric_type = 'DAU';"""

        DwhOperations.execute_on_dwh(
            context=context,
            query=query,
            params=(operation_date, country_id)
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(table_name=TABLE_NAME, additional_prefix="mobile_app_", additional_suffix="_dau"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description="""Job to process and store daily active user metrics for the mobile app.
                   Date range: yesterday.
                   Countries: all.""",
)
def mobile_app_activity_metrics_dau_job():
    instances = activity_metrics_dau_get_sql_instance()
    instances.map(activity_metrics_dau_query_on_db).collect()
