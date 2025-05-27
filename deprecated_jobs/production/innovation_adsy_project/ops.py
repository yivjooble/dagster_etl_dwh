import os
from datetime import datetime, timedelta
from copy import deepcopy

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field,
)

# project import
from ..utils.io_manager_path import get_io_manager_path

# module import
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, get_datediff, generate_job_name


TABLE_NAME = "innovation_adsy_project"
SCHEMA = "aggregation"
COUNTRY_COLUMN = "country_id"
DATE_COLUMN = "session_date"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)
CUSTOM_COUNTRY_LIST = ['us']


@op(out=DynamicOut(), required_resource_keys={'globals'})
def innovation_adsy_project_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on a separate instance.
    Args:
        context: The context object.
    Output:
        A generator that yields a DynamicOutput object.
    """
    # delete previously stored data
    Operations.delete_files(context, PATH_TO_DATA)

    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context, instance_type="prod", query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def innovation_adsy_project_query_on_db(context, sql_instance_country_query: dict):
    """
    Save DataFrame to the data warehouse table.
    Args:
        context (object): The context object containing resources and information about the execution.
        file_paths (list): A list of file paths.
    Raises:
        Exception: If there is an error while saving to the DWH.
    Returns:
        None
    """
    try:
        country_code = sql_instance_country_query["country_code"]
        country_id = sql_instance_country_query["country_id"]

        for date in sql_instance_country_query['date_range']:
            # create local copy of dict
            local_sql_instance_country_query = deepcopy(sql_instance_country_query)

            operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
            operation_date = date.strftime('%Y-%m-%d')

            local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff

            file_paths = DbOperations.execute_query_and_save_to_parquet(
                context=context,
                path_to_data=PATH_TO_DATA,
                sql_instance_country_query=local_sql_instance_country_query,
                country_column=COUNTRY_COLUMN
            )

            if not file_paths:
                context.log.info(f'No data found for: {sql_instance_country_query["db_name"]}')
            else:
                DwhOperations.delete_data_from_dwh_table(context=context,
                                                         schema=SCHEMA,
                                                         table_name=TABLE_NAME,
                                                         date_column=DATE_COLUMN,
                                                         date_start=operation_date,
                                                         country_column=COUNTRY_COLUMN,
                                                         country=country_id
                                                         )

                for file_path in file_paths:
                    DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, file_path=file_path)

                context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRY_LIST),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def innovation_adsy_project_job():
    instances = innovation_adsy_project_get_sqlinstance()
    instances.map(innovation_adsy_project_query_on_db)
