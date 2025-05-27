from copy import deepcopy
from datetime import datetime, timedelta

from dagster import DynamicOut, fs_io_manager, job, op, make_values_resource, Field
import pandas as pd

# module import
from etl_jooble_internal.utils.io_manager_path import get_io_manager_path
from utility_hub import (
    DwhOperations,
    Operations,
    DbOperations,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "apply_conversion_service"
SCHEMA = "imp_statistic"
DATE_COLUMN = "date_created"

TODAY_DATE = datetime.now().strftime("%Y-%m-%d")
YESTERDAY_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={"globals"})
def apply_conversion_service_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    context.log.info('Getting SQL instances...\n'
                     f"I> Date range: {context.resources.globals['reload_date_start']} - {context.resources.globals['reload_date_end']}\n"
                     f"I> Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="conversions_service_info",
            db_name="conversions-service",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy)
def apply_conversion_service_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    df_combined = pd.DataFrame()

    for date in sql_instance_country_query['date_range']:
        date_start_time = date.strftime("%Y-%m-%d 00:00:00.000000")
        date_end_time = date.strftime("%Y-%m-%d 23:59:59.999999")

        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)

        local_sql_instance_country_query['to_sqlcode_date_start'] = date_start_time
        local_sql_instance_country_query['to_sqlcode_date_end'] = date_end_time

        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return pd.DataFrame()

        df_combined = pd.concat([df_combined, first_chunk, *chunk_generator])

    return df_combined


@op(required_resource_keys={"globals"})
def apply_conversion_service_rewrite_data_on_dwh(context, results):
    '''
    Delete data from dwh table.
    '''
    destination_db = context.resources.globals["destination_db"]
    date_start_time = f"{context.resources.globals['reload_date_start']} 00:00:00.000000"
    date_end_time = f"{context.resources.globals['reload_date_end']} 23:59:59.999999"

    # Delete data from dwh table
    DwhOperations.delete_data_from_dwh_table(
        context=context,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        date_column=DATE_COLUMN,
        date_start=date_start_time,
        date_end=date_end_time,
        destination_db=destination_db
    )

    # Filter out empty DataFrames and combine results from all instances
    df_final = pd.concat([df for df in results if not df.empty], ignore_index=True)

    if df_final.empty:
        context.log.error("No data to process - queries returned empty results")
        return

    # Save the results
    DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=df_final, destination_db=destination_db)


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(destination_db=Field(str, default_value="both"),
                                                   reload_date_start=Field(str, default_value=TODAY_DATE),
                                                   reload_date_end=Field(str, default_value=TODAY_DATE)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{TODAY_DATE}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'Full rewrite today data every 5 minutes from 6am to 8pm for - {SCHEMA}.{TABLE_NAME}'
)
def apply_conversion_service_job():
    instances = apply_conversion_service_get_sqlinstance()
    results = instances.map(apply_conversion_service_query_on_db).collect()
    apply_conversion_service_rewrite_data_on_dwh(results)


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(destination_db=Field(str, default_value="both"),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME, '_yesterday'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'Full rewrite yesterday data for - {SCHEMA}.{TABLE_NAME}'
)
def apply_conversion_service_job_yesterday():
    instances = apply_conversion_service_get_sqlinstance()
    results = instances.map(apply_conversion_service_query_on_db).collect()
    apply_conversion_service_rewrite_data_on_dwh(results)
