from datetime import datetime, timedelta
from dagster import DynamicOut, fs_io_manager, job, op, Field, make_values_resource

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


TABLE_NAME = "facebook_adset_new"
SCHEMA = "traffic"
DATE_COLUMN = "date"

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
THREE_DAYS_AGO_DATE = (datetime.now().date() - timedelta(3)).strftime('%Y-%m-%d')

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut())
def facebook_adset_new_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    context.log.info('Getting SQL instances...\n'
                     f"I> Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="warehouse_jo",
            db_name="Facebook",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={"globals"})
def facebook_adset_new_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    try:
        destination_db = context.resources.globals["destination_db"]
        date_start = context.resources.globals["reload_date_start"]
        date_end = context.resources.globals["reload_date_end"]
        sql_instance_country_query['to_sqlcode_date_start'] = date_start
        sql_instance_country_query['to_sqlcode_date_end'] = date_end
        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        # Truncate target table
        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_COLUMN,
            date_start=date_start,
            date_end=date_end,
            destination_db=destination_db
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk, destination_db=destination_db)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk, destination_db=destination_db)

    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={
        "globals": make_values_resource(reload_date_start=Field(str, default_value=THREE_DAYS_AGO_DATE),
                                        reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                        destination_db=Field(str, default_value='cloudberry')),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{THREE_DAYS_AGO_DATE} - {YESTERDAY_DATE}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def facebook_adset_new_job():
    instances = facebook_adset_new_get_sqlinstance()
    instances.map(facebook_adset_new_query_on_db)
