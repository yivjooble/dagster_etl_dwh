from datetime import datetime, timedelta

from dagster import (
    DynamicOut,
    Field,
    fs_io_manager,
    job,
    make_values_resource,
    op,
)

# project import
from ...utils.io_manager_path import get_io_manager_path

from utility_hub import (
    DwhOperations,
    Operations,
    DbOperations,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "subscription_model"
SCHEMA = "aggregation"
DATE_COLUMN = 'date_paid'
TODAY_DATE = datetime.now().date().strftime('%Y-%m-%d')
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def subscription_model_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    context.log.info('Getting SQL instances...\n'
                     f'I> Date range: [{context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}]\n'
                     f"I> Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="employer_account",
            db_name="employer_account",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def subscription_model_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    sql_instance_country_query['to_sqlcode_date_start'] = context.resources.globals["reload_date_start"]
    sql_instance_country_query['to_sqlcode_date_end'] = context.resources.globals["reload_date_end"]

    try:
        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        # Delete old data only if data is returned
        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_COLUMN,
            date_start=context.resources.globals["reload_date_start"],
            date_end=context.resources.globals["reload_date_end"]
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)

    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={
        "globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                        reload_date_end=Field(str, default_value=TODAY_DATE)),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {TODAY_DATE}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def subscription_model_job():
    instances = subscription_model_get_sqlinstance()
    instances.map(subscription_model_query_on_db)
