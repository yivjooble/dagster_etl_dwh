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


TABLE_NAME = "facebook_adset"
SCHEMA = "traffic"
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut())
def facebook_adset_get_sqlinstance(context):
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
            db_name="PublicStatistic",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={"globals"})
def facebook_adset_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    try:
        destination_db = context.resources.globals["destination_db"]
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
        DwhOperations.truncate_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
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
        "globals": make_values_resource(destination_db=Field(str, default_value='both')),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True"
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def facebook_adset_job():
    instances = facebook_adset_get_sqlinstance()
    instances.map(facebook_adset_query_on_db)
