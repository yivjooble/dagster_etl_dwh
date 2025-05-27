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


TABLE_NAME = "profile_crm_order"
SCHEMA = "dte"
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut())
def profile_crm_order_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    context.log.info('Getting SQL instances...\n'
                     f"Gitlab sql-code link:\n{GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="ea_crm_flowchart",
            db_name="ProfileCrm",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={"globals"})
def profile_crm_order_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    try:
        # Get max id from dwh table
        if not context.resources.globals["max_id"]:
            query = "SELECT MAX(id) as max_id FROM dte.profile_crm_order"

            max_id = DwhOperations.execute_on_dwh(
                context=context,
                query=query,
                fetch_results=True,
            )

            sql_instance_country_query['to_sqlcode_max_id'] = max_id[0]['max_id']
        else:
            sql_instance_country_query['to_sqlcode_max_id'] = context.resources.globals["max_id"]

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        # Delete data from dwh table
        deletion_query = f"""DELETE
                             FROM dte.profile_crm_order
                             WHERE id > {sql_instance_country_query['to_sqlcode_max_id']}"""
        DwhOperations.execute_on_dwh(
            context=context,
            query=deletion_query,
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
    resource_defs={"globals": make_values_resource(max_id=Field(str, default_value='')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def profile_crm_order_job():
    instances = profile_crm_order_get_sqlinstance()
    instances.map(profile_crm_order_query_on_db)
