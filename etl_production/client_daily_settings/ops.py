from copy import deepcopy

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field
)

# project import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    all_countries_list,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "client_daily_settings"
SCHEMA = "aggregation"
COUNTRY_COLUMN = "country_id"
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def client_daily_settings_get_sql_instance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.
    Args: sql_query.
    Output: sql_instance, db, query.
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context, instance_type="prod", query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def client_daily_settings_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    try:
        destination_db = context.resources.globals["destination_db"]
        country_code = sql_instance_country_query['country_code']

        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query,
            country_column=COUNTRY_COLUMN,
        )

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk, destination_db=destination_db)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   destination_db=Field(str, default_value="both")),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def client_daily_settings_job():
    instances = client_daily_settings_get_sql_instance()
    instances.map(client_daily_settings_query_on_db).collect()
