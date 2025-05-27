import os

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
)

# module import
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    all_countries_list,
    job_config,
    retry_policy,
)

from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "conversion"
SCHEMA = "auction"
COUNTRY_COLUMN = "country_id"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def conversion_get_sql_instance(context):
    """
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sql_instance, db, query.
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
def conversion_launch_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    try:
        destination_db = context.resources.globals["destination_db"]
        country_code = sql_instance_country_query["country_code"]
        country_id = sql_instance_country_query["country_id"]

        file_paths = DbOperations.execute_query_and_save_to_parquet(
            context=context,
            path_to_data=PATH_TO_DATA,
            sql_instance_country_query=sql_instance_country_query,
            country_column=COUNTRY_COLUMN,
        )

        if not file_paths:
            context.log.info(f'No data found for: {sql_instance_country_query["country_code"]}')
        else:
            # delete previously stored data
            DwhOperations.delete_data_from_dwh_table(
                context=context,
                schema=SCHEMA,
                table_name=TABLE_NAME,
                date_column=None,
                date_start=None,
                country_column=COUNTRY_COLUMN,
                country=country_id,
                destination_db=destination_db
            )
            # save to dwh table
            for file_path in file_paths:
                DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, file_path=file_path, destination_db=destination_db)

            context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    destination_db=Field(str, default_value="both")),
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
def conversion_job():
    instances = conversion_get_sql_instance()
    instances.map(conversion_launch_query_on_db)
