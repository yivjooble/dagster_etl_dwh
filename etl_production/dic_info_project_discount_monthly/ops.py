from datetime import datetime
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
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


TABLE_NAME = "dic_info_project_discount_monthly"
SCHEMA = "dimension"
DATE_COLUMN = "date_month"
COUNTRY_COLUMN = "country_id"

# Get the first day of the current month
FIRST_DAY_OF_MONTH = datetime.now().date().replace(day=1).strftime('%Y-%m-%d')

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={"globals"})
def dic_info_project_discount_monthly_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    """
    launch_countries = context.resources.globals["reload_countries"]
    reload_month = context.resources.globals["reload_month"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Date: {reload_month}\n"
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context, instance_type="prod", query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={"globals"})
def dic_info_project_discount_monthly_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    try:
        destination_db = context.resources.globals["destination_db"]
        country_id = sql_instance_country_query["country_id"]
        country_code = sql_instance_country_query["country_code"]

        reload_month = context.resources.globals["reload_month"]
        date_month = datetime.strptime(reload_month, '%Y-%m-%d').replace(day=1).strftime('%Y-%m-%d')

        sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = date_month

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query,
            country_column=COUNTRY_COLUMN
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_COLUMN,
            date_start=date_month,
            country_column=COUNTRY_COLUMN,
            country=country_id,
            destination_db=destination_db
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk, destination_db=destination_db)

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
                                                   reload_month=Field(str, default_value=FIRST_DAY_OF_MONTH),
                                                   destination_db=Field(str, default_value='both')
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{FIRST_DAY_OF_MONTH}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"Writing project discounts for the current month to - [{SCHEMA}.{TABLE_NAME}]",
)
def dic_info_project_discount_monthly_job():
    instances = dic_info_project_discount_monthly_get_sqlinstance()
    instances.map(dic_info_project_discount_monthly_query_on_db)
