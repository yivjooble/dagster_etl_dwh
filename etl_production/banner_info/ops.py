from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput
)

# project import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "banner_info"
SCHEMA = "traffic"
COUNTRY_COLUMN = "country_id"
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)
GITLAB_SQL_Q_2, GITLAB_SQL_URL_2 = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name="banner_info_pt2",
)

# Define custom country list
CUSTOM_COUNTRY_LIST = [
    "de",
    "fr",
    "pl",
    "it",
    "es",
    "ca",
    "us",
    "uk",
    "br",
    "nl",
    "at",
    "ch",
    "be",
    "mx",
    "cz",
]


@op(out=DynamicOut(), required_resource_keys={"globals"})
def banner_info_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on a separate instance.
    Args:
        context: The context object.
    Output:
        A generator that yields a DynamicOutput object containing the SQL instance, country database, country ID, query, and date range.

    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}\n{GITLAB_SQL_URL_2}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context, instance_type="prod", query=GITLAB_SQL_Q):
        country_code = sql_instance.value["country_code"]
        if country_code in ('uk', 'us', 'ca', 'br', 'mx'):
            modified_instance = sql_instance.value.copy()
            modified_instance["query"] = GITLAB_SQL_Q_2
            yield DynamicOutput(
                value=modified_instance,
                mapping_key=f"Job_{country_code.upper()}"
            )
        else:
            yield sql_instance


@op(retry_policy=retry_policy)
def banner_info_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    try:
        country_code = sql_instance_country_query["country_code"]
        country_id = sql_instance_country_query["country_id"]

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
            date_column=None,
            date_start=None,
            country_column=COUNTRY_COLUMN,
            country=country_id
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRY_LIST)),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "gitlab_sql_url_2": f"{GITLAB_SQL_URL_2}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True"
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def banner_info_job():
    instances = banner_info_get_sqlinstance()
    instances.map(banner_info_query_on_db)
