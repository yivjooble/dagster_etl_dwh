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


TABLE_NAME = "adv_revenue_ea_new"
SCHEMA = "imp_statistic"
COUNTRY_COLUMN = "country_id"
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={"globals"})
def adv_revenue_ea_new_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on a separate instance.
    Args:
        context: The context object.
    Output:
        A generator that yields a DynamicOutput object.
    """
    destination_db = context.resources.globals["destination_db"]
    launch_countries = context.resources.globals["reload_countries"]

    # Truncate target table
    DwhOperations.truncate_dwh_table(
        context=context,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context,
                                                         instance_type="prod",
                                                         query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def adv_revenue_ea_new_query_on_db(context, sql_instance_country_query: dict):
    """
    Save DataFrame to the data warehouse (DWH) table.
    Args:
        context (object): The context object containing resources and information about the execution.
    Raises:
        Exception: If there is an error while saving to the DWH.
    Returns:
        None
    """
    destination_db = context.resources.globals["destination_db"]
    try:
        country_code = sql_instance_country_query["country_code"]

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

        # Add country_code column to the first chunk
        first_chunk["country_code"] = country_code

        # Save the first chunk
        DwhOperations.save_to_dwh_pandas(context=context,
                                         schema=SCHEMA,
                                         table_name=TABLE_NAME,
                                         df=first_chunk,
                                         destination_db=destination_db)

        # Save the remaining chunks
        for chunk in chunk_generator:
            # Add country_code column to each chunk
            chunk["country_code"] = country_code
            DwhOperations.save_to_dwh_pandas(context=context,
                                             schema=SCHEMA,
                                             table_name=TABLE_NAME,
                                             df=chunk,
                                             destination_db=destination_db)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   destination_db=Field(str, default_value='both')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True"
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def adv_revenue_ea_new_job():
    instances = adv_revenue_ea_new_get_sqlinstance()
    instances.map(adv_revenue_ea_new_query_on_db)
