from datetime import datetime, timedelta
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
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff


TABLE_NAME = "auction_job_multiplication"
SCHEMA = "imp"
COUNTRY_COLUMN = "country_id"

SNAPSHOT_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
LOAD_DATE = datetime.now().date().strftime('%Y-%m-%d')

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={"globals"})
def auction_job_multiplication_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on a separate instance.
    Args:
        context: The context object.
    Output:
        A generator that yields a DynamicOutput object.
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context, instance_type="prod", query=GITLAB_SQL_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def auction_job_multiplication_query_on_db(context, sql_instance_country_query: dict):
    """
    Save DataFrame to the data warehouse (DWH) table.
    Args:
        context (object): The context object containing resources and information about the execution.
    Raises:
        Exception: If there is an error while saving to the DWH.
    Returns:
        None
    """
    try:
        destination_db = context.resources.globals["destination_db"]
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

        # Compute date_diff based on scheduled or manual run
        if getattr(context, "scheduled_execution_time", None) is not None:
            snapshot_datediff = get_datediff(SNAPSHOT_DATE)
        else:
            now = datetime.now()
            if now.hour > 4 or (now.hour == 4 and now.minute >= 50):
                snapshot_datediff = get_datediff(LOAD_DATE)
            else:
                snapshot_datediff = get_datediff(SNAPSHOT_DATE)
        load_date = LOAD_DATE

        # Add date_diff and load_datediff columns
        first_chunk["snapshot_datediff"] = snapshot_datediff
        first_chunk["load_date"] = load_date

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk, destination_db=destination_db)

        # Save the remaining chunks with added date columns
        for chunk in chunk_generator:
            # Add date_diff and load_datediff columns
            chunk["snapshot_datediff"] = snapshot_datediff
            chunk["load_date"] = load_date
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk, destination_db=destination_db)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   destination_db=Field(str, default_value='cloudberry')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description="""The job records a snapshot from the auction.job_multiplication table, assigning two dates to each record:
    snapshot_datediff - yesterday's datediff if the job was executed on schedule, otherwise the current datediff;
    load_date - the current date of job execution.
    The job does not delete records, it only appends.""",
)
def auction_job_multiplication_job():
    instances = auction_job_multiplication_get_sqlinstance()
    instances.map(auction_job_multiplication_query_on_db)
