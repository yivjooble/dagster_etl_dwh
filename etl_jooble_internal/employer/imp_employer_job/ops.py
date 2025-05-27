from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field
)

# module import
from service.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name, fetch_gitlab_data
from utility_hub import TrinoOperations, DwhOperations, Operations
import pandas as pd
import itertools

TABLE_NAME = "job"
SCHEMA = "imp_employer"

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def ea_job_query_on_db(context):
    # Get Trino catalog from resources
    trino_catalog = "new_dwh"
    destination_db = context.resources.globals["destination_db"]

    context.log.info(f"GITLAB_SQL_URL: {GITLAB_SQL_URL}")

    # Fetch data from Trino
    df_generator = TrinoOperations.fetch_data_from_trino(
        context=context,
        sql_code_to_execute=GITLAB_SQL_Q,
        catalog=trino_catalog,
        params=None
    )

    # Fetch first chunk to check if we have data
    first_chunk = next(df_generator, None)
    if first_chunk is None or first_chunk.empty:
        context.log.warning("No data returned from Trino query, skipping save to DWH")
        return
    context.log.info(f"Fetched first chunk with {len(first_chunk)} rows from Trino, starting upload to {SCHEMA}.{TABLE_NAME}")

    # Query to get column names and data types in their original order from the specified table
    query = f"""
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
        AND table_name = '{TABLE_NAME}'
        ORDER BY ordinal_position;
    """

    results = DwhOperations.execute_on_dwh(
        context=context,
        query=query,
        fetch_results=True,
        destination_db=destination_db
    )

    # Build dtype map for table schema
    dtype_map_full = {col['column_name']: Operations.map_postgres_to_pandas_types(col['data_type']) for col in results}
    # Only apply dtype conversion to columns present in the data
    dtype_map = {col: dtype for col, dtype in dtype_map_full.items() if col in first_chunk.columns}
    context.log.debug(f"Dtype map for conversion: {dtype_map}")

    # Truncate destination table
    DwhOperations.truncate_dwh_table(context=context,
                                     schema=SCHEMA,
                                     table_name=TABLE_NAME,
                                     destination_db=destination_db)

    # Stream chunks to DWH using COPY method to reduce peak memory usage
    for chunk in itertools.chain([first_chunk], df_generator):
        context.log.debug(f"Uploading chunk with {len(chunk)} rows")
        # Convert types
        chunk = chunk.astype(dtype_map)
        DwhOperations.save_to_dwh_copy_method(
            context=context,
            df=chunk,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            destination_db=destination_db
        )

    context.log.info(f"Successfully saved data to {SCHEMA}.{TABLE_NAME}")


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(destination_db=Field(str, default_value='cloudberry'))},
    name=generate_job_name(TABLE_NAME, additional_prefix='ea_'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_SQL_URL}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True"
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def ea_job_job():
    ea_job_query_on_db()
