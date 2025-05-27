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


TABLE_NAME = "employer"
SCHEMA = "imp_employer"

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def ea_employer_query_on_db(context):
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

    # Get first chunk to check if we have data
    df = next(df_generator, None)
    if df is None or df.empty:
        context.log.warning("No data returned from Trino query, skipping save to DWH")
        return

    # Process remaining chunks if any
    for chunk_df in df_generator:
        if not chunk_df.empty:
            df = pd.concat([df, chunk_df], ignore_index=True)

    context.log.info(f"Fetched {len(df)} rows from Trino, saving to {SCHEMA}.{TABLE_NAME}")
    context.log.debug(f"DataFrame columns: {list(df.columns)}")

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

    # Convert DataFrame types
    # Build full dtype map and filter for DataFrame columns
    dtype_map_full = {
        col['column_name']: Operations.map_postgres_to_pandas_types(col['data_type'])
        for col in results
    }
    context.log.debug(f"Dtype map full: {dtype_map_full}")

    dtype_map = {col: dtype for col, dtype in dtype_map_full.items() if col in df.columns}
    context.log.debug(f"Dtype map: {dtype_map}")

    context.log.debug(f"Converting DataFrame types: {dtype_map}")
    df = df.astype(dtype_map)

    # Save data to DWH
    DwhOperations.save_to_dwh_upsert(context=context,
                                     schema=SCHEMA,
                                     table_name=TABLE_NAME,
                                     df=df,
                                     exclude_update_columns=["date_created", "daterec"],
                                     destination_db=destination_db)

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
    },
    description=f'The table {TABLE_NAME}.{SCHEMA} is upserted with the (id, sources) columns as primary key',
)
def ea_employer_job():
    ea_employer_query_on_db()
