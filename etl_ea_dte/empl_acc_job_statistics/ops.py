from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# module import
from service.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name, fetch_gitlab_data
from utility_hub import TrinoOperations, DwhOperations
import pandas as pd

TABLE_NAME = "empl_acc_job_statistics"
SCHEMA_NAME = "employer"

GITLAB_SQL, GITLAB_URL = fetch_gitlab_data(
    ref="EA_20_new_tabels",
    config_key="default",
    dir_name="employer",
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def empl_acc_job_statistics(context):
    # Get Trino catalog from resources
    trino_catalog = "new_dwh"
    destination_db = context.resources.globals["destination_db"]
    
    context.log.info(f"GITLAB_URL: {GITLAB_URL}")
    
    # Fetch data from Trino
    df_generator = TrinoOperations.fetch_data_from_trino(
        context=context,
        sql_code_to_execute=GITLAB_SQL,
        catalog=trino_catalog,
        params=None
    )
    
    # Get first chunk to check if we have data
    first_chunk = next(df_generator, None)
    if first_chunk is None or first_chunk.empty:
        context.log.warning("No data returned from Trino query, skipping save to DWH")
        return
    
    # Truncate table before concurrent writes
    DwhOperations.truncate_dwh_table(
        context=context, 
        schema=SCHEMA_NAME, 
        table_name=TABLE_NAME, 
        destination_db=destination_db
    )

    def save_chunk(chunk_df):
        if not chunk_df.empty:
            DwhOperations.save_to_dwh_copy_method(
                context=context,
                df=chunk_df,
                schema=SCHEMA_NAME,
                table_name=TABLE_NAME,
                destination_db=destination_db
            )
            context.log.info(f"Saved chunk with {len(chunk_df)} rows to {SCHEMA_NAME}.{TABLE_NAME}")

    # Use ThreadPoolExecutor to save chunks concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        # Submit the first chunk
        futures.append(executor.submit(save_chunk, first_chunk))
        # Submit the rest of the chunks
        for chunk_df in df_generator:
            if not chunk_df.empty:
                futures.append(executor.submit(save_chunk, chunk_df))
        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()  # Will raise exception if any occurred

    context.log.info(f"Successfully saved all chunks to {SCHEMA_NAME}.{TABLE_NAME}")


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(destination_db=Field(str, default_value='cloudberry'))},
    name=generate_job_name('empl_acc_job_statistics'),
    description=f'{TABLE_NAME}.{SCHEMA_NAME}',
    metadata={"URL": f"{GITLAB_URL}"},
)
def empl_acc_job_statistics_job():
    empl_acc_job_statistics()
