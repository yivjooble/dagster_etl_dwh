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
from utility_hub import TrinoOperations, DwhOperations
import pandas as pd

TABLE_NAME = "empl_acc_funnel_by_reg_date"
SCHEMA_NAME = "employer"

GITLAB_SQL, GITLAB_URL = fetch_gitlab_data(
    ref="EA_20_new_tabels",
    config_key="default",
    dir_name="employer",
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def empl_acc_funnel_by_reg_date(context):
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
    df = next(df_generator, None)
    if df is None or df.empty:
        context.log.warning("No data returned from Trino query, skipping save to DWH")
        return
    
    # Process remaining chunks if any
    for chunk_df in df_generator:
        if not chunk_df.empty:
            df = pd.concat([df, chunk_df], ignore_index=True)
    
    context.log.info(f"Fetched {len(df)} rows from Trino, saving to {SCHEMA_NAME}.{TABLE_NAME}")

    # Truncate table
    DwhOperations.truncate_dwh_table(context=context, 
                                     schema=SCHEMA_NAME, 
                                     table_name=TABLE_NAME, 
                                     destination_db=destination_db)
    
    # Save data to DWH using COPY method
    DwhOperations.save_to_dwh_copy_method(
        context=context,
        df=df,
        schema=SCHEMA_NAME,
        table_name=TABLE_NAME,
        destination_db=destination_db  # Can be "dwh", "cloudberry", or "both"
    )
    
    context.log.info(f"Successfully saved data to {SCHEMA_NAME}.{TABLE_NAME}")


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(destination_db=Field(str, default_value='cloudberry'))},
    name=generate_job_name('empl_acc_funnel_by_reg_date'),
    description=f'{TABLE_NAME}.{SCHEMA_NAME}',
    metadata={"URL": f"{GITLAB_URL}"},
)
def empl_acc_funnel_by_reg_date_job():
    empl_acc_funnel_by_reg_date()
