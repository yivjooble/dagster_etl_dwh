from datetime import datetime, timedelta
from typing import List
import pandas as pd

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    DynamicOutput,
    Field,
)

# module import
from utility_hub import (
    DbOperations,
    TrinoOperations,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from utility_hub.data_collections import trino_catalog_map
from ..utils.io_manager_path import get_io_manager_path

TABLE_NAME = "click_metric_agg_trino"
SCHEMA_NAME = "an"

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
CURRENT_DATE = datetime.now().date().strftime('%Y-%m-%d')

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name="dwh_test",
    file_name="sql_server_yiv_test_click_metric_agg",
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def click_metric_agg_trino_get_sqlinstance(context):
    """Generate a DynamicOutput for each Trino catalog to process

    Args:
        context: Dagster context with access to resources

    Yields:
        DynamicOutput: Trino catalog configurations for parallel execution
    """
    # Get catalogs from resources (passed by user in web UI)
    trino_catalogs = context.resources.globals["trino_catalogs"]
    
    context.log.info(f'Selected trino catalogs: {trino_catalogs}\n'
                     f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                     f"DDL run on replica:\n{GITLAB_SQL_URL}")
    
    # Yield each catalog as a dynamic output
    for catalog in trino_catalogs:        
        catalog_info = TrinoOperations.prepare_catalog_info(context, catalog)
        if not catalog_info:
            continue
        
        yield DynamicOutput(
            value=catalog_info,
            mapping_key=f"catalog_{catalog}"
        )


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def click_metric_agg_trino_query_on_db(context, catalog_info: dict):
    """Process data for a specific Trino catalog with concurrent chunk processing

    Args:
        context: Dagster context for logging and resources
        catalog_info: Dictionary containing catalog and processing information

    Returns:
        None
    """         
    trino_catalog = catalog_info['trino_catalog']
    country_code = catalog_info['country_code']
    host = catalog_info['host']
    cluster_name = catalog_info['cluster_name']

    # Add the missing keys for rpl db connection
    catalog_info['credential_key'] = 'repstat'
    catalog_info['db_name'] = country_code    
    catalog_info['db_type'] = 'postgresql'

    for date in catalog_info['date_range']:
        operation_date_str = date.strftime('%Y-%m-%d')
        operation_date_diff = get_datediff(operation_date_str)    
        
        # Process SQL query to replace database references
        modified_sql = TrinoOperations.replace_database_references(GITLAB_SQL_Q, trino_catalog)
        
        # Prepare query parameters as a tuple (required by Trino)
        params = (operation_date_diff, operation_date_str)
        
        # Consolidate all processing information into a single log entry
        context.log.info(
            f"Processing data:\n"
            f"  Catalog: {trino_catalog} (country: {country_code})\n"
            f"  Replica: {cluster_name} (host: {host})\n"
            f"  Parameters: {params}\n"
        )

        # Get chunk generator from Trino with the modified SQL
        chunk_generator = TrinoOperations.fetch_data_from_trino(
            context=context,
            sql_code_to_execute=modified_sql,
            catalog=trino_catalog,
            params=params
        )

        # Get first chunk to check if we have data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None or first_chunk.empty:
            context.log.warning("No data returned from Trino query, skipping")
            return
        
        # Delete existing data only if we have new data to insert
        DbOperations.delete_data_from_table(
            context=context,
            sql_instance_country_query=catalog_info,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            date_column="load_datediff",
            date_start=operation_date_diff,
            date_end=operation_date_diff,
        )

        # Process first chunk
        TrinoOperations.save_to_replica_copy_method(
            context=context,
            sql_instance_country_query=catalog_info,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            df=first_chunk
        )

        # Collect all remaining chunks
        chunks: List[pd.DataFrame] = []
        for chunk_df in chunk_generator:
            if not chunk_df.empty:
                chunks.append(chunk_df)

        if not chunks:
            context.log.info("No additional chunks to process")
            continue

        processed_rows = TrinoOperations.save_chunks_concurrently(
            context=context,
            sql_instance_country_query=catalog_info,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            chunks=chunks
        )
        

@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                   trino_catalogs=Field(list,
                                                                        default_value=list(trino_catalog_map.values())),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA_NAME}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_SQL_URL}",
        "destination_db": "rpl",
        "target_table": f"{SCHEMA_NAME}.{TABLE_NAME}",
    },
    description=f'{SCHEMA_NAME}.{TABLE_NAME} - job getting data from prod and saving into rpl dbs',
)
def click_metric_agg_trino_job():
    instances = click_metric_agg_trino_get_sqlinstance()
    instances.map(click_metric_agg_trino_query_on_db)
