import pandas as pd
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput,
)

from utility_hub import TrinoOperations, DwhOperations, map_country_code_to_id, trino_rpl_catalog, retry_policy
from service.utils.io_manager_path import get_io_manager_path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure job execution limits
job_config = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 10,  # Limit to 10 concurrent SQL executions
            }
        }
    }
}


@op(out=DynamicOut(), required_resource_keys={'globals'})
def process_trino_catalog(context):
    """Prepare Trino catalogs for parallel execution

    Args:
        context: Dagster context with access to resources

    Yields:
        DynamicOutput: Individual Trino catalog configurations for parallel execution
    """
    # Get catalogs from resources (passed by user in web UI)
    trino_catalogs = context.resources.globals["trino_catalogs"]

    context.log.info(f"Running SQL queries on {len(trino_catalogs)} Trino catalogs: {trino_catalogs}")

    # Yield each catalog as a dynamic output
    for catalog in trino_catalogs:
        yield DynamicOutput(
            value={
                'trino_catalog': catalog,
                'trino_db_name': catalog.split('_')[1]
            },
            mapping_key=f"catalog_{catalog}"
        )

@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def fetch_and_insert_data(context, catalog) -> None:
    catalog_name = catalog["trino_catalog"]
    catalog_db_name = catalog["trino_db_name"]
    country_id = map_country_code_to_id.get(catalog_db_name)
    schema_name = context.resources.globals["schema_name"]
    table_name = context.resources.globals["table_name"]   
    cloudberry_db_name = context.resources.globals["cloudberry_db_name"]
    sql_code_to_execute = context.resources.globals["sql_code_to_execute"]
    max_workers = 4

    context.log.info(f"Processing catalog: {catalog_name}")
    context.log.info(f"SQL query to execute: {sql_code_to_execute}")

    def _convert_float_columns_to_int(df):
        """
        Convert float columns to int if all values are whole numbers.
        Handles both regular integers and very large integers that might be in scientific notation.
        """
        for col in df.select_dtypes(include=['float64', 'float32']).columns:
            # Check if all non-null values are whole numbers
            if (df[col].dropna() % 1 == 0).all():
                try:
                    # First convert to numpy's int64, then to pandas' Int64 (nullable)
                    df[col] = df[col].astype('float64').apply(
                        lambda x: int(x) if pd.notnull(x) else None
                    ).astype('Int64')
                except (ValueError, OverflowError):
                    # If conversion fails (number too large), keep as float
                    context.log.warning(
                        f"Could not convert column '{col}' to Int64 due to large values. "
                        "Keeping as float64."
                    )
        return df       

    def _process_chunk(chunk: pd.DataFrame, country_id: int, catalog_db_name: str, schema_name: str, 
                      table_name: str, cloudberry_db_name: str, context) -> int:
        """Process a single chunk and save it to the database."""
        if chunk is None or chunk.empty:
            return 0
        
        # Process the chunk
        chunk = _convert_float_columns_to_int(chunk.copy())
        chunk['country_id'] = country_id
        chunk['country_code'] = catalog_db_name
        
        # Save the chunk
        DwhOperations.save_to_dwh_copy_method(
            context=context,
            schema=schema_name,
            table_name=table_name,
            df=chunk,
            destination_db="cloudberry",
            cloudberry_db_name=cloudberry_db_name
        )
        return len(chunk)   

    # Initialize generator
    chunk_generator = TrinoOperations.fetch_data_from_trino(
        context=context,
        sql_code_to_execute=sql_code_to_execute,
        catalog=catalog_name,
    )

    total_rows = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk_number = 0
        
        for chunk in chunk_generator:
            if chunk is None or chunk.empty:
                continue
                
            # Process chunk in a separate thread
            future = executor.submit(
                _process_chunk,
                chunk=chunk,
                country_id=country_id,
                catalog_db_name=catalog_db_name,
                schema_name=schema_name,
                table_name=table_name,
                cloudberry_db_name=cloudberry_db_name,
                context=context
            )
            futures.append(future)
            chunk_number += 1
            context.log.info(f"Submitted chunk {chunk_number} for processing")
        
        # Process completed tasks
        for future in as_completed(futures):
            try:
                rows_processed = future.result()
                total_rows += rows_processed
                context.log.info(f"Processed chunk with {rows_processed} rows. Total so far: {total_rows}")
            except Exception as e:
                context.log.error(f"Error processing chunk: {str(e)}")
                raise

    if total_rows == 0:
        context.log.warning("No data was processed from Trino query")
        return

    context.log.info(f"Successfully processed {total_rows} rows from {catalog_name}")


@job(
    config=job_config,
    name="service__trino_to_cloudberry",
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(
            trino_catalogs=Field(list, default_value=trino_rpl_catalog,
                                 description="List of Trino catalogs to run SQL on"),
            schema_name=Field(str, default_value="dwh_test", description="Destination schema name"),
            table_name=Field(str, default_value="test", description="Destination table name"),
            cloudberry_db_name=Field(str, default_value="testdb_stat", description="Cloudberry database name"),
            sql_code_to_execute=Field(str, default_value="select 1", description="SQL code to execute on Trino"),
        )
    },
    description="Fetch data from Trino, add country_id and country_code columns, and load it into Cloudberry.",
)
def trino_to_cloudberry_job():
    """
    Job to fetch data from Trino and load it into Cloudberry.
    Configure catalogs through the Dagster UI when triggering the job.
    """
    catalogs = process_trino_catalog()
    catalogs.map(fetch_and_insert_data)
