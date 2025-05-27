from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput,
    RetryPolicy,
    Backoff,
    Jitter
)

# module import
from service.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name, fetch_gitlab_data
from utility_hub import TrinoOperations

# Configure retry policy
retry_policy = RetryPolicy(
    max_retries=5,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS
)

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

SQL, URL = fetch_gitlab_data(
    config_key="default",
    dir_name="dwh_team",
    file_name="exec_trino_sql",
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def prepare_trino_catalogs(context):
    """Prepare Trino catalogs for parallel execution

    Args:
        context: Dagster context with access to resources

    Yields:
        DynamicOutput: Individual Trino catalog configurations for parallel execution
    """
    # Get catalogs from resources (passed by user in web UI)
    trino_catalogs = context.resources.globals["trino_catalogs"]
    
    context.log.info(f"Running SQL queries on {len(trino_catalogs)} Trino catalogs: {trino_catalogs}")
    context.log.info(f"SQL query source: {URL}")
    
    # Yield each catalog as a dynamic output
    for catalog in trino_catalogs:
        context.log.info(f"Preparing execution for catalog: {catalog}")
        yield DynamicOutput(
            value=catalog,
            mapping_key=f"catalog_{catalog}"
        )


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def execute_sql_on_trino_catalog(context, catalog):
    """Execute SQL on a specific Trino catalog

    Args:
        context: Dagster context
        catalog: Trino catalog name to execute SQL on

    Returns:
        str: Execution result message
    """
    context.log.info(f"Executing SQL on catalog: {catalog}")
    
    # Execute the SQL query on the specific catalog
    result = TrinoOperations.exec_on_trino(
        context=context, 
        sql_code_to_execute=SQL, 
        catalog=catalog
    )
    
    context.log.info(f"Completed SQL execution on catalog: {catalog}")
    return f"Successfully executed SQL on {catalog}"


@job(
    config=job_config,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(
            trino_catalogs=Field(list, default_value=['new_dwh'], description="List of Trino catalogs to run SQL on")
        )
    },
    name=generate_job_name('trino_exec_sql'),
    description='execute sql code on multiple trino catalogs concurrently',
    metadata={"URL": f"{URL}"},
)
def execute_sql_on_trino_job():
    # Generate dynamic outputs for each catalog
    catalogs = prepare_trino_catalogs()
    
    # Map the execution function to each catalog for parallel execution
    catalogs.map(execute_sql_on_trino_catalog)
