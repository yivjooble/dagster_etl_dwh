import os

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)

# project import
from ...utils.io_manager_path import get_io_manager_path

# module import
from utility_hub import (
    DwhOperations,
    TrinoOperations,
    job_config,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name

TABLE_NAME = "scan_flow_statistic"
SCHEMA = "aggregation"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={"globals"})
def scan_flow_statistic_run_on_trino(context):
    """
    Run the scan_flow_statistic job for the US on Trino.
    Args:
        context (object): The context object containing resources and information about the execution.
    """
    destination_db = context.resources.globals["destination_db"]
    # Drop the table if it already exists
    drop_table_query = f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE_NAME}"
    DwhOperations.execute_on_dwh(context=context, query=drop_table_query, destination_db=destination_db)

    # Execute the SQL code on Trino
    context.log.info(f"Trino sql script:\n{GITLAB_SQL_URL}")
    trino_catalog = context.resources.globals["trino_catalog"]
    TrinoOperations.exec_on_trino(context=context, sql_code_to_execute=GITLAB_SQL_Q, catalog=trino_catalog)

    # Grant select access to the readonly user
    grant_select_query = f"grant select on {SCHEMA}.{TABLE_NAME} to readonly"
    DwhOperations.execute_on_dwh(context=context, query=grant_select_query, destination_db=destination_db)

    # Redistribute data
    redistribute_data_query = f"ALTER TABLE {SCHEMA}.{TABLE_NAME} SET WITH (REORGANIZE=TRUE) DISTRIBUTED RANDOMLY"
    DwhOperations.execute_on_dwh(context=context, query=redistribute_data_query, destination_db=destination_db)


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(trino_catalog=Field(str, default_value='postgres_2_103_scan'),
                                                   destination_db=Field(str, default_value="cloudberry")),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME, ),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def scan_flow_statistic_job():
    scan_flow_statistic_run_on_trino()
