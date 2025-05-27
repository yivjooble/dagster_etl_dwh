from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from utility_hub import DwhOperations


TABLE_NAME = 'test_iterations' # The procedure also writes to the table >> imp_statistic.test_group
SCHEMA = 'imp_statistic'

PROCEDURE_CALL = 'call imp_statistic.transfer_data_from_protestservice();'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={"globals"})
def test_iterations_query_on_db(context):
    destination_db = context.resources.globals["destination_db"]

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        destination_db=destination_db,
    )


@job(
    resource_defs={
        "globals": make_values_resource(destination_db=Field(str, default_value="both")),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_tables": f"[{SCHEMA}.{TABLE_NAME}], [{SCHEMA}.test_group]",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def test_iterations_job():
    test_iterations_query_on_db()
