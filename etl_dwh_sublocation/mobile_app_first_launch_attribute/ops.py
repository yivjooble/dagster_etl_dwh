from dagster import (
    op,
    job,
    fs_io_manager,
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from utility_hub import DwhOperations


TABLE_NAME = 'first_launch_attribute'
SCHEMA = 'mobile_app'

PROCEDURE_CALL = "call mobile_app.prc_first_launch_attribute();"
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op
def mobile_app_first_launch_attribute_query_on_db(context):
    context.log.info(f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
    )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(table_name=TABLE_NAME, additional_prefix="mobile_app_"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True",
        "triggered_by_job": "rpl__session_feature_action"
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def mobile_app_first_launch_attribute_job():
    mobile_app_first_launch_attribute_query_on_db()
