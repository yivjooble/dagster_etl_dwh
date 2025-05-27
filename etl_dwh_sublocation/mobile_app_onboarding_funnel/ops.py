from datetime import datetime, timedelta
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from utility_hub import DwhOperations


TABLE_NAME = 'onboarding_funnel'
SCHEMA = 'mobile_app'

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = "call mobile_app.prc_onboarding_funnel(%s);"
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def mobile_app_onboarding_funnel_query_on_db(context):
    operation_date_start = context.resources.globals["reload_date_start"]
    operation_datediff = get_datediff(operation_date_start)

    context.log.info(f"Date: {operation_date_start}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        params=(operation_datediff,)
    )


@job(
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE))},
    name=generate_job_name(table_name=TABLE_NAME, additional_prefix="mobile_app_"),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def mobile_app_onboarding_funnel_job():
    mobile_app_onboarding_funnel_query_on_db()
