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


TABLE_NAME = 'seo_abtest_agg'
SCHEMA = 'aggregation'

PROCEDURE_CALL = 'call aggregation.insert_seo_abtest_agg_last_3_month();'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={"globals"})
def seo_abtest_agg_query_on_db(context):
    destination_db = context.resources.globals["destination_db"]

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        destination_db=destination_db,
    )


@job(
    resource_defs={
        "globals": make_values_resource(destination_db=Field(str, default_value="cloudberry")),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "cloudberry",
        "target_tables": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def seo_abtest_agg_job():
    seo_abtest_agg_query_on_db()
