from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from utility_hub import DwhOperations


TABLE_NAME = "webmaster_agg"
SCHEMA = "traffic"

DATE = (datetime.now().date() - timedelta(7)).strftime("%Y-%m-%d")

PROCEDURE_CALL = 'call traffic.prc_webmaster_agg(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={"globals"})
def webmaster_agg_query_on_db(context):
    operation_date = (context.resources.globals["reload_date"],)

    context.log.info(f"operation_date: {operation_date}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        params=operation_date
    )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date=Field(str, default_value=DATE))
                  },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"Aggregates data from the table [traffic.webmaster_statistic] into the [{SCHEMA}.{TABLE_NAME}]",
)
def webmaster_agg_job():
    webmaster_agg_query_on_db()
