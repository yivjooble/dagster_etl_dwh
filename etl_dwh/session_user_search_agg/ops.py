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
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from utility_hub import DwhOperations


TABLE_NAME = 'session_user_search_agg'
SCHEMA = 'traffic'

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = 'call traffic.insert_session_user_search_agg(%s, %s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def session_user_search_agg_query_on_db(context):
    operation_datediff_start = get_datediff(context.resources.globals["reload_date_start"])
    operation_datediff_end = get_datediff(context.resources.globals["reload_date_end"])

    context.log.info(f"Date range: {context.resources.globals['reload_date_start']} - {context.resources.globals['reload_date_end']}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        params=(operation_datediff_start, operation_datediff_end)
    )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE)),
                  },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def session_user_search_agg_job():
    session_user_search_agg_query_on_db()
