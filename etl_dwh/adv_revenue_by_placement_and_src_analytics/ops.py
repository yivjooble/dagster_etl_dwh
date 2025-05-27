from datetime import datetime, timedelta
import pandas as pd
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

TABLE_NAME = 'adv_revenue_by_placement_and_src_analytics'
SCHEMA = 'aggregation'

TWO_DAYS_BEFORE_DATE = (datetime.now().date() - timedelta(2)).strftime('%Y-%m-%d')

PROCEDURE_CALL = 'call aggregation.prc_adv_revenue_by_placement_and_src_analytics_past_two_days(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name="prc_adv_revenue_by_placement_and_src_analytics_past_two_days",
    manual_object_type="procedures"
)


@op(required_resource_keys={'globals'})
def adv_revenue_by_placement_and_src_analytics_query_on_db(context):
    destination_db = context.resources.globals["destination_db"]
    operation_date_start = context.resources.globals["reload_date_start"]
    operation_date_end = context.resources.globals["reload_date_end"]
    context.log.info(f"Date: {operation_date_start} - {operation_date_end}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    date_range = pd.date_range(pd.to_datetime(operation_date_start), pd.to_datetime(operation_date_end))
    for date in date_range:
        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            params=(date.strftime('%Y-%m-%d'),),
            destination_db=destination_db
        )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date_start=Field(str, default_value=TWO_DAYS_BEFORE_DATE),
                                                   reload_date_end=Field(str, default_value=TWO_DAYS_BEFORE_DATE),
                                                   destination_db=Field(str, default_value='both')
                                                   )
                   },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{TWO_DAYS_BEFORE_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'Reload data two days before: {SCHEMA}.{TABLE_NAME}',
)
def adv_revenue_by_placement_and_src_analytics_job():
    adv_revenue_by_placement_and_src_analytics_query_on_db()
