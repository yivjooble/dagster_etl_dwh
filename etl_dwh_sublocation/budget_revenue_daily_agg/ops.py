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


TABLE_NAME = 'budget_revenue_daily_agg'
SCHEMA = 'aggregation'

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = 'call aggregation.insert_budget_revenue_daily_agg(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def budget_revenue_daily_agg_query_on_db(context):
    destination_db = context.resources.globals["destination_db"]

    date_range = pd.date_range(
                    pd.to_datetime(context.resources.globals["reload_date_start"]),
                    pd.to_datetime(context.resources.globals["reload_date_end"])
                )

    context.log.info(f"Date range: {context.resources.globals['reload_date_start']} - {context.resources.globals['reload_date_end']}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    for date in date_range:
        operation_date = date.strftime('%Y-%m-%d')

        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            params=(operation_date,),
            destination_db=destination_db
        )
        context.log.info(f"Procedure call for {operation_date} completed successfully.")



@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                   destination_db=Field(str, default_value='both'))
                  },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def budget_revenue_daily_agg_job():
    budget_revenue_daily_agg_query_on_db()
