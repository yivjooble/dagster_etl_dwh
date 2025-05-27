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


TABLE_NAME = 'open_contact_by_feature'
SCHEMA = 'employer'

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

PROCEDURE_CALL = 'call employer.insert_open_contact_by_feature(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={'globals'})
def open_contact_by_feature_query_on_db(context):
    operation_date_start = context.resources.globals["reload_date_start"]
    operation_date_end = context.resources.globals["reload_date_end"]

    date_range = pd.date_range(start=operation_date_start, end=operation_date_end)

    context.log.info(f"Date range: {operation_date_start} - {operation_date_end}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    for date in date_range:
        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            params=(date.strftime('%Y-%m-%d'),)
        )



@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),)
                  },
     name=generate_job_name(TABLE_NAME),
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{YESTERDAY_DATE}",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "dwh",
         "target_table": f"{SCHEMA}.{TABLE_NAME}",
     },
     description=f'{SCHEMA}.{TABLE_NAME}')
def open_contact_by_feature_job():
    open_contact_by_feature_query_on_db()
