import pytz
from datetime import datetime

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, run_tableau_object_refresh
from utility_hub import DwhOperations
from utility_hub.data_collections import tableau_object_uid

TABLE_NAME = 'paid_acquisition_prod_metrics'
SCHEMA = 'aggregation'

CURRENT_DATE = (datetime.now().date()).strftime('%Y-%m-%d')

PROCEDURE_CALL = 'call aggregation.prc_paid_acquisition_prod_metrics(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)

PROCEDURE_CALL_SHORT_VERSION = 'call aggregation.prc_paid_acquisition_prod_metrics_short_version(%s);'
GITLAB_DDL_Q_SHORT_VERSION, GITLAB_DDL_URL_SHORT_VERSION = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name='paid_acquisition_prod_metrics_short_version',
)


@op(required_resource_keys={'globals'})
def paid_acquisition_prod_metrics_query_on_db(context):
    operation_date_start = context.resources.globals["reload_date_start"]

    kyiv_tz = pytz.timezone('Europe/Kiev')
    start_time = datetime.now(kyiv_tz).strftime('%Y-%m-%d %H:%M:%S')

    # Determine which job is running
    job_name = context.dagster_run.job_name

    # Execute the appropriate procedure based on the job name
    if job_name == paid_acquisition_prod_metrics_job.name:
        context.log.info(f"operation_date: {operation_date_start}\n"
                         f"DDL run on dwh:\n{GITLAB_DDL_URL}")

        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            params=(operation_date_start,)
        )
    elif job_name == paid_acquisition_prod_metrics_short_version_job.name:
        context.log.info(f"operation_date: {operation_date_start}\n"
                         f"DDL run on dwh:\n{GITLAB_DDL_URL_SHORT_VERSION}")

        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL_SHORT_VERSION,
            ddl_query=GITLAB_DDL_Q_SHORT_VERSION,
            params=(operation_date_start,)
        )

    end_time = datetime.now(kyiv_tz).strftime('%Y-%m-%d %H:%M:%S')

    # Send notification to slack thread
    slack_msg = f'''select dc.send_notification_to_message_thread_in_slack('*Checking _Data Completeness_*',
                                                                           ':abacus: PPC_{job_name} - *Loading finish*' ||  E'\n' ||
                                                                           '*start load:* _{start_time}_\n*end load:* _{end_time}_ <@U02JWJNLW2E>',
                                                                           'C01FZCHQ8LR');'''

    DwhOperations.execute_on_dwh(context=context, query=slack_msg)

    # """
    # Data sources of workbook: Paid Acquisition Statistics, view: Total Metrics:
    #   - aggregation.paid_acquisition_prod_metrics,
    #   - aggregation.project_conversions_daily,
    #   - aggregation.v_paid_cost_finance,
    # """
    # datasource_id = tableau_object_uid['datasource_by_name']['aggregation_paid_acquisition_prod_metrics']
    # run_tableau_object_refresh(context=context, datasource_id=datasource_id)


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(reload_date_start=Field(str, default_value=CURRENT_DATE)),
                   },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{CURRENT_DATE}",
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}'
)
def paid_acquisition_prod_metrics_job():
    paid_acquisition_prod_metrics_query_on_db()


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(reload_date_start=Field(str, default_value=CURRENT_DATE)),
                    },
     name=generate_job_name(TABLE_NAME, "_short_version"),
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{CURRENT_DATE}",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL_SHORT_VERSION}",
         "destination_db": "dwh",
         "target_table": f"{SCHEMA}.{TABLE_NAME}",
     },
     description=f'{SCHEMA}.{TABLE_NAME}\n'
                 f'This version of the job has removed the calculation of budgets and cumulative budgets\n'
                 f'(which calculated with aggregation.budget_revenue_daily_agg), and part of the conversions\n'
                 f'(which calculated with aggregation.project_conversions_daily).'
     )
def paid_acquisition_prod_metrics_short_version_job():
    paid_acquisition_prod_metrics_query_on_db()
