import inspect
from datetime import datetime, timedelta
from dagster import op, job, Field, make_values_resource, fs_io_manager

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import log_written_data, job_prefix, call_dwh_procedure

JOB_PREFIX = job_prefix()
TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_DIFF = (TARGET_DATE - datetime(1900, 1, 1)).days
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


@op(required_resource_keys={'globals'})
def insert_project_budgets_op(context) -> bool:
    """
    Calls DWH procedure affiliate.insert_project_budgets(include_daily_budget) to store 
    data into affiliate.project_budgets
    """
    schema_name = context.resources.globals["schema_name"]
    
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    call_dwh_procedure(schema_name, 'insert_project_budgets({})'.format( False))
    call_dwh_procedure(schema_name, 'insert_project_budgets({})'.format( True))

    log_written_data(context=context, table_schema=schema_name, table_name='project_budgets',
                     date=datetime.now().strftime('%Y-%m-%d'), start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name, send_slack_message=True)
    return True


@job(resource_defs={"globals": make_values_resource(target_date_diff=Field(int, default_value=TARGET_DATE_DIFF),
                                                    target_date_str=Field(str, default_value=TARGET_DATE_STR),
                                                    schema_name=Field(str, default_value='affiliate')),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'insert_project_budgets',
     description=f'Full rewrite table affiliate.project_budgets.'
     )
def insert_project_budgets_job():
    insert_project_budgets_op()
