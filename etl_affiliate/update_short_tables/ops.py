from dagster import op, job, fs_io_manager
import inspect
from datetime import datetime, timedelta

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, log_written_data, call_dwh_procedure

SCHEMA = 'affiliate'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
YESTERDAY_DATE_DIFF = ((datetime.now() - timedelta(days=1)) - datetime(1900, 1, 1)).days
JOB_PREFIX = job_prefix()
PRC_DICT = {
    'prc_insert_short_click_price_change_all_night_run_op': 'insert_short_click_price_change_all_night_run({})',
    'prc_insert_short_job_in_feed_log_night_run_op': 'insert_short_job_in_feed_log_night_run({})'
}


@op
def prc_insert_short_click_price_change_all_night_run_op(context):
    """
    Call DWH procedure affiliate.insert_short_click_price_change_all_night_run
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
    log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     table_name='short_click_price_change', table_schema=SCHEMA, write_to_context=False,
                     send_slack_message=False)


@op
def prc_insert_short_job_in_feed_log_night_run_op(context):
    """
    Call DWH procedure affiliate.insert_short_job_in_feed_log_night_run
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
    log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     table_name='short_job_in_feed_log', table_schema=SCHEMA, write_to_context=False,
                     send_slack_message=False)


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX+'update_short_tables',
     description=f'Update affiliate.short_click_price_change and affiliate.short_job_in_feed_log by removing '
                 f'unnecessary data.')
def update_short_tables_job():
    prc_insert_short_click_price_change_all_night_run_op()
    prc_insert_short_job_in_feed_log_night_run_op()
