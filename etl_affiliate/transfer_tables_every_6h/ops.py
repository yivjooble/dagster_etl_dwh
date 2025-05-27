import inspect
from datetime import datetime, timedelta
from dagster import op, job, fs_io_manager

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, log_written_data, call_dwh_procedure

SCHEMA = 'affiliate'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
JOB_PREFIX = job_prefix()
PRC_DICT = {
    'prc_insert_job_in_feed_log_op1': 'insert_job_in_feed_log()',
    'prc_insert_click_price_change_op1': 'insert_click_price_change()',
    'prc_insert_publisher_external_stat_op': 'insert_publisher_external_stat(current_date)'
}


@op
def prc_insert_job_in_feed_log_op1(context):
    """
    Calls DWH procedure affiliate.insert_job_in_feed_log() to identify new records in the job_in_feed_log table
    (affiliate db) by max record ID in NL & US servers and transfer them to the DWH table affiliate.job_in_feed_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
    log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     table_name='job_in_feed_log', table_schema=SCHEMA, send_slack_message=False,
                     write_to_context=False)


@op
def prc_insert_click_price_change_op1(context):
    """
    Calls DWH procedure affiliate.insert_click_price_change() to identify new records in the click_price_change table
    (affiliate db) by max record ID in NL & US servers and transfer them to the DWH table affiliate.click_price_change.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
    log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     table_name='click_price_change', table_schema=SCHEMA, send_slack_message=False,
                     write_to_context=False)


@op
def prc_insert_publisher_external_stat_op(context):
    """
    Rewrites data in the affiliate.publisher_external_stat table (DWH) using data from
    the affiliate_publisher_external_stat table (10.0.0.137 server).
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
    log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     table_name='publisher_external_stat', table_schema=SCHEMA, send_slack_message=False,
                     write_to_context=False)


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}
                                                           )},
     name=JOB_PREFIX + 'transfer_tables_every_6h',
     description=f'Transfers new data to DWH tables affiliate.job_in_feed_log, affiliate.click_price_change from the '
                 f'affiliate databases by max record id. In addition, data in the affiliate.publisher_external_stat '
                 f'is fully rewritten using the data from the affiliate_publisher_external_stat on the 10.0.0.137.')
def transfer_tables_every_6h_job():
    prc_insert_job_in_feed_log_op1()
    prc_insert_click_price_change_op1()
    prc_insert_publisher_external_stat_op()
