import inspect
from datetime import datetime, timedelta
from dagster import op, job, fs_io_manager, Failure, Out

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, log_written_data, call_dwh_procedure

SCHEMA = 'affiliate'
TODAY_DATE = (datetime.now().date())
TODAY_DATE_DIFF = ((datetime.now()) - datetime(1900, 1, 1)).days
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
YESTERDAY_DATE_DIFF = ((datetime.now() - timedelta(days=1)) - datetime(1900, 1, 1)).days
JOB_PREFIX = job_prefix()


@op
def prc_delete_old_data_api_request_history_op(context) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_api_request_history(current_date - 1) 
    to delete data which is older than 120 days in the affiliate.api_request_history table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_api_request_history(\'{}\')'.format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='api_request_history', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)     
        return True   
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_delete_old_data_job_stat_agg_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_job_stat_agg(fn_get_date_diff(current_date - 1)) 
    to delete data which is older than 210 days in the tables affiliate schema:
    job_stat_agg, job_stat_agg_category, job_stat_agg_abtest.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_job_stat_agg(\'{}\')'.format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='job_stat_agg', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_transfer_old_data_short_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.transfer_old_data_short_table(current_date - 90) 
    to transfer old data which is older than 90 days to archive.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'transfer_old_data_short_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=90)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=90)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='short_click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_delete_old_data_short_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_short_table(current_date - 90) 
    to delete old data which is older than 90 days in the DWH affiliate schema.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_short_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=90)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=90)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='short_click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_delete_old_data_short_archive_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_short_archive_table(current_date - 180) 
    to delete old data which is older than 180 days in the DWH archive schema.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_short_archive_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=180)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=180)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='short_click_price_change', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_transfer_old_data_full_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.transfer_old_data_full_table(current_date - 30) 
    to transfer old data which is older than 30 days to archive.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'transfer_old_data_full_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=30)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=30)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_delete_old_data_full_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_full_table(current_date - 30) 
    to delete old data which is older than 30 days in the DWH affiliate schema.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_full_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=30)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=30)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool))
def prc_delete_old_data_full_archive_table_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_full_archive_table(current_date - 101) 
    to delete old data which is older than 101 days in the DWH archive schema.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_full_archive_table(\'{}\')'.format(
            (TODAY_DATE - timedelta(days=101)).strftime('%Y-%m-%d')))
        log_written_data(context, date=(TODAY_DATE - timedelta(days=101)).strftime('%Y-%m-%d'), 
                         start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                         op_name=op_name, table_name='click_price_change', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False) 
        return True
    except:
        raise Failure(description=f"{op_name} error")
    
    
@op(out=Out(bool))
def prc_delete_old_data_cloudflare_log_data_op(context, _prev_result) -> bool:
    """
    Calls the DWH procedure affiliate.delete_old_data_cloudflare_log_data(current_date - 1) 
    to delete data which is older than 210 days in the affiliate.cloudflare_log_data table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_cloudflare_log_data(\'{}\')'.format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='cloudflare_log_data', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)   
        return True  
    except:
        raise Failure(description=f"{op_name} error")


@op
def prc_delete_old_data_billable_closed_job_clicks_op(context, _prev_result):
    """
    Calls the DWH procedure affiliate.delete_old_data_billable_closed_job_clicks(current_date_diff - 1) 
    to delete data which is older than 90 days in the affiliate.billable_closed_job_clicks table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, 'delete_old_data_billable_closed_job_clicks(\'{}\')'.format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='billable_closed_job_clicks', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
    except:
        raise Failure(description=f"{op_name} error")


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'delete_old_data',
     description=f'Delete old data from the tables in affiliate schema: api_request_history, job_stat_agg, '
                 f'job_stat_agg_category, job_stat_agg_abtest, click_price_change, job_in_feed_log, '
                 f'short_click_price_change, short_job_in_feed_log, cloudflare_log_data, '
                 f'billable_closed_job_clicks.'
                 f'Transfer data from the tables in from affiliate schema to archive: '
                 f'click_price_change, job_in_feed_log, short_click_price_change, short_job_in_feed_log. '
                 f'Delete old data from the tables in archive schema: click_price_change, '
                 f'job_in_feed_log, short_click_price_change, short_job_in_feed_log'
     )
def delete_old_data_job():
    api_request_history_result = prc_delete_old_data_api_request_history_op()
    job_stat_agg_result = prc_delete_old_data_job_stat_agg_op(api_request_history_result)
    transfer_short_table_result = prc_transfer_old_data_short_table_op(job_stat_agg_result)
    delete_short_table_result = prc_delete_old_data_short_table_op(transfer_short_table_result)
    delete_short_archive_table_result = prc_delete_old_data_short_archive_table_op(delete_short_table_result)
    transfer_full_table_result = prc_transfer_old_data_full_table_op(delete_short_archive_table_result)
    delete_full_table_result = prc_delete_old_data_full_table_op(transfer_full_table_result)
    delete_full_archive_table_result = prc_delete_old_data_full_archive_table_op(delete_full_table_result)
    delete_old_data_cloudflare_log_data = prc_delete_old_data_cloudflare_log_data_op(delete_full_archive_table_result)
    prc_delete_old_data_billable_closed_job_clicks_op(delete_old_data_cloudflare_log_data)
