import inspect
from datetime import datetime, timedelta
from dagster import (
    op,
    job,
    fs_io_manager,
    Out,
    Failure,
    Field,
    make_values_resource
)

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, log_written_data, call_aff_procedure, call_dwh_procedure
from utility_hub.core_tools import get_creds_from_vault

SCHEMA = 'affiliate'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
JOB_PREFIX = job_prefix()

PRC_DICT = {
    'prc_check_feeds_logged_op': 'check_feeds_logged(\'{dt}\', \'pentaho\', \'!faP@jhWYWb&3rwv\')',
    'prc_insert_click_price_change_missing_rows_op':
        'insert_click_price_change_missing_rows(\'{dt}\', \'{user}\', \'{pw}\')',
    'prc_insert_job_in_feed_log_missing_rows_op':
        'insert_job_in_feed_log_missing_rows(\'{dt}\', \'{user}\', \'{pw}\')',
}


@op(required_resource_keys={"globals"})
def prc_check_feeds_logged_op(context):
    """
    call procedure public.check_feeds_logged(current_date - 1); in the affiliate databases to check if all yesterday
    feed gathers were logged already and send message to slack if it's not the case
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    target_date_str = context.resources.globals["target_date_str"]

    call_aff_procedure('public', PRC_DICT[op_name].format(dt=target_date_str), 'nl')
    call_aff_procedure('public', PRC_DICT[op_name].format(dt=target_date_str), 'us')
    log_written_data(context, date=target_date_str, start_dt=start_dt,
                     end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                     send_slack_message=False, write_to_context=False, dagster_log_schema=SCHEMA)


@op(out=Out(bool), required_resource_keys={"globals"})
def prc_insert_click_price_change_missing_rows_op(context):
    """
    Calls DWH procedure affiliate.insert_click_price_change_missing_rows that uses function
    public.check_load_click_price_change in the affiliate databases to identify missing rows in the click_price_change
    table (rows recorded yesterday that were not transferred to DWH) and transfer them to DWH tables
    affiliate.click_price_change and affiliate.short_click_price_change.
    """
    try:
        start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        op_name = inspect.currentframe().f_code.co_name
        target_date_str = context.resources.globals["target_date_str"]

        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(
            dt=target_date_str, user=get_creds_from_vault('AFF_USER'), pw=get_creds_from_vault('AFF_PASSWORD')))

        log_written_data(context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        log_written_data(context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='short_click_price_change', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True
    except:
        raise Failure


@op(out=Out(bool), required_resource_keys={"globals"})
def prc_insert_job_in_feed_log_missing_rows_op(context):
    """
    Calls DWH procedure affiliate.insert_job_in_feed_log_missing_rows that uses function
    public.check_load_job_in_feed_log in the affiliate databases to identify missing rows in the job_in_feed_log table
    (rows recorded yesterday that were not transferred to DWH) and transfer them to DWH tables affiliate.job_in_feed_log
    and affiliate.short_job_in_feed_log.
    """
    try:
        start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        op_name = inspect.currentframe().f_code.co_name
        target_date_str = context.resources.globals["target_date_str"]

        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(
            dt=target_date_str, user=get_creds_from_vault('AFF_USER'), pw=get_creds_from_vault('AFF_PASSWORD')))

        log_written_data(context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='job_in_feed_log', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        log_written_data(context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='short_job_in_feed_log', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return True
    except:
        raise Failure


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(target_date_str=Field(str, default_value=YESTERDAY_DATE))
                    },
     name=JOB_PREFIX + 'transfer_missing_rows',
     description=f'Identify and transfer to DWH missing rows for yesterday in the click_price_change and '
                 f'job_in_feed_log affiliate tables. Missing rows are transferred to both short and long versions of '
                 f'the tables.')
def transfer_missing_rows_job():
    prc_check_feeds_logged_op()
    prc_insert_click_price_change_missing_rows_op()
    prc_insert_job_in_feed_log_missing_rows_op()
