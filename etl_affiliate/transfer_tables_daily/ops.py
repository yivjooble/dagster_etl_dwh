import inspect
from datetime import datetime, timedelta
from dagster import op, job, fs_io_manager, Out, Failure, Field, make_values_resource

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, log_written_data, call_dwh_procedure, send_dwh_alert_slack_message

SCHEMA = 'affiliate'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
BEFORE_YESTERDAY_DATE = (datetime.now().date() - timedelta(2)).strftime('%Y-%m-%d')
YESTERDAY_DATE_DIFF = ((datetime.now() - timedelta(days=1)) - datetime(1900, 1, 1)).days
JOB_PREFIX = job_prefix()
PRC_DICT = {
    'prc_insert_job_stat_agg_op': 'insert_job_stat_agg(\'{}\')',
    'prc_insert_job_in_feed_log_op': 'insert_job_in_feed_log()',
    'prc_insert_click_price_change_op': 'insert_click_price_change()',
    'prc_insert_short_job_in_feed_log_scheduler_run_op': 'insert_short_job_in_feed_log_scheduler_run({})',
    'prc_insert_short_click_price_change_scheduler_run_op': 'insert_short_click_price_change_all_scheduler_run({})',
    'prc_insert_ab_history_op': 'insert_ab_history({})',
    'prc_insert_api_request_history_op': 'insert_api_request_history({})',
    'prc_insert_api_request_stat_agg_op': 'insert_api_request_stat_agg(\'{}\')',
    'prc_insert_task_status_log_op': 'insert_task_status_log({})',
    'prc_insert_partner_settings_change_log_op': 'insert_partner_settings_change_log({})',
    'prc_insert_project_cpc_ratio_change_log_op': 'insert_project_cpc_ratio_change_log({})',
    'prc_insert_functionality_matrix_op': 'insert_functionality_matrix()',
    'prc_insert_cpa_static_settings_log_op': 'insert_cpa_static_settings_log({})',
    'prc_insert_underutilised_budget_category_op': 'insert_underutilised_budget_category()',
    'prc_insert_underutilised_budget_category_log_op': 'insert_underutilised_budget_category_log(\'{}\')',
    'prc_insert_partner_settings_op1': 'insert_partner_settings({})',
    'prc_insert_job_stat_agg_daily_op': 'insert_job_stat_agg_daily()',
    'prc_insert_metaverse_data_op': 'insert_metaverse_data({})'
    
}


@op(out=Out(bool))
def prc_insert_job_stat_agg_op(context, _prev_result):
    """
    Calls DWH procedure affiliate.insert_job_stat_agg(current_date - 1) to transfer yesterday data from the
    affiliate_stat table (10.0.0.137 server) to the DWH table affiliate.job_stat_agg.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE))
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(BEFORE_YESTERDAY_DATE))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='job_stat_agg', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return True
    except:
        raise Failure


@op
def prc_insert_job_stat_agg_daily_op(context, _prev_result):
    """
    Calls DWH procedure affiliate.insert_job_stat_agg_daily() to transfer last 5 days data from the
    affiliate.job_stat_agg, affiliate.job_stat_agg_category to the DWH table affiliate.job_stat_agg_daily
    and delete data older than 90 days.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='job_stat_agg_daily', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_job_in_feed_log_op(context):
    """
    Calls DWH procedure affiliate.insert_job_in_feed_log() to identify new records in the job_in_feed_log table
    (affiliate db) by max record ID in NL & US servers and transfer them to the DWH table affiliate.job_in_feed_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='job_in_feed_log', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return True
    except Exception:
        raise Failure from Exception


@op(out=Out(bool))
def prc_insert_click_price_change_op(context):
    """
    Calls DWH procedure affiliate.insert_click_price_change() to identify new records in the click_price_change table
    (affiliate db) by max record ID in NL & US servers and transfer them to the DWH table affiliate.click_price_change.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='click_price_change', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return True
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_short_job_in_feed_log_scheduler_run_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_short_job_in_feed_log_scheduler_run to transfer yesterday data from the
    affiliate.job_in_feed_log to the affiliate.short_job_in_feed_log table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='short_job_in_feed_log', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_short_click_price_change_scheduler_run_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_short_click_price_change_scheduler_run to transfer yesterday data from the
    affiliate.click_price_change to the affiliate.short_click_price_change table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='short_click_price_change', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op
def prc_insert_ab_history_op(context):
    """
    Calls DWH procedure affiliate.insert_ab_history to transfer yesterday data from the
    ab_history table (affiliate databases) to the DWH table affiliate.ab_history.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='ab_history', table_schema=SCHEMA, send_slack_message=False, write_to_context=False)
        return True
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_task_status_log_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_task_status_log to transfer yesterday data from the
    task_status_log table (affiliate databases) to the DWH table affiliate.task_status_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='task_status_log', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_partner_settings_change_log_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_partner_settings_change_log to transfer yesterday data from the
    partner_settings_change_log table (affiliate databases) to the DWH table affiliate.partner_settings_change_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='partner_settings_change_log', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_functionality_matrix_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_functionality_matrix() to rewrite data in the DWH table
    affiliate.functionality_matrix using data from the functionality_matrix and functionality tables in the affiliate NL
    database.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='functionality_matrix', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(out=Out(bool))
def prc_insert_project_cpc_ratio_change_log_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_project_cpc_ratio_change_log to transfer yesterday data from the
    partner_settings_change_log table (affiliate databases) to the DWH table affiliate.partner_settings_change_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='project_cpc_ratio_change_log', table_schema=SCHEMA,
                         send_slack_message=False, write_to_context=False)
        return prev_result
    except:
        raise Failure


@op
def prc_insert_api_request_history_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_api_request_history to transfer yesterday data from the
    api_request_history table (affiliate databases) to the DWH table affiliate.api_request_history.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='api_request_history', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op
def prc_insert_api_request_stat_agg_op(context, prev_result):
    """
    Calls the DWH procedure affiliate.insert_api_request_stat_agg(fn_get_date_diff(current_date - 1))
    to aggregate yesterday's data from affiliate.api_request_history table into the 
    affiliate.api_request_stat_agg table.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='api_request_stat_agg', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(required_resource_keys={"globals"})
def prc_insert_cpa_static_settings_log_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_cpa_static_settings_log to transfer yesterday data from the
    cpa_static table (affiliate databases) to the DWH table affiliate.cpa_static_settings_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='cpa_static_settings_log', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)

        if send_slack_info:
            send_dwh_alert_slack_message(":checkered_flag: Main job - *DONE*")
        return prev_result
    except:
        raise Failure


@op
def prc_insert_underutilised_budget_category_op(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_underutilised_budget_category() to transfer actual data 
    from the underutilised_budget_category table (affiliate databases) to the DWH table 
    affiliate.underutilised_budget_category.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name])
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='underutilised_budget_category', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False)
        return prev_result
    except:
        raise Failure


@op
def prc_insert_underutilised_budget_category_log_op(context, prev_result):
    """
    Calls the DWH procedure affiliate.insert_underutilised_budget_category_log(fn_get_date_diff(current_date - 1))
    to transfer yesterday data from the underutilised_budget_category_log table (affiliate databases) to the 
    DWH table affiliate.underutilised_budget_category_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='underutilised_budget_category_log', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False)
        return prev_result
    except:
        raise Failure


@op
def prc_insert_metaverse_data_op(context, prev_result):
    """
    Calls the DWH procedure affiliate.insert_metaverse_alerts(1) and affiliate.insert_metaverse_alerts_log(1)
    to transfer yesterday data from the metaverse.alert and metaverse.alert_log tables (affiliate databases) to the 
    DWH table affiliate.metaverse_alerts and affiliate.metaverse_alerts_log.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='metaverse_alerts', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False)
        return prev_result
    except:
        raise Failure


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_partner_settings_op1(context) -> bool:
    """
    Calls insert_partner_settings procedure.

    Args:
        context: Dagster run context object.

    Returns:
        bool: result of the operation to pass to the next step.
    """
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        call_dwh_procedure(SCHEMA, PRC_DICT[op_name].format(YESTERDAY_DATE_DIFF))
        log_written_data(context, date=YESTERDAY_DATE, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='partner_settings', table_schema=SCHEMA, 
                         send_slack_message=False, write_to_context=False)
        return True
    except:
        raise Failure(description=f"{op_name} error")


@job(resource_defs={"globals": make_values_resource(send_slack_info=Field(bool, default_value=True)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'transfer_tables_daily',
     description=f'Transfers new data from the affiliate databases to the DWH tables affiliate.job_in_feed_log,'
                 f'affiliate.click_price_change, affiliate.task_status_log, affiliate.ab_history, '
                 f'affiliate.api_request_history, api_request_stat_agg, affiliate.partner_settings_change_log, '
                 f'affiliate.project_cpc_ratio_change_log, affiliate.functionality_matrix, '
                 f'underutilised_budget_category, underutilised_budget_category_log, affiliate.metaverse_alerts, '
                 f'affiliate.metaverse_alerts_log. Transfers yesterday data '
                 f'from the affiliate.job_in_feed_log, affiliate.click_price_change to affiliate.short_job_in_feed_log '
                 f'and affiliate.short_click_price_change (DWH).'
                 f'Transfers new data to the DWH table affiliate.job_stat_agg from the 10.0.0.137 server.')
def transfer_tables_daily_job():
    partner_settings_result = prc_insert_partner_settings_op1()
    job_stat_agg_result = prc_insert_job_stat_agg_op(partner_settings_result)
    prc_insert_job_stat_agg_daily_op(job_stat_agg_result)

    jfd = prc_insert_job_in_feed_log_op()
    prc_insert_short_job_in_feed_log_scheduler_run_op(jfd)

    cpc = prc_insert_click_price_change_op()
    prc_insert_short_click_price_change_scheduler_run_op(cpc)

    ab = prc_insert_ab_history_op()
    tsl = prc_insert_task_status_log_op(ab)
    fm = prc_insert_functionality_matrix_op(tsl)
    pscl = prc_insert_partner_settings_change_log_op(fm)
    pcrcl = prc_insert_project_cpc_ratio_change_log_op(pscl)
    arh = prc_insert_api_request_history_op(pcrcl)
    arsa = prc_insert_api_request_stat_agg_op(arh)
    cssl = prc_insert_cpa_static_settings_log_op(arsa)
    ubc = prc_insert_underutilised_budget_category_op(cssl)
    ubcl = prc_insert_underutilised_budget_category_log_op(ubc)
    prc_insert_metaverse_data_op(ubcl)
