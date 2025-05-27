import inspect
import pandas as pd
from datetime import datetime, timedelta
from dagster import op, job, Failure, Field, make_values_resource, fs_io_manager, Out

from utility_hub.core_tools import get_creds_from_vault
from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.aff_job_config import job_config
from etl_affiliate.utils.tableau_api_utils import get_task_id, run_extract_refresh
from etl_affiliate.utils.aff_job_config import retry_policy
from etl_affiliate.utils.utils import (
    log_written_data,
    send_dwh_alert_slack_message,
    job_prefix,
    call_dwh_procedure,
    create_conn, 
    exec_query_pg
)

SCHEMA = 'affiliate'
JOB_PREFIX = job_prefix()
TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


def generate_data_completeness_alert_query(title: str, link: str) -> str:
    return f"""
            SELECT 
                ':incident: *<{link}|{title}>*' ||
                CHR(10) || CHR(10) || 
                STRING_AGG(text, CHR(10)) || CHR(10) || CHR(10) ||
                '<@U06E7T8LP8X>, <@U018GESMPDJ>' AS text
            FROM (
                SELECT 
                    ':black_small_square: *' || COALESCE(metric_name, check_type) || ':* ' ||
                    STRING_AGG(DISTINCT table_name, ', ' ORDER BY table_name) AS text
                FROM affiliate.data_completeness_report
                WHERE is_incomplete = true 
                GROUP BY metric_name, check_type
            ) AS a
            """


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_data_completeness_report_actions_op(context) -> bool:
    """
    Calls DWH procedure affiliate.insert_data_completeness_report_actions() to insert
    action data to the DWH table affiliate.data_completeness_report.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    try:
        call_dwh_procedure(SCHEMA, 'insert_data_completeness_report_actions()')
        log_written_data(context, date=TARGET_DATE_STR, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='data_completeness_report', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)

        if send_slack_info:
            send_dwh_alert_slack_message(":done-1: *prc_insert_data_completeness_report_actions_op* is completed.")
        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_data_completeness_report_feeds_op(context) -> bool:
    """
    Calls DWH procedure affiliate.insert_data_completeness_report_feeds() to insert
    action data to the DWH table affiliate.data_completeness_report.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    try:
        call_dwh_procedure(SCHEMA, 'insert_data_completeness_report_feeds()')
        log_written_data(context, date=TARGET_DATE_STR, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='data_completeness_report', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)

        if send_slack_info:
            send_dwh_alert_slack_message(":done-1: *prc_insert_data_completeness_report_feeds_op* is completed.")
        return True 
    except:
        raise Failure(description=f"{op_name} error")
    

@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_analyze_gp_segment_distribution_op(context, _prev_result1, _prev_result2) -> bool:
    """
    Calls DWH procedure affiliate.analyze_gp_segment_distribution() to insert
    action data to the DWH table affiliate.segment_distribution.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    try:
        call_dwh_procedure(SCHEMA, 'analyze_gp_segment_distribution()', 'cloudberry')
        log_written_data(context, date=TARGET_DATE_STR, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='segment_distribution', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)

        if send_slack_info:
            send_dwh_alert_slack_message(":done-1: *prc_analyze_gp_segment_distribution_op* is completed.")
        return True 
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"},
    retry_policy=retry_policy)
def run_data_completeness_tableau_refresh_op(context, _prev_result1):
    """
    Runs "Affiliate data completeness alert" workbook extract refresh in Tableau using Tableau REST API.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        task_id = get_task_id(workbook_name="Affiliate data completeness alert", datasource_name=None)
        run_extract_refresh(task_id)
        log_written_data(context=context, date=TARGET_DATE_STR, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False, write_to_context=True, dagster_log_schema=SCHEMA)

        if send_slack_info:
            send_dwh_alert_slack_message(
                ":tableau: *Affiliate data completeness alert* workbook refresh was launched.")
        
        text_info_dwh = pd.read_sql(generate_data_completeness_alert_query(
                                        title="Data completeness alert in DWH",
                                        link="https://tableau.jooble.com/#/views/Affiliatedatacompletenessalert/Affiliatedatacompletenessconsistencyalert?:iid=1"
                                    ), con=create_conn("dwh"))
        
        if not text_info_dwh.isnull().values.any():
            send_dwh_alert_slack_message(text_info_dwh.iloc[0, 0])
            
        text_info_cb = pd.read_sql(generate_data_completeness_alert_query(
                                        title="Data completeness alert in Cloudberry",
                                        link="https://tableau.jooble.com/#/views/amo-newdwhAffiliatedatacompletenessalert/Affiliatedatacompletenessconsistencyalert?:iid=1"
                                    ), con=create_conn("cloudberry"))
        
        if not text_info_cb.isnull().values.any():
            send_dwh_alert_slack_message(text_info_cb.iloc[0, 0])

        
    except:
        if send_slack_info:
            send_dwh_alert_slack_message(
                ":error_alert: *Affiliate data completeness alert* workbook refresh has failed. "
                "<@U06E7T8LP8X>, <@U018GESMPDJ>")
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_get_discrepancy_cb_pg_op(context, _prev_result1) -> bool:
    """
    Calls DWH procedure affiliate.get_discrepancy_cb_pg(varchar, varchar) to check
    discrepancy between old dwh and new cloudberry.
    """
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    try:
        exec_query_pg("select * from {schema}.get_discrepancy_cb_pg('{login}'::varchar, '{password}'::varchar)".format(
            schema=SCHEMA, 
            login=get_creds_from_vault('AFF_USER'), 
            password=get_creds_from_vault('AFF_PASSWORD')), "dwh", "dwh")
        log_written_data(context, date=TARGET_DATE_STR, start_dt=start_dt,
                         end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name,
                         table_name='get_discrepancy_cb_pg', table_schema=SCHEMA, send_slack_message=False,
                         write_to_context=False)

        if send_slack_info:
            send_dwh_alert_slack_message(":done-1: *prc_get_discrepancy_cb_pg_op* is completed.")
        return True 
    except:
        raise Failure(description=f"{op_name} error")
    

@job(
    resource_defs={
        "globals": make_values_resource(
            send_slack_info=Field(bool, default_value=True),
        ),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    config=job_config,
    name=JOB_PREFIX + "data_completeness",
    description=f"Collects data on the completeness of the affiliate schema."
                f"Refresh *Affiliate data completeness alert* workbook in Tableua."
                f"Check segment distribution and compare data in cb and pg",
)
def data_completeness_job():
    result_dcr_actions = prc_insert_data_completeness_report_actions_op()
    result_dcr_feeds = prc_insert_data_completeness_report_feeds_op()
    result_segment_distribution = prc_analyze_gp_segment_distribution_op(result_dcr_actions, result_dcr_feeds)
    run_data_completeness_tableau_refresh_op(result_segment_distribution)
    prc_get_discrepancy_cb_pg_op(result_segment_distribution)
    