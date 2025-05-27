import inspect
import requests
import pandas as pd
from dagster import op, Out, Failure, DynamicOut, DynamicOutput
from datetime import datetime

from etl_affiliate.utils.utils import log_written_data, send_dwh_alert_slack_message, create_conn, \
    exec_query_pg, exec_query_ms, get_file_path, get_gitlab_file_content, call_dwh_procedure
from etl_affiliate.utils.aff_job_config import retry_policy
from etl_affiliate.utils.tableau_api_utils import get_task_id, run_extract_refresh
from etl_affiliate.utils.job_xx_hosts import COUNTRY_CODE_TO_ID
from utility_hub.core_tools import get_creds_from_vault


def clear_cpc_optimizer_cache() -> None:
    """
    Clears cache of the cpc ratio optimizer.
    """
    url_us = "http://us-nginx1.jooble.com/affiliate-cpc-optimizer-service/prod/api/calculate/clean-cache"
    url_nl = "http://nl-internal-nginx1.jooble.com/affiliate-cpc-optimizer-service/prod/api/calculate/clean-cache"
    for _ in range(10):
        _r = requests.get(url_us)
        _r = requests.get(url_nl)


def issues_after_check_row_comparison(schema_name: str, target_date_str: str, destination_db: str) -> list:
    if destination_db == "dwh":
        df_total = pd.read_sql("select * from {}.get_row_cnt_comparison('{}')".format(schema_name, target_date_str),
                               con=create_conn("dwh"))
    elif destination_db == "cloudberry":
        df_total = pd.read_sql("select * from {}.get_row_cnt_comparison('{}')".format(schema_name, target_date_str),
                               con=create_conn("cloudberry"))

    issue_list = [tuple(x) for x in
                  df_total[df_total["row_cnt_aff"] != df_total["row_cnt_dwh"]][["table_name", "source"]].values]

    return issue_list


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_partner_settings_op(context) -> bool:
    """
    Calls insert_partner_settings procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_partner_settings({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="partner_settings", date=target_date_str,
                         start_dt=start_dt, end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=send_slack_info)
        return True
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_project_cpc_ratio_op(context, prev_result) -> bool:
    """
    Calls insert_project_cpc_ratio procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_project_cpc_ratio({})".format(target_date_diff),
                           destination_db=destination_db)
        log_written_data(context=context, table_schema=schema_name, table_name="project_cpc_ratio",
                         date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         op_name=op_name, send_slack_message=send_slack_info)

        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def transfer_dic_manager_op(context, prev_result) -> bool:
    """
    Collects data from Soska database to the dic_manager table.
    """
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        manager_q = get_gitlab_file_content(file_path=get_file_path(dir_name="affiliate",
                                                                    file_name="dic_manager.sql"))
        managers_df = exec_query_ms(manager_q, "soska")
        exec_query_pg("delete from {schema}.{table}".format(
            schema=schema_name, table="dic_manager"), "dwh")
        # Postgres dwh
        managers_df.to_sql(name="dic_manager",
                           schema=schema_name,
                           con=create_conn("dwh"),
                           if_exists="append",
                           index=False)

        # Cloudberry dwh
        managers_df.to_sql(name="dic_manager",
                           schema=schema_name,
                           con=create_conn("cloudberry"),
                           if_exists="append",
                           index=False)

        log_written_data(context=context, table_schema=schema_name, table_name="dic_manager", date=target_date_str,
                         start_dt=start_dt, end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def func_get_row_cnt_comparison_op(context, _prev_result) -> bool:
    """
    Compare row counts in the prod and DWH tables for target date.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]
    check_row_cnt = context.resources.globals["check_row_cnt"]
    op_name = inspect.currentframe().f_code.co_name

    if check_row_cnt:

        issue_list = issues_after_check_row_comparison(schema_name, target_date_str, destination_db=destination_db)

        processed_issues = []

        for row in issue_list:
            if row[0] in ("job_stat_agg", "click_price_change", "job_in_feed_log"):
                if row[0] not in processed_issues:
                    if send_slack_info:
                        send_dwh_alert_slack_message(
                            f":error_alert: Data completeness check for *{row[0]}* has failed. \n"
                            f"The data reloading has started.")

                    if row[0] == "job_stat_agg":
                        call_dwh_procedure(schema_name, "insert_job_stat_agg('{}')".format(target_date_str),
                                           destination_db=destination_db)

                    if row[0] == "click_price_change":
                        call_dwh_procedure(
                            schema_name,
                            "insert_click_price_change_missing_rows('{date}', '{login}', '{password}')".format(
                                date=target_date_str, login=get_creds_from_vault('AFF_USER'),
                                password=get_creds_from_vault('AFF_PASSWORD')),
                            destination_db=destination_db)

                    if row[0] == "job_in_feed_log":
                        call_dwh_procedure(
                            schema_name,
                            "insert_job_in_feed_log_missing_rows('{date}', '{login}', '{password}')".format(
                                date=target_date_str, login=get_creds_from_vault('AFF_USER'),
                                password=get_creds_from_vault('AFF_PASSWORD')),
                            destination_db=destination_db)

                    processed_issues.append(row[0])

        issue_list = issues_after_check_row_comparison(schema_name, target_date_str, destination_db=destination_db)

        processed_issues = []

        for row in issue_list:
            if row[0] in ("click_price_change", "job_in_feed_log"):
                if row[0] not in processed_issues:
                    if send_slack_info:
                        send_dwh_alert_slack_message(
                            f":error_alert: Data completeness check for *{row[0]}* has failed again. \n"
                            f"The data reloading has started in short table.")

                    if row[0] == "click_price_change":
                        call_dwh_procedure(schema_name,
                                           "insert_short_click_price_change_all_scheduler_run({date})".format(
                                               date=target_date_diff),
                                           destination_db=destination_db)

                    if row[0] == "job_in_feed_log":
                        call_dwh_procedure(schema_name,
                                           "insert_short_job_in_feed_log_scheduler_run({date})".format(
                                               date=target_date_diff),
                                           destination_db=destination_db)

                    processed_issues.append(row[0])

        issue_list = issues_after_check_row_comparison(schema_name, target_date_str, destination_db=destination_db)

        if len(issue_list) != 0:
            if send_slack_info:
                send_dwh_alert_slack_message(
                    f":x: *affiliate-agg-script ({target_date_str}) stopped* <!subteam^S02ETK2JYLF|dwh.analysts> "
                    f"<@U018GESMPDJ>, <@U06E7T8LP8X> "
                    f"There were noticed issues with data transferred from feed generator: `{issue_list}`"
                )
            raise Failure(
                description=f"There were noticed issues with data transferred from feed generator: `{issue_list}`"
            )

    context.log.info(f"{op_name} successfully executed")
    log_written_data(context=context, date=target_date_str, start_dt=start_dt,
                     end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                     send_slack_message=False, write_to_context=False, dagster_log_schema=schema_name)
    return True


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_optimal_cpc_ratio_op(context, prev_result: bool) -> bool:
    """
    Calls insert_optimal_cpc_ratio procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]
    op_name = inspect.currentframe().f_code.co_name

    try:
        call_dwh_procedure(schema_name, "insert_optimal_cpc_ratio({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="optimal_cpc_ratio",
                            date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            op_name=op_name, send_slack_message=send_slack_info)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=DynamicOut(),
    required_resource_keys={"globals"})
def collect_temp_prod_click_op(context, _prev_result) -> dict:
    """
    Created DynamicOutput object to collect data from prod databases to the temp_prod_click table.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_date_diff = context.resources.globals["target_date_diff"]
    schema_name = context.resources.globals["schema_name"]
    op_name = inspect.currentframe().f_code.co_name

    try:
        q_country_list = 'select distinct country from affiliate.partner_settings'
        country_list = pd.read_sql(q_country_list, con=create_conn("dwh"))["country"].to_list()

        exec_query_pg("delete from {schema}.{table}".format(schema=schema_name, table="temp_prod_click"), "dwh")
        context.log.info("deleted from {schema}.{table}".format(schema=schema_name, table="temp_prod_click"))

        click_q = get_gitlab_file_content(
            file_path=get_file_path(dir_name="affiliate",
                                    file_name="temp_prod_click.sql")).format(date_diff=target_date_diff)

        for country in country_list:
            yield DynamicOutput(
                value={
                    "country": str(country).upper(),
                    "click_q": click_q,
                    "start_dt": start_dt
                },
                mapping_key="Job_" + country
            )
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(tuple),
    retry_policy=retry_policy)
def run_temp_prod_click_op(context, prod_query_info: dict) -> tuple:
    """
    Collects click data from prod databases.

    Args:
        context: Dagster run context object.
        prod_query_info (dict): dictionary with specified country and query to run, as well as start datetime of the
            previous step.

    Returns:
        tuple:
            pd.DataFrame: collected data.
            str:  start datetime of the previous step.
    """
    result_df = exec_query_ms(
        prod_query_info["click_q"],
        "prod",
        prod_query_info["country"]
    )
    context.log.info(f"success for {prod_query_info['country']}")
    return result_df, prod_query_info["start_dt"]


@op(out=Out(bool),
    required_resource_keys={"globals"})
def save_temp_prod_click_op(context, prev_step_results: list) -> bool:
    """
    Inserts collected data to temp_prod_click table.

    Args:
        context: Dagster run context object.
        prev_step_results (list): list of tuples with collected data and start datetime of data collection.

    Returns:
        bool: result of the operation to pass to the next step.
    """
    destination_db = context.resources.globals["destination_db"]
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    op_name = inspect.currentframe().f_code.co_name
    try:
        df = pd.concat([x[0] for x in prev_step_results])

        if destination_db == "dwh":
            # Postgres dwh
            df.to_sql(schema=schema_name,
                      name="temp_prod_click",
                      if_exists="append",
                      con=create_conn("dwh"),
                      index=False)

        if destination_db == "cloudberry":
            # Cloudberry dwh
            df.to_sql(schema=schema_name,
                      name="temp_prod_click",
                      if_exists="append",
                      con=create_conn("cloudberry"),
                      index=False)

        log_written_data(context=context, table_schema=schema_name, table_name="temp_prod_click",
                         date=target_date_str, start_dt=[x[1] for x in prev_step_results][0],
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False)
        return True
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_partner_daily_snapshot_op(context, prev_result1, prev_result2) -> bool:
    """
    Calls insert_partner_daily_snapshot procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_partner_daily_snapshot({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="partner_daily_snapshot",
                            date=target_date_str, start_dt=start_dt,
                            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            send_slack_message=send_slack_info, op_name=op_name)
        return prev_result1 and prev_result2
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_statistics_daily_raw_op(context, prev_result) -> bool:
    """
    Calls insert_statistics_daily_raw procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_statistics_daily_raw({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="statistics_daily_raw",
                            date=target_date_str, start_dt=start_dt,
                            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            send_slack_message=send_slack_info, op_name=op_name)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_click_data_op(context, prev_result) -> bool:
    """
    Materialize v_click_data into affiliate.click_data.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    # target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        # Cloudberry: materialise affiliate.v_click_data
        call_dwh_procedure(schema_name, "insert_click_data()", destination_db="cloudberry")

        log_written_data(context=context, table_schema=schema_name, table_name="statistics_daily_raw",
                         date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         send_slack_message=send_slack_info, op_name=op_name)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_insert_statistics_daily_agg_op(context, prev_result) -> bool:
    """
    Calls insert_statistics_daily_agg procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name,
                           "insert_statistics_daily_agg({target_date_diff}, {target_date_diff})".format(
                               schema_name=schema_name,
                               target_date_diff=target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="statistics_daily_agg",
                            date=target_date_str, start_dt=start_dt, send_slack_message=send_slack_info,
                            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def prc_insert_prediction_features_v2_op(context, _prev_result):
    """
    Calls insert_prediction_features_v2 procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_prediction_features_v2({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="prediction_features_v2",
                            date=target_date_str, start_dt=start_dt,
                            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                            send_slack_message=send_slack_info)
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def prc_insert_abtest_stat_agg_op(context, _prev_result):
    """
    Calls insert_abtest_stat_agg procedure.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "insert_abtest_stat_agg({})".format(target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name="abtest_stat_agg",
                            date=target_date_str, start_dt=start_dt,
                            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                            send_slack_message=send_slack_info)
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=DynamicOut(),
    required_resource_keys={"globals"})
def collect_temp_bot_click_op(context) -> dict:
    """
    Create DynamicOutput object to collect data from prod databases to the temp_bot_click table.
    """
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_date_diff = context.resources.globals["target_date_diff"] - 1

    try:
        q_country_list = 'select distinct country from affiliate.partner_settings'
        country_list = pd.read_sql(q_country_list, con=create_conn("dwh"))["country"].to_list()
        collect_bot_q = get_gitlab_file_content(
            file_path=get_file_path(dir_name="affiliate",
                                    file_name="temp_bot_click.sql")).format(date_diff=target_date_diff)

        for country in country_list:
            yield DynamicOutput(
                value={
                    "country": str(country).upper(),
                    "bot_q": collect_bot_q,
                    "start_dt": start_dt
                },
                mapping_key="Job_" + country
            )
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(out=Out(tuple),
    retry_policy=retry_policy)
def run_temp_bot_click_op(context, prod_query_info: dict) -> tuple:
    """
    Collects bot click data from prod databases.

    Args:
        context: Dagster run context object.
        prod_query_info (dict): dictionary with specified country and query to run, as well as start datetime of the
            previous step.

    Returns:
        tuple:
            pd.DataFrame: collected data.
            str:  start datetime of the previous step.
    """
    result_df = exec_query_ms(
        prod_query_info["bot_q"],
        "prod",
        prod_query_info["country"]
    )
    context.log.info(f"success for {prod_query_info['country']}")
    return result_df, prod_query_info["start_dt"]


@op(out=Out(bool),
    required_resource_keys={"globals"})
def save_temp_bot_click_op(context, prev_step_results: list):
    """
    Inserts collected data to temp_bot_click table.

    Args:
        context: Dagster run context object.
        prev_step_results (list): list of tuples with collected data and start datetime of data collection.

    Returns:
        bool: result of the operation to pass to the next step.
    """
    destination_db = context.resources.globals["destination_db"]
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    op_name = inspect.currentframe().f_code.co_name
    try:
        country_dict = pd.read_sql(
            "select id, lower(alpha_2) as country from dimension.countries",
            create_conn("dwh"),
            index_col="country"
        ).to_dict()["id"]

        df = pd.concat([x[0] for x in prev_step_results])
        df["country"] = df["country"].map(country_dict)

        if destination_db == "dwh":
            # Postgres dwh
            df.to_sql(schema=schema_name,
                      name="temp_bot_click",
                      if_exists="append",
                      con=create_conn("dwh"),
                      index=False)

        if destination_db == "cloudberry":
            # Cloudberry dwh
            df.to_sql(schema=schema_name,
                      name="temp_bot_click",
                      if_exists="append",
                      con=create_conn("cloudberry"),
                      index=False)

        log_written_data(context=context, table_schema=schema_name, table_name="temp_bot_click",
                         date=target_date_str, start_dt=[x[1] for x in prev_step_results][0],
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False)
        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_update_bot_clicks_op(context, prev_result, _prev_result2) -> bool:
    """
    Calls update_bot_clicks procedure to update bot clicks in the statistics_daily_raw table for the day
    before yesterday and update the statistics_daily_agg table for the same day.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]

    try:
        call_dwh_procedure(schema_name, "update_bot_clicks({})".format(target_date_diff),
                           destination_db=destination_db)
        log_written_data(context=context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False, write_to_context=True, dagster_log_schema=schema_name)
        return prev_result
    except Exception:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def prc_update_conversions_op(context, _prev_result) -> bool:
    """
    Calls update_conversions procedure to update conversions in the statistics_daily_raw and statistics_daily_agg tables
    for the last 30 days.
    """
    destination_db = context.resources.globals["destination_db"]
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, "update_conversions({})".format(target_date_diff),
                           destination_db=destination_db)
        log_written_data(context=context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False, write_to_context=True, dagster_log_schema=schema_name)

        if send_slack_info:
            send_dwh_alert_slack_message(":done-1: *prc_update_conversions_op* is completed.")

        return True
    except Exception:
        raise Failure(description=f"{op_name} error")


def transfer_external_applies(date_diff: int, schema_name: str) -> None:
    """
    Collects data from Job_XX to {schema_name}.external_apply_raw table for the date_diff for each country in
        COUNTRY_LIST.

    Args:
        date_diff (int): date_diff to collect data for.
        schema_name (str): DWH schema to insert data to.
    """
    q_country_list = 'select distinct country from affiliate.partner_settings'
    country_list = pd.read_sql(q_country_list, con=create_conn("dwh"))["country"].to_list()

    applies_q = get_gitlab_file_content(
        file_path=get_file_path(dir_name="affiliate", file_name="external_apply_raw.sql"))

    exec_query_pg('delete from {schema}.external_apply_raw where date_diff = {target_date_diff}'.format(
        schema=schema_name, target_date_diff=date_diff), 'dwh')

    for country in country_list:
        df = exec_query_ms(applies_q.format(target_date_diff=date_diff), 'prod', country.upper())
        df['country_id'] = COUNTRY_CODE_TO_ID[country.upper()]
        # Postgres dwh
        df.to_sql(name='external_apply_raw',
                  schema=schema_name,
                  con=create_conn('dwh'),
                  if_exists='append',
                  index=False)

        # Cloudberry dwh
        df.to_sql(name='external_apply_raw',
                  schema=schema_name,
                  con=create_conn('cloudberry'),
                  if_exists='append',
                  index=False)


@op(required_resource_keys={'globals'})
def transfer_external_applies_op(context) -> bool:
    """
    Collects data from Job_XX to affiliate.external_apply_raw table for the target_date_diff.
    """
    destination_db = context.resources.globals["destination_db"]
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]

    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]

    transfer_external_applies(target_date_diff, schema_name)

    log_written_data(context=context, table_schema=schema_name, table_name='external_apply_raw',
                        date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        op_name=op_name, send_slack_message=send_slack_info)
    return True


@op(required_resource_keys={"globals"},
    retry_policy=retry_policy)
def run_tableau_refresh_op(context, _prev_result: bool) -> bool:
    """
    Runs "Affiliate data" datasource extract refresh in Tableau using Tableau REST API.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        task_id = get_task_id(workbook_name=None, datasource_name="Affiliate data")
        run_extract_refresh(task_id)
        log_written_data(context=context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False, write_to_context=True, dagster_log_schema=schema_name)

        if send_slack_info:
            send_dwh_alert_slack_message(":tableau: *Affiliate data* datasource refresh was launched.")
        
        return True
    except Exception:
        if send_slack_info:
            send_dwh_alert_slack_message(":error_alert: *Affiliate data* datasource refresh has failed. "
                                         "<@U06E7T8LP8X>, <@U018GESMPDJ>")
        raise Failure(description=f"{op_name} error")
    
    
@op(required_resource_keys={'globals'})
def prc_insert_fixed_costs(context, prev_result, prev_result2) -> bool:
    """
    Calls DWH procedure affiliate.insert_fixed_costs() to calculate and insert 
    affiliate.fixed_costs.
    """
    destination_db = context.resources.globals["destination_db"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        call_dwh_procedure(schema_name, 'insert_fixed_costs()', destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name='fixed_costs',
                            date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            op_name=op_name, send_slack_message=send_slack_info)
        return prev_result 
    except Exception:
        raise Failure(description=f"{op_name} error")
    

@op(required_resource_keys={'globals'})
def refresh_mv_summarized_metrics_op(context, prev_result) -> bool:
    """
    Refresh materialized view mv_summarized_metrics in target schema.
    
    Args:
        context: Dagster run context object.
        prev_result: result of the previous operation.
    """

    destination_db = context.resources.globals["destination_db"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]

    exec_query_pg(('refresh materialized view {target_schema}.mv_summarized_metrics;'.format(
        target_schema=schema_name)), host='dwh', destination_db=destination_db)

    log_written_data(context=context, table_schema=schema_name, table_name='mv_summarized_metrics',
                     date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     op_name=op_name, send_slack_message=send_slack_info)
    return prev_result


@op(required_resource_keys={'globals'})
def prc_insert_billable_closed_job_clicks(context, prev_result) -> bool:
    """
    Calls DWH procedure affiliate.insert_billable_closed_job_clicks() to calculate and insert 
    affiliate.billable_closed_job_clicks.
    """
    destination_db = context.resources.globals["destination_db"]
    target_date_str = context.resources.globals["target_date_str"]
    target_date_diff = context.resources.globals["target_date_diff"]
    schema_name = context.resources.globals["schema_name"]
    
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    send_slack_info = context.resources.globals["send_slack_info"]
    
    try:
        call_dwh_procedure(schema_name, 'insert_billable_closed_job_clicks({date_diff})'.format(date_diff=target_date_diff),
                           destination_db=destination_db)

        log_written_data(context=context, table_schema=schema_name, table_name='billable_closed_job_clicks',
                            date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            op_name=op_name, send_slack_message=send_slack_info)
        return True
    except Exception:
        raise Failure(description=f"{op_name} error")
    
