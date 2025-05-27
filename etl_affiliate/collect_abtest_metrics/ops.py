import inspect
from datetime import datetime, timedelta
from dagster import op, job, Failure, Field, make_values_resource, fs_io_manager

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.aff_job_config import job_config
from etl_affiliate.utils.tableau_api_utils import get_task_id, run_extract_refresh
from etl_affiliate.utils.aff_job_config import retry_policy
from etl_affiliate.utils.utils import (
    log_written_data,
    exec_query_pg,
    send_dwh_alert_slack_message,
    job_prefix
)
from etl_affiliate.utils.ab_test_utils import (
    get_data_for_calculate_metrics,
    calculate_ab_test_metrics,
    SETTINGS,
)

JOB_PREFIX = job_prefix()
TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_DIFF = (TARGET_DATE - datetime(1900, 1, 1)).days
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


def delete_abtest_metrics(target_datediff: int, schema_name: str) -> bool:
    """
    The function is designed to delete A/B testing metrics from the database for a
    specified target date. It deletes records whose end date is the same as the
    target date or one day earlier.

    Args:
        target_datediff (int): Target date difference.
        schema_name (str): The name of the schema in the database from which the records will be deleted.

    Returns:
        bool: constant True value, signaling that the records were successfully deleted.
        
    Note:
        This function remove old data in DB before updating the metrics.
    """
    target_date = (timedelta(days=target_datediff) + datetime(1900, 1, 1)).date()
    del_q = """
        delete from {schema}.abtest_significance_metrics 
        where date_end = \'{target_date}\'::date
            or date_end = \'{target_date}\'::date - 1""".format(
        schema=schema_name, target_date=target_date
    )
    exec_query_pg(del_q, "dwh")
    return True


@op(required_resource_keys={"globals"})
def delete_old_data_op(context) -> bool:
    """
    The operation is designed to remove old data from the abtest_significance_metrics
    table in the database based on a target date.

    Args:
        context: Dagster run context object.

    Returns:
        bool: constant True value, signaling that the records were successfully deleted.
    
    Notes:
        This operation utilizes the delete_abtest_metrics function to perform the deletion of records.
        It logs the operation's start and end datetime, along with other relevant information,
        to track its execution.
        In case of an exception, it raises a Dagster Failure with an appropriate error message.

    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]

    try:
        delete_abtest_metrics(target_date_diff, schema_name)
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )
        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def func_get_abtest_job_metrics_raw_op(context, _delete_old_data_result):
    """
    The operation is designed to retrieve A/B test job metrics from the database for further
    processing. It relies on the provided get_abtest_job_metrics_raw() function,
    which receives and prepares data for calculating A/B testing job metrics.
    It also handles metric cleaning by removing old data before updating the metrics.

    Args:
        context: Dagster run context object.
        _delete_old_data_result: The result from a prior operation in the pipeline that deletes old data.

    Returns:
        pandas.DataFrame: Returns a DataFrame containing the data fetched from the specified function for
        calculation job metrics.
        
    Notes:
        The operation calls the get_data_for_calculate_metrics function with parameters obtained from the
        global settings and the Dagster context.
        It formats the function name with the target date difference. 
        After fetching the data, it logs the operation's start and end time, alongside other relevant information.
        In case of an exception, a Dagster Failure is raised with a descriptive message.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]

    try:
        job_metrics_df = get_data_for_calculate_metrics(
            schema_name=schema_name,
            func_name=SETTINGS["job_metrics"]["function_name"].format(target_date_diff),
            full_reload_metric_list=SETTINGS["job_metrics"]["full_reload_metrics_list"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )
        return job_metrics_df
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def calc_job_metrics_op(context, job_metrics_df) -> bool:
    """
    Calculate job metrics and stores them in the database.

    Args:
        context: Dagster run context object.
        job_metrics_df (pandas.DataFrame): A DataFrame containing the data from
        the get_abtest_job_metrics_raw() function for which A/B testing job metrics will be calculated.

    Returns:
        bool: constant True value, indicating that the A/B testing job metrics were
        successfully calculated and the database was updated.
    
    Notes:
        This operation utilizes the calculate_ab_test_metrics function to perform the metric calculations
        and database updates. It uses global settings to determine the specific columns and metrics to be processed.
        After successfully calculating and updating the metrics, it logs the operation's execution
        details, including the start and end times, operation name, and target date. If an error occurs, a Dagster
        Failure is raised with an appropriate error message.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        calculate_ab_test_metrics(
            schema_name=schema_name,
            target_date=datetime.strptime(target_date_str, "%Y-%m-%d").date(),
            col_list=SETTINGS["job_metrics"]["column_list"],
            metric_list=SETTINGS["job_metrics"]["metrics_list"],
            df=job_metrics_df,
            full_reload_metric_list=SETTINGS["job_metrics"]["full_reload_metrics_list"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )

        if send_slack_info:
            send_dwh_alert_slack_message(":ab: *calc_job_metrics_op* has been successfully executed.")

        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def func_get_abtest_click_metrics_raw_op(context, _calc_job_metrics_result):
    """
    The operation is designed to retrieve A/B test click metrics from the database for further
    processing. It relies on the provided get_abtest_click_metrics_raw() function,
    which receives and prepares data for calculating A/B testing click metrics.
    It also handles metric cleaning by removing old data before updating the metrics.

    Args:
        context: Dagster run context object.
        _calc_job_metrics_result: The result from a prior operation in the pipeline .

    Returns:
        pandas.DataFrame: Returns a DataFrame containing the data fetched from the specified function for
        calculation percentage metrics.
    
    Notes:
        The operation calls the get_data_for_calculate_metrics function with parameters obtained from the
        global settings and the Dagster context.
        It formats the function name with the target date difference.
        After fetching the data, it logs the operation's start and end time, alongside other relevant information.
        In case of an exception, a Dagster Failure is raised with a descriptive message.

    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]

    try:
        click_metrics_df = get_data_for_calculate_metrics(
            schema_name=schema_name,
            func_name=SETTINGS["click_metrics"]["function_name"].format(target_date_diff),
            full_reload_metric_list=SETTINGS["click_metrics"]["full_reload_metrics_list"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )
        return click_metrics_df
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def calc_click_metrics_op(context, click_metrics_df) -> bool:
    """
    Calculate click metrics and stores them in the database.

    Args:
        context: Dagster run context object.
        click_metrics_df (pandas.DataFrame): A DataFrame containing the data from
        the get_abtest_click_metrics_raw() function for which A/B testing click metrics will be calculated.

    Returns:
        bool: constant True value, indicating that the A/B testing click metrics were
        successfully calculated and the database was updated.
        
    Notes:
        This operation utilizes the calculate_ab_test_metrics function to perform the metric calculations
        and database updates. It uses global settings to determine the specific columns and metrics to be processed.
        After successfully calculating and updating the metrics, it logs the operation's execution
        details, including the start and end times, operation name, and target date. If an error occurs, a Dagster
        Failure is raised with an appropriate error message.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        calculate_ab_test_metrics(
            schema_name=schema_name,
            target_date=datetime.strptime(target_date_str, "%Y-%m-%d").date(),
            col_list=SETTINGS["click_metrics"]["column_list"],
            metric_list=SETTINGS["click_metrics"]["metrics_list"],
            df=click_metrics_df,
            full_reload_metric_list=SETTINGS["click_metrics"]["full_reload_metrics_list"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )

        if send_slack_info:
            send_dwh_alert_slack_message(":ab: *calc_click_metrics_op* has been successfully executed.")

        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def func_get_abtest_percentage_metrics_raw_op(context, _calc_click_metrics_result):
    """
    The operation is designed to retrieve A/B test percentage metrics from the database for further
    processing. It relies on the provided get_abtest_percentage_metrics_raw() function,
    which receives and prepares data for calculating A/B testing percentage metrics.
    It also handles metric cleaning by removing old data before updating the metrics.

    Args:
        context: Dagster run context object.
        _calc_click_metrics_result: The result from a prior operation in the pipeline .

    Returns:
        pandas.DataFrame: Returns a DataFrame containing the data fetched from the specified function for
        calculation percentage metrics.
    
    Notes:
        The operation calls the get_data_for_calculate_metrics function with parameters obtained from the
        global settings and the Dagster context.
        It formats the function name with the target date difference.
        After fetching the data, it logs the operation's start and end time, alongside other relevant information.
        In case of an exception, a Dagster Failure is raised with a descriptive message.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_diff = context.resources.globals["target_date_diff"]
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]

    try:
        percentage_metrics_df = get_data_for_calculate_metrics(
            schema_name=schema_name,
            func_name=SETTINGS["percentage_metrics"]["function_name"].format(target_date_diff),
            full_reload_metric_list=SETTINGS["percentage_metrics"]["full_reload_metrics_list"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )
        return percentage_metrics_df
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"})
def calc_percentage_metrics_op(context, percentage_metrics_df) -> bool:
    """
    Calculate percentage metrics and stores them in the database.

    Args:
        context: Dagster run context object.
        percentage_metrics_df (pandas.DataFrame): A DataFrame containing the data from
        the get_abtest_percentage_metrics_raw() function for which A/B testing percentage metrics will be calculated.

    Returns:
        bool: constant True value, indicating that the A/B testing percentage metrics were
        successfully calculated and the database was updated.
    
    Notes:
        This operation utilizes the calculate_ab_test_metrics function to perform the metric calculations
        and database updates. It uses global settings to determine the specific columns and metrics to be processed.
        After successfully calculating and updating the metrics, it logs the operation's execution
        details, including the start and end times, operation name, and target date. If an error occurs, a Dagster
        Failure is raised with an appropriate error message.

    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        calculate_ab_test_metrics(
            schema_name=schema_name,
            target_date=datetime.strptime(target_date_str, "%Y-%m-%d").date(),
            col_list=SETTINGS["percentage_metrics"]["column_list"],
            metric_list=SETTINGS["percentage_metrics"]["metrics_list"],
            df=percentage_metrics_df,
            full_reload_metric_list=SETTINGS["percentage_metrics"]["full_reload_metrics_list"],
            stat_calc_func=SETTINGS["percentage_metrics"]["stat_calc_function"],
        )
        log_written_data(
            context=context,
            table_schema=schema_name,
            table_name="abtest_significance_metrics",
            date=target_date_str,
            start_dt=start_dt,
            end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            op_name=op_name,
            send_slack_message=False,
        )
        if send_slack_info:
            send_dwh_alert_slack_message(":ab: *calc_percentage_metrics_op* has been successfully executed.")
        return True
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={"globals"},
    retry_policy=retry_policy)
def run_ab_tableau_refresh_op(context, _prev_result: bool):
    """
    Runs "Affiliate AB tests" datasource extract refresh in Tableau using Tableau REST API.

    Args:
        context: Dagster run context object.
        _prev_result: result of the previous operation.
    """
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    op_name = inspect.currentframe().f_code.co_name
    schema_name = context.resources.globals["schema_name"]
    target_date_str = context.resources.globals["target_date_str"]
    send_slack_info = context.resources.globals["send_slack_info"]

    try:
        task_id = get_task_id(workbook_name="Affiliate AB tests", datasource_name=None)
        run_extract_refresh(task_id)
        log_written_data(context=context, date=target_date_str, start_dt=start_dt,
                         end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), op_name=op_name,
                         send_slack_message=False, write_to_context=True, dagster_log_schema=schema_name)

        if send_slack_info:
            send_dwh_alert_slack_message(":tableau: *Affiliate AB tests* datasource refresh was launched.")
    except:
        if send_slack_info:
            send_dwh_alert_slack_message(":error_alert: *Affiliate AB tests* datasource refresh has failed. "
                                         "<@U06E7T8LP8X>, <@U018GESMPDJ>")
        raise Failure(description=f"{op_name} error")


# Run job
@job(
    resource_defs={
        "globals": make_values_resource(
            target_date_str=Field(str, default_value=TARGET_DATE_STR),
            target_date_diff=Field(int, default_value=TARGET_DATE_DIFF),
            schema_name=Field(str, default_value="affiliate"),
            send_slack_info=Field(bool, default_value=True),
        ),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    config=job_config,
    name=JOB_PREFIX + "collect_abtest_significance_metrics",
    description=f"Collects data to the DWH tables of the affiliate schema."
    f"Calculate AB test metrics and store them to abtest_significance_metrics.",
)
def collect_abtest_significance_metrics_job():
    delete_old_data_result = delete_old_data_op()
    get_abtest_job_metrics_result = func_get_abtest_job_metrics_raw_op(delete_old_data_result)
    calc_job_metrics_result = calc_job_metrics_op(get_abtest_job_metrics_result)
    get_abtest_click_metrics_raw_result = func_get_abtest_click_metrics_raw_op(calc_job_metrics_result)
    calc_click_metrics_result = calc_click_metrics_op(get_abtest_click_metrics_raw_result)
    func_get_abtest_percentage_metrics_raw_result = func_get_abtest_percentage_metrics_raw_op(calc_click_metrics_result)
    calc_percentage_metrics_result = calc_percentage_metrics_op(func_get_abtest_percentage_metrics_raw_result)
    run_ab_tableau_refresh_op(calc_percentage_metrics_result)
    