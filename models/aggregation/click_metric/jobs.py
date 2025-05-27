from datetime import datetime, timedelta
from typing import List, Dict, Any

from dagster import (
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)

from utility_hub import all_countries_list
from etl_repstat.utils.io_manager_path import get_io_manager_path

# Import all operations
from models.aggregation.click_metric.click_metric_agg_to_replica.ops import (
    prc_click_metric_agg_get_sqlinstance,
    prc_click_metric_agg_query_on_db,
    CLICK_METRIC_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.click_data_agg.ops import (
    click_data_agg_get_sql_instance,
    click_data_agg_query_on_db,
    click_data_agg_check_data_discrepancy,
    CLICK_DATA_GITLAB_DDL_URL,
    click_data_refresh_tableau_object,
)
from models.aggregation.click_metric.session_abtest_agg_new.ops import (
    session_abtest_new_get_sqlinstance,
    session_abtest_new_query_on_db,
    session_abtest_new_check_data_discrepancy,
    SESSION_ABTEST_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.session_abtest_main_metrics_agg.ops import (
    session_abtest_main_metrics_agg_query_on_db,
    SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.email_abtest_agg.ops import (
    email_abtest_agg_get_sqlinstance,
    email_abtest_agg_query_on_db,
    EMAIL_ABTEST_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.email_abtest_agg_by_account.ops import (
    email_abtest_agg_by_account_get_sqlinstance,
    email_abtest_agg_by_account_query_on_db,
    EMAIL_BY_ACCOUNT_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.email_abtest_account_agg.ops import (
    email_abtest_account_agg_get_sqlinstance,
    email_abtest_account_agg_query_on_db,
    launch_cabacus,
    EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q_URL,
)
from models.aggregation.click_metric.account_revenue_to_dwh.ops import (
    dwh_account_revenue_get_sqlinstance,
    dwh_account_revenue_query_on_db,
    dwh_account_revenue_check_data_discrepancy,
    DWH_ACCOUNT_REVENUE_GITLAB_DDL_URL,
    dwh_account_revenue_refresh_tableau_object,
)
from models.aggregation.click_metric.account_revenue_abtest_agg.ops import (
    account_revenue_abtest_agg_get_sql_instance,
    account_revenue_abtest_agg_query_on_db,
    ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.account_revenue_to_replica.ops import (
    rpl_account_revenue_get_sqlinstance,
    rpl_account_revenue_query_on_db,
    RPL_ACCOUNT_REVENUE_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.mobile_app_business_metrics.ops import (
    mobile_app_business_metrics_get_sql_instance,
    mobile_app_business_metrics_query_on_db,
    mobile_app_business_metrics_refresh_tableau_object,
    MOBILE_APP_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.mobile_app_activity_metrics_dau.ops import (
    activity_metrics_dau_get_sql_instance,
    activity_metrics_dau_query_on_db,
    MOBILE_APP_DAU_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.mobile_app_activity_metrics_wau.ops import (
    activity_metrics_wau_get_sql_instance,
    activity_metrics_wau_query_on_db,
    MOBILE_APP_WAU_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.mobile_app_activity_metrics_mau.ops import (
    activity_metrics_mau_get_sql_instance,
    activity_metrics_mau_query_on_db,
    MOBILE_APP_MAU_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.mobile_app_push_metrics.ops import (
    push_metrics_get_sql_instance,
    push_metrics_query_on_db,
    push_metrics_refresh_tableau_object,
    MOBILE_APP_PUSH_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.budget_revenue_daily_agg.ops import (
    budget_revenue_daily_agg_query_on_db,
    budget_revenue_daily_agg_check_data_discrepancy,
    launch_sf_project_country,
    v_budget_and_revenue_submit_tableau_refresh,
    v_de_goals_2023_submit_tableau_refresh,
    BUDGET_REVENUE_GITLAB_DDL_URL,
)
from models.aggregation.click_metric.insert_finance_report.ops import (
    insert_finance_report_query_on_db,
    insert_finance_report_check_data_discrepancy,
    FINANCE_REPORT_GITLAB_DDL_URL,
)

# Default configuration
DEFAULT_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
DEFAULT_CONFIG = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 67,
            }
        }
    }
}

DEFAULT_METADATA = {
    "click_metric_agg": CLICK_METRIC_GITLAB_DDL_URL,
    "click_data_agg": CLICK_DATA_GITLAB_DDL_URL,
    "session_abtest_agg": SESSION_ABTEST_GITLAB_DDL_URL,
    "session_abtest_main_metrics_agg": SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_URL,
    "email_abtest_agg": EMAIL_ABTEST_GITLAB_DDL_URL,
    "rpl_account_revenue": RPL_ACCOUNT_REVENUE_GITLAB_DDL_URL,
    "dwh_account_revenue": DWH_ACCOUNT_REVENUE_GITLAB_DDL_URL,
    "account_revenue_abtest_agg": ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_URL,
    "budget_revenue_daily_agg": BUDGET_REVENUE_GITLAB_DDL_URL,
    "email_abtest_agg_by_account": EMAIL_BY_ACCOUNT_GITLAB_DDL_URL,
    "email_abtest_account_agg": EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q_URL,
    "prc_mobile_app_business_metrics": MOBILE_APP_GITLAB_DDL_URL,
    "prc_mobile_app_activity_metrics_dau": MOBILE_APP_DAU_GITLAB_DDL_URL,
    "prc_mobile_app_activity_metrics_wau": MOBILE_APP_WAU_GITLAB_DDL_URL,
    "prc_mobile_app_activity_metrics_mau": MOBILE_APP_MAU_GITLAB_DDL_URL,
    "prc_mobile_app_push_metrics": MOBILE_APP_PUSH_GITLAB_DDL_URL,
    "insert_finance_report": FINANCE_REPORT_GITLAB_DDL_URL,
}


def process_click_metrics(click_metric_result: Any) -> Any:
    """Process click data and session AB test metrics."""
    click_data_instance = click_data_agg_get_sql_instance(click_metric_result)
    click_data_result = click_data_instance.map(click_data_agg_query_on_db).collect()
    click_data_discrepancy = click_data_agg_check_data_discrepancy(click_data_result)
    click_data_refresh_tableau_object(click_data_discrepancy)

    session_abtest_instance = session_abtest_new_get_sqlinstance(click_metric_result)
    session_abtest_result = session_abtest_instance.map(session_abtest_new_query_on_db).collect()
    session_abtest_discrepancy = session_abtest_new_check_data_discrepancy(session_abtest_result)

    session_abtest_main_metrics_agg_query_on_db(session_abtest_discrepancy)

    return click_data_discrepancy


def process_email_metrics(click_metric_result: Any) -> Any:
    """Process email-related metrics."""
    email_abtest_instance = email_abtest_agg_get_sqlinstance(click_metric_result)
    email_abtest_instance.map(email_abtest_agg_query_on_db)

    email_abtest_agg_by_account_instance = email_abtest_agg_by_account_get_sqlinstance(click_metric_result)
    email_abtest_agg_by_account_result = email_abtest_agg_by_account_instance.map(
        email_abtest_agg_by_account_query_on_db
    ).collect()

    email_abtest_account_instance = email_abtest_account_agg_get_sqlinstance(email_abtest_agg_by_account_result)
    email_abtest_account_result = email_abtest_account_instance.map(
        email_abtest_account_agg_query_on_db
    ).collect()
    launch_cabacus(email_abtest_account_result)


def process_account_revenue(click_metric_result: Any) -> Any:
    """Process account revenue metrics."""
    rpl_account_revenue_instance = rpl_account_revenue_get_sqlinstance(click_metric_result)
    rpl_account_revenue_result = rpl_account_revenue_instance.map(rpl_account_revenue_query_on_db).collect()

    dwh_account_revenue_instance = dwh_account_revenue_get_sqlinstance(rpl_account_revenue_result)
    dwh_account_revenue_result = dwh_account_revenue_instance.map(dwh_account_revenue_query_on_db).collect()
    dwh_account_revenue_check_data_discrepancy(dwh_account_revenue_result)
    dwh_account_revenue_refresh_tableau_object(dwh_account_revenue_result)

    account_revenue_abtest_agg_instance = account_revenue_abtest_agg_get_sql_instance(rpl_account_revenue_result)
    account_revenue_abtest_agg_instance.map(account_revenue_abtest_agg_query_on_db)


def process_mobile_app_metrics(click_metric_result: Any) -> Any:
    """Process mobile app related metrics."""
    mobile_app_instance = mobile_app_business_metrics_get_sql_instance(click_metric_result)
    mobile_app_result = mobile_app_instance.map(mobile_app_business_metrics_query_on_db).collect()
    mobile_app_business_metrics_refresh_tableau_object(mobile_app_result)

    mobile_app_dau_instance = activity_metrics_dau_get_sql_instance(click_metric_result)
    mobile_app_dau_result = mobile_app_dau_instance.map(activity_metrics_dau_query_on_db).collect()

    mobile_app_wau_instance = activity_metrics_wau_get_sql_instance(mobile_app_dau_result)
    mobile_app_wau_result = mobile_app_wau_instance.map(activity_metrics_wau_query_on_db).collect()

    mobile_app_mau_instance = activity_metrics_mau_get_sql_instance(mobile_app_wau_result)
    mobile_app_mau_result = mobile_app_mau_instance.map(activity_metrics_mau_query_on_db).collect()

    mobile_app_push_metrics_instance = push_metrics_get_sql_instance(mobile_app_mau_result)
    mobile_app_push_metrics_result = mobile_app_push_metrics_instance.map(push_metrics_query_on_db).collect()
    push_metrics_refresh_tableau_object(mobile_app_push_metrics_result)


def process_finance_and_budget(click_data_discrepancy: Any) -> None:
    """Process finance report and budget metrics."""
    finance_report_result = insert_finance_report_query_on_db(click_data_discrepancy)
    insert_finance_report_check_data_discrepancy(finance_report_result)

    budget_instance = budget_revenue_daily_agg_query_on_db(click_data_discrepancy)
    budget_discrepancy = budget_revenue_daily_agg_check_data_discrepancy(budget_instance)
    launch_sf_project_country(budget_discrepancy)
    v_de_goals_2023_submit_tableau_refresh(budget_discrepancy)
    v_budget_and_revenue_submit_tableau_refresh(budget_discrepancy)


@job(
    config=DEFAULT_CONFIG,
    resource_defs={
        "globals": make_values_resource(
            reload_countries=Field(list, default_value=all_countries_list),
            reload_date_start=Field(str, default_value=DEFAULT_DATE),
            reload_date_end=Field(str, default_value=DEFAULT_DATE),
            destination_db=Field(str, default_value='both'),
            check_discrepancy=Field(bool, default_value=True)
        ),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})
    },
    name="agg_click_metric_model",
    tags={"model_type": "aggregation"},
    description='Model consists of: click_metric_agg, click_data_agg, budget_revenue_daily_agg',
    metadata=DEFAULT_METADATA,
)
def click_metric_yesterday_date_model() -> None:
    """Main job function that orchestrates the click metric aggregation process."""
    # Initialize click metric processing
    click_metric_instance = prc_click_metric_agg_get_sqlinstance()
    click_metric_result = click_metric_instance.map(prc_click_metric_agg_query_on_db).collect()

    # Process click metrics and get discrepancy
    click_data_discrepancy = process_click_metrics(click_metric_result)

    # Process email metrics    
    process_email_metrics(click_metric_result)

    # Process account revenue
    process_account_revenue(click_metric_result)

    # Process mobile app metrics
    process_mobile_app_metrics(click_metric_result)

    # Process finance and budget metrics
    process_finance_and_budget(click_data_discrepancy)
