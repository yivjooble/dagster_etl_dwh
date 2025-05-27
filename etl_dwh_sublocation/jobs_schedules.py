import os

from dotenv import load_dotenv
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)

# DEFINE JOBS
from .mobile_app_session_data.ops import mobile_app_session_data_job
from .paid_acquisition_prod_metrics.ops import (
    paid_acquisition_prod_metrics_job,
    paid_acquisition_prod_metrics_short_version_job,
)
from .paid_acquisition_prod_metrics_cloudberry.ops import paid_acquisition_prod_metrics_cloudberry_version_job
from .insert_finance_report.ops import insert_finance_report_job
from .session_abtest_main_metrics_agg.ops import session_abtest_main_metrics_agg_job
from .budget_revenue_daily_agg.ops import budget_revenue_daily_agg_job
from .search_seo_initiatives.ops import search_seo_initiatives_job
from .search_seo_breadcrumbs.ops import search_seo_breadcrumbs_job
from .mobile_app_first_launch_attribute.ops import mobile_app_first_launch_attribute_job
from .mobile_app_onboarding_funnel.ops import mobile_app_onboarding_funnel_job
from .mobile_app_account_source.ops import mobile_app_account_source_job

# Load env
load_dotenv()


# DEFINE SCHEDULES
def define_schedule(
    job,
    cron_schedule,
    execution_timezone="Europe/Kiev",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if os.environ.get("INSTANCE") == "PRD"
        else DefaultScheduleStatus.STOPPED
    ),
):
    return ScheduleDefinition(
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone=execution_timezone,
        default_status=default_status,
    )


# DEFINE SCHEDULES
schedule_search_seo_breadcrumbs = define_schedule(
    job=search_seo_breadcrumbs_job,
    cron_schedule="0 15 * * *",
)
schedule_search_seo_initiatives = define_schedule(
    job=search_seo_initiatives_job,
    cron_schedule="0 16 * * 1,3,5-7",
)
# schedule_insert_finance_report = define_schedule(
#     job=insert_finance_report_job,
#     cron_schedule="0 13 * * *",
# )
schedule_mobile_app_session_data = define_schedule(
    job=mobile_app_session_data_job,
    cron_schedule="15 10 * * *",
)
schedule_paid_acquisition_prod_metrics = define_schedule(
    job=paid_acquisition_prod_metrics_job,
    cron_schedule="25 12,09 * * *",
)
# schedule_paid_acquisition_prod_metrics_short_version = define_schedule(
#    job=paid_acquisition_prod_metrics_short_version_job,
#    cron_schedule="05 09 * * *",
# )
schedule_paid_acquisition_prod_metrics_cloudberry_version = define_schedule(
    job=paid_acquisition_prod_metrics_cloudberry_version_job,
    cron_schedule="30 12,09 * * *",
)
schedule_onboarding_funnel = define_schedule(
    job=mobile_app_onboarding_funnel_job,
    cron_schedule="05 11 * * *",
)
schedule_account_source = define_schedule(
    job=mobile_app_account_source_job,
    cron_schedule="10 11 * * *",
)
