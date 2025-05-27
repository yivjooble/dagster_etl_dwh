from dagster import Definitions

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *
from .sensors import *


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_search_seo_breadcrumbs,
        schedule_search_seo_initiatives,
        schedule_mobile_app_session_data,
        schedule_paid_acquisition_prod_metrics,
        # schedule_paid_acquisition_prod_metrics_short_version,
        # schedule_insert_finance_report,
        schedule_onboarding_funnel,
        schedule_account_source,
        schedule_paid_acquisition_prod_metrics_cloudberry_version,
    ],
    assets=[],
    jobs=[
        search_seo_breadcrumbs_job,
        search_seo_initiatives_job,
        budget_revenue_daily_agg_job,
        mobile_app_session_data_job,
        paid_acquisition_prod_metrics_job,
        paid_acquisition_prod_metrics_short_version_job,
        paid_acquisition_prod_metrics_cloudberry_version_job,
        insert_finance_report_job,
        session_abtest_main_metrics_agg_job,
        mobile_app_first_launch_attribute_job,
        mobile_app_onboarding_funnel_job,
        mobile_app_account_source_job,
    ],
    sensors=[
        mobile_app_first_launch_attribute_to_replica_sensor,
        monitor_all_jobs_sensor,
        sf_project_country_sensor
    ],
)
