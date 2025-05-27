import os

from dotenv import load_dotenv
from dagster import (
    define_asset_job,
    ScheduleDefinition,
    DefaultScheduleStatus,
)

# DEFINE JOBS
from etl_dwh.paid_labels_channel.ops import paid_labels_channel_job
from .cpc_segmentation_script.ops import cpc_segmentation_script_job
from .seo_test_url_keyword.ops import seo_test_url_keyword_job
from .open_contact_by_feature.ops import open_contact_by_feature_job
from .ga_organic_agg.ops import ga_organic_job
from .session_user_search_agg.ops import session_user_search_agg_job
from .test_iterations.ops import test_iterations_job
# from .insert_mobile_application_data.ops import insert_mobile_application_data_job
# from .paid_acquisition_prod_metrics.ops import (
#     paid_acquisition_prod_metrics_job,
#     paid_acquisition_prod_metrics_short_version_job,
# )
# from .insert_finance_report.ops import insert_finance_report_job
from .jobget_aff_report_first.ops import jobget_aff_report_first_job
from .jobget_aff_report_second.ops import jobget_aff_report_second_job

# from .account_metrics_agg.ops import account_metrics_agg_job
from .r2r_value_detail.ops import r2r_value_detail_job
from .gs_top_client.job import (
    last_week_revenue_data_to_gs_job,
    last_month_revenue_data_to_gs_job,
)

#
# from .budget_revenue_daily_agg.ops import budget_revenue_daily_agg_job
# from .session_abtest_main_metrics_agg.ops import session_abtest_main_metrics_agg_job
# from .webmaster_agg.ops import webmaster_agg_job
from .easy_widget_usage.ops import easy_widget_usage_job
from .jobs_stat_daily.ops import jobs_stat_daily_job
from .adv_revenue_by_placement_and_src_analytics.ops import adv_revenue_by_placement_and_src_analytics_job
from .seo_abtest_agg.ops import seo_abtest_agg_job


# DEFINE ASSETS
from .assets import *

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


# DEFINE JOBS
# test job
forecast_revenue_job = define_asset_job(
    name="dwh__forecast_revenue",
    selection=forecast_revenue_assets,
    description="[ono,aggregation].revenue_month_forecast",
)
net_revenue_month_forecast_job = define_asset_job(
    name="dwh__net_revenue_month_forecast",
    selection=net_revenue_month_forecast_assets,
    description="[ono,aggregation].net_revenue_month_forecast",
)


# DEFINE ASSETS
schedule_paid_labels_channel = define_schedule(
    job=paid_labels_channel_job,
    cron_schedule="0 9 * * *",
)
schedule_adv_revenue_by_placement_and_src_analytics_job = define_schedule(
    job=adv_revenue_by_placement_and_src_analytics_job,
    cron_schedule="30 15 * * *",
)
schedule_jobs_stat_daily = define_schedule(
    job=jobs_stat_daily_job,
    cron_schedule="35 03 * * *",
)
schedule_jobget_aff_report_second = define_schedule(
    job=jobget_aff_report_second_job,
    cron_schedule="0 14 * * *",
)
schedule_jobget_aff_report_first = define_schedule(
    job=jobget_aff_report_first_job,
    cron_schedule="0 14 * * *",
)
# schedule_insert_finance_report = define_schedule(
#     job=insert_finance_report_job,
#     cron_schedule="0 13 * * *",
# )
# schedule_insert_mobile_application_data = define_schedule(
#     job=insert_mobile_application_data_job,
#     cron_schedule="15 10 * * *",
# )
schedule_cpc_segmentation_script = define_schedule(
    job=cpc_segmentation_script_job,
    cron_schedule="0 11 23 * *",
)
# schedule_seo_test_url_keyword = define_schedule(
#     job=seo_test_url_keyword_job,
#     cron_schedule="0 22 * * 1,3",
# )
schedule_r2r_value_detail = define_schedule(
    job=r2r_value_detail_job,
    cron_schedule="0 12 * * *",

)
# schedule_budget_revenue_daily_agg = define_schedule(job=budget_revenue_daily_agg_job, cron_schedule="0 10 * * *", execution_timezone='Europe/Kiev',)
# schedule_account_metrics_agg = define_schedule(job=account_metrics_agg_job, cron_schedule="15 12 * * *", execution_timezone='Europe/Kiev',)
schedule_open_contact_by_feature = define_schedule(
    job=open_contact_by_feature_job,
    cron_schedule="0 10 * * *",
)
schedule_session_user_search_agg = define_schedule(
    job=session_user_search_agg_job,
    cron_schedule="0 09 * * *",
)
schedule_ga_organic = define_schedule(
    job=ga_organic_job,
    cron_schedule="45 9 * * *",
)

# forecast schedule
schedule_forecast_revenue = define_schedule(
    job=forecast_revenue_job,
    cron_schedule="0 10 * * *",
)
schedule_net_revenue_month_forecast = define_schedule(
    job=net_revenue_month_forecast_job,
    cron_schedule="15 10 * * *",
)

schedule_test_iterations = define_schedule(
    job=test_iterations_job,
    cron_schedule="0 11 * * *",
)
#
# schedule_last_week_revenue_data_to_gs = define_schedule(
#     job=last_week_revenue_data_to_gs_job,
#     cron_schedule="00 13 * * 1",
# )
# schedule_last_month_revenue_data_to_gs = define_schedule(
#     job=last_month_revenue_data_to_gs_job,
#     cron_schedule="00 12 2 * *",
#
# )
# schedule_webmaster_agg_job = define_schedule(
#     job=webmaster_agg_job,
#     cron_schedule="35 12 * * *",
# )
schedule_easy_widget_usage = define_schedule(
    job=easy_widget_usage_job,
    cron_schedule="35 11 * * *",
)
# schedule_paid_acquisition_prod_metrics = define_schedule(
#     job=paid_acquisition_prod_metrics_job,
#     cron_schedule="05 12 * * *",
# )
# schedule_paid_acquisition_prod_metrics_short_version = define_schedule(
#     job=paid_acquisition_prod_metrics_short_version_job,
#     cron_schedule="05 09 * * *",
# )
schedule_seo_abtest_agg = define_schedule(
    job=seo_abtest_agg_job,
    cron_schedule="15 18 * * *",
)
