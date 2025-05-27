from dagster import Definitions

# IMPORT SENSORS
from .sensors import *

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_paid_labels_channel,
        schedule_adv_revenue_by_placement_and_src_analytics_job,
        schedule_forecast_revenue,
        schedule_net_revenue_month_forecast,
        schedule_ga_organic,
        schedule_session_user_search_agg,
        schedule_open_contact_by_feature,
        schedule_test_iterations,
        schedule_r2r_value_detail,
        # schedule_seo_test_url_keyword,
        schedule_cpc_segmentation_script,
        # schedule_insert_mobile_application_data,
        # schedule_account_metrics_agg,
        # schedule_last_week_revenue_data_to_gs,
        # schedule_last_month_revenue_data_to_gs,
        # schedule_webmaster_agg_job,
        schedule_easy_widget_usage,
        # schedule_paid_acquisition_prod_metrics,
        # schedule_paid_acquisition_prod_metrics_short_version,
        # schedule_insert_finance_report,
        schedule_jobget_aff_report_first,
        schedule_jobget_aff_report_second,
        schedule_jobs_stat_daily,
        schedule_seo_abtest_agg
    ],
    assets=[
        *forecast_revenue_assets,
        *net_revenue_month_forecast_assets,
    ],
    jobs=[
        paid_labels_channel_job,
        adv_revenue_by_placement_and_src_analytics_job,
        ga_organic_job,
        session_user_search_agg_job,
        open_contact_by_feature_job,
        test_iterations_job,
        # account_metrics_agg_job,
        # budget_revenue_daily_agg_job,
        r2r_value_detail_job,
        seo_test_url_keyword_job,
        cpc_segmentation_script_job,
        # session_abtest_main_metrics_agg_job,
        # insert_mobile_application_data_job,
        last_week_revenue_data_to_gs_job,
        last_month_revenue_data_to_gs_job,
        # webmaster_agg_job,
        forecast_revenue_job,
        net_revenue_month_forecast_job,
        easy_widget_usage_job,
        # paid_acquisition_prod_metrics_job,
        # paid_acquisition_prod_metrics_short_version_job,
        # insert_finance_report_job,
        jobget_aff_report_first_job,
        jobget_aff_report_second_job,
        jobs_stat_daily_job,
        seo_abtest_agg_job
    ],
    sensors=[
        monitor_all_jobs_sensor
    ],
)
