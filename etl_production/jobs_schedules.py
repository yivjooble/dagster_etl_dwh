import os

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)

# IMPORT ASSETS
from .assets import *

# DEFINE JOBS
# import
from etl_production.auction_login_log.ops import login_log_job
from etl_production.adv_revenue_ea_new.ops import adv_revenue_ea_new_job
from etl_production.auction_campaign.ops import auction_campaign_job
from .u_traffic_source.ops import u_traffic_source_job
from .info_project.ops import info_project_job
from .info_currency.ops import info_currency_job
from .info_region_other.ops import info_region_other_job
from .email_account_test_settings.ops import email_account_test_settings_job
from .session_test_agg.ops import session_test_agg_job
from .session_feature.ops import session_feature_job
from .banner_stat_history.ops import banner_stat_history_job
from .banner_filter.ops import banner_filter_job
from .banner_info.ops import banner_info_job
from .banner_limit.ops import banner_limit_job
from .push_firebase_subscriptions.ops import push_firebase_subscriptions_job
# from .push_sent.ops import push_sent_job
from .client_daily_settings.ops import client_daily_settings_job
from .jdp_away_clicks_today.ops import jdp_away_clicks_today_job

# aggregation jobs
# start by sensor
from .scroll_click_position_agg.ops import scroll_click_position_agg_job

# start by scheduler
from .additional_pages_click_daily_agg.ops import additional_pages_click_daily_agg_job
from .ext_stat_away_match.ops import ext_stat_away_match_job
# from .salary_page_click_daily_agg.ops import PRD_salary_page_click_daily_agg_job
from .session_daily_agg.ops import session_daily_agg_job
from .serp_campaign_rivals_agg.ops import serp_campaign_rivals_agg_job
from .email_account_test.ops import email_account_test_job
from .email_metric_daily.ops import email_metric_daily_job
# from .field_from_source_daily.ops import field_from_source_daily_job
from .campaign.ops import campaign_job

# from .bot_jdp_away_clicks_agg.ops import PRD_bot_jdp_away_clicks_agg_job
from .impression_statistic.ops import PRD_impression_statistic_agg_job
from .jdp_away_clicks_agg.ops import jdp_away_clicks_agg_job
from .jdp_away_clicks_agg.ops import jdp_away_clicks_agg_last_seven_days
from .project_job_apply_agg.ops import project_job_apply_agg_job
from .project_job_type_agg.ops import project_job_type_agg_job
from .seo_ab_prod_metrics.ops import seo_ab_prod_metrics_job
from .account_agg.ops import account_agg_job
from .funnel_agg.ops import funnel_agg_job
from .dic_info_project_discount.ops import dic_info_project_discount_job
from .dic_info_project_discount_monthly.ops import dic_info_project_discount_monthly_job
from .jobs_stat_hourly.ops import jobs_stat_hourly_job
# from .auction_external_stat.ops import auction_external_stat_job
from .campaign_auto_esc_stopped.ops import campaign_auto_esc_stopped_job
from .info_currency_history.ops import info_currency_history_job
from .project_tracking_setting.ops import project_tracking_setting_job
from .project_tracking_setting_test.ops import project_tracking_setting_test_job

# revenue
from .adv_revenue_by_placement_and_src.ops import adv_revenue_by_placement_and_src_job
from .auction_click_statistic.ops import auction_click_statistic_job
from .auction_click_statistic.ops import auction_click_statistic_reload_us_job
from .auction_click_statistic.ops import auction_click_statistic_reload_last_month_job
from .auction_click_statistic_analytics.ops import auction_click_statistic_analytics_job
from .adv_revenue_by_placement_and_src_analytics.ops import (
    adv_revenue_by_placement_and_src_analytics_agg_job,
)
from .conversion.ops import conversion_job
from .conversion_action.ops import conversion_action_job
from .general_feed_ignored_projects.ops import general_feed_ignored_projects_job
# from .general_feed_ignored_companies.ops import general_feed_ignored_companies_job

from .user_budget_log.ops import user_budget_log_job
from .campaign_log.ops import campaign_log_yesterday_date_job, campaign_log_current_date_job
from .user_log.ops import user_log_job
from .banner_stat_today.ops import banner_stat_today_job
# from .innovation_adsy_project.ops import innovation_adsy_project_job
from .account_contact.ops import account_contact_job
from .campaign_feature.ops import campaign_feature_job
from .auction_site.ops import site_job
from .auction_site_manager.ops import site_manager_job
from .auction_user.ops import auction_user_job
from .scheduled_operation.ops import scheduled_operation_job
from .auction_job_multiplication.ops import auction_job_multiplication_job
from .session_test.ops import session_test_job


# Define Schedule Function
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
schedule_auction_login_log = define_schedule(
    job=login_log_job,
    cron_schedule="0 8 * * *"
)
schedule_adv_revenue_ea_new = define_schedule(
    job=adv_revenue_ea_new_job,
    cron_schedule="50 8,11 * * *"
)
schedule_auction_campaign_job = define_schedule(
    job=auction_campaign_job,
    cron_schedule="35 03 * * *"
)
schedule_u_traffic_source_job = define_schedule(
    job=u_traffic_source_job,
    cron_schedule="05 03 * * *"
)
schedule_info_project_job = define_schedule(
    job=info_project_job,
    cron_schedule="05 07 * * *"
)
schedule_info_currency_job = define_schedule(
    job=info_currency_job,
    cron_schedule="45 08 * * *"
)
schedule_jdp_away_clicks_today = define_schedule(
    job=jdp_away_clicks_today_job,
    cron_schedule=["30 08,12 * * *", "0 15,18 * * *"]
)
schedule_client_daily_settings = define_schedule(
    job=client_daily_settings_job,
    cron_schedule="30 03 * * *"
)
schedule_session_feature = define_schedule(
    job=session_feature_job,
    cron_schedule="0 09 * * *"
)
schedule_user_budget_log = define_schedule(
    job=user_budget_log_job,
    cron_schedule="45 11 * * *"
)
# Revenue Schedules
schedule_adv_revenue_by_placement_and_src = define_schedule(
    job=adv_revenue_by_placement_and_src_job,
    cron_schedule=["50 10 * * *", "20 15 * * *"],
)
schedule_auction_click_statistic = define_schedule(
    job=auction_click_statistic_job,
    cron_schedule="00 11 * * *"
)
schedule_auction_click_statistic_reload_us = define_schedule(
    job=auction_click_statistic_reload_us_job,
    cron_schedule="00 15 * * *"
)
schedule_auction_click_statistic_reload_last_month = define_schedule(
    job=auction_click_statistic_reload_last_month_job,
    cron_schedule="00 19 2 1-12 *"
)
schedule_auction_click_statistic_analytics = define_schedule(
    job=auction_click_statistic_analytics_job,
    cron_schedule="10 11 * * *"
)
schedule_adv_revenue_by_placement_and_src_analytics_agg = define_schedule(
    job=adv_revenue_by_placement_and_src_analytics_agg_job,
    cron_schedule="0 09,11 * * *",
)
# Aggregation Schedules
schedule_account_agg = define_schedule(
    job=account_agg_job,
    cron_schedule="0 05 * * *"
)
schedule_additional_pages_click_daily_agg = define_schedule(
    job=additional_pages_click_daily_agg_job,
    cron_schedule="30 09 * * *"
)
# schedule_ext_stat_away_match = define_schedule(
#     job=ext_stat_away_match_job,
#     cron_schedule="15 15 * * *"
# )
schedule_seo_ab_prod_metrics = define_schedule(
    job=seo_ab_prod_metrics_job,
    cron_schedule="30 12 * * *"
)
# schedule_jdp_away_clicks_agg = define_schedule(
#     job=jdp_away_clicks_agg_job,
#     cron_schedule="0 11 * * *"
# )
# schedule_jdp_away_clicks_agg_last_seven_days = define_schedule(
#     job=jdp_away_clicks_agg_last_seven_days,
#     cron_schedule="0 07 * * *"
# )
# schedule_salary_page_click_daily_agg = define_schedule(
#     job=PRD_salary_page_click_daily_agg_job,
#     cron_schedule="25 09 * * *"
# )
schedule_session_daily_agg = define_schedule(
    job=session_daily_agg_job,
    cron_schedule="45 11 * * *"
)
# schedule_serp_campaign_rivals_agg = define_schedule(
#     job=serp_campaign_rivals_agg_job,
#     cron_schedule="15 12 * * *"
# )
schedule_email_metric_daily = define_schedule(
    job=email_metric_daily_job,
    cron_schedule="30 13 * * *"
)
# schedule_bot_jdp_away_clicks_agg = define_schedule(
#     job=PRD_bot_jdp_away_clicks_agg_job,
#     cron_schedule="30 10 * * *"
# )
schedule_impression_statistic_agg = define_schedule(
    job=PRD_impression_statistic_agg_job,
    cron_schedule="10 11 * * *"
)
schedule_funnel_agg = define_schedule(
    job=funnel_agg_job,
    cron_schedule="45 11 * * *"
)
schedule_dic_info_project_discount = define_schedule(
    job=dic_info_project_discount_job,
    cron_schedule="45 08 * * *"
)
schedule_jobs_stat_hourly = define_schedule(
    job=jobs_stat_hourly_job,
    cron_schedule="0 * * * *"
)
# schedule_auction_external_stat = define_schedule(
#     job=auction_external_stat_job,
#     cron_schedule="05 03 * * *"
# )
schedule_campaign_auto_esc_stopped = define_schedule(
    job=campaign_auto_esc_stopped_job,
    cron_schedule="50 08 * * *"
)

# Import Schedules
schedule_info_region_other = define_schedule(
    job=info_region_other_job,
    cron_schedule="20 03 * * *"
)
schedule_email_account_test_settings = define_schedule(
    job=email_account_test_settings_job,
    cron_schedule="0 08 * * *"
)
schedule_email_account_test = define_schedule(
    job=email_account_test_job,
    cron_schedule="30 05 * * *"
)
schedule_banner_stat_history = define_schedule(
    job=banner_stat_history_job,
    cron_schedule="40 10 * * *"
)
schedule_banner_filter = define_schedule(
    job=banner_filter_job,
    cron_schedule="35 10 * * *"
)
schedule_banner_info = define_schedule(
    job=banner_info_job,
    cron_schedule="35 10 * * *"
)
schedule_banner_limit = define_schedule(
    job=banner_limit_job,
    cron_schedule="35 10 * * *"
)
schedule_info_currency_history = define_schedule(
    job=info_currency_history_job,
    cron_schedule="0 10 * * *"
)
schedule_project_tracking_setting = define_schedule(
    job=project_tracking_setting_job,
    cron_schedule="10 09 * * *"
)
schedule_conversion = define_schedule(
    job=conversion_job,
    cron_schedule="40 08 * * *"
)
schedule_project_job_type_agg = define_schedule(
    job=project_job_type_agg_job,
    cron_schedule="45 09 * * *"
)
schedule_project_job_apply_agg = define_schedule(
    job=project_job_apply_agg_job,
    cron_schedule="00 10 * * *"
)
schedule_conversion_action = define_schedule(
    job=conversion_action_job,
    cron_schedule="45 08 * * *"
)
schedule_session_test_agg = define_schedule(
    job=session_test_agg_job,
    cron_schedule="00 10 * * *"
)
schedule_push_firebase_subscriptions = define_schedule(
    job=push_firebase_subscriptions_job,
    cron_schedule="45 09 * * *"
)
# schedule_push_sent = define_schedule(
#     job=push_sent_job,
#     cron_schedule="45 09 * * *"
# )
schedule_general_feed_ignored_projects = define_schedule(
    job=general_feed_ignored_projects_job,
    cron_schedule="45 12 * * *"
)
# schedule_general_feed_ignored_companies = define_schedule(
#     job=general_feed_ignored_companies_job,
#     cron_schedule="15 14 * * *"
# )
schedule_campaign_log_yesterday_date = define_schedule(
    job=campaign_log_yesterday_date_job,
    cron_schedule="15 06 * * *"
)
schedule_campaign_log_current_date = define_schedule(
    job=campaign_log_current_date_job,
    cron_schedule="45 12 * * *"
)
schedule_user_log = define_schedule(
    job=user_log_job,
    cron_schedule="30 12 * * *"
)
schedule_banner_stat_today = define_schedule(
    job=banner_stat_today_job,
    cron_schedule="30 12 * * *"
)
# schedule_innovation_adsy_project = define_schedule(
#     job=innovation_adsy_project_job,
#     cron_schedule="45 11 * * *"
# )
# schedule_field_from_source_daily = define_schedule(
#     job=field_from_source_daily_job,
#     cron_schedule="00 11 * * *"
# )
schedule_campaign = define_schedule(
    job=campaign_job,
    cron_schedule="00 09 * * *"
)
schedule_campaign_feature = define_schedule(
    job=campaign_feature_job,
    cron_schedule="20 04 * * *"
)
schedule_dic_info_project_discount_monthly = define_schedule(
    job=dic_info_project_discount_monthly_job,
    cron_schedule="50 04 * * *"
)
schedule_auction_site = define_schedule(
    job=site_job,
    cron_schedule="10 03 * * *"
)
schedule_auction_site_manager = define_schedule(
    job=site_manager_job,
    cron_schedule="15 03 * * *"
)
schedule_auction_user = define_schedule(
    job=auction_user_job,
    cron_schedule="30 03 * * *"
)
schedule_scheduled_operation = define_schedule(
    job=scheduled_operation_job,
    cron_schedule="50 08 * * *"
)
schedule_auction_job_multiplication = define_schedule(
    job=auction_job_multiplication_job,
    cron_schedule="45 04 * * *"
)
schedule_session_test = define_schedule(
    job=session_test_job,
    cron_schedule="45 05 * * *"
)
