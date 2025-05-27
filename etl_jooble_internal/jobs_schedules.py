import os

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)

# Import jobs
# internal
from .seo_server.seo_gfj_stat.ops import seo_gfj_stat_job
from etl_jooble_internal.postgres_137.undelivered_email.ops import undelivered_email_job
from etl_jooble_internal.postgres_2_103.scan_flow_statistic.ops import scan_flow_statistic_job
from .job_history.vacancy_job_history_agg_new.ops import vacancy_job_history_job
from .postgres_1_170.apply.ops import apply_conversion_service_job, apply_conversion_service_job_yesterday
from .server_1c.invoice_info_report.ops import invoice_info_report_job
from .amt_projects.balkans_competitor_scans_infostud.ops import balkans_competitor_scans_infostud_job

from .postgres_137.conversions_joveo.ops import conversions_joveo_job
from .postgres_137.conversions_case.ops import conversions_case_job
from .postgres_137.conversions_project_case.ops import conversions_project_case_job
from .postgres_137.conversions_indeed.ops import conversions_indeed_job
from .postgres_137.conversions_appcast.ops import conversions_appcast_job
from .postgres_137.scan_flow_high_potential.ops import scan_flow_high_potential_job
from .postgres_137.scan_flow_low_potential.ops import scan_flow_low_potential_job
from .postgres_137.invoice_row.ops import invoice_row_job
from .postgres_137.domain_responsible_snapshot.ops import domain_responsible_snapshot_job
from .postgres_137.domains_per_user_daily.ops import domains_per_user_daily_job
from .postgres_137.conversions.ops import conversions_job
from .postgres_137.conversions_campaign.ops import conversions_campaign_job
from .postgres_137.conversions_source.ops import conversions_source_job
from .postgres_137.amt_scan_weekly_goal.ops import amt_scan_weekly_goal_job

# from .employer.subscription_model_metrics_raw.ops import subscription_model_metrics_raw_job
# from .employer.subscription_model.ops import subscription_model_job
# from .employer.subscription_model_jobs.ops import subscription_model_jobs_job
# from .employer.subscription_model_funnel_full.ops import subscription_model_funnel_full_job
from .employer.balkans_employers.ops import balkans_employers_job
from .employer.balkans_payments.ops import balkans_payments_job
from .employer.balkans_trials.ops import balkans_trials_job
from .employer.balkans_subscriptions_with_payments.ops import balkans_subscriptions_with_payments_job
from .employer.imp_employer_job_to_uid_mapping.ops import ea_job_to_uid_mapping_job
from .employer.imp_employer_job.ops import ea_job_job
from .employer.imp_employer_employer.ops import ea_employer_job

from .warehouse_jo.paid_ad_groups_cost_statistic.ops import paid_ad_groups_cost_statistic_job
from .warehouse_jo.paid_ad_groups.ops import paid_ad_groups_job
from .warehouse_jo.paid_campaigns.ops import paid_campaigns_job
from .warehouse_jo.paid_accounts.ops import paid_accounts_job
from .warehouse_jo.paid_info_currency.ops import paid_info_currency_job
from .warehouse_jo.facebook_adset.ops import facebook_adset_job
from .warehouse_jo.facebook_2018.ops import facebook_2018_job
from .warehouse_jo.paid_campaign_channel.ops import paid_campaign_channel_job
# from .warehouse_jo.m_ea_sales_usd_fakevsreal_revenue.ops import m_ea_sales_usd_fakevsreal_revenue_job
from .warehouse_jo.adwords.ops import adwords_yesterday_job, adwords_current_date_job
from .warehouse_jo.bing.ops import bing_job
from .warehouse_jo.paid_legend_labels_channel.ops import paid_legend_labels_channel_job
from .warehouse_jo.currency_source.ops import currency_source_job
from .warehouse_jo.google.google_account.ops import google_account_job
# from .warehouse_jo.google.google_ad_group.ops import google_ad_group_job
from .warehouse_jo.google.google_campaign.ops import google_campaign_job
from .warehouse_jo.google.google_mcc.ops import google_mcc_job
# from .warehouse_jo.reddit.ops import reddit_job
from .warehouse_jo.ppc_automatization_pmax_log.ops import ppc_automatization_pmax_log_job
from .warehouse_jo.facebook_adset_new.ops import facebook_adset_new_job
from .warehouse_jo.facebook_2025.ops import facebook_2025_job

# seo_server
from .seo_server.web_bigquery_statistic.ops import web_bigquery_statistic_job
from .seo_server.web_bigquery_statistic.ops import web_bigquery_statistic_reload_two_days_job
from .seo_server.web_bigquery_statistic.ops import web_bigquery_statistic_reload_us_job
from .seo_server.ahrefs_organic_v3.ops import ahrefs_organic_v3_job
from .seo_server.ahrefs_metrics_extended_v3.ops import ahrefs_metrics_extended_v3_job
# from .seo_server.webmaster_full_agg.ops import webmaster_full_agg_job
# from .seo_server.webmaster_statistic.ops import webmaster_statistic_job
from .seo_server.crawler_stat.ops import crawler_stat_job
from .seo_server.ea_seo_urls_potential.ops import ea_seo_urls_potential_job
from .seo_server.seo_matrix_stat.ops import seo_matrix_stat_job
from .seo_server.dic_seo_search_category.ops import dic_seo_search_category_job
from .seo_server.seo_test_groups.ops import seo_test_groups_job

# ea_crm_flowchart
from .ea_crm_flowchart.landing_callback_form.ops import landing_callback_form_job
from .ea_crm_flowchart.balkans_click_on_try_agg.ops import balkans_click_on_try_agg_job
from .ea_crm_flowchart.profile_crm_calls.ops import profile_crm_calls_job
from .ea_crm_flowchart.profile_crm_order.ops import profile_crm_order_job

# soska
from .soska.campaign_cpa.ops import campaign_cpa_job
from .soska.auction_user_target_action.ops import auction_user_target_action_job
from .soska.first_site_manager.ops import first_site_manager_job
from .soska.devman_task.ops import devman_task_job
from .soska.exclude_ppc_traffic_request.ops import exclude_ppc_traffic_request_job

# temporary job
# from .warehouse_jo.yiv_write_to_dwh.ops import yiv_to_dwh_job


# DEFINE SCHEDULES
def schedule_definition(
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


schedule_seo_gfj_stat = schedule_definition(
    job=seo_gfj_stat_job, cron_schedule="0 09 * * *"
)
schedule_undelivered_email_job = schedule_definition(
    job=undelivered_email_job, cron_schedule="30 05 * * *"
)
schedule_scan_flow_statistic = schedule_definition(
    job=scan_flow_statistic_job, cron_schedule="15 09 * * *"
)
schedule_seo_matrix_stat_job = schedule_definition(
    job=seo_matrix_stat_job, cron_schedule="30 09 * * *"
)
# warehouse
schedule_google_mcc = schedule_definition(
    job=google_mcc_job, cron_schedule="0 09 * * *"
)
schedule_google_campaign = schedule_definition(
    job=google_campaign_job, cron_schedule="0 09 * * *"
)
# schedule_google_ad_group = schedule_definition(
#     job=google_ad_group_job, cron_schedule="0 09 * * *"
# )
schedule_google_account = schedule_definition(
    job=google_account_job, cron_schedule="0 09 * * *"
)
schedule_paid_campaign_channel = schedule_definition(
    job=paid_campaign_channel_job, cron_schedule="0 07,14 * * *"
)
schedule_facebook_adset = schedule_definition(
    job=facebook_adset_job, cron_schedule="55 09,13 * * *"
)
schedule_facebook_2018 = schedule_definition(
    job=facebook_2018_job, cron_schedule="0 09,10,12 * * *"
)
schedule_paid_info_currency = schedule_definition(
    job=paid_info_currency_job, cron_schedule="0 09 * * *"
)
schedule_paid_accounts = schedule_definition(
    job=paid_accounts_job, cron_schedule="0 09,10,12 * * *"
)
schedule_paid_campaigns = schedule_definition(
    job=paid_campaigns_job, cron_schedule="0 09,10,12 * * *"
)
schedule_paid_ad_groups = schedule_definition(
    job=paid_ad_groups_job, cron_schedule="55 08,09,11 * * *"
)
schedule_paid_ad_groups_cost_statistic = schedule_definition(
    job=paid_ad_groups_cost_statistic_job, cron_schedule="0 09,10,12 * * *"
)
# schedule_m_ea_sales_usd_fakevsreal_revenue = schedule_definition(
#     job=m_ea_sales_usd_fakevsreal_revenue_job, cron_schedule="05 11 * * *"
# )
schedule_adwords_yesterday = schedule_definition(
    job=adwords_yesterday_job, cron_schedule=["15 */2 * * *"]
)
schedule_adwords_current_date = schedule_definition(
    job=adwords_current_date_job, cron_schedule=["59 5,7,10,15,20 * * *"]
)
schedule_bing = schedule_definition(
    job=bing_job, cron_schedule="55 10 * * *"
)
schedule_paid_legend_labels_channel = schedule_definition(
    job=paid_legend_labels_channel_job, cron_schedule=["50 08 * * *", "50 10 * * *"]
)
schedule_currency_source = schedule_definition(
    job=currency_source_job, cron_schedule="15 09 * * *"
)
schedule_ppc_automatization_pmax_log = schedule_definition(
    job=ppc_automatization_pmax_log_job, cron_schedule="45 09,10,11,15 * * *"
)
schedule_facebook_adset_new = schedule_definition(
    job=facebook_adset_new_job, cron_schedule="0 09 * * *"
)
schedule_facebook_2025 = schedule_definition(
    job=facebook_2025_job, cron_schedule="0 09 * * *"
)
# soska
schedule_campaign_cpa = schedule_definition(
    job=campaign_cpa_job, cron_schedule="0 10 * * *"
)
schedule_auction_user_target_action = schedule_definition(
    job=auction_user_target_action_job, cron_schedule="0 10 * * *"
)
#
schedule_apply_conversion_service = schedule_definition(
    job=apply_conversion_service_job, cron_schedule="*/8 6-19 * * *"
)
schedule_apply_conversion_service_yesterday = schedule_definition(
    job=apply_conversion_service_job_yesterday, cron_schedule="15 05 * * *"
)
schedule_vacancy_job_history = schedule_definition(
    job=vacancy_job_history_job, cron_schedule="0 18 1 * *"
)
schedule_conversions_joveo = schedule_definition(
    job=conversions_joveo_job, cron_schedule="0 13 * * *"
)
schedule_conversions_indeed = schedule_definition(
    job=conversions_indeed_job, cron_schedule="0 13 * * *"
)
schedule_conversions_appcast = schedule_definition(
    job=conversions_appcast_job, cron_schedule="05 15 * * *"
)
#
schedule_conversions_case = schedule_definition(
    job=conversions_case_job, cron_schedule="35 12 * * *"
)
schedule_conversions_project_case = schedule_definition(
    job=conversions_project_case_job, cron_schedule="40 12 * * *"
)
schedule_invoice_row = schedule_definition(
    job=invoice_row_job, cron_schedule="15 14 * * *"
)
#
# schedule_subscription_model_metrics_raw = schedule_definition(
#     job=subscription_model_metrics_raw_job, cron_schedule="55 07 * * *"
# )
# schedule_subscription_model = schedule_definition(
#     job=subscription_model_job, cron_schedule="0 09,14 * * *"
# )
# schedule_subscription_model_jobs = schedule_definition(
#     job=subscription_model_jobs_job, cron_schedule="0 09 * * *"
# )
# schedule_subscription_model_funnel_full = schedule_definition(
#     job=subscription_model_funnel_full_job, cron_schedule="0 09 * * *"
# )
# ea_crm_flowchart
schedule_landing_callback_form = schedule_definition(
    job=landing_callback_form_job, cron_schedule="0 09 * * *"
)
schedule_balkans_click_on_try_agg = schedule_definition(
    job=balkans_click_on_try_agg_job, cron_schedule="0 08 * * *"
)
schedule_profile_crm_calls = schedule_definition(
    job=profile_crm_calls_job, cron_schedule="30 07 * * *"
)
schedule_profile_crm_order = schedule_definition(
    job=profile_crm_order_job, cron_schedule="30 07 * * *"
)
# seo_server
schedule_web_bigquery_statistic = schedule_definition(
    job=web_bigquery_statistic_job, cron_schedule="05 14 * * *"
)
schedule_web_bigquery_statistic_reload_two_days = schedule_definition(
    job=web_bigquery_statistic_reload_two_days_job, cron_schedule="40 06 * * *"
)
schedule_web_bigquery_statistic_reload_us = schedule_definition(
    job=web_bigquery_statistic_reload_us_job, cron_schedule="05 06 * * *"
)
schedule_ahrefs_organic_v3 = schedule_definition(
    job=ahrefs_organic_v3_job, cron_schedule="45 05 * * *"
)
schedule_ahrefs_metrics_extended_v3 = schedule_definition(
    job=ahrefs_metrics_extended_v3_job, cron_schedule="45 05 * * *"
)
# schedule_webmaster_full_agg = schedule_definition(
#     job=webmaster_full_agg_job, cron_schedule="05 08 * * *"
# )
# schedule_webmaster_statistic = schedule_definition(
#     job=webmaster_statistic_job, cron_schedule="30 12 * * *"
# )
schedule_balkans_employers = schedule_definition(
    job=balkans_employers_job, cron_schedule="00 08 * * *"
)
schedule_balkans_payments = schedule_definition(
    job=balkans_payments_job, cron_schedule="00 08 * * *"
)
schedule_balkans_trials = schedule_definition(
    job=balkans_trials_job, cron_schedule="00 08 * * *"
)
schedule_balkans_subscriptions_with_payments = schedule_definition(
    job=balkans_subscriptions_with_payments_job, cron_schedule="50 07 * * *"
)
schedule_crawler_stat = schedule_definition(
    job=crawler_stat_job, cron_schedule="00 11 * * *"
)
schedule_dic_seo_search_category = schedule_definition(
    job=dic_seo_search_category_job, cron_schedule="30 21 * * 7"
)
schedule_seo_test_groups = schedule_definition(
    job=seo_test_groups_job, cron_schedule="0 18 * * 1,3"
)
# mssql_1_62
schedule_ea_seo_urls_potential = schedule_definition(
    job=ea_seo_urls_potential_job, cron_schedule="0 09 1 * *"
)
schedule_first_site_manager = schedule_definition(
    job=first_site_manager_job, cron_schedule="00 08 * * *"
)
schedule_devman_task = schedule_definition(
    job=devman_task_job, cron_schedule="00 08 * * *"
)
schedule_exclude_ppc_traffic_request = schedule_definition(
    job=exclude_ppc_traffic_request_job, cron_schedule="00 08 * * *"
)
schedule_scan_flow_high_potential = schedule_definition(
    job=scan_flow_high_potential_job, cron_schedule="20 08 * * *"
)
schedule_scan_flow_low_potential = schedule_definition(
    job=scan_flow_low_potential_job, cron_schedule="20 08 * * *"
)
schedule_domain_responsible_snapshot = schedule_definition(
    job=domain_responsible_snapshot_job, cron_schedule="55 * * * *"
)
schedule_domains_per_user_daily = schedule_definition(
    job=domains_per_user_daily_job, cron_schedule="55 * * * *"
)
schedule_invoice_info_report = schedule_definition(
    job=invoice_info_report_job, cron_schedule="00 23 * * *"
)
schedule_conversions = schedule_definition(
    job=conversions_job, cron_schedule="0 11 * * *"
)
schedule_conversions_campaign = schedule_definition(
    job=conversions_campaign_job, cron_schedule="05 11 * * *"
)
schedule_conversions_source = schedule_definition(
    job=conversions_source_job, cron_schedule="10 11 * * *"
)
# schedule_reddit = schedule_definition(
#     job=reddit_job, cron_schedule=["30 11,18 * * *", "00 08 * * *"]
# )
schedule_amt_scan_weekly_goal = schedule_definition(
    job=amt_scan_weekly_goal_job, cron_schedule="30 05 * * *"
)
schedule_balkans_competitor_scans_infostud = schedule_definition(
    job=balkans_competitor_scans_infostud_job, cron_schedule="0 08 * * *"
)
schedule_ea_job_to_uid_mapping = schedule_definition(
    job=ea_job_to_uid_mapping_job, cron_schedule="35 04 * * *"
)
schedule_ea_job = schedule_definition(
    job=ea_job_job, cron_schedule="40 02 * * *"
)
schedule_ea_employer = schedule_definition(
    job=ea_employer_job, cron_schedule="15 06 * * *"
)
