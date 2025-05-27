import os

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)


# DEFINE JOBS
from etl_repstat.insert_public_push_firebase_subscriptions.ops import prc_insert_public_push_firebase_subscriptions_job
from etl_repstat.click_metric_agg_trino.ops import click_metric_agg_trino_job
from etl_repstat.mobile_app_first_launch_attribute.ops import prc_first_launch_attribute_job
from .ins_job_snapshot.ops import prc_insert_job_snapshot_job
from .ins_job_history_snapshot.ops import (prc_insert_job_history_snapshot_job_yesterday_date,
                                           prc_insert_job_history_snapshot_job_current_date)
from .ins_job_region_snapshot.ops import prc_insert_job_region_snapshot_job
from .ins_info_currency_snapshot.ops import prc_insert_info_currency_snapshot_job
from .ins_campaign_log_snapshot.ops import prc_insert_campaign_log_snapshot_job
from .account_revenue_to_replica.ops import (prc_imp_account_revenue_job_current_date,
                                             prc_imp_account_revenue_job_yesterday_date)
from .click_metric_agg_to_replica.ops import (prc_click_metric_agg_job_yesterday_date,
                                              prc_click_metric_agg_job_current_date)

#
from .clicks_collars.ops import clicks_collars_job
from .serp_rivals_agg.ops import serp_rivals_agg_job
from .vacancy_collars.ops import vacancy_collars_job
from .insert_emails.ops import (insert_emails_job_yesterday_date,
                                insert_emails_job_current_date)
from .job_categories.ops import job_categories_job
from .account_revenue_abtest_agg.ops import account_revenue_abtest_agg_job
from .action_agg.ops import action_agg_job
from etl_repstat.session_abtest_agg_new.ops import (session_abtest_agg_new_job_yesterday_date,
                                                    session_abtest_agg_new_job_current_date)
from .email_abtest_agg.ops import (email_abtest_agg_job_yesterday_date,
                                   email_abtest_agg_job_current_date)
from .session_revenue_by_placement.ops import session_revenue_by_placement_job
from .product_metrics.ops import product_metrics_agg_job
from .search_agg.ops import search_agg_abtest_agg_job
from .sentinel_statistic.ops import sentinel_statistic_job
from .account_revenue_to_dwh.ops import account_revenue_job
from etl_repstat.recommendations_agg.ops import recommendations_agg_job
# from etl_repstat.registration_email_funnel.ops import registration_email_funnel_job
# from etl_repstat.letter_job_click.ops import letter_job_click
from etl_repstat.itc_jdp.ops import itc_jdp
from etl_repstat.cv_information_agg.ops import cv_information_agg_job
from etl_repstat.impression_statistic.ops import impression_statistic_job
from etl_repstat.ea_search_conversion.ops import ea_search_conversion_job
from etl_repstat.prc_tmp_session_tables_materialise.ops import prc_tmp_session_tables_materialise_job
from etl_repstat.project_conversions_daily.ops import project_conversions_daily_reload_two_days_job
from etl_repstat.project_conversions_daily.ops import project_conversions_daily_reload_five_days_job
from etl_repstat.conversion_daily_check.ops import conversion_daily_check_job
from etl_repstat.email_abtest_agg_by_account.ops import (email_abtest_agg_by_account_job_yesterday_date,
                                                          email_abtest_agg_by_account_job_current_date)
from etl_repstat.balkans_job_stats.ops import balkans_job_stats_job
from etl_repstat.balkans_session_cnt_by_traff_source.ops import balkans_session_cnt_by_traff_source_job
from etl_repstat.balkans_user_metrics_by_traff_source.ops import balkans_user_metrics_by_traff_source_job
from etl_repstat.email_abtest_account_agg.ops import email_abtest_account_agg_job
from etl_repstat.vacancy_job_search_prod_agg.ops import vacancy_job_search_prod_agg_job
from etl_repstat.mobile_app_business_metrics.ops import mobile_app_business_metrics_job
from etl_repstat.mobile_app_activity_metrics_dau.ops import mobile_app_activity_metrics_dau_job
from etl_repstat.mobile_app_activity_metrics_wau.ops import mobile_app_activity_metrics_wau_job
from etl_repstat.mobile_app_activity_metrics_mau.ops import mobile_app_activity_metrics_mau_job
from etl_repstat.aoj_apply_action_agg.ops import aoj_apply_action_agg_job
from etl_repstat.apply_conversion_service.ops import apply_conversion_service_job
from etl_repstat.session_feature_action.ops import session_feature_action_job
from etl_repstat.email_metric_daily.ops import email_metric_daily_job
from etl_repstat.mobile_app_push_metrics.ops import mobile_app_push_metrics_job
from etl_repstat.email_spam_report_agg.ops import email_spam_report_agg_job
from etl_repstat.client_job_dynamics.ops import client_job_dynamics_job
from etl_repstat.client_job_dynamics_history.ops import client_job_dynamics_h_job
from etl_repstat.serp_campaign_rivals_agg.ops import serp_campaign_rivals_agg_job
from etl_repstat.mobile_app_install_info.ops import mobile_app_install_info_job
from etl_repstat.mobile_app_install_info_temp.ops import mobile_app_install_info_temp_to_replica_job
from etl_repstat.mobile_session.ops import mobile_session_job
from etl_repstat.mobile_session_feature.ops import mobile_session_feature_job
from etl_repstat.mobile_session_feature_action.ops import mobile_session_feature_action_job


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
schedule_prc_insert_public_push_firebase_subscriptions = define_schedule(
    job=prc_insert_public_push_firebase_subscriptions_job,
    cron_schedule="0 09 * * *"
)
schedule_campaign_log_snapshot = define_schedule(
    job=prc_insert_campaign_log_snapshot_job,
    cron_schedule="15 09 * * *"
)
schedule_info_currency_snapshot = define_schedule(
    job=prc_insert_info_currency_snapshot_job,
    cron_schedule="15 09 * * *"
)
schedule_job_region_snapshot = define_schedule(
    job=prc_insert_job_region_snapshot_job,
    cron_schedule=["30 05 * * *", "15 09 * * *", "30 14 * * *"]
)
schedule_ins_job_snapshot = define_schedule(
    job=prc_insert_job_snapshot_job,
    cron_schedule=["30 05 * * *", "15 09 * * *", "30 14 * * *"]
)
schedule_job_history_snapshot_yesterday_date = define_schedule(
    job=prc_insert_job_history_snapshot_job_yesterday_date,
    cron_schedule=["0 05 * * *", "0 10 * * *"]
)
schedule_job_history_snapshot_current_date = define_schedule(
    job=prc_insert_job_history_snapshot_job_current_date,
    cron_schedule="45 14 * * *"
)
schedule_itc_jdp = define_schedule(
    job=itc_jdp,
    cron_schedule="40 09 * * *"
)
schedule_insert_emails_yesterday_date = define_schedule(
    job=insert_emails_job_yesterday_date,
    cron_schedule=["0 05 * * *", "50 09 * * *"]
)
schedule_insert_emails_job_current_date = define_schedule(
    job=insert_emails_job_current_date,
    cron_schedule="0 15 * * *"
)
schedule_sentinel_statistic = define_schedule(
    job=sentinel_statistic_job,
    cron_schedule="30 13 * * *"
)
schedule_session_revenue_by_placement = define_schedule(
    job=session_revenue_by_placement_job,
    cron_schedule="30 10 * * *"
)
# schedule_prc_imp_account_revenue_job_yesterday_date = define_schedule(
#     job=prc_imp_account_revenue_job_yesterday_date,
#     cron_schedule="40 10 * * *"
# )
schedule_prc_imp_account_revenue_job_current_date = define_schedule(
    job=prc_imp_account_revenue_job_current_date,
    cron_schedule="15 15 * * *"
)
# schedule_registration_email_funnel = define_schedule(
#     job=registration_email_funnel_job,
#     cron_schedule="45 10 * * *"
# )

# schedule_account_revenue_abtest = define_schedule(
#     job=account_revenue_abtest_agg_job,
#     cron_schedule="05 11 * * *"
# )
schedule_serp_rivals_agg = define_schedule(
    job=serp_rivals_agg_job,
    cron_schedule="10 11 * * *"
)
# schedule_session_abtest_agg_new_yesterday_date = define_schedule(
#     job=session_abtest_agg_new_job_yesterday_date,
#     cron_schedule="20 11 * * *"
# )
# schedule_session_abtest_agg_new_current_date = define_schedule(
#     job=session_abtest_agg_new_job_current_date,
#     cron_schedule="0 16 * * *"
# )
# schedule_letter_job_click = define_schedule(
#     job=letter_job_click,
#     cron_schedule="20 11 * * *"
# )
schedule_action_agg = define_schedule(
    job=action_agg_job,
    cron_schedule="45 11 * * *"
)
schedule_clicks_collars = define_schedule(
    job=clicks_collars_job,
    cron_schedule="55 11 * * *"
)
schedule_vacancy_collars = define_schedule(
    job=vacancy_collars_job,
    cron_schedule="58 11 * * *"
)
# schedule_email_abtest_agg_job_yesterday_date = define_schedule(
#     job=email_abtest_agg_job_yesterday_date,
#     cron_schedule="15 11 * * *"
# )
# schedule_email_abtest_agg_job_current_date = define_schedule(
#     job=email_abtest_agg_job_current_date,
#     cron_schedule="0 16 * * *"
# )
schedule_product_metrics_agg = define_schedule(
    job=product_metrics_agg_job,
    cron_schedule="15 12 * * *"
)
# schedule_account_revenue = define_schedule(
#     job=account_revenue_job,
#     cron_schedule="30 12 * * *"
# )
schedule_impression_statistic = define_schedule(
    job=impression_statistic_job,
    cron_schedule="35 11 * * *"
)

schedule_search_agg_abtest_agg = define_schedule(
    job=search_agg_abtest_agg_job,
    cron_schedule="30 12 * * *"
)
schedule_recommendations_agg = define_schedule(
    job=recommendations_agg_job,
    cron_schedule="00 13 * * *"
)

#
schedule_click_metric_agg_yesterday_date = define_schedule(
    job=prc_click_metric_agg_job_yesterday_date,
    cron_schedule=['0 07 * * *', '30 11 * * *']
)
schedule_click_metric_agg_current_date = define_schedule(
    job=prc_click_metric_agg_job_current_date,
    cron_schedule="0 16 * * *"
)
#
schedule_job_categories = define_schedule(
    job=job_categories_job,
    cron_schedule="10 13 * * *"
)
schedule_cv_information_agg = define_schedule(
    job=cv_information_agg_job,
    cron_schedule="20 13 * * *"
)
schedule_ea_search_conversion = define_schedule(
    job=ea_search_conversion_job,
    cron_schedule="00 08 * * *"
)
#
schedule_prc_tmp_session_tables_materialise = define_schedule(
    job=prc_tmp_session_tables_materialise_job,
    cron_schedule="20 10,15 * * *"
)
# schedule_project_conversions_daily_reload_two_days = define_schedule(
#     job=project_conversions_daily_reload_two_days_job,
#     cron_schedule="50 10 * * *"
# )
# schedule_project_conversions_daily_reload_five_days = define_schedule(
#     job=project_conversions_daily_reload_five_days_job,
#     cron_schedule="15 07 * * *"
# )
schedule_conversion_daily_check = define_schedule(
    job=conversion_daily_check_job,
    cron_schedule="00 15 * * *"
)
# schedule_email_abtest_agg_by_account_job_yesterday_date = define_schedule(
#     job=email_abtest_agg_by_account_job_yesterday_date,
#     cron_schedule="05 13 * * *"
# )
schedule_balkans_job_stats = define_schedule(
    job=balkans_job_stats_job,
    cron_schedule="50 07 * * *"
)
schedule_balkans_session_cnt_by_traff_source = define_schedule(
    job=balkans_session_cnt_by_traff_source_job,
    cron_schedule="53 07 * * *"
)
schedule_balkans_user_metrics_by_traff_source = define_schedule(
    job=balkans_user_metrics_by_traff_source_job,
    cron_schedule="56 07 * * *"
)
schedule_vacancy_job_search_prod_agg = define_schedule(
    job=vacancy_job_search_prod_agg_job,
    cron_schedule="00 08 1 * *"
)
schedule_aoj_apply_action_agg = define_schedule(
    job=aoj_apply_action_agg_job,
    cron_schedule="50 08 * * *"
)
schedule_session_feature_action = define_schedule(
    job=session_feature_action_job,
    cron_schedule="40 09 * * *"
)
schedule_email_metric_daily = define_schedule(
    job=email_metric_daily_job,
    cron_schedule="10 12 * * *"
)
schedule_client_job_dynamics = define_schedule(
    job=client_job_dynamics_job,
    cron_schedule="50 11 * * *"
)
schedule_serp_campaign_rivals_agg = define_schedule(
    job=serp_campaign_rivals_agg_job,
    cron_schedule="45 10 * * *"
)
schedule_mobile_app_install_info = define_schedule(
    job=mobile_app_install_info_job,
    cron_schedule="20 10 * * *"
)
schedule_mobile_session = define_schedule(
    job=mobile_session_job,
    cron_schedule="10 11 * * *"
)
schedule_mobile_session_feature = define_schedule(
    job=mobile_session_feature_job,
    cron_schedule="15 11 * * *"
)
schedule_mobile_session_feature_action = define_schedule(
    job=mobile_session_feature_action_job,
    cron_schedule="20 11 * * *"
)