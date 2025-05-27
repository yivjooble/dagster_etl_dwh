import os
import time
from time import timezone

from dagster import (
    run_status_sensor,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    run_failure_sensor,
    RunFailureSensorContext,
)
from utility_hub.sensor_messages import send_dwh_alert_slack_message
from utility_hub.core_tools import submit_external_job_run

from etl_repstat.click_metric_agg_to_replica.ops import (prc_click_metric_agg_job_yesterday_date,
                                                         prc_click_metric_agg_job_current_date)
from etl_repstat.session_abtest_agg_new.ops import (session_abtest_agg_new_job_yesterday_date,
                                                    session_abtest_agg_new_job_current_date)
from etl_repstat.click_data_agg.ops import click_data_agg_job
from etl_repstat.email_abtest_agg.ops import (email_abtest_agg_job_yesterday_date,
                                              email_abtest_agg_job_current_date)
from etl_repstat.email_abtest_agg_by_account.ops import (email_abtest_agg_by_account_job_yesterday_date,
                                                         email_abtest_agg_by_account_job_current_date)
from etl_repstat.email_abtest_account_agg.ops import email_abtest_account_agg_job
from etl_repstat.account_revenue_to_replica.ops import prc_imp_account_revenue_job_yesterday_date
from etl_repstat.account_revenue_to_dwh.ops import account_revenue_job
from etl_repstat.account_revenue_abtest_agg.ops import account_revenue_abtest_agg_job
from etl_repstat.mobile_app_business_metrics.ops import mobile_app_business_metrics_job
from etl_repstat.mobile_app_activity_metrics_dau.ops import mobile_app_activity_metrics_dau_job
from etl_repstat.mobile_app_activity_metrics_wau.ops import mobile_app_activity_metrics_wau_job
from etl_repstat.mobile_app_activity_metrics_mau.ops import mobile_app_activity_metrics_mau_job
from etl_repstat.mobile_app_push_metrics.ops import mobile_app_push_metrics_job
from etl_repstat.session_feature_action.ops import session_feature_action_job
from etl_repstat.email_spam_report_agg.ops import email_spam_report_agg_job
from etl_repstat.insert_emails.ops import insert_emails_job_yesterday_date


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=click_data_agg_job,
    description='monitored_jobs=[prc_click_metric_agg_job_yesterday_date] -> start click_data_agg_job'
)
def click_data_agg_job_sensor(context):
    if context.dagster_run.job_name != click_data_agg_job.name:
        return RunRequest(run_key=None,
                          job_name=click_data_agg_job.name)
    else:
        return SkipReason(f"Don't report status of {click_data_agg_job}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=session_abtest_agg_new_job_yesterday_date,
    description='monitored_jobs=[prc_click_metric_agg_job_yesterday_date] -> start session_abtest_agg_new_job_yesterday_date'
)
def click_metric_agg_job_yesterday_date_sensor(context):
    if context.dagster_run.job_name != session_abtest_agg_new_job_yesterday_date.name:
        return RunRequest(run_key=None,
                          job_name=session_abtest_agg_new_job_yesterday_date.name)
    else:
        return SkipReason(f"Don't report status of {session_abtest_agg_new_job_yesterday_date}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_current_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=session_abtest_agg_new_job_current_date,
    description='monitored_jobs=[prc_click_metric_agg_job_current_date] -> start session_abtest_agg_new_job_current_date'
)
def click_metric_agg_job_current_date_sensor(context):
    if context.dagster_run.job_name != session_abtest_agg_new_job_current_date.name:
        return RunRequest(run_key=None,
                          job_name=session_abtest_agg_new_job_current_date.name)
    else:
        return SkipReason(f"Don't report status of {session_abtest_agg_new_job_current_date}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_abtest_agg_job_yesterday_date,
    description='monitored_jobs=[prc_click_metric_agg_job_yesterday_date] -> start email_abtest_agg_job_yesterday_date'
)
def email_abtest_agg_job_yesterday_date_sensor(context):
    if context.dagster_run.job_name != email_abtest_agg_job_yesterday_date.name:
        return RunRequest(run_key=None,
                          job_name=email_abtest_agg_job_yesterday_date.name)
    else:
        return SkipReason(f"Don't report status of {email_abtest_agg_job_yesterday_date}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_current_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_abtest_agg_job_current_date,
    description='monitored_jobs=[prc_click_metric_agg_job_current_date] -> start email_abtest_agg_job_current_date'
)
def email_abtest_agg_job_current_date_sensor(context):
    if context.dagster_run.job_name != email_abtest_agg_job_current_date.name:
        return RunRequest(run_key=None,
                          job_name=email_abtest_agg_job_current_date.name)
    else:
        return SkipReason(f"Don't report status of {email_abtest_agg_job_current_date}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_current_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_abtest_agg_by_account_job_current_date,
    description='monitored_jobs=[prc_click_metric_agg_job_current_date] -> start email_abtest_agg_by_account_job_current_date'
)
def email_abtest_agg_by_account_job_current_date_sensor(context):
    if context.dagster_run.job_name != email_abtest_agg_by_account_job_current_date.name:
        return RunRequest(run_key=None,
                          job_name=email_abtest_agg_by_account_job_current_date.name)
    else:
        return SkipReason(f"Don't report status of {email_abtest_agg_by_account_job_current_date}")


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_abtest_agg_by_account_job_yesterday_date,
    description='monitored_jobs=[prc_click_metric_agg_job_yesterday_date] -> start email_abtest_agg_by_account_job_yesterday_date'
)
def email_abtest_agg_by_account_job_yesterday_date_sensor(context):
    if context.dagster_run.job_name != email_abtest_agg_by_account_job_yesterday_date.name:
        return RunRequest(run_key=None,
                          job_name=email_abtest_agg_by_account_job_yesterday_date.name)
    else:
        return SkipReason(f"Don't report status of {email_abtest_agg_by_account_job_yesterday_date}")


@run_status_sensor(
    monitored_jobs=[email_abtest_agg_by_account_job_yesterday_date, email_abtest_agg_by_account_job_current_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_abtest_account_agg_job,
    description='''monitored_jobs=[email_abtest_agg_by_account_job_yesterday_date, email_abtest_agg_by_account_job_current_date]
                   -> start email_abtest_account_agg_job'''
)
def email_abtest_account_agg_job_sensor(context):
    if context.dagster_run.job_name != email_abtest_account_agg_job.name:
        return RunRequest(run_key=None,
                          job_name=email_abtest_account_agg_job.name)
    else:
        return SkipReason(f"Don't report status of {email_abtest_account_agg_job}")


@run_status_sensor(
    monitored_jobs=[session_abtest_agg_new_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    # request_job=session_abtest_main_metrics_agg_job,
    description='monitored_jobs=[session_abtest_agg_new_job_yesterday_date] -> start session_abtest_main_metrics_agg_job'
)
def session_abtest_agg_run_main_metrics_job_sensor():
    submit_external_job_run(job_name_with_prefix='dwh__session_abtest_main_metrics_agg',
                            repository_location_name='etl_dwh_sublocation')


@run_status_sensor(
    monitored_jobs=[click_data_agg_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    # request_job=budget_revenue_daily_agg_job,
    description='monitored_jobs=[click_data_agg_job] -> start budget_revenue_daily_agg_job'
)
def budget_revenue_daily_agg_job_sensor():
    submit_external_job_run(job_name_with_prefix='dwh__budget_revenue_daily_agg',
                            repository_location_name='etl_dwh_sublocation')


@run_status_sensor(
    monitored_jobs=[click_data_agg_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[click_data_agg_job] -> start finance_report'
)
def finance_report_sensor():
    submit_external_job_run(job_name_with_prefix='dwh__finance_report',
                            repository_location_name='etl_dwh_sublocation')


@run_status_sensor(
    monitored_jobs=[email_abtest_account_agg_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    # request_job=budget_revenue_daily_agg_job,
    description='monitored_jobs=[email_abtest_account_agg_job] -> start cabacus'
)
def cabacus_sensor():
    # Skip cabacus if current time is 09:00 or later
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    if current_time.tm_hour >= 9:
        return SkipReason(f"Current time is {current_time.tm_hour}:00, skip cabacus")
    else:
        submit_external_job_run(job_name_with_prefix='main_job',
                                repository_location_name='cabacus',
                                instance='cabacus')


@run_status_sensor(
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=prc_imp_account_revenue_job_yesterday_date,
    description='''monitored_jobs=[prc_click_metric_agg_job_yesterday_date]
                   -> start prc_imp_account_revenue_job_yesterday_date. || Waiting for second [click_metric_agg_job] run at 11:30'''
)
def prc_imp_account_revenue_job_yesterday_date_sensor(context):
    # Skip if it's not the 9:30 utc run
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 570:
        return SkipReason("Waiting for 11:30 run")

    if context.dagster_run.job_name != prc_imp_account_revenue_job_yesterday_date.name:
        return RunRequest(run_key=None,
                          job_name=prc_imp_account_revenue_job_yesterday_date.name)
    else:
        return SkipReason(f"Don't report status of {prc_imp_account_revenue_job_yesterday_date}")


@run_status_sensor(
    monitored_jobs=[prc_imp_account_revenue_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=account_revenue_job,
    description='''monitored_jobs=[prc_imp_account_revenue_job_yesterday_date]
                   -> start account_revenue_job'''
)
def account_revenue_job_sensor(context):
    if context.dagster_run.job_name != account_revenue_job.name:
        return RunRequest(run_key=None,
                          job_name=account_revenue_job.name)
    else:
        return SkipReason(f"Don't report status of {account_revenue_job}")


@run_status_sensor(
    monitored_jobs=[prc_imp_account_revenue_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=account_revenue_abtest_agg_job,
    description='''monitored_jobs=[prc_imp_account_revenue_job_yesterday_date]
                   -> start account_revenue_abtest_agg_job'''
)
def account_revenue_abtest_agg_job_sensor(context):
    if context.dagster_run.job_name != account_revenue_abtest_agg_job.name:
        return RunRequest(run_key=None,
                          job_name=account_revenue_abtest_agg_job.name)
    else:
        return SkipReason(f"Don't report status of {account_revenue_abtest_agg_job}")


@run_status_sensor(
    minimum_interval_seconds=120,
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=mobile_app_business_metrics_job,
    description='''monitored_jobs=[prc_click_metric_agg_job_yesterday_date]
                   -> start mobile_app_business_metrics_job. || Waiting for second [click_metric_agg_job] run at 11:30'''
)
def mobile_app_business_metrics_job_sensor(context):
    # Skip if it's not the 9:30 utc run
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 570:
        return SkipReason("Waiting for 11:30 run")

    if context.dagster_run.job_name != mobile_app_business_metrics_job.name:
        return RunRequest(run_key=None,
                          job_name=mobile_app_business_metrics_job.name)
    else:
        return SkipReason(f"Don't report status of {mobile_app_business_metrics_job}")


@run_status_sensor(
    minimum_interval_seconds=180,
    monitored_jobs=[prc_click_metric_agg_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=mobile_app_activity_metrics_dau_job,
    description='''monitored_jobs=[prc_click_metric_agg_job_yesterday_date]
                   -> start mobile_app_activity_metrics_dau_job. || Waiting for second [click_metric_agg_job] run at 11:30'''
)
def mobile_app_activity_metrics_dau_job_sensor(context):
    # Skip if it's not the 9:30 utc run
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 570:
        return SkipReason("Waiting for 11:30 run")

    if context.dagster_run.job_name != mobile_app_activity_metrics_dau_job.name:
        return RunRequest(run_key=None,
                          job_name=mobile_app_activity_metrics_dau_job.name)
    else:
        return SkipReason(f"Don't report status of {mobile_app_activity_metrics_dau_job}")


@run_status_sensor(
    minimum_interval_seconds=120,
    monitored_jobs=[mobile_app_activity_metrics_dau_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=mobile_app_activity_metrics_wau_job,
    description='''monitored_jobs=[mobile_app_activity_metrics_dau_job]
                   -> start mobile_app_activity_metrics_wau_job'''
)
def mobile_app_activity_metrics_wau_job_sensor(context):
    if context.dagster_run.job_name != mobile_app_activity_metrics_wau_job.name:
        return RunRequest(run_key=None,
                          job_name=mobile_app_activity_metrics_wau_job.name)
    else:
        return SkipReason(f"Don't report status of {mobile_app_activity_metrics_wau_job}")


@run_status_sensor(
    minimum_interval_seconds=120,
    monitored_jobs=[mobile_app_activity_metrics_wau_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=mobile_app_activity_metrics_mau_job,
    description='''monitored_jobs=[mobile_app_activity_metrics_wau_job]
                   -> start mobile_app_activity_metrics_mau_job'''
)
def mobile_app_activity_metrics_mau_job_sensor(context):
    if context.dagster_run.job_name != mobile_app_activity_metrics_mau_job.name:
        return RunRequest(run_key=None,
                          job_name=mobile_app_activity_metrics_mau_job.name)
    else:
        return SkipReason(f"Don't report status of {mobile_app_activity_metrics_mau_job}")


@run_status_sensor(
    minimum_interval_seconds=120,
    monitored_jobs=[mobile_app_activity_metrics_mau_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=mobile_app_push_metrics_job,
    description='''monitored_jobs=[mobile_app_activity_metrics_mau_job]
                   -> start mobile_app_push_metrics_job'''
)
def mobile_app_push_metrics_job_sensor(context):
    if context.dagster_run.job_name != mobile_app_push_metrics_job.name:
        return RunRequest(run_key=None,
                          job_name=mobile_app_push_metrics_job.name)
    else:
        return SkipReason(f"Don't report status of {mobile_app_push_metrics_job}")


@run_status_sensor(
    monitored_jobs=[session_feature_action_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[session_feature_action_job] -> start dwh__mobile_app_first_launch_attribute'
)
def mobile_app_first_launch_attribute_job_sensor():
    submit_external_job_run(job_name_with_prefix='dwh__mobile_app_first_launch_attribute',
                            repository_location_name='etl_dwh_sublocation')


@run_status_sensor(
    minimum_interval_seconds=120,
    monitored_jobs=[insert_emails_job_yesterday_date],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=email_spam_report_agg_job,
    description='''monitored_jobs=[insert_emails_job_yesterday_date]
                   -> start email_spam_report_agg_job || Waiting for second [insert_emails_job] run at 09:50'''
)
def email_spam_report_agg_job_sensor(context):
    # Skip if it's not the 7:50 utc run
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 480:
        return SkipReason("Waiting for 09:50 run")

    if context.dagster_run.job_name != email_spam_report_agg_job.name:
        return RunRequest(run_key=None,
                          job_name=email_spam_report_agg_job.name)
    else:
        return SkipReason(f"Don't report status of {email_spam_report_agg_job}")
