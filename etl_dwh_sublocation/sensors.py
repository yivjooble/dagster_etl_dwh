import time
from time import timezone
import os
from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    run_failure_sensor,
    run_status_sensor,
    DagsterRunStatus,
    SkipReason
)
from utility_hub.sensor_messages import send_dwh_alert_slack_message
from utility_hub.core_tools import submit_external_job_run
from etl_dwh_sublocation.mobile_app_first_launch_attribute.ops import mobile_app_first_launch_attribute_job
from etl_dwh_sublocation.budget_revenue_daily_agg.ops import budget_revenue_daily_agg_job


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')


@run_status_sensor(
    monitored_jobs=[mobile_app_first_launch_attribute_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[mobile_app_first_launch_attribute_job] -> start rpl__mobile_app_first_launch_attribute_to_replica'
)
def mobile_app_first_launch_attribute_to_replica_sensor():
    submit_external_job_run(job_name_with_prefix='rpl__mobile_app_first_launch_attribute_to_replica',
                            repository_location_name='etl_repstat')


@run_status_sensor(
    monitored_jobs=[budget_revenue_daily_agg_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='''monitored_jobs=[budget_revenue_daily_agg_job] -> start api__sf_project_country.
                  || Waiting for second [budget_revenue_daily_agg_job] run'''
)
def sf_project_country_sensor():
    # Skip if it's not the 10:00 utc runtimezone
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 600:
        return SkipReason("Waiting for 12:00 run")
    else:
        submit_external_job_run(job_name_with_prefix='api__sf_project_country',
                                repository_location_name='etl_api')
