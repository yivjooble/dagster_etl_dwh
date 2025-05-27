import os
from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    DagsterRunStatus,
    run_failure_sensor,
    run_status_sensor
)
from utility_hub.sensor_messages import send_dwh_alert_slack_message
from utility_hub.core_tools import submit_external_job_run

from etl_jooble_internal.warehouse_jo.adwords.ops import adwords_current_date_job, adwords_yesterday_job
from etl_jooble_internal.postgres_1_170.apply.ops import apply_conversion_service_job_yesterday
from etl_jooble_internal.seo_server.seo_test_groups.ops import seo_test_groups_job
from etl_jooble_internal.postgres_137.conversions_appcast.ops import conversions_appcast_job

@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')


# @run_status_sensor(
#     monitored_jobs=[adwords_yesterday_job],
#     run_status=DagsterRunStatus.SUCCESS,
#     default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
#     description='monitored_jobs=[adwords_yesterday_job] -> start adwords_to_compare_yesterday_job'
# )
# def adwords_to_compare_yesterday_job_sensor():
#     submit_external_job_run(job_name_with_prefix='api__adwords_to_compare_yesterday',
#                             repository_location_name='etl_api')


@run_status_sensor(
    monitored_jobs=[apply_conversion_service_job_yesterday],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[apply_conversion_service_job_yesterday] -> start rpl__apply_conversion_service_to_replica'
)
def apply_conversion_service_to_replica_sensor():
    submit_external_job_run(job_name_with_prefix='rpl__apply_conversion_service_to_replica',
                            repository_location_name='etl_repstat')


@run_status_sensor(
    monitored_jobs=[seo_test_groups_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[seo_test_groups_job] -> start seo_test_url_keyword_job'
)
def seo_test_url_keyword_job_sensor():
    submit_external_job_run(job_name_with_prefix='dwh__seo_test_url_keyword',
                            repository_location_name='etl_dwh')


@run_status_sensor(
    monitored_jobs=[conversions_appcast_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='monitored_jobs=[conversions_appcast_job] -> start ext_stat_away_match_job'
)
def ext_stat_away_match_job_sensor():
    submit_external_job_run(job_name_with_prefix='prd__ext_stat_away_match',
                            repository_location_name='etl_production')
