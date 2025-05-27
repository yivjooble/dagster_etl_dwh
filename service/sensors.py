import os
from dagster import (
    sensor,
    RunFailureSensorContext,
    DefaultSensorStatus,
    run_failure_sensor,
)

from utility_hub.core_tools import submit_external_job_run
from service.utils.db_operations import select_from_db
from utility_hub.sensor_messages import send_dwh_alert_slack_message


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')


@sensor(
    minimum_interval_seconds=720,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    description='Check postgres table for Tableau jobs status, if there is any job in a table, check and send slack message if job failed'
)
def tableau_run_failure_sensor(context: RunFailureSensorContext):
    # Fetch data from postgres table
    jobs = select_from_db("SELECT _id, _datasource_name FROM public.tableau_jobs")

    if not jobs:
        context.log.info("No jobs to check")
        return

    submit_external_job_run(job_name_with_prefix='service__check_tableau_jobs_status',
                            repository_location_name='service')
