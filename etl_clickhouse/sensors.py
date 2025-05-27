import os
from dagster import (
    run_failure_sensor,
    DefaultSensorStatus,
    RunFailureSensorContext
)
from utility_hub.sensor_messages import send_dwh_alert_slack_message


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')
