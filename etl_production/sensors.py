import os

from dagster import (
    run_failure_sensor, 
    run_status_sensor,
    DagsterRunStatus,
    RunFailureSensorContext,
    DefaultSensorStatus,
    RunRequest,
    SkipReason
)

from utility_hub.sensor_messages import send_dwh_alert_slack_message

# IMPORT JOBS
from .scroll_click_position_agg.ops import scroll_click_position_agg_job
from .session_daily_agg.ops import session_daily_agg_job


# slack error alert
@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
                    minimum_interval_seconds=30,
                    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')
    

@run_status_sensor(
    monitored_jobs=[session_daily_agg_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=scroll_click_position_agg_job,
    description='monitored_jobs=[session_daily_agg_job] -> start scroll_click_position_agg_job'
    )
def session_daily_agg_sensor(context):
    if context.dagster_run.job_name != scroll_click_position_agg_job.name:
        return RunRequest(run_key=None,
                        job_name=scroll_click_position_agg_job.name)
    else:
        return SkipReason(f"Don't report status of {session_daily_agg_job}")
