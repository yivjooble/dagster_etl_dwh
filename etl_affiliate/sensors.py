import os

from .collect_click_statistics.jobs import collect_click_statistics_job
from .collect_abtest_metrics.ops import collect_abtest_significance_metrics_job

from dagster import (
    run_status_sensor,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    SkipReason,
    run_failure_sensor,
    RunFailureSensorContext
)
from etl_affiliate.utils.messages import send_dwh_alert_slack_message


@run_failure_sensor(default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
                    minimum_interval_seconds=30,
                    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>\n'
                                 f'<@U06E7T8LP8X>\n'
                                 f'<@U018GESMPDJ>')


@run_status_sensor(
    monitored_jobs=[collect_click_statistics_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=(
        DefaultSensorStatus.RUNNING
        if os.environ.get("INSTANCE") == "PRD"
        else DefaultSensorStatus.STOPPED
    ),
    request_job=collect_abtest_significance_metrics_job,
    description="monitored_jobs=[collect_click_statistics_job] -> start collect_abtest_significance_metrics_job",
)
def collect_abtest_significance_metrics_sensor(context):
    if context.dagster_run.job_name != collect_abtest_significance_metrics_job.name:
        return RunRequest(
            run_key=None, job_name=collect_abtest_significance_metrics_job.name
        )
    else:
        return SkipReason(f"Don't report status of {collect_click_statistics_job}")
