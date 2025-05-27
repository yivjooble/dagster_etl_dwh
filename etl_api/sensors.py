import os
from dagster import (
    run_status_sensor,
    run_failure_sensor,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    SkipReason,
    DagsterRunStatus
)
from utility_hub.sensor_messages import send_dwh_alert_slack_message
from etl_api.salesforce.contractor.ops import sf_contractor_job
from etl_api.salesforce.account.ops import sf_account_job
from etl_api.salesforce.project_country.ops import sf_project_country_job
from etl_api.salesforce.auction_campaign.ops import sf_auction_campaign_job
from etl_api.salesforce.auction_campaign_metrics.ops import sf_auction_campaign_metrics_job


@run_failure_sensor(
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description='send slack msg to dwh_alert if job error')
def monitor_all_jobs_sensor(context: RunFailureSensorContext):
    send_dwh_alert_slack_message(f':error_alert: *ERROR*\n> {context.failure_event.message}\n'
                                 f'<!subteam^S02ETK2JYLF|dwh.analysts>')


@run_status_sensor(
    minimum_interval_seconds=60,
    monitored_jobs=[sf_contractor_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=sf_account_job,
    description='''monitored_jobs=[sf_contractor_job]
                   -> start sf_account_job'''
)
def sf_account_job_sensor(context):
    if context.dagster_run.job_name != sf_account_job.name:
        return RunRequest(run_key=None,
                          job_name=sf_account_job.name)
    else:
        return SkipReason(f"Don't report status of {sf_account_job}")


@run_status_sensor(
    minimum_interval_seconds=60,
    monitored_jobs=[sf_project_country_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=sf_auction_campaign_job,
    description='''monitored_jobs=[sf_project_country_job]
                   -> start sf_auction_campaign_job'''
)
def sf_auction_campaign_sensor(context):
    if context.dagster_run.job_name != sf_auction_campaign_job.name:
        return RunRequest(run_key=None,
                          job_name=sf_auction_campaign_job.name)
    else:
        return SkipReason(f"Don't report status of {sf_auction_campaign_job}")


@run_status_sensor(
    minimum_interval_seconds=60,
    monitored_jobs=[sf_auction_campaign_job],
    run_status=DagsterRunStatus.SUCCESS,
    default_status=DefaultSensorStatus.RUNNING if os.environ.get('INSTANCE') == 'PRD' else DefaultSensorStatus.STOPPED,
    request_job=sf_auction_campaign_metrics_job,
    description='''monitored_jobs=[sf_auction_campaign_job]
                   -> start sf_auction_campaign_metrics_job'''
)
def sf_auction_campaign_metrics_job_sensor(context):
    if context.dagster_run.job_name != sf_auction_campaign_metrics_job.name:
        return RunRequest(run_key=None,
                          job_name=sf_auction_campaign_metrics_job.name)
    else:
        return SkipReason(f"Don't report status of {sf_auction_campaign_metrics_job}")
