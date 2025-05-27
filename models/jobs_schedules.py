import os

from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)


# DEFINE JOBS
from models.aggregation.click_metric.jobs import click_metric_yesterday_date_model


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
schedule_click_metric_yesterday_date_model = define_schedule(
    job=click_metric_yesterday_date_model,
    cron_schedule=["0 07 * * *", "30 11 * * *"]
)
