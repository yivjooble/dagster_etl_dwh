import os
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus
)

from etl_freshdesk.freshdesk.jobs import update_data_ev_2min_job, update_data_ev_1h_job, remove_tickets_job
from etl_freshdesk.reviews.jobs import update_reviews_data_job


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


schedule_update_data_ev_2min = define_schedule(job=update_data_ev_2min_job,
                                               cron_schedule="*/2 * * * *", )
schedule_update_data_ev_1h = define_schedule(job=update_data_ev_1h_job,
                                             cron_schedule="0 * * * *", )
schedule_update_reviews_data = define_schedule(job=update_reviews_data_job,
                                               cron_schedule="0 9,17 * * *", )
schedule_remove_tickets = define_schedule(job=remove_tickets_job,
                                          cron_schedule="*/30 * * * *", )
