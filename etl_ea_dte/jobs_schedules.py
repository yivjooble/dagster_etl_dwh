import os
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus
)

from etl_ea_dte.empl_acc_funnel_by_reg_date.ops import empl_acc_funnel_by_reg_date_job
from etl_ea_dte.empl_acc_revenue.ops import empl_acc_revenue_job
from etl_ea_dte.empl_acc_job_statistics.ops import empl_acc_job_statistics_job


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


schedule_empl_acc_funnel_by_reg_date = define_schedule(
    job=empl_acc_funnel_by_reg_date_job,
    cron_schedule="0 08 * * *",
)
schedule_empl_acc_revenue = define_schedule(
    job=empl_acc_revenue_job,
    cron_schedule="0 08 * * *",
)
schedule_empl_acc_job_statistics = define_schedule(
    job=empl_acc_job_statistics_job,
    cron_schedule="0 08 * * *",
)
