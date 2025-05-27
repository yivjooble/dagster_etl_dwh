import os
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus
)

# DEFINE JOBS
# prod_etl
from etl_affiliate.transfer_tables_every_6h.ops import transfer_tables_every_6h_job
from etl_affiliate.transfer_missing_rows.ops import transfer_missing_rows_job
from etl_affiliate.transfer_tables_daily.ops import transfer_tables_daily_job
from etl_affiliate.collect_click_statistics.jobs import collect_click_statistics_job, collect_click_statistics_cloudberry_job
from etl_affiliate.update_short_tables.ops import update_short_tables_job
from etl_affiliate.collect_additional_statistics.ops import collect_additional_statistics_job
from etl_affiliate.delete_old_data.ops import delete_old_data_job
from etl_affiliate.data_completeness.ops import data_completeness_job
from etl_affiliate.refresh_tracking_project_list.ops import refresh_tracking_project_list_job
from etl_affiliate.transfer_cloudflare_logs.ops import collect_cloudflare_log_data_job
from etl_affiliate.insert_project_budget.ops import insert_project_budgets_job


# DEFINE SCHEDULES
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


# schedule_start_cpc_clustering = ScheduleDefinition(job=start_cpc_clustering_job, cron_schedule="0 10 * * *",
# ,)
schedule_update_short_tables = define_schedule(
    job=update_short_tables_job,
    cron_schedule="0 07 * * *",
)
schedule_transfer_tables_every_6h = define_schedule(
    job=transfer_tables_every_6h_job,
    cron_schedule="0 18,0,6,12 * * *",
)
schedule_transfer_missing_rows = define_schedule(
    job=transfer_missing_rows_job,
    cron_schedule="15 10 * * *",
)
schedule_transfer_tables_daily = define_schedule(
    job=transfer_tables_daily_job,
    cron_schedule="30 09 * * *",
)
schedule_collect_click_statistics = define_schedule(
    job=collect_click_statistics_job,
    cron_schedule="40 10 * * *",
)
schedule_collect_click_statistics_cloudberry_job = define_schedule(
    job=collect_click_statistics_cloudberry_job,
    cron_schedule="40 10 * * *",
)
schedule_collect_additional_statistics = define_schedule(
    job=collect_additional_statistics_job,
    cron_schedule="15 12 * * *",
)
schedule_delete_old_data = define_schedule(
    job=delete_old_data_job,
    cron_schedule="00 15 * * *",
)
schedule_data_completeness = define_schedule(
    job=data_completeness_job,
    cron_schedule="00 16 * * *",
)
schedule_refresh_tracking_project_list = define_schedule(
    job=refresh_tracking_project_list_job,
    cron_schedule="15 10 * * *",
)
schedule_collect_cloudflare_log_data = define_schedule(
    job=collect_cloudflare_log_data_job,
    cron_schedule="30 09 * * *",
)
schedule_insert_project_budgets_job = define_schedule(
    job=insert_project_budgets_job,
    cron_schedule="30 13 * * *",
)