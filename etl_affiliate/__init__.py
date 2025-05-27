from dagster import (
    Definitions
)

# IMPORT SENSORS
from .sensors import *

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *
from etl_affiliate.transfer_testing_data.ops import transfer_testing_data_job
from etl_affiliate.cpc_clustering.ops import start_cpc_clustering_job

# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_collect_click_statistics_cloudberry_job,
        schedule_collect_click_statistics,
        schedule_transfer_tables_daily,
        schedule_transfer_missing_rows,
        schedule_transfer_tables_every_6h,
        schedule_update_short_tables,
        schedule_collect_additional_statistics,
        schedule_delete_old_data,
        schedule_data_completeness,
        schedule_refresh_tracking_project_list,
        schedule_collect_cloudflare_log_data,
        schedule_insert_project_budgets_job
        #    schedule_start_cpc_clustering,
    ],
    jobs=[
        collect_click_statistics_cloudberry_job,
        collect_click_statistics_job,
        collect_additional_statistics_job,
        transfer_tables_daily_job,
        transfer_missing_rows_job,
        transfer_tables_every_6h_job,
        update_short_tables_job,
        start_cpc_clustering_job,
        transfer_testing_data_job,
        delete_old_data_job,
        data_completeness_job,
        refresh_tracking_project_list_job,
        collect_cloudflare_log_data_job,
        insert_project_budgets_job
    ],
    sensors=[
        monitor_all_jobs_sensor,
        collect_abtest_significance_metrics_sensor
    ]
)
