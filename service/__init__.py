from dagster import (
    Definitions
)

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *

# IMPORT SENSORS
from .sensors import (
    monitor_all_jobs_sensor,
    tableau_run_failure_sensor
)

# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_grafana_monitor_rpl_us,
        schedule_grafana_monitor_rpl_nl,
        schedule_google_sheet_ssu,
        schedule_analyse_an_schema,
        schedule_analyse_public_schema,
        schedule_sync_duplicate_events_marking_with_mssql,
        schedule_sync_session_click_primary_click_with_mssql,
        schedule_watcher_check_locks,
        schedule_check_non_renewable_tables,
        schedule_check_pentaho_and_pgagent_logs,
        schedule_check_pentaho_scheduler,
        schedule_check_processes,
        schedule_analyse_partitions_job,
        schedule_analyse_partitions_job_current_date,
        schedule_partition_unpartitioned_table_us,
        schedule_archive_and_cleanup_tables,
        schedule_refresh_skew_report,
        schedule_analyse_schema_tables_cb,
    ],
    assets=[
    ],
    jobs=[
        trino_to_cloudberry_job,
        grafana_monitor_rpl_us,
        grafana_monitor_rpl_nl,
        google_sheet_ssu_job,
        analyse_public_schema,
        analyse_an_schema,
        dwh_postgres_exec_sql_code_job,
        dwh_cloudberry_exec_sql_code_job,
        copy_table_job,
        execute_sql_on_trino_job,
        check_tableau_jobs_status_job,
        repstat_exec_sql_code_job,
        repstat_prod_reload_data_job,
        sync_duplicate_events_marking_with_mssql_job,
        sync_session_click_primary_click_with_mssql_job,
        watcher_check_locks,
        check_non_renewable_tables,
        check_pentaho_and_pgagent_logs,
        check_pentaho_scheduler,
        check_processes,
        analyse_partitions_job,
        analyse_partitions_job_current_date,
        partition_unpartitioned_table_job,
        partition_unpartitioned_table_us_job,
        multi_table_copy_job,
        archive_and_cleanup_tables_job,
        refresh_skew_report_job,
        analyse_schema_tables_cb_job,
    ],
    sensors=[
        monitor_all_jobs_sensor,
        tableau_run_failure_sensor
    ],

)
