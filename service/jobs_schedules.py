import os

from dotenv import load_dotenv

from dagster import (
    define_asset_job,
    ScheduleDefinition,
    DefaultScheduleStatus,
)


# DEFINE JOBS
from service.trino.trino_to_cloudberry.ops import trino_to_cloudberry_job
from service.grafana.ram_monitor.ops import grafana_monitor_rpl_nl, grafana_monitor_rpl_us
from service.google_sheet_ssu.ops import google_sheet_ssu_job
from service.analyse_schema_dwh.ops import (analyze_schema_dwh_aggregation_job,
                                            analyze_schema_dwh_imp_job,
                                            analyze_schema_dwh_imp_api_job,
                                            analyze_schema_dwh_imp_employer_job,
                                            analyze_schema_dwh_imp_statistic_job,
                                            analyze_schema_dwh_mobile_app_job,
                                            analyze_schema_dwh_affiliate_job)
from service.analyse_schema.ops import analyse_public_schema, analyse_an_schema
from service.dwh_postgres.exec_sql_code.ops import dwh_postgres_exec_sql_code_job
from service.dwh_cloudberry.exec_sql_code.ops import dwh_cloudberry_exec_sql_code_job
from service.test_copy.ops import copy_table_job
from service.trino.trino_exec.ops import execute_sql_on_trino_job
from .repstat_exec_sql_code.ops import repstat_exec_sql_code_job
from .repstat_prod_reload_data.ops import repstat_prod_reload_data_job
from .sync_duplicate_events_marking_with_mssql.ops import sync_duplicate_events_marking_with_mssql_job
from .sync_session_click_primary_click_with_mssql.ops import sync_session_click_primary_click_with_mssql_job
from service.vacuum_full.ops import vacuum_full_job
from service.watcher.check_locks.main import watcher_check_locks
from service.watcher.check_non_renewable_tables.main import check_non_renewable_tables
from service.watcher.check_pentaho_and_pgagent_logs.main import check_pentaho_and_pgagent_logs
from service.watcher.check_pentaho_scheduler.main import check_pentaho_scheduler
from service.watcher.check_process.main import check_processes
from service.partition_analyse.ops import analyse_partitions_job, analyse_partitions_job_current_date
# from service.partition_full_analyse.ops import analyse_full_partitions_job
from service.partition_unpartitioned_table.ops import (
    partition_unpartitioned_table_job,
    partition_unpartitioned_table_us_job
)
from service.tableau_monitor.jobs_monitor.ops import check_tableau_jobs_status_job
from service.multi_table_copy.ops import multi_table_copy_job
from service.archive_and_cleanup_tables.ops import archive_and_cleanup_tables_job
from service.refresh_skew_report.ops import refresh_skew_report_job
from service.analyse_schema_tables_cb.ops import analyse_schema_tables_cb_job

load_dotenv()


def define_schedule(
        job,
        cron_schedule,
        execution_timezone='Europe/Kiev',
        default_status=DefaultScheduleStatus.RUNNING if os.environ.get(
            'INSTANCE') == 'PRD' else DefaultScheduleStatus.STOPPED,
):
    return ScheduleDefinition(
        job=job,
        cron_schedule=cron_schedule,
        execution_timezone=execution_timezone,
        default_status=default_status,
    )


# DEFINE SCHEDULES
# schedule_check_tableau_jobs_status = define_schedule(
#     job=check_tableau_jobs_status_job,
#     cron_schedule="0 * * * *",
# )
# aggregation
# schedule_analyse_full_partitions_job = define_schedule(
#     job=analyse_full_partitions_job,
#     cron_schedule="0 08 * * 7",
# )
schedule_grafana_monitor_rpl_us = define_schedule(
    job=grafana_monitor_rpl_us,
    cron_schedule="*/30 * * * *",
)
schedule_grafana_monitor_rpl_nl = define_schedule(
    job=grafana_monitor_rpl_nl,
    cron_schedule="*/30 * * * *",
)
schedule_google_sheet_ssu = define_schedule(
    job=google_sheet_ssu_job,
    cron_schedule="0 09 * * *",
)
schedule_analyze_schema_dwh_aggregation = define_schedule(
    job=analyze_schema_dwh_aggregation_job,
    cron_schedule="25 20 * * *",
)
schedule_analyze_schema_dwh_imp = define_schedule(
    job=analyze_schema_dwh_imp_job,
    cron_schedule="0 20 * * *",
)
schedule_analyze_schema_dwh_imp_api = define_schedule(
    job=analyze_schema_dwh_imp_api_job,
    cron_schedule="0 20 * * *",
)
schedule_analyze_schema_dwh_imp_employer = define_schedule(
    job=analyze_schema_dwh_imp_employer_job,
    cron_schedule="0 20 * * *",
)
schedule_analyze_schema_dwh_imp_statistic = define_schedule(
    job=analyze_schema_dwh_imp_statistic_job,
    cron_schedule="0 20 * * *",
)
schedule_analyze_schema_dwh_mobile_app = define_schedule(
    job=analyze_schema_dwh_mobile_app_job,
    cron_schedule="0 20 * * *",
)
schedule_analyze_schema_dwh_affiliate = define_schedule(
    job=analyze_schema_dwh_affiliate_job,
    cron_schedule="0 20 * * *",
)


schedule_analyse_an_schema = define_schedule(
    job=analyse_an_schema,
    cron_schedule="0 20 * * *",
)
schedule_analyse_public_schema = define_schedule(
    job=analyse_public_schema,
    cron_schedule="25 20 * * *",
)
schedule_analyse_partitions_job = define_schedule(
    job=analyse_partitions_job,
    cron_schedule="0 08 * * *",
)
schedule_analyse_partitions_job_current_date = define_schedule(
    job=analyse_partitions_job_current_date,
    cron_schedule="35 15 * * *",
)
schedule_sync_duplicate_events_marking_with_mssql = define_schedule(
    job=sync_duplicate_events_marking_with_mssql_job,
    cron_schedule="0 09 * * *",
)
schedule_sync_session_click_primary_click_with_mssql = define_schedule(
    job=sync_session_click_primary_click_with_mssql_job,
    cron_schedule="0 10,11 * * *",
)
schedule_vacuum_full_job = define_schedule(
    job=vacuum_full_job,
    cron_schedule="0 18 * * 6"
)
schedule_watcher_check_locks = define_schedule(
    job=watcher_check_locks,
    cron_schedule="*/30 * * * *"
)
schedule_check_non_renewable_tables = define_schedule(
    job=check_non_renewable_tables,
    cron_schedule="0 15 * * *"
)
schedule_check_pentaho_and_pgagent_logs = define_schedule(
    job=check_pentaho_and_pgagent_logs,
    cron_schedule="*/60 * * * *"
)
schedule_check_pentaho_scheduler = define_schedule(
    job=check_pentaho_scheduler,
    cron_schedule="*/60 * * * *"
)
schedule_check_processes = define_schedule(
    job=check_processes,
    cron_schedule="50 23 * * *"
)
schedule_partition_unpartitioned_table_us = define_schedule(
    job=partition_unpartitioned_table_us_job,
    cron_schedule="45 19 * * *"
)
schedule_archive_and_cleanup_tables = define_schedule(
    job=archive_and_cleanup_tables_job,
    cron_schedule="05 18 * * *"
)
schedule_refresh_skew_report = define_schedule(
    job=refresh_skew_report_job,
    cron_schedule="15 04 * * 1,3,5"
)
schedule_analyse_schema_tables_cb = define_schedule(
    job=analyse_schema_tables_cb_job,
    cron_schedule=["45 03 * * *", "20 12 * * *"]
)
