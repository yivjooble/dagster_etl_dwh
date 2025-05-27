import os

from dotenv import load_dotenv
from dagster import (
    ScheduleDefinition,
    DefaultScheduleStatus,
)

# Import jobs
from etl_clickhouse.info_region.ops import info_region_job
from etl_clickhouse.info_region_submission.ops import info_region_submission_job
from etl_clickhouse.info_currency.ops import info_currency_job
from etl_clickhouse.session.ops import session_job
from etl_clickhouse.session_filter_action.ops import session_filter_action_job
from etl_clickhouse.session_search.ops import session_search_job
from etl_clickhouse.v_click_data_agg.ops import v_click_data_agg_job
from etl_clickhouse.v_search_agg.ops import v_search_agg_job
from etl_clickhouse.pr_project.pr_tmp_raw.ops import pr_tmp_raw_job
from etl_clickhouse.pr_project.search_cnt_pre_agg.ops import search_cnt_pre_agg_job
# history data
from etl_clickhouse.historical_job_sentinel_transfer.ops import historical_job_sentinel_transfer_job
from etl_clickhouse.historical_job_cluster_transfer.ops import historical_job_cluster_transfer_job

# Load env
load_dotenv()


# DEFINE SCHEDULES
def schedule_definition(job_name,
                        cron_schedule,
                        execution_timezone='Europe/Kiev',
                        default_status=DefaultScheduleStatus.RUNNING if os.environ.get(
                            'INSTANCE') == 'PRD' else DefaultScheduleStatus.STOPPED):
    return ScheduleDefinition(job=job_name,
                              cron_schedule=cron_schedule,
                              execution_timezone=execution_timezone,
                              default_status=default_status)


# DEFINE JOBS
schedule_historical_job_cluster_transfer = schedule_definition(
    job_name=historical_job_cluster_transfer_job,
    cron_schedule='15 20 * * *'
)
schedule_historical_job_sentinel_transfer = schedule_definition(
    job_name=historical_job_sentinel_transfer_job,
    cron_schedule='15 20 * * *'
)
schedule_v_search_agg = schedule_definition(
    job_name=v_search_agg_job,
    cron_schedule='30 13 * * *'
)
schedule_search_cnt_pre_agg = schedule_definition(
    job_name=search_cnt_pre_agg_job,
    cron_schedule='0 06 * * *'
)
schedule_pr_tmp_raw = schedule_definition(
    job_name=pr_tmp_raw_job,
    cron_schedule='0 06 * * *'
)
schedule_info_region = schedule_definition(
    job_name=info_region_job,
    cron_schedule='0 06 * * *'
)
schedule_info_region_submission = schedule_definition(
    job_name=info_region_submission_job,
    cron_schedule='0 06 * * *'
)
schedule_info_currency = schedule_definition(
    job_name=info_currency_job,
    cron_schedule='0 06 * * *'
)
schedule_session = schedule_definition(
    job_name=session_job,
    cron_schedule='0 07 * * *'
)
schedule_session_filter_action = schedule_definition(
    job_name=session_filter_action_job,
    cron_schedule='0 07 * * *'
)
schedule_session_search = schedule_definition(
    job_name=session_search_job,
    cron_schedule='0 07 * * *'
)
schedule_v_click_data_agg = schedule_definition(
    job_name=v_click_data_agg_job,
    cron_schedule='30 13 * * *'
)
