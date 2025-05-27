from dagster import (
    Definitions
)

from .sensors import *
# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
            schedule_info_region,
            schedule_session,
            schedule_session_filter_action,
            schedule_session_search,
            schedule_info_region_submission,
            schedule_info_currency,
            schedule_v_click_data_agg,
            schedule_pr_tmp_raw,
            schedule_search_cnt_pre_agg,
            schedule_v_search_agg,
            schedule_historical_job_sentinel_transfer,
            schedule_historical_job_cluster_transfer,
               ],
    assets=[
            ],
    jobs=[
            info_region_job,
            session_job,
            session_filter_action_job,
            session_search_job,
            info_region_submission_job,
            info_currency_job,
            v_click_data_agg_job,
            pr_tmp_raw_job,
            search_cnt_pre_agg_job,
            v_search_agg_job,
            historical_job_sentinel_transfer_job,
            historical_job_cluster_transfer_job,
          ],
    sensors=[
        monitor_all_jobs_sensor
    ],
    
)
