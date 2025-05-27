from dagster import Definitions
from etl_freshdesk.jobs_schedules import *
from .sensors import *

defs = Definitions(
    schedules=[
        schedule_update_data_ev_2min,
        schedule_update_data_ev_1h,
        schedule_update_reviews_data,
        schedule_remove_tickets
    ],
    jobs=[
        update_data_ev_2min_job,
        update_data_ev_1h_job,
        update_reviews_data_job,
        remove_tickets_job
    ],
    sensors=[
        monitor_all_jobs_sensor
    ]
)
