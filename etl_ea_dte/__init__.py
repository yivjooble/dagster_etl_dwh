from dagster import Definitions
from .jobs_schedules import *
from .sensors import *

defs = Definitions(
    schedules=[
        schedule_empl_acc_funnel_by_reg_date,
        schedule_empl_acc_revenue,
        schedule_empl_acc_job_statistics,
    ],
    jobs=[
        empl_acc_funnel_by_reg_date_job,
        empl_acc_revenue_job,
        empl_acc_job_statistics_job,
    ],
    sensors=[
        monitor_all_jobs_sensor,
    ]
)
