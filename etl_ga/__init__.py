from dagster import (
    Definitions
)

# IMPORT SENSORS
from .sensors import (
    monitor_all_jobs_sensor
)

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
               schedule_ga4_dte,
               schedule_ga4_general,
               schedule_ga4_landing_page_bounce,
               schedule_ga4_dte_ea_sha_reg_rs,
               schedule_adsense_etl,
               schedule_ga4_employer,
               ],
    assets=[
            #*ga4_dte_assets,
            *ga4_general_assets,
            ],
    jobs=[
          ga4_dte_job,
          ga4_general_job,
          ga4_landing_page_bounce_job,
          ga4_dte_ea_sha_reg_rs_job,
          adsense_etl_job,
          ga4_landing_page_bounce_job,
          ga4_employer_job,
          ],
    sensors=[
        monitor_all_jobs_sensor
    ],
)
