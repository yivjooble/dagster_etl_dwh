import os
from dagster import (
    define_asset_job,
    ScheduleDefinition,
    DefaultScheduleStatus
)

# IMPORT ASSETS
from .assets import *
from .ga4.ga4_dte.ops import ga4_dte_job
from .ga4.ga4_dte.ops import ga4_dte_ea_sha_reg_rs_job
from .ga4.adsense_etl.ops import adsense_etl_job

from .ga4.ga4_landing_page_bounce.ops import ga4_landing_page_bounce_job
from .ga4.ga4_employer.ops import ga4_employer_job


# DEFINE JOBS
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


# etl_ga
# ga4_dte_job = define_asset_job(name="ga4__dte", selection=ga4_dte_assets, description='imp_api.ga4_dte')
ga4_general_job = define_asset_job(
    name="ga4__general", selection=ga4_general_assets, description="imp_api.ga4_general"
)
# ga4_landing_page_bounce_job = define_asset_job(name="ga4__landing_page_bounce", selection=ga4_landing_page_bounce_assets, description='imp_api.ga4_landing_page_bounce')


# DEFINE SCHEDULES
# imp_api
schedule_adsense_etl = define_schedule(
    job=adsense_etl_job,
    cron_schedule="15 9 * * *",
)
schedule_ga4_dte_ea_sha_reg_rs = define_schedule(
    job=ga4_dte_ea_sha_reg_rs_job,
    cron_schedule="0 9 * * *",
)
schedule_ga4_dte = define_schedule(
    job=ga4_dte_job,
    cron_schedule="0 9 * * *",
    
)
schedule_ga4_general = define_schedule(
    job=ga4_general_job,
    cron_schedule="00 08 * * *",
)
schedule_ga4_landing_page_bounce = define_schedule(
    job=ga4_landing_page_bounce_job,
    cron_schedule="30 10,13 * * *",
)
schedule_ga4_employer = define_schedule(
    job=ga4_employer_job,
    cron_schedule="00 08 * * *",
)
