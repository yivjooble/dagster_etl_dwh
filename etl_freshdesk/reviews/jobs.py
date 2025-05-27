from dagster import (
    job,
    fs_io_manager
)
from etl_freshdesk.utils import get_io_manager_path, job_prefix
from etl_freshdesk.reviews.ops import *


JOB_PREFIX = job_prefix()


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'update_reviews_data',
     description=f'Updates trustpilot & mobile app reviews data.')
def update_reviews_data_job():
    collect_trustpilot_data_op()
    collect_reviews_data_op()
