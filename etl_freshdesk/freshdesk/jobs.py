from dagster import (
    job,
    fs_io_manager
)
from etl_freshdesk.utils import get_io_manager_path, job_prefix
from etl_freshdesk.freshdesk.ops import *


JOB_PREFIX = job_prefix()


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'update_data_ev_2min',
     description=f'Updates ticket data.')
def update_data_ev_2min_job():
    int_update_ticket_info()
    ext_update_ticket_info()


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'remove_tickets',
     description=f'Remove tickets.')
def remove_tickets_job():
    int_spam = int_remove_spam_tickets()
    ext_spam = ext_remove_spam_tickets()

    int_remove_deleted_tickets(int_spam)
    ext_remove_deleted_tickets(ext_spam)


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'update_data_ev_1h',
     description=f'Updates satisfaction surveys and solution articles data.')
def update_data_ev_1h_job():
    res = ext_update_satisfaction_surveys()
    res2 = ext_update_solution_articles(res)
    delete_old_logs_op(res2)
