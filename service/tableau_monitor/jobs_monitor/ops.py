import time
import os
import tableauserverclient as tsc
import pandas as pd
from dagster import (
    op,
    job,
    fs_io_manager,
    Failure
)
from sqlalchemy import text
from utility_hub.utils import db_conn_sqlalchemy

# project import
from ...utils.io_manager_path import get_io_manager_path
# module import
from ...utils.rplc_job_config import job_config
from ...utils.utils import job_prefix
from ...utils.db_operations import execute_on_db, select_from_db
from ...utils.messages import send_dwh_alert_slack_message
from utility_hub.core_tools import get_creds_from_vault


JOB_NAME = "check_tableau_jobs_status"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
USERS_TO_INFORM = "<!subteam^S02ETK2JYLF|dwh.analysts>"  # tag dwh team


def update_job_completion_time_in_db(job_id: str, completion_time: pd.Timestamp, table_name: str = 'tableau_jobs', schema: str = 'public'):
    """
    Update the _completed_at timestamp for a given job_id in the specified table.
    """
    engine = db_conn_sqlalchemy()
    try:
        with engine.connect() as connection:
            stmt = text(f"""
                UPDATE {schema}.{table_name}
                SET _completed_at = :completion_time
                WHERE _id = :job_id
            """)
            connection.execute(stmt, {"completion_time": completion_time, "job_id": job_id})
            connection.commit()
    except Exception as e:
        raise Failure(f"Failed to update job completion time in the database for job_id {job_id}: {str(e)}")


def handle_finished_job(job_id, job_notes, context, status_message, datasource_name):
    context.log.info(f"{status_message}\n"
                   f"Datasource name: {datasource_name}\n"
                   f"Job id: {job_id}\n"
                   f"Job notes: {job_notes}\n")
    update_job_completion_time_in_db(job_id=job_id, completion_time=pd.Timestamp.now(tz='UTC'))


def handle_failed_job(job_id, job_notes, context, status_message, datasource_name):
    context.log.info(f"{status_message}\n"
                     f"Datasource name: {datasource_name}\n"
                     f"Job id: {job_id}\n"
                     f"Job notes: {job_notes}\n")
    send_dwh_alert_slack_message(
        f":rotating_light: *Tableau job failed*\n"
        f"*Datasource Name:* `{datasource_name}`\n"
        f"*Job Notes:*\n"
        f"> {job_notes}\n"
        f"*Inform:* {USERS_TO_INFORM}\n"
        f"*Link:* <https://tableau.jooble.com/#/jobs?order=status:desc,jobRequestedTime:desc|Jobs page link>"
    )
    # execute_on_db(f"DELETE FROM public.tableau_jobs WHERE _id = '{job_id}'")
    update_job_completion_time_in_db(job_id=job_id, completion_time=pd.Timestamp.now(tz='UTC'))


def handle_in_progress_job(job_notes, context, status_message, datasource_name):
    context.log.info(f"{status_message}\n"
                     f"Job in progress\n"
                     f"Job notes: {job_notes}\n"
                     f"Datasource name: {datasource_name}")


def handle_unknown_status(context):
    context.log.info("Unknown status")


@op
def check_tableau_jobs_status(context):
    """
    Check the status of the Tableau jobs
    """
    # Fetch data from postgres table
    jobs = select_from_db("SELECT _id, _datasource_name FROM public.tableau_jobs")

    if not jobs:
        context.log.info("No jobs to check")
        return

    # Connect to Tableau server
    tableau_auth = tsc.TableauAuth(username=get_creds_from_vault('TABLEAU_USERNAME'),
                                   password=get_creds_from_vault('TABLEAU_PASSWORD'),
                                   site_id='')
    server = tsc.Server('https://tableau.jooble.com')
    server.version = '3.1'

    job_status_mapping = {
        0: ("Job completed successfully", handle_finished_job),
        1: ("Job failed", handle_failed_job),
        2: ("Job cancelled", handle_failed_job),
        -1: ("Job in progress", handle_in_progress_job)
    }

    with server.auth.sign_in(tableau_auth):
        for checked_job in jobs:
            retry_attempts = 3
            while retry_attempts > 0:
                try:
                    job_item = server.jobs.get_by_id(checked_job[0])
                    job_id = checked_job[0]
                    datasource_name = checked_job[1]

                    status_message, handler = job_status_mapping.get(job_item.finish_code, ("Unknown status", handle_unknown_status))
                    if handler:
                        if job_item.finish_code == 0:
                            handler(job_id, job_item.notes, context, status_message, datasource_name)
                        elif job_item.finish_code in [1, 2]:
                            handler(job_id, job_item.notes, context, status_message, datasource_name)
                        elif job_item.finish_code == -1:
                            handler(job_item.notes, context, status_message, datasource_name)
                        else:
                            handler(context)
                    break

                except tsc.ServerResponseError as e:
                    retry_attempts -= 1
                    if retry_attempts == 0:
                        context.log.error(f"Failed to retrieve job details for ID {checked_job[0]} after multiple attempts: {str(e)}")
                    else:
                        context.log.warning(f"Temporary issue retrieving job details for ID {checked_job[0]}. Retrying... ({3 - retry_attempts} of 3)")
                        time.sleep(5)


@job(config=job_config,
     resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + JOB_NAME,
     description="This job is used to monitor Tableau jobs status.",
     tags={"monitoring": "tableau_jobs"}
     )
def check_tableau_jobs_status_job():
    check_tableau_jobs_status()
