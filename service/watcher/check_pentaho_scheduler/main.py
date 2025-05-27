import os
import pandas as pd
import requests
import inspect

from datetime import datetime, date

# third-party modules
from slack_sdk import WebClient
from dotenv import load_dotenv
from typing import List, Tuple
from dagster import (
    op,
    job,
    fs_io_manager
)

# custom modules
from service.watcher.utils.logger import update_watcher_log_entry
from service.watcher.utils.database import Session, truncate_dwh_table, create_dwh_sqlalchemy_engine
from service.watcher.utils.models import LogWatcher

# import ORM models of db tables
from service.watcher.check_pentaho_scheduler.models import PentahoSchedules
from service.utils.utils import job_prefix
from service.utils.io_manager_path import get_io_manager_path

load_dotenv()

JOB_NAME = "watcher_check_pentaho_scheduler"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def send_dwh_alert_slack_message(context, df, watcher_update_datetime):
    send_msg = True

    df_str = '```\n' + df.to_string(columns=['job_name', 'last_run_datetime'], index=False) + '\n```'

    # Initialize a WebClient instance with your Slack API token
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))

    # ID of the channel where the thread was created
    # dwh_alert
    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')

    if watcher_update_datetime != str(date.today()) and send_msg:
        # Send a new message to a channel
        client.chat_postMessage(
            channel=channel_id,
            text=":error_alert: *The Watcher:* Pentaho missing scheduled jobs <!subteam^S02ETK2JYLF|dwh.analysts>",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":error_alert: *The Watcher:* Pentaho missing scheduled jobs <!subteam^S02ETK2JYLF|dwh.analysts>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": df_str
                    }
                }
            ]
        )
        context.log.info("Sent error message as a new post")
    elif watcher_update_datetime == str(date.today()):
        # Call the conversations.history method with the channel ID and count=50 to retrieve the most recent message
        response = client.conversations_history(channel=channel_id, limit=50)

        # Get the latest message's timestamp
        messages = response["messages"]
        latest_ts = None
        for message in messages:
            # Check if the text of the message matches "*The Watcher:* Pentaho missing scheduled jobs"
            if "*The Watcher:* Pentaho missing scheduled jobs" in message["text"]:
                latest_ts = message["ts"]
                break
    
        # Send a message in the same thread as the previous message
        client.chat_postMessage(
            channel=channel_id,
            thread_ts=latest_ts,
            text=":error_alert: *The Watcher:* Pentaho missing scheduled jobs <!subteam^S02ETK2JYLF|dwh.analysts>",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":error_alert: *The Watcher:* Pentaho missing scheduled jobs <!subteam^S02ETK2JYLF|dwh.analysts>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": df_str
                    }
                }
            ]
        )
        context.log.info("Sent error message to thread")

        


def get_pentaho_schedules(auth: Tuple[str, str]) -> dict:
    """makes request to Pentaho API to get scheduled jobs as json"""
    response = requests.get(
        'http://dwh.jooble.com:8080/pentaho/api/scheduler/getJobs',
        auth=auth
    )

    return response.json()


def parse_schedules(data: dict) -> List[dict]:
    jobs = []

    for job_schedule in data['job']:
        if job_schedule['jobName'] == 'PentahoSystemVersionCheck':
            continue
        job = {}
        for job_param in job_schedule['jobParams']['jobParams']:
            if job_param['name'] == 'ActionAdapterQuartzJob-StreamProvider-InputFile':
                job['input_file'] = job_param['value']
            if job_param['name'] == 'job':
                job['job_name'] = job_param['value']
            if job_param['name'] == 'directory':
                job['job_directory'] = job_param['value']

        job['cron_string'] = job_schedule['jobTrigger'].get('cronString')
        job['recurrence'] = job_schedule['jobTrigger'].get('uiPassParam')
        job['last_run_datetime'] = job_schedule.get('lastRun')
        job['next_run_datetime'] = job_schedule.get('nextRun')

        jobs.append(job)

    return jobs


def write_to_dwh(context, df):
    schema = "dc"
    table_name = "pentaho_schedules"

    truncate_dwh_table(schema, table_name)
    
    df.drop_duplicates().to_sql(
        table_name,
        con=create_dwh_sqlalchemy_engine(),
        schema='dc',
        if_exists='append',
        index=False
    )
    context.log.info(f'[dc.{table_name}] truncated')
    context.log.info(f'{df.shape[0]} row are inserted into [dc.{table_name}]')


@op
def overwrite_pentaho_schedule(context):
    context.log.info('Starting pentaho repo download')

    # get scheduled pentaho jobs and write to dwh
    schedules_data = get_pentaho_schedules((os.environ.get('PENTAHO_USER'), os.environ.get('PENTAHO_PASS')))
    schedules_df = pd.DataFrame(parse_schedules(schedules_data))
    schedules_df['recurrence'] = schedules_df['recurrence'].str.lower()
    
    write_to_dwh(
        context,
        schedules_df.drop(columns=['input_file'])
    )
    context.log.info('pentaho_schedules is written to dwh')
     

@op
def check_pentaho_scheduler(context):
    context.log.info("Got newest info from schedule, start to check")
    session = Session()
    
    pentaho_scheduler = session.query(PentahoSchedules).filter(PentahoSchedules.recurrence == 'daily').order_by(PentahoSchedules.last_run_datetime.asc()).all()

    session.close()
    
    current_time = datetime.now().strftime("%H:%M:%S")
    current_date = datetime.now().strftime("%Y-%m-%d")

    missed_jobs = {}

    for scheduler in pentaho_scheduler:
        if scheduler.last_run_datetime:
            # missed jobs
            if current_time > str(scheduler.last_run_datetime.strftime("%H:%M:%S")) and str(scheduler.last_run_datetime.strftime("%Y-%m-%d")) != str(current_date):
                missed_jobs[scheduler.job_name] = scheduler.last_run_datetime
        else:
            context.log.info("scheduler.last_run_datetime is empty\nthe watcher has ended")


    # check if missed_jobs is not empty
    if missed_jobs:
        df = pd.DataFrame.from_records({'job_name': missed_jobs.keys(),
                                        'last_run_datetime': missed_jobs.values()})

        if df.empty:
            context.log.info("no missing jobs")
        else:
            session = Session()
            previous_error_cnt = session.query(LogWatcher).filter(LogWatcher.module_name == inspect.currentframe().f_code.co_name).one()
            watcher_update_datetime = str(previous_error_cnt.update_date.strftime("%Y-%m-%d"))

            # check if last saved cnt of errors is today
            if watcher_update_datetime != str(date.today()):
                send_dwh_alert_slack_message(context, df, watcher_update_datetime)
                update_watcher_log_entry(previous_error_cnt.id, df.shape[0])
                context.log.info("check if last saved cnt of errors is today")
            else:

                # compare if previous locks cnt the same and a period of time no more than 1 hour
                if df.shape[0] == previous_error_cnt.module_check_count and watcher_update_datetime == str(date.today()):
                    pass
                    context.log.info("amount of errors doesn't change for today")
                else:
                    send_dwh_alert_slack_message(context, df, watcher_update_datetime)
                    update_watcher_log_entry(previous_error_cnt.id, df.shape[0])
            
            session.close()

    else:
        context.log.info("No missings\n\n")


@job(
    name=JOB_PREFIX + JOB_NAME,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
)
def check_pentaho_scheduler():
    """
    Review pentaho scheduler on missing jobs
    """
    overwrite_pentaho_schedule()
    check_pentaho_scheduler()
