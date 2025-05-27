import os
import inspect
import pandas as pd

from datetime import date

# third-party modules
from slack_sdk import WebClient
from dotenv import load_dotenv
from sqlalchemy import cast, Date, func
from dagster import (
    op,
    job,
    fs_io_manager
)

# custom modules
from service.watcher.utils.database import create_dwh_psycopg2_connection, Session
from service.watcher.utils.logger import update_watcher_log_entry
from service.watcher.utils.models import LogWatcher

# import ORM models of db tables
from service.watcher.check_pentaho_and_pgagent_logs.models import PentahoTransformationsLog, CheckPgAdminFailed, PythonETLLogs
from service.utils.utils import job_prefix
from service.utils.io_manager_path import get_io_manager_path

load_dotenv()

JOB_NAME = "watcher_check_pentaho_and_pgagent_logs"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def send_dwh_alert_slack_message(context, df, watcher_update_datetime: str):
    send_msg = True
    
    # define final df to pass to dwh_channel
    df_str = '```\n' + df.to_string(columns=['date', 'pentaho', 'pgadmin', 'py_etl'], index=False) + '\n```'

    # Initialize a WebClient instance with your Slack API token
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))

    # ID of the channel where the thread was created
    # dwh_alert
    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')

    if watcher_update_datetime != str(date.today()) and send_msg:
        # send a new post with errors cnt
        client.chat_postMessage(
                        channel=channel_id,
                        text=":error_alert: *The Watcher:* Pentaho | pgAdmin errors <!subteam^S02ETK2JYLF|dwh.analysts>",
                        blocks=[
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": ":error_alert: *The Watcher:* Pentaho | pgAdmin errors <!subteam^S02ETK2JYLF|dwh.analysts>"
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
        context.log.info("Sent a new post with an error message")
    elif watcher_update_datetime == str(date.today()):
        # Call the conversations.history method with the channel ID and count=50 msg to retrieve the most recent message
        response = client.conversations_history(channel=channel_id, limit=50)

        # Get the latest message's timestamp
        messages = response["messages"]
        latest_ts = None
        for message in messages:
            if "*The Watcher:* Pentaho | pgAdmin errors" in message["text"]:
                latest_ts = message["ts"]
                break

        client.chat_postMessage(
                        channel=channel_id,
                        thread_ts=latest_ts,
                        text=":error_alert: *The Watcher:* Pentaho | pgAdmin errors <!subteam^S02ETK2JYLF|dwh.analysts>",
                        blocks=[
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": ":error_alert: *The Watcher:* Pentaho | pgAdmin errors <!subteam^S02ETK2JYLF|dwh.analysts>"
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
        context.log.info("Sent an error message to a thread")


def check_pentaho_logs(context):
    context.log.info("Start to check pentaho errors")
    # open new session for ORM
    session = Session()
    
    # get errors cnt 
    pentaho_errors_cnt = session.query(func.count(PentahoTransformationsLog.transname)).filter(PentahoTransformationsLog.status == 'stop', cast(PentahoTransformationsLog.enddate, Date) == date.today()).scalar()

    # close the session
    session.close()
    context.log.info(f"pentaho_errors_cnt: {pentaho_errors_cnt}")

    return pentaho_errors_cnt
    

def check_pgagent_logs(context):
    '''
    materialize predefined view in dwh
    then count number of errors
    '''
    context.log.info("Start to check pgAgent errors")

    sql = """refresh materialized view dwh_test.mat_check_pg_admin_failed;"""

    conn = create_dwh_psycopg2_connection()
    cursor = conn.cursor()

    # materialize a view
    cursor.execute(sql)
    conn.commit()

    # close connection
    cursor.close()
    conn.close()

    # open new session for ORM
    session = Session()
    pgagent_errors_cnt = session.query(func.count(CheckPgAdminFailed.jobname)).scalar()
    
    session.close()
    context.log.info("refreshed materialized dwh_test.mat_check_pg_admin_failed")
    context.log.info(f"pgagent_errors_cnt: {pgagent_errors_cnt}")

    return pgagent_errors_cnt


@op
def check_pentaho_and_pgagent_logs(context):
    '''
    This app are going to check non-renewable tables that writes with today data
    '''
    session = Session()

    # define getting numer of errors
    pentaho_errors_cnt = check_pentaho_logs(context)
    pgadmin_errors_cnt = check_pgagent_logs(context)
    get_python_etl_errors_cnt = session.query(func.count(PythonETLLogs.module_name)).filter(PythonETLLogs.status == 'error', PythonETLLogs.flag == None, cast(PythonETLLogs.update_date, Date) == date.today()).scalar()

    df = pd.DataFrame.from_records({'pentaho': [pentaho_errors_cnt], 
                                    'pgadmin': [pgadmin_errors_cnt],
                                    'py_etl': [get_python_etl_errors_cnt]})
    df['date'] = date.today()


    # define alerts decision logic
    all_errors_cnt = pentaho_errors_cnt + pgadmin_errors_cnt + get_python_etl_errors_cnt

    if all_errors_cnt > 0:
        
        previous_error_cnt = session.query(LogWatcher).filter(LogWatcher.module_name == inspect.currentframe().f_code.co_name).one()
        watcher_update_datetime = str(previous_error_cnt.update_date.strftime("%Y-%m-%d"))

        # check if update_date is not today
        if watcher_update_datetime != str(date.today()):
            context.log.info(f"last saved watcher_update_datetime is: {watcher_update_datetime}")
            send_dwh_alert_slack_message(context, df, watcher_update_datetime)
            update_watcher_log_entry(previous_error_cnt.id, all_errors_cnt)
        else:
            # compare errors cnt and date
            if watcher_update_datetime == str(date.today()) and all_errors_cnt == previous_error_cnt.module_check_count:
                pass
                context.log.info("amount of errors doesn't change for today")
            else:
                send_dwh_alert_slack_message(context, df, watcher_update_datetime)
                update_watcher_log_entry(previous_error_cnt.id, all_errors_cnt)
        
        session.close()
    

@job(
    name=JOB_PREFIX + JOB_NAME,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
)
def check_pentaho_and_pgagent_logs():
    """
    Check pentaho and pgAgent logs of errors
    """
    check_pentaho_and_pgagent_logs()
