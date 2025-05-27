import os
import pandas as pd

from datetime import datetime, timedelta
from slack_sdk import WebClient
from dotenv import load_dotenv

# custom modules
from service.watcher.utils.database import create_dwh_psycopg2_connection, Session
from dagster import (
    op,
    job,
    fs_io_manager
)

# import ORM models of db tables
from service.watcher.check_non_renewable_tables.models import (
    VacancyCollars, JobsStatDaily, SentinelStatistic, SessionAbtestAgg
)
from service.utils.utils import job_prefix
from service.utils.io_manager_path import get_io_manager_path

load_dotenv()

JOB_NAME = "watcher_check_non_renewable_tables"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def send_dwh_alert_slack_message(context, df):
    # Initialize a WebClient instance with your Slack API token
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))

    # ID of the channel where the thread was created
    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')

    # Call the conversations.history method with the channel ID and count=1 to retrieve the most recent message
    response = client.conversations_history(channel=channel_id, limit=20)

    # Get the latest message's timestamp
    messages = response["messages"]
    latest_ts = None
    for message in messages:
        if "*The Watcher:* Non-renewable tables missing" in message["text"]:
            latest_ts = message["ts"]
            break

    send_msg = True

    df_str = '```\n' + df.to_string(columns=['table_name', 'newest_date'], index=False) + '\n```'

    if send_msg:
        if latest_ts:
            client.chat_postMessage(
                channel=channel_id,
                thread_ts=latest_ts,
                text=":error_alert: *The Watcher:* Non-renewable tables missing <!subteam^S02ETK2JYLF|dwh.analysts>",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":error_alert: *The Watcher:* Non-renewable tables missing <!subteam^S02ETK2JYLF|dwh.analysts>"
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
            context.log.info(f"Sent one more message to a slack channel, latest_ts: {latest_ts}")
        else:
            client.chat_postMessage(
                channel=channel_id,
                text=":error_alert: *The Watcher:* Non-renewable tables missing <!subteam^S02ETK2JYLF|dwh.analysts>",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": ":error_alert: *The Watcher:* Non-renewable tables missing <!subteam^S02ETK2JYLF|dwh.analysts>"
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
            context.log.info("Sent message to a slack channel")


@op
def check_non_renewable_tables(context):
    """
    This app checks non-renewable tables to ensure they are updated with yesterday's data.
    """

    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Convert yesterday's date to an integer format
    yest_date_int = (yesterday.date() - datetime(1900, 1, 1).date()).days

    with Session() as session, create_dwh_psycopg2_connection() as conn:
        cursor = conn.cursor()

        # Convert yesterday's date to the integer format in the database
        cursor.execute(f"SELECT {yest_date_int};")
        action_datediff_int = cursor.fetchone()[0]

        # Create a dictionary to hold table names and their query results for yesterday's date
        tables_to_check = {
            'aggregation.vacancy_collars': (VacancyCollars, VacancyCollars.action_datediff, action_datediff_int),
            'aggregation.jobs_stat_daily': (JobsStatDaily, JobsStatDaily.date, yesterday_str),
            'aggregation.sentinel_statistic': (SentinelStatistic, SentinelStatistic.action_date, yesterday_str),
            'aggregation.session_abtest_agg': (SessionAbtestAgg, SessionAbtestAgg.action_datediff, action_datediff_int)
        }

        final_results = {}

        for tablename, (model, date_field, date_value) in tables_to_check.items():
            # Query for records matching yesterday's date or integer representation
            record_exists = session.query(model).filter(date_field == date_value).first() is not None
            if not record_exists:
                final_results[
                    tablename] = yesterday_str if tablename != 'aggregation.vacancy_collars' and tablename != 'aggregation.session_abtest_agg' else action_datediff_int

        df = pd.DataFrame(list(final_results.items()), columns=['table_name', 'newest_date'])

    if not df.empty:
        send_dwh_alert_slack_message(context, df)
        context.log.info("Alerts sent for tables with outdated data.")
    else:
        context.log.info("All checked tables are up to date.")


@job(
    name=JOB_PREFIX + JOB_NAME,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
)
def check_non_renewable_tables():
    """
    Monitor: vacancy_collars, jobs_stat_daily, ahrefs_organic_reserve, sentinel_statistic
    """
    check_non_renewable_tables()
