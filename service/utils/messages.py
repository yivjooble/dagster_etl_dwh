import os
import ssl
import certifi
from datetime import datetime

from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()

ssl_context = ssl.create_default_context(cafile=certifi.where())

SLACK_CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'),
                         ssl=ssl_context)
SLACK_MESSAGE_BLOCK = ":tableau: *Tableau Job Status*"
SLACK_CHANNEL_ID = os.environ.get('DB_AND_DWH_ALERT')


def get_conversations_history(message_to_search: str):
    response = SLACK_CLIENT.conversations_history(channel=SLACK_CHANNEL_ID, limit=50)
    for message in response["messages"]:
        if f"{message_to_search}" in message["text"]:
            return message["ts"]


def send_message(message: str, html_report_fname: str = None, latest_ts: bool = None, alert: bool = False):
    if not latest_ts:
        SLACK_CLIENT.chat_postMessage(channel=SLACK_CHANNEL_ID, text=SLACK_MESSAGE_BLOCK)
        latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)

    if alert:
        SLACK_CLIENT.files_upload_v2(channel=SLACK_CHANNEL_ID,
                                     thread_ts=latest_ts,
                                     initial_comment=message + "\n<@U01SYT6FXAS>")
    else:
        SLACK_CLIENT.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message,
            thread_ts=latest_ts
        )


def send_dwh_alert_slack_message(message: str, html_report_fname: str = None, alert: bool = False):
    latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)
    if latest_ts:
        ts_datetime = datetime.fromtimestamp(float(latest_ts)).strftime("%Y-%m-%d")
        if ts_datetime == datetime.today().strftime("%Y-%m-%d"):
            send_message(message, html_report_fname, latest_ts, alert)
            return

    send_message(message, html_report_fname, alert)
