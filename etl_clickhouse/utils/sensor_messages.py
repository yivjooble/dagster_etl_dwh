import os

from slack_sdk import WebClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

import ssl
import certifi
ssl_context = ssl.create_default_context(cafile=certifi.where())

SLACK_CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'),
                         ssl=ssl_context)
SLACK_MESSAGE_BLOCK = ":warning-dwh: *DAGSTER-SENSOR alert:* :arrow-down:"
SLACK_CHANNEL_ID = os.environ.get('DWH_ALERTS_CHANNEL_ID')


def get_conversations_history(message_to_search: str):
    response = SLACK_CLIENT.conversations_history(channel=SLACK_CHANNEL_ID, limit=50)
    for message in response["messages"]:
        if f"{message_to_search}" in message["text"]:
            return message["ts"]


def send_message(message: str, latest_ts: bool=None):
    if not latest_ts:
        SLACK_CLIENT.chat_postMessage(channel=SLACK_CHANNEL_ID, text=SLACK_MESSAGE_BLOCK)
        latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)

    SLACK_CLIENT.chat_postMessage(
        channel=SLACK_CHANNEL_ID,
        text=message,
        thread_ts=latest_ts)


def send_dwh_alert_slack_message(message: str):
    latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)
    if latest_ts:
        ts_datetime = datetime.fromtimestamp(float(latest_ts)).strftime("%Y-%m-%d")
        if ts_datetime == datetime.today().strftime("%Y-%m-%d"):
            send_message(message, latest_ts)
            return

    send_message(message)