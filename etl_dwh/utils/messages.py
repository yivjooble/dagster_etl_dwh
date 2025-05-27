import datetime
import os

from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()


import ssl
import certifi
ssl_context = ssl.create_default_context(cafile=certifi.where())

CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'),
                         ssl=ssl_context)
SLACK_MESSAGE_BLOCK = ":python-agg: *DWH-ETL: executing logs* :arrow-down:"
CHANNEL_ID = os.environ.get('DWH_ALERTS_CHANNEL_ID')


def get_conversations_history(message_to_search: str):
    response = CLIENT.conversations_history(channel=CHANNEL_ID, limit=50)
    messages = response["messages"]
    latest_ts = None
    for message in messages:
        if f"{message_to_search}" in message["text"]:
            latest_ts = message["ts"]
            break
    return latest_ts


def send_message(message: str, latest_ts=None):
    if latest_ts is None:  
        CLIENT.chat_postMessage(channel=CHANNEL_ID, 
                                text=f"{SLACK_MESSAGE_BLOCK}")
        latest_ts =get_conversations_history(f"{SLACK_MESSAGE_BLOCK}")

        CLIENT.chat_postMessage(channel=CHANNEL_ID,
                                text=f"{message}",
                                thread_ts=latest_ts)
    else:
        CLIENT.chat_postMessage(channel=CHANNEL_ID,
                                text=f"{message}",
                                thread_ts=latest_ts)


def send_dwh_alert_slack_message(message: str):
    latest_ts = get_conversations_history(f"{SLACK_MESSAGE_BLOCK}")
    if latest_ts:
        ts_datetime = datetime.datetime.fromtimestamp(float(latest_ts)).strftime("%Y-%m-%d")
        if ts_datetime != datetime.datetime.today().strftime("%Y-%m-%d"):
            send_message(message)
        else:
            send_message(message, latest_ts)
    else:
        send_message(message)