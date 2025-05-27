import os

from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()


import datetime
SLACK_MESSAGE_BLOCK = ":python-agg: *DWH-ETL: executing logs* :arrow-down:"


def get_conversations_history(message_to_search: str):
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))

    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')

    response = client.conversations_history(channel=channel_id, limit=50)

    messages = response["messages"]
    latest_ts = None
    for message in messages:
        if f"{message_to_search}" in message["text"]:
            latest_ts = message["ts"]
            break

    return latest_ts

def send_message(message: str, latest_ts=None):
    # Initialize a WebClient instance with your Slack API token
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))
    # ID of the channel dwh_alert
    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')  

    if latest_ts is None:  
        client.chat_postMessage(channel=channel_id, 
                                text=f"{SLACK_MESSAGE_BLOCK}")

        latest_ts = get_conversations_history(f"{SLACK_MESSAGE_BLOCK}")

        client.chat_postMessage(channel=channel_id,
                                text=f"{message}",
                                thread_ts=latest_ts)
    else:
        client.chat_postMessage(channel=channel_id,
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