import os

from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()


def send_dwh_alert_slack_message(message: str):
    # Initialize a WebClient instance with your Slack API token
    client = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))

    # ID of the channel dwh_alert
    channel_id = os.environ.get('DWH_ALERTS_CHANNEL_ID')

    send_msg = True

    if send_msg:
        client.chat_postMessage(channel=channel_id, text=message)