import os

# third-party modules
from slack_sdk import WebClient
from dotenv import load_dotenv

load_dotenv()



from datetime import datetime
SLACK_MESSAGE_BLOCK = ":google: *GA4-ETL: executing logs* :arrow-down:"
SLACK_CHANNEL_ID = os.environ.get('DWH_ALERT')
SLACK_CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))


def get_conversations_history(message_to_search: str):
    response = SLACK_CLIENT.conversations_history(channel=SLACK_CHANNEL_ID, limit=50)
    for message in response["messages"]:
        if f"{message_to_search}" in message["text"]:
            return message["ts"]


def send_message(message: str, html_report_fname: str=None, latest_ts: bool=None, alert: bool=False):
    if not latest_ts:
        SLACK_CLIENT.chat_postMessage(channel=SLACK_CHANNEL_ID, text=SLACK_MESSAGE_BLOCK)
        latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)

    if alert:
        SLACK_CLIENT.files_upload_v2(channel=SLACK_CHANNEL_ID,
                                    thread_ts=latest_ts,
                                    file=html_report_fname,
                                    title="alert_report.html",
                                    initial_comment=message + "\n<@U01SYT6FXAS>")
    else:
        SLACK_CLIENT.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message,
            thread_ts=latest_ts
        )


def send_dwh_alert_slack_message(message: str, html_report_fname: str=None, alert: bool=False):
    latest_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)
    if latest_ts:
        ts_datetime = datetime.fromtimestamp(float(latest_ts)).strftime("%Y-%m-%d")
        if ts_datetime == datetime.today().strftime("%Y-%m-%d"):
            send_message(message, html_report_fname, latest_ts, alert)
            return

    send_message(message, html_report_fname, alert)