import os
import ssl
import certifi
from datetime import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from dagster import Failure
from sqlalchemy import create_engine
from utility_hub.core_tools import get_creds_from_vault
from typing import Optional

load_dotenv()

ssl_context = ssl.create_default_context(cafile=certifi.where())

SLACK_CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'),
                         ssl=ssl_context)
SLACK_MESSAGE_BLOCK = ":warning-dwh: *DAGSTER-SENSOR alert:* :arrow-down:"
SLACK_CHANNEL_ID = os.environ.get('DWH_ALERT')
NOTIFY_USER = "<!subteam^S02ETK2JYLF|dwh.analysts>"

# Emoji mapping for different message types
EMOJI_MAP = {
    "WARNING": "⚠️",
    "ERROR": "❌",
    "INFO": "ℹ️",
    "SUCCESS": "✅",
    "EXCEPTION": "🚨"
}


def db_conn_sqlalchemy(user: str = 'DWH_USER',
                       password: str = 'DWH_PASSWORD',
                       host: str = 'DWH_HOST',
                       port: str = 'DWH_PORT',
                       dbname: str = 'an_dwh'):
    """
    Connect to the database using sqlalchemy
    """
    try:
        user = get_creds_from_vault(user),
        password = get_creds_from_vault(password),
        host = get_creds_from_vault(host),
        port = int(get_creds_from_vault(port))
        return create_engine(f"postgresql+psycopg2://{user[0]}:{password[0]}@{host[0]}:{port}/{dbname}")
    except Exception as e:
        raise Failure(f"Failed to connect to the database: {str(e)}")


def save_to_db(df, table_name='tableau_jobs', schema='public'):
    """
    Save data to the database
    """
    try:
        df.to_sql(
            table_name,
            con=db_conn_sqlalchemy(),
            schema=schema,
            if_exists='append',
            index=False,
        )
    except Exception as e:
        raise Failure(f"Failed to save data to the database: {str(e)}")


def get_conversations_history(message_to_search: str):
    """
    Get conversation history and find a specific message timestamp.

    Args:
        message_to_search: The message text to search for

    Returns:
        The timestamp of the found message or None if not found
    """
    try:
        response = SLACK_CLIENT.conversations_history(channel=SLACK_CHANNEL_ID, limit=50)
        for message in response.get("messages", []):
            if f"{message_to_search}" in message.get("text", ""):
                return message.get("ts")
        return None
    except Exception as e:
        print(f"Error getting conversation history: {str(e)}")
        return None


def send_slack_message(message: str, thread_ts=None, tag_dwh: bool = True, create_thread: bool = False):
    """
    Send a message to Slack, with options for threading and tagging users.

    Args:
        message: The message text to send
        thread_ts: Thread timestamp to reply to. If None and create_thread=True,
                  a new thread will be created
        tag_dwh: Whether to tag the DWH team
        create_thread: If True and thread_ts is None, create a new thread using
                      the SLACK_MESSAGE_BLOCK header

    Returns:
        Thread timestamp that was used or created (can be used for subsequent calls)
    """
    try:
        # If no thread provided but we should create one
        if thread_ts is None and create_thread:
            # Post thread header message
            response = SLACK_CLIENT.chat_postMessage(
                channel=SLACK_CHANNEL_ID,
                text=SLACK_MESSAGE_BLOCK
            )
            thread_ts = response.get('ts')

        # Add tag if needed
        if tag_dwh:
            message += f"\n{NOTIFY_USER}"

        # Send message
        SLACK_CLIENT.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message,
            thread_ts=thread_ts
        )

        return thread_ts
    except Exception as e:
        print(f"Error sending message to Slack: {str(e)}")
        return thread_ts


def send_text_as_file(
    content: str,
    filename: str = "report.html",
    thread_ts=None,
    tag_dwh: bool = True,
    initial_comment_text: Optional[str] = None,
):
    """
    Upload text content as a file to Slack with an initial comment.

    Args:
        content: The text content to include in the file
        filename: The name of the file (default: "details.txt")
        thread_ts: Thread timestamp to reply in; if None, posts to channel
        tag_dwh: Whether to tag the DWH team in the initial comment
        initial_comment_text: Header message to send before the file; if None, filename is used

    Returns:
        Thread timestamp used for the upload (may be None)
    """
    # Compose DWH mention if needed
    notification = f"\n{NOTIFY_USER}" if tag_dwh else ""
    # Determine initial comment
    if initial_comment_text:
        initial_comment = f"{initial_comment_text}{notification}"
    else:
        initial_comment = f"{filename}{notification}"
    try:
        response = SLACK_CLIENT.files_upload_v2(
            channel=SLACK_CHANNEL_ID,
            content=content,
            filename=filename,
            initial_comment=initial_comment,
            thread_ts=thread_ts,
            title=filename,
        )
        # Retrieve thread_ts if not provided
        if thread_ts is None:
            file_info = response.get("file", {})
            shares = file_info.get("shares", {}).get("public", {}).get(SLACK_CHANNEL_ID, [])
            if shares:
                thread_ts = shares[0].get("ts")
        return thread_ts
    except Exception as e:
        print(f"Error uploading file to Slack: {e}")
        return thread_ts


def send_formatted_message(message: str, severity: str = None, thread_ts: bool = None, tag_dwh: bool = True):
    """
    Send a formatted message to Slack using Block Kit format for better appearance.

    Args:
        message: The message text to send
        severity: The severity level (WARNING, ERROR, INFO, SUCCESS, EXCEPTION)
        thread_ts: Thread timestamp to reply to
        tag_dwh: Whether to tag the DWH team
    """
    # If no thread_ts provided, try to find today's thread
    if not thread_ts:
        thread_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)

    # If still no thread found, create a new one
    if not thread_ts:
        response = SLACK_CLIENT.chat_postMessage(channel=SLACK_CHANNEL_ID, text=SLACK_MESSAGE_BLOCK)
        thread_ts = response.get('ts')

    # Get emoji for severity
    emoji = EMOJI_MAP.get(severity, "ℹ️") if severity else "ℹ️"

    # Process message to improve formatting for data discrepancy messages
    formatted_message = message
    if "data discrepancy" in message:
        # Split the message into parts
        parts = message.split(", ")
        if len(parts) > 1:
            # Format the message to keep related information together
            hostname_part = parts[0]
            discrepancy_part = ", ".join(parts[1:])

            # Create a more structured message
            formatted_message = f">{hostname_part}\n >{discrepancy_part}"

    # Create blocks for the message
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{severity if severity else 'INFO'}*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": formatted_message
            }
        }
    ]

    # Add divider for visual separation
    blocks.append({"type": "divider"})

    # Prepare the message payload
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "blocks": blocks,
        "thread_ts": thread_ts
    }

    # Add user mention if needed
    if tag_dwh:
        payload["text"] = f"{NOTIFY_USER}"  # Fallback text for clients that don't support blocks

    # Send the message
    try:
        SLACK_CLIENT.chat_postMessage(**payload)
    except Exception as e:
        print(f"Error sending formatted message to Slack: {str(e)}")
        # Fallback to simple message if blocks fail
        send_slack_message(
            message=f"{emoji} *{severity if severity else 'INFO'}*\n{message}",
            thread_ts=thread_ts,
            tag_dwh=tag_dwh
        )


def send_dwh_alert_slack_message(message: str, tag_dwh: bool = True, as_file: bool = False, filename: str = "report.html", file_content: Optional[str] = None):
    """
    Send an alert message to Slack, checking if there's already a message thread for today.

    Args:
        message: The message text to send
        tag_dwh: Whether to tag the DWH team
        as_file: Whether to send the message as a file attachment (useful for large reports)
        filename: The filename to use when sending as a file
        file_content: Optional string content to upload as file; if provided, used instead of message
    """
    # Try to find today's thread
    thread_ts = get_conversations_history(SLACK_MESSAGE_BLOCK)

    # Check if we have a thread from today
    if thread_ts:
        try:
            ts_datetime = datetime.fromtimestamp(float(thread_ts)).strftime("%Y-%m-%d")
            if ts_datetime == datetime.today().strftime("%Y-%m-%d"):
                # Use existing thread
                if as_file:
                    content = file_content or ""
                    send_text_as_file(
                        content,
                        filename=filename,
                        thread_ts=thread_ts,
                        tag_dwh=tag_dwh,
                        initial_comment_text=message,
                    )
                else:
                    send_slack_message(message, thread_ts=thread_ts, tag_dwh=tag_dwh)
                return
        except (ValueError, TypeError):
            # If timestamp is invalid, continue with creating a new thread
            pass

    # If no thread from today, create a new one
    if as_file:
        content = file_content or ""
        send_text_as_file(
            content,
            filename=filename,
            thread_ts=None,
            tag_dwh=tag_dwh,
            initial_comment_text=message,
        )
    else:
        send_slack_message(message, thread_ts=None, tag_dwh=tag_dwh, create_thread=True)
