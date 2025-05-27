import slack_logger
from slack_logger import SlackLogger
from utility_hub.core_tools import get_creds_from_vault

SLACK_TOKEN = get_creds_from_vault('DWH_ALERTS_TOKEN')


def get_slack_logger(job_name: str) -> slack_logger.SlackLogger:
    """Create SlackLogger to send messages to Slack.

    Args:
        job_name (str): Dagster job name.

    Returns:
        slack_logger.SlackLogger: instantiated logger
    """
    options = {
        "service_name": job_name,
        "service_environment": "Dagster",
        "display_hostname": True,
        "default_level": "info",
    }
    logger = SlackLogger(token=SLACK_TOKEN, **options)
    return logger


def send_message(logger: slack_logger.SlackLogger,
                 channel: str,
                 title: str = None,
                 description: str = None,
                 error: str = None,
                 metadata: dict = None) -> dict:
    """Send message to Slack using SlackLogger object with the passed information.

    Args:
        logger (slack_logger.SlackLogger): instantiated logger object.
        channel (str): Slack channel ID.
        title (str): text to show as title.
        description (str): text to show as description.
        error (str): text to show as error.
        metadata (dict): dictionary with metadata to show in the message.

    Returns:
        dict: dictionary, Slack API request for sending message response parameters.
    """
    response = logger.send(
        channel=channel,
        title=title,
        description=description,
        error=error,
        metadata=metadata
    )
    return response
