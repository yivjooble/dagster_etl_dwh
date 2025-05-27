from etl_freshdesk.freshdesk.utils.satisfaction_survey import update_surveys
from etl_freshdesk.freshdesk.utils.update_ticket_data import update_tickets_info
from etl_freshdesk.freshdesk.utils.delete_tickets import update_deleted_tickets
from etl_freshdesk.freshdesk.utils.solution_articles import update_solution_articles
from etl_freshdesk.freshdesk.utils.slack_logging import send_message, get_slack_logger
from etl_freshdesk.freshdesk.utils.config import SLACK_CHANNEL, SLACK_MESSAGE_TITLE
from etl_freshdesk.freshdesk.utils.database_io import delete_old_logs
from etl_freshdesk.utils import retry_policy

from dagster import (
    op,
    Out
)
import traceback

SLACK_ERROR_DESCRIPTION_TEMPLATE = '{} freshdesk data collection failed.'


@op(out=Out(bool))
def int_remove_spam_tickets(context) -> bool:
    """
    Removes data on tickets marked as spam from the freshdesk_internal schema.
    """
    fd_type = 'internal'
    try:
        update_deleted_tickets('spam', 2, fd_type)
        return True
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op(out=Out(bool))
def ext_remove_spam_tickets(context) -> bool:
    """
    Removes data on tickets marked as spam from the freshdesk_external schema.
    """
    fd_type = 'external'
    try:
        update_deleted_tickets('spam', 2, fd_type)
        return True
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op()
def int_remove_deleted_tickets(context, prev_result):
    """
    Removes data on deleted tickets from the freshdesk_internal schema.
    """
    fd_type = 'internal'
    try:
        update_deleted_tickets('deleted', 3, fd_type)
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op()
def ext_remove_deleted_tickets(context, prev_result):
    """
    Removes data on deleted tickets from the freshdesk_external schema.
    """
    fd_type = 'external'
    try:
        update_deleted_tickets('deleted', 3, fd_type)
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op()
def int_update_ticket_info(context):
    """
    Updates data on new & existing tickets in the freshdesk_internal schema.
    """
    fd_type = 'internal'
    job_name = context.run.job_name
    try:
        update_tickets_info(fd_type, job_name)
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op(retry_policy=retry_policy)
def ext_update_ticket_info(context):
    """
    Updates data on new & existing tickets in the freshdesk_external schema.
    """
    fd_type = 'external'
    job_name = context.run.job_name
    try:
        update_tickets_info(fd_type, job_name)
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op(out=Out(bool))
def ext_update_satisfaction_surveys(context) -> bool:
    """
    Updates data on satisfaction_surveys in the freshdesk_external schema.
    """
    fd_type = 'external'
    try:
        update_surveys(fd_type)
        return True
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op(out=Out(bool))
def ext_update_solution_articles(context, prev_result) -> bool:
    """
    Updates data on solution_articles in the freshdesk_external schema.
    """
    fd_type = 'external'
    try:
        update_solution_articles(fd_type)
        return True
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(fd_type), error[:2000],
                     metadata=None)
        raise e


@op()
def delete_old_logs_op(context, prev_result):
    """
    Updates data on solution_articles in the freshdesk_external schema.
    """
    try:
        delete_old_logs('freshdesk_internal')
        delete_old_logs('freshdesk_external')
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        send_message(logger, SLACK_CHANNEL, SLACK_MESSAGE_TITLE,
                     SLACK_ERROR_DESCRIPTION_TEMPLATE.format(''), error[:2000],
                     metadata=None)
        raise e
