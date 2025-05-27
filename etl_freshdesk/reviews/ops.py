import traceback
from dagster import op

from etl_freshdesk.reviews.utils.trustpilot import collect_trustpilot_data
from etl_freshdesk.reviews.utils.mobile_app_reviews import collect_reviews_data
from etl_freshdesk.reviews.utils.utils import get_slack_logger
from etl_freshdesk.utils import retry_policy


@op
def collect_trustpilot_data_op(context):
    try:
        collect_trustpilot_data()
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        logger.send(
            channel="U018GESMPDJ",
            title="Health Check",
            description=error[:2500]
        )
        raise e


@op(retry_policy=retry_policy)
def collect_reviews_data_op(context):
    try:
        context.log.info("Collecting reviews data")
        collect_reviews_data(context)
    except Exception as e:
        logger = get_slack_logger(context.run.job_name)
        error = traceback.format_exc()
        logger.send(
            channel="U018GESMPDJ",
            title="Health Check",
            description=error[:2500]
        )
        raise e
