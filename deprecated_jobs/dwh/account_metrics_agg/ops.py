import os
import pandas as pd

from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    Out,
)

# project import
from ..utils.io_manager_path import get_io_manager_path
# module import
from ..utils.messages import send_dwh_alert_slack_message
from ..utils.dwh_db_operations import start_query_on_dwh_db
from ..utils.date_format_settings import get_datediff


SCHEMA = 'affiliate'
TABLE_NAME = 'account_metrics_agg'
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')



@op(out=Out(str))
def account_metrics_agg_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('Loaded SQL query')
    return q


@op()
def account_metrics_agg_launch_sql_on_db(context, query):

    params = {
        'query': query,
    }
    start_query_on_dwh_db(context, params)

    send_dwh_alert_slack_message(f":add: *{SCHEMA}.{TABLE_NAME}*\n")


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),},
     name='dwh__'+TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}')
def account_metrics_agg_job():
    # start procedure on replica
    query = account_metrics_agg_get_sql_query()
    account_metrics_agg_launch_sql_on_db(query)