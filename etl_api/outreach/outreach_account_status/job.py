from dagster import op, job, In, Out, fs_io_manager
import os

# built in modules
import requests
import pandas as pd
import traceback
import logging

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy


@op(out=Out(str))
def get_sql_query_outreach_account_status(context):
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "outreach_account_status.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    context.log.info("GOT sql_query for outreach_account_status")
    return sql_query


@op(ins={'sql_query_outreach_account_status': In(str)}, out=Out(pd.DataFrame))
def create_df_outreach_account_status(context, sql_query_outreach_account_status):
    context.log.info("Creating a df: [aggregation.outreach_account_status]")
    df_outreach_account_status = pd.read_sql_query(sql_query_outreach_account_status,
                                              con=ltf_conn_sqlalchemy())
    return df_outreach_account_status
    

@op(ins={'df': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def truncate_outreach_account_status(context, df):
    truncate_dwh_table(
        context,
        table_name="outreach_account_status",
        schema="aggregation"
    )
    return df


@op(ins={'df_outreach_account_status': In(pd.DataFrame)})
def save_outreach_account_status(context, df_outreach_account_status):
    try:
        save_to_dwh(
            df_outreach_account_status,
            table_name="outreach_account_status",
            schema="aggregation"
        )
        context.log.info("Wrote [aggregation.outreach_account_status] table")
        send_dwh_alert_slack_message(f":white_check_mark: outreach_account_status executed successfully")
    except Exception:
        send_dwh_alert_slack_message(f":error_alert: outreach_account_status failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')
        raise Exception


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_account_status')
def outreach_account_status_job():
    '''aggregation.outreach_account_status'''
    sql_query_outreach_account_status =  get_sql_query_outreach_account_status()
    df_outreach_account_status = create_df_outreach_account_status(sql_query_outreach_account_status)
    df = truncate_outreach_account_status(df_outreach_account_status)
    save_outreach_account_status(df)