from dagster import op, Out, In, job, fs_io_manager

# import modules
import pandas as pd
import traceback
import logging
import os

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy


@op(out=Out(str), description="load sql query to perform a request")
def get_sql_query_op():
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "outreach_period.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info("Got sql_query for outreach_period")
    return sql_query


@op(ins={"sql_query": In(str)}, out=Out(pd.DataFrame), description='perform sql request to Outreach server')
def create_df_op(context, sql_query):
    context.log.info("start to create dataframe")
    df = pd.read_sql_query(sql_query, con=ltf_conn_sqlalchemy())
    return df


@op(ins={"df": In(pd.DataFrame)}, description='truncate destincation table in DWH')
def truncate_target_table_op(context, df):
    context.log.info("start to truncate target table")
    table_name="outreach_period"
    schema="aggregation"
    truncate_dwh_table(context, table_name=table_name, schema=schema)
    return df


@op(ins={"df": In(pd.DataFrame)}, description='save created df to DWH table: aggregation.outreach_period')
def save_to_target_table_op(context, df):
    context.log.info("save [outreach_period]")
    try:
        save_to_dwh(df=df, table_name="outreach_period", schema="aggregation")
        status = "success"
    except Exception:
        status = "failed"
    return status


@op(ins={"status": In(str)}, description='send notificatoin into dwh_alert')
def send_notification_to_slack_op(context, status):
    if status == 'success':
        send_dwh_alert_slack_message(":white_check_mark: outreach_period executed successfully")
    else:
        send_dwh_alert_slack_message(":error_alert: outreach_period failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')
        raise Exception


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_period')
def outreach_period_job():
    '''aggregation.outreach_period'''
    sql_query = get_sql_query_op()
    df = create_df_op(sql_query)
    df_after_truncate = truncate_target_table_op(df)
    status = save_to_target_table_op(df_after_truncate)
    send_notification_to_slack_op(status)