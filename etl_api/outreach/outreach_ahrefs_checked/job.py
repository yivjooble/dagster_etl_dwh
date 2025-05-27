from dagster import (
    op, 
    Out, 
    In, 
    Field,
    job, 
    fs_io_manager,
    make_values_resource
)

# import modules
import pandas as pd
import traceback
import logging
import os

from datetime import datetime, timedelta
from sqlalchemy import text
# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy
from ...utils.dwh_db_operations import delete_data_from_dwh_table


SCHEMA = 'aggregation'
TABLE_NAME = 'outreach_ahrefs_checked'
DELETE_DATE_DIFF_COLUMN = "date_add"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')



@op(out=Out(str), description="load sql query to perform a request")
def outreach_ahrefs_get_sql_query_op():
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info(f"Got sql_query for {TABLE_NAME}")
    return sql_query


@op(ins={"sql_query": In(str)}, out=Out(pd.DataFrame), 
    required_resource_keys={'globals'},
    description='perform sql request to Outreach server')
def outreach_ahrefs_create_df_op(context, sql_query):
    context.log.info("start to create dataframe")
    
    df = pd.read_sql_query(text(sql_query), 
                           con=ltf_conn_sqlalchemy(),
                           params={'to_sqlcode_date_or_datediff_start': context.resources.globals['date_start'],
                                   'to_sqlcode_date_or_datediff_end': context.resources.globals['date_end']})
    return df


@op(required_resource_keys={'globals'})
def outreach_ahrefs_acc_revenue_delete_history_data(context, df):
    delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_DATE_DIFF_COLUMN, context.resources.globals['date_start'], context.resources.globals['date_end'])
    return df


@op(ins={"df": In(pd.DataFrame)}, 
    required_resource_keys={'globals'},
    description=f'save created df to DWH table: {SCHEMA}.{TABLE_NAME}')
def outreach_ahrefs_save_to_target_table_op(context, df):
    save_to_dwh(df=df, table_name=TABLE_NAME, schema=SCHEMA)
    context.log.info(f"saved: [{TABLE_NAME}]")

    rows = int(df.shape[0])
    send_dwh_alert_slack_message(f":done-1: *{SCHEMA}.{TABLE_NAME}*\n"
                                 f">*{context.resources.globals['date_start']}/-/{context.resources.globals['date_end']}*: {rows}")


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    date_end=Field(str, default_value=YESTERDAY_DATE))},
     name='api__'+TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}')
def outreach_ahrefs_checked_job():
    '''aggregation.outreach_period'''
    sql_query = outreach_ahrefs_get_sql_query_op()
    df = outreach_ahrefs_create_df_op(sql_query)
    df = outreach_ahrefs_acc_revenue_delete_history_data(df)
    outreach_ahrefs_save_to_target_table_op(df)