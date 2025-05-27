# built in modules
import requests
import pandas as pd
import json
import os
import traceback
import logging

from dagster import asset, Failure

# third party modules
from dotenv import load_dotenv

# project import
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table, dwh_conn_psycopg2
# module import
from ...utils.messages import send_dwh_alert_slack_message

URL = "http://10.0.1.154:5001/calc_team_salary"

load_dotenv()




def connect_to_dwh():
    '''Create connection to dwh database'''
    conn = dwh_conn_psycopg2()
    cursor = conn.cursor()
    return conn, cursor


@asset
def get_data_from_ltf(context):
    '''Send a request to: ltf.jooble.com:5001/calc_team_salary'''
    context.log.info("GET info from ltf server")
    # get request to ltf server
    response = requests.post(url=URL, headers=json.loads(os.environ.get('HEADERS')), timeout=600).json()
    result_df = pd.DataFrame.from_dict(data=response)
    
    if result_df is None:
        context.log.info(f"no result from: {URL}")
        raise Failure(
            description=f"response: {response}"
        )
    
    context.log.info("Successfully created dataframe from ltf server")
    return result_df

@asset
def truncate_dwh_raw_table(context):
    '''Truncate a raw destination table'''
    try:
        truncate_dwh_table(
            context,
            table_name="outreach_kpi_current_raw",
            schema="dwh_test"
        )
        return True
    except Exception:
        raise Exception


@asset
def save_to_dwh_raw_table(context, get_data_from_ltf, truncate_dwh_raw_table):
    '''Save dataframe with raw data to a raw dwh table (table to save data before parse)'''
    if get_data_from_ltf is None:
        context.log.info(f"{get_data_from_ltf}")
        raise Failure(
            description=f"there is nothing from LTF API"
        )
    save_to_dwh(
        df=get_data_from_ltf,
        table_name="outreach_kpi_current_raw",
        schema="dwh_test"
    )


@asset()
def parse_row_table(context, save_to_dwh_raw_table):
    '''Call dwh procedure: truncate, parse and insert data into aggregation table'''
    conn, cursor = connect_to_dwh()
    try:
        cursor.execute("call aggregation.insert_upd_outreach_kpi_current();")
        conn.commit()
        cursor.close()
        context.log.info("Parsed raw table to: [aggregation.outreach_kpi_current]")
        send_dwh_alert_slack_message(":white_check_mark: outreach_kpi_current executed successfully")
    except Exception as e:
        send_dwh_alert_slack_message(":error_alert: outreach_kpi_current failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        raise Exception
