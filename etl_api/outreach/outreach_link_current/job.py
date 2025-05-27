from dagster import op, Out, In, job, fs_io_manager

# built in modules
import requests
import pandas as pd
import json
import os
import traceback
import logging

# third party modules
from dotenv import load_dotenv

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import truncate_dwh_table, dwh_conn_psycopg2
# module import
from ...utils.messages import send_dwh_alert_slack_message

load_dotenv()

URL = "http://10.0.1.154:5001/calc_outreach_salary"


@op(out=Out(pd.DataFrame), description='send request to ltf.jooble.com/calc_outreach_salary')
def get_data_from_ltf_op(context):
    context.log.info("GET info from ltf server")
    # get request to ltf server
    response = requests.post(url=URL, headers=json.loads(os.environ.get('HEADERS')), timeout=300).json()
    if response:
        result_list_df = []

        for value in response:
            split_data = json.loads(value['account_details'])
            del value['account_details']
            split_data.update(value)
            result_list_df.append(split_data)

        result_df = pd.DataFrame(data=result_list_df)
        context.log.info("Success load data from ltf server")

        return result_df
    

@op(ins={'df': In(pd.DataFrame)}, out=Out(pd.DataFrame), description='truncate a raw table before write')    
def truncate_dwh_raw_table_op(context, df):
    truncate_dwh_table(
        context,
        table_name="outreach_link_current_raw",
        schema="dwh_test"
    )
    return df
    

@op(ins={'df': In(pd.DataFrame)}, out=Out(str), description='save data from df to dwh raw table')
def save_to_raw_table_op(context, df):
    try:
        # connect to dwh
        conn = dwh_conn_psycopg2()
        # create a session with db
        cursor = conn.cursor()

        # insert data to temp table
        context.log.info("Start to insert: [dwh_test.outreach_link_current_raw]")
        for index, row in df.iterrows():
            cursor.execute("""insert into dwh_test.outreach_link_current_raw (account_name, account_login, account_busyness, account_pay_type_name, account_team_id, account_team_name, account_region_id, 
                                            account_region_name, account_payment_login, account_email, account_deleted_date, is_deleted, account_email_login, account_lead_id, 
                                            account_lead_name, 
                                            bonus, bonus_currency, bonus_link, bonus_score, country_bonus_details, email_send_count, id_account, id_period, 
                                            pay_type, salary, salary_currency) 
                            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                                                                                row.account_name,
                                                                                row.account_login,
                                                                                row.account_busyness,
                                                                                row.account_pay_type_name,
                                                                                row.account_team_id,
                                                                                row.account_team_name,
                                                                                row.account_region_id,
                                                                                row.account_region_name,
                                                                                row.account_payment_login,
                                                                                row.account_email,
                                                                                row.account_deleted_date,
                                                                                row.is_deleted,
                                                                                row.account_email_login,
                                                                                row.account_lead_id,
                                                                                row.account_lead_name,
                                                                                row.bonus,
                                                                                row.bonus_currency,
                                                                                row.bonus_link,
                                                                                row.bonus_score,
                                                                                row.country_bonus_details,
                                                                                row.email_send_count,
                                                                                row.id_account,
                                                                                row.id_period,
                                                                                row.pay_type,
                                                                                row.salary,
                                                                                row.salary_currency,))
        conn.commit()
        cursor.close()
        context.log.info("Inserted into: [dwh_test.outreach_link_current_raw]")
        
        status = "success"
    except Exception as e:
        status = "failed"
        context.log.error(f'{traceback.format_exc()}')
    return status


@op(ins={'status': In(str)}, description='parse raw table and save to main')
def parse_row_table_op(context, status):
    if status == 'success':
        # connect to dwh
        conn = dwh_conn_psycopg2()
        # create a session with db
        cursor = conn.cursor()
        cursor.execute("call aggregation.insert_upd_outreach_link_current();")
        conn.commit()
        cursor.close()
        send_dwh_alert_slack_message(":white_check_mark: outreach_link_current executed successfully")
    else:
        context.log.error(f'{traceback.format_exc()}')
        send_dwh_alert_slack_message(":error_alert: outreach_link_current failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        raise Exception


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_link_current')
def outreach_link_current_job():
    '''aggregation.outreach_link_current'''
    ltf_df = get_data_from_ltf_op()
    df_after_truncate = truncate_dwh_raw_table_op(ltf_df)
    status = save_to_raw_table_op(df_after_truncate)
    parse_row_table_op(status)