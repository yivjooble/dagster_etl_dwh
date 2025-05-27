import gspread
import os
import pandas as pd

from dagster import (
    op, job, Field, make_values_resource
)
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

from ..utils.dwh_connections import dwh_conn_psycopg2

load_dotenv()

SHEET_NAME = "Jobget Aff Report"
WORKSHEET_NAME = "Sheet2"

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_KEY_PATH')


@op(required_resource_keys={'globals'})
def jobget_aff_report_second_export_from_dwh(context):
    dwh_connection = dwh_conn_psycopg2()
    # 
    sql_filename = context.resources.globals['sql_filename']
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{sql_filename}.sql"))
    # current_datetime = datetime.now().strftime('%Y-%m-%d')
    # 
    with open(sql_file, 'r') as file:
        query = file.read()
    df = pd.read_sql(query, dwh_connection)
    # df['update_date'] = current_datetime
    return df


@op(required_resource_keys={'globals'})
def jobget_aff_report_second_insert_into(context, df: pd.DataFrame):
    worksheet_name = context.resources.globals['worksheet_name']

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('17kYXoanVtw3GV9V3xqanVRGNT2pKn-Jclq8vAeV-ThI')
    worksheet = spreadsheet.worksheet(worksheet_name)

    # Get the data without the header (column names)
    data_to_insert = df.values.tolist()
    context.log.info(data_to_insert)

    # Method 1: Read entire first column to find the first empty row
    first_column = worksheet.col_values(1)
    start_row = len(first_column) + 1

    # Prepare the values to insert
    values_to_insert = list(map(list, data_to_insert))

    # Insert multiple rows starting from the first empty row
    worksheet.append_rows(values_to_insert,
                          value_input_option='RAW',
                          insert_data_option='INSERT_ROWS',
                          table_range='A{}'.format(start_row))

    context.log.info("Data appended successfully!")


@job(
    resource_defs={"globals": make_values_resource(sheet_name=Field(str, default_value=SHEET_NAME),
                                                   worksheet_name=Field(str, default_value=WORKSHEET_NAME),
                                                   sql_filename=Field(str, default_value='jobget_aff_report_second'))
                   },
    name='dwh__' + 'jobget_aff_report_second',
    description='dwh --> google sheet'
)
def jobget_aff_report_second_job():
    data_from_dwh = jobget_aff_report_second_export_from_dwh()
    jobget_aff_report_second_insert_into(data_from_dwh)
