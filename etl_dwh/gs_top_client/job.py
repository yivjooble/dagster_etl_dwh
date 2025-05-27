import gspread
import os
import pandas as pd

from dagster import (
    op, job, Field, make_values_resource
)
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime

from ..utils.dwh_connections import dwh_conn_psycopg2

load_dotenv()

SHEET_NAME = "TOP clients (weekly reports agg)"
LAST_WEEK_DATA_SHEET = "last week_row_data"
LAST_MONTH_DATA_SHEET = 'last month_row_data'

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_KEY_PATH')
PROJECT_ID = 'jooble-prj'


@op(
    required_resource_keys={'globals'}
)
def gs_top_client_export_from_dwh(context):
    dwh_connection = dwh_conn_psycopg2()
    # 
    sql_filename = context.resources.globals['sql_filename']
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{sql_filename}.sql"))
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    # 
    with open(sql_file, 'r') as file:
        query = file.read()
    df = pd.read_sql(query, dwh_connection)
    df['update_date'] = current_datetime
    return df


@op(
    required_resource_keys={'globals'}
)
def gs_top_client_insert_into(context, df: pd.DataFrame):
    sheet_name = context.resources.globals['sheet_name']

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open(SHEET_NAME)
    worksheet = spreadsheet.worksheet(sheet_name)

    # Get the data without the header (column names)
    data_to_insert = df.values.tolist()
    context.log.info(data_to_insert)

    # Method 1: Read entire first column to find the first empty row
    first_column = worksheet.col_values(1)
    start_row = len(first_column) + 1

    # Prepare the values to insert
    values_to_insert = list(map(list, data_to_insert))

    # Insert multiple rows starting from the first empty row
    worksheet.append_rows(values_to_insert, value_input_option='RAW', insert_data_option='INSERT_ROWS',
                          table_range='A{}'.format(start_row))

    context.log.info("Data appended successfully!")


@job(
    resource_defs={"globals": make_values_resource(sheet_name=Field(str, default_value=LAST_WEEK_DATA_SHEET),
                                                   sql_filename=Field(str, default_value='last_week_data'))
                   },
    name='dwh__ga_last_week_revenue_data',
    description='dwh --> google sheet'
)
def last_week_revenue_data_to_gs_job():
    data_from_dwh = gs_top_client_export_from_dwh()
    gs_top_client_insert_into(data_from_dwh)


@job(
    resource_defs={"globals": make_values_resource(sheet_name=Field(str, default_value=LAST_MONTH_DATA_SHEET),
                                                   sql_filename=Field(str, default_value='last_month_data'))
                   },
    name='dwh__ga_last_month_revenue_data',
    description='dwh --> google sheet'
)
def last_month_revenue_data_to_gs_job():
    data_from_dwh = gs_top_client_export_from_dwh()
    gs_top_client_insert_into(data_from_dwh)
