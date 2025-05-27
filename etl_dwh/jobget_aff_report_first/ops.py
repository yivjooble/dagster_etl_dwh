import gspread
import os
import pandas as pd
import datetime

from dagster import (
    op, job, Field, make_values_resource
)
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

from ..utils.dwh_connections import dwh_conn_psycopg2

load_dotenv()

SHEET_NAME = "Jobget Aff Report"
WORKSHEET_NAME = "Sheet1"

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_KEY_PATH')


@op(required_resource_keys={'globals'})
def jobget_aff_report_first_export_from_dwh(context):
    dwh_connection = dwh_conn_psycopg2()
    # 
    sql_filename = context.resources.globals['sql_filename']
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{sql_filename}.sql"))

    with open(sql_file, 'r') as file:
        query = file.read()
    df = pd.read_sql(query, dwh_connection)
    return df


@op(required_resource_keys={'globals'})
def jobget_aff_report_first_insert_into(context, df: pd.DataFrame):
    worksheet_name = context.resources.globals['worksheet_name']

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key('17kYXoanVtw3GV9V3xqanVRGNT2pKn-Jclq8vAeV-ThI')
    worksheet = spreadsheet.worksheet(worksheet_name)

    # Log DataFrame information
    context.log.info(f"Total rows to process: {len(df)}")
    context.log.info(f"Columns: {', '.join(df.columns)}")

    # Convert DataFrame to list of lists, handling different data types
    values_to_insert = []
    for row_idx, row in df.iterrows():
        new_row = []
        for col_idx, value in enumerate(row):
            original_type = type(value).__name__
            if pd.isna(value):
                new_row.append('')
                context.log.info(f"Row {row_idx}, Column {col_idx}: NULL value converted to empty string")
            elif isinstance(value, (datetime.date, datetime.datetime)):
                new_row.append(value.isoformat())
                context.log.info(f"Row {row_idx}, Column {col_idx}: {original_type} converted to ISO format: {value.isoformat()}")
            elif isinstance(value, float):
                new_row.append(float(value))
                context.log.info(f"Row {row_idx}, Column {col_idx}: {original_type} value preserved: {value}")
            else:
                new_row.append(str(value))
                context.log.info(f"Row {row_idx}, Column {col_idx}: {original_type} converted to string: {value}")
        values_to_insert.append(new_row)
        context.log.info(f"Completed processing row {row_idx}")

    # Get the first empty row
    first_column = worksheet.col_values(1)
    start_row = len(first_column) + 1 if first_column else 1
    context.log.info(f"Starting to write data at row {start_row}")

    # Insert data with proper formatting
    try:
        # First, insert headers if this is the first row
        if start_row == 1:
            worksheet.append_row(df.columns.tolist(), value_input_option='USER_ENTERED')
            context.log.info("Headers written successfully")
            start_row += 1

        # Insert the data
        worksheet.append_rows(values_to_insert,
                            value_input_option='USER_ENTERED',
                            insert_data_option='INSERT_ROWS',
                            table_range=f'A{start_row}')

        context.log.info(f"Successfully inserted {len(values_to_insert)} rows starting from row {start_row}")
        context.log.info(f"Data range: A{start_row}:{chr(65 + len(df.columns) - 1)}{start_row + len(values_to_insert) - 1}")
    except Exception as e:
        context.log.error(f"Error inserting data: {str(e)}")
        raise


@job(
    resource_defs={"globals": make_values_resource(sheet_name=Field(str, default_value=SHEET_NAME),
                                                   worksheet_name=Field(str, default_value=WORKSHEET_NAME),
                                                   sql_filename=Field(str, default_value='jobget_aff_report_first'))
                   },
    name='dwh__' + 'jobget_aff_report_first',
    description='https://docs.google.com/spreadsheets/u/2/d/17kYXoanVtw3GV9V3xqanVRGNT2pKn-Jclq8vAeV-ThI/edit?gid=0#gid=0'
)
def jobget_aff_report_first_job():
    data_from_dwh = jobget_aff_report_first_export_from_dwh()
    jobget_aff_report_first_insert_into(data_from_dwh)
