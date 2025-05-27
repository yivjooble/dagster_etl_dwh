import gspread
import os
import pandas as pd

from dagster import (
    op, job, Failure, fs_io_manager, make_values_resource, Field
)

# module import
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub import DwhOperations
from utility_hub.core_tools import generate_job_name

# Load environment variables
load_dotenv()
# Constants
TABLE_NAME = "dic_ppc_comments"
SCHEMA = "traffic"
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_KEY_PATH')
SHEET_ID = '12QLgN2laJAe9XtwAvmHlQ9bQWL2Fp91_zOZXW853YVM'
WORKSHEET_NAME = 'comments'


@op(required_resource_keys={'globals'})
def dic_ppc_comments_save_to_dwh(context):
    """
    Function to fetch data from Google Sheets and save it to the DWH.
    :param context: Dagster context
    """
    try:
        destination_db = context.resources.globals["destination_db"]
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
        client = gspread.authorize(credentials)

        sheet = client.open_by_key(SHEET_ID)
        worksheet_data = sheet.worksheet(WORKSHEET_NAME)

        list_of_dicts = worksheet_data.get_all_records()
        df = pd.DataFrame(list_of_dicts)
        context.log.info(f"Data fetched successfully.")

        # Cast all columns to string
        df = df.astype(str)

        # Delete rows if comment column is empty
        df = df[df['comment'] != '']

        if not df.empty:
            # Get latest date from df if date column is string
            df['date'] = pd.to_datetime(df['date'])
            latest_date = df['date'].max().strftime('%Y-%m-%d')

            # Cast date from format 9/11/2024 to 2024-11-09
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

            # Save only data for today
            df = df[df['date'] == latest_date]

            # Delete rows with today date from dwh
            DwhOperations.delete_data_from_dwh_table(context=context,
                                                     schema=SCHEMA,
                                                     table_name=TABLE_NAME,
                                                     date_column='date',
                                                     date_start=latest_date,
                                                     date_end=latest_date,
                                                     destination_db=destination_db)

            DwhOperations.save_to_dwh_copy_method(context=context,
                                                  schema=SCHEMA,
                                                  table_name=TABLE_NAME,
                                                  df=df,
                                                  destination_db=destination_db)
            context.log.info(f"Saved to dwh: {SCHEMA}.{TABLE_NAME}")

    except gspread.exceptions.WorksheetNotFound:
        error_message = "Worksheet not found in the Google Sheets document."
        context.log.error(error_message)
        raise Failure(description=error_message)

    except gspread.exceptions.SpreadsheetNotFound:
        error_message = "Google Sheets document not found."
        context.log.error(error_message)
        raise Failure(description=error_message)

    except pd.errors.EmptyDataError:
        error_message = "No data found to load into the DataFrame."
        context.log.error(error_message)
        raise Failure(description=error_message)

    except Exception as e:
        error_message = f"An unexpected error occurred: {str(e)}"
        context.log.error(error_message)
        raise Failure(description=error_message)


@job(
    resource_defs={"globals": make_values_resource(destination_db=Field(str, default_value='both')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "dwh, cloudberry, both",
        "sheet_url": "https://docs.google.com/spreadsheets/d/12QLgN2laJAe9XtwAvmHlQ9bQWL2Fp91_zOZXW853YVM/edit?gid=0#gid=0"
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def dic_ppc_comments_job():
    dic_ppc_comments_save_to_dwh()
