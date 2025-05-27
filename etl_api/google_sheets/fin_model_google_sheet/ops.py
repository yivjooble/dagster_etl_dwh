import gspread
import os
import pandas as pd

from dagster import (
    op, job, Failure, fs_io_manager, make_values_resource, Field
)

# module import
from utility_hub import DwhOperations, DatabaseConnectionManager
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime
from etl_jooble_internal.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name

load_dotenv()

TABLE_NAME = "fin_model_google_sheet"
SCHEMA = "ono"
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_KEY_PATH')


def drop_temp_table(context, schema, table_name):
    """
    Drops a temporary table from the database.
    
    Args:
        context: The dagster context
        schema: The database schema
        table_name: The name of the temporary table to drop
    """
    try:
        DwhOperations.execute_on_dwh(context=context,
                                    query=f"DROP TABLE IF EXISTS {schema}.{table_name};")
        context.log.debug(f"Dropped a temp table: {schema}.{table_name}")
    except Exception as e:
        context.log.warning(f"Failed to drop temp table {schema}.{table_name}: {str(e)}")


@op
def fin_model_google_sheet_get_client(context):
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
        client = gspread.authorize(credentials)
        context.log.info("Google Sheets client successfully authorized.")
    except FileNotFoundError:
        context.log.error("Failed to find the credentials file.")
        raise Failure("Failed to find the credentials file.")
    except gspread.exceptions.APIError as e:
        context.log.error(f"API error occurred: {e}")
        raise Failure(f"API error occurred: {e}")
    except gspread.exceptions.GSpreadException as e:
        context.log.error(f"GSpread error occurred: {e}")
        raise Failure(f"GSpread error occurred: {e}")
    except Exception as e:
        context.log.error(f"An unexpected error occurred: {e}")
        raise Failure(f"An unexpected error occurred: {e}")

    return client


@op(required_resource_keys={'globals'})
def fin_model_google_sheet_save_to_dwh(context, client):
    temp_table_name = "temp_google_sheet_data"
    try:
        destination_db = context.resources.globals["destination_db"]
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_APPLICATION_CREDENTIALS, scope)
        client = gspread.authorize(credentials)

        sheet = client.open_by_key("1QJO_h_iuUf5Q6r09iGj-7LgdAfAtn6LHWWv5rAAPpHk")
        worksheet_data = sheet.worksheet('tableau')

        list_of_dicts = worksheet_data.get_all_records()
        df = pd.DataFrame(list_of_dicts)

        # Delete empty named columns - check if empty string exists before dropping
        df.columns = df.columns.str.strip()  # Strip whitespace from column names
        empty_cols = [col for col in df.columns if col == '']
        if empty_cols:
            df = df.drop(columns=empty_cols)
        context.log.info(f"Data fetched successfully.")

        # Create a temporary table in PostgreSQL and insert the data
        db_manager = DatabaseConnectionManager()
        sqlalchemy_engine = db_manager.get_sqlalchemy_engine()

        df.to_sql(temp_table_name, schema=SCHEMA, con=sqlalchemy_engine, if_exists='replace', index=False)

        # Apply the filter in PostgreSQL
        query = f"""
            SELECT country, year, month, channel, sessions, revenue, "Cost" as cost,
                   click_per_session, paid_click_share, budget, new_clients_cnt, new_clients_budget,
                   new_clients_revenue
            FROM {SCHEMA}.{temp_table_name}
            WHERE 1=1 
            and make_date(year::int, month::int, 1) >= make_date(cast(EXTRACT(year FROM current_date) AS int), cast(EXTRACT(month FROM current_date) AS int), 1)
            and month::varchar != ''::varchar;
        """

        filtered_df = pd.read_sql(query, con=sqlalchemy_engine)

        # Add the 'date_add' column with the current date
        filtered_df["dateadd"] = datetime.now().date().strftime('%Y-%m-%d')
        context.log.debug(f"Filtered data fetched successfully. {filtered_df}")

        # Drop the temporary table
        drop_temp_table(context, SCHEMA, temp_table_name)

        if destination_db == "both":
            # dwh
            DwhOperations.save_to_dwh_copy_method(context,
                                                  schema=SCHEMA,
                                                  table_name=TABLE_NAME,
                                                  df=filtered_df,
                                                  destination_db="dwh")
            context.log.info(f"Saved to dwh: {SCHEMA}.{TABLE_NAME}")

            # cloudberry
            DwhOperations.save_to_dwh_copy_method(context,
                                                  schema="dimension",
                                                  table_name=TABLE_NAME,
                                                  df=filtered_df,
                                                  destination_db="cloudberry")
            context.log.info(f"Saved to dwh: {SCHEMA}.{TABLE_NAME}")

        elif destination_db == "dwh":
            # dwh
            DwhOperations.save_to_dwh_copy_method(context,
                                                  schema=SCHEMA,
                                                  table_name=TABLE_NAME,
                                                  df=filtered_df,
                                                  destination_db="dwh")
            context.log.info(f"Saved to dwh: {SCHEMA}.{TABLE_NAME}")

        elif destination_db == "cloudberry":
            # cloudberry
            DwhOperations.save_to_dwh_copy_method(context,
                                                  schema="dimension",
                                                  table_name=TABLE_NAME,
                                                  df=filtered_df,
                                                  destination_db="cloudberry")
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
    finally:
        # Ensure the temp table is dropped even if an exception occurs
        drop_temp_table(context, SCHEMA, temp_table_name)


@job(
    resource_defs={"globals": make_values_resource(destination_db=Field(str, default_value='both')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "dwh, cloudberry, both",
        "google_sheet": "https://docs.google.com/spreadsheets/u/2/d/1QJO_h_iuUf5Q6r09iGj-7LgdAfAtn6LHWWv5rAAPpHk/edit?gid=767916316#gid=767916316"
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def fin_model_google_sheet_job():
    client = fin_model_google_sheet_get_client()
    fin_model_google_sheet_save_to_dwh(client)
