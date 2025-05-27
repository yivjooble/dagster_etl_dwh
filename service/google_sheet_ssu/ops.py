import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from decimal import Decimal
import traceback
from pathlib import Path
from dagster import op, job
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global variables for sheet and worksheet names
SHEET_NAME = 'Заблокованні'
WORKSHEET_NAME = 'Вакансії'


# Google Sheets setup
def setup_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # Get the directory of the current script
    script_dir = Path(__file__).parent

    # Construct the path to the JSON file
    json_file_path = script_dir / 'credentials' / 'steel-earth-427014-n5-fc9689181b62.json'

    # Check if the file exists
    if not json_file_path.exists():
        raise FileNotFoundError(f"JSON file not found in the script directory: {json_file_path}")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        str(json_file_path),  # Convert Path object to string
        scope
    )
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NGPIRNikiY1bQIzLhihqwooIv3KO3mIpZJ6qjahWQO8/edit?usp=sharing')

    try:
        worksheet_hu = spreadsheet.worksheet(WORKSHEET_NAME)  # Use the global variable for worksheet name
    except gspread.exceptions.WorksheetNotFound:
        worksheet_hu = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows='10000', cols='11')

    return worksheet_hu


# Database setup
def setup_database():
    db_config = {
        'host': 'nl89.jooble.com',
        'user': os.getenv('JOB_EXPORTER_USER'),
        'password': os.getenv('JOB_EXPORTER_PASSWORD'),
        'database': 'employer_account',
        'port': 3306
    }
    engine = create_engine(
        f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    Session = sessionmaker(bind=engine)
    return Session()


# Convert data types for Google Sheets
def convert_data_types(row):
    return [
        item.strftime('%Y-%m-%d %H:%M:%S') if isinstance(item, datetime) else
        float(item) if isinstance(item, Decimal) else
        item
        for item in row
    ]


# Fetch data from the database
def fetch_data(session):
    query = text("""
        SELECT  
            e.id,
            cdp.company_name,
            CASE ebr.id_reason
                WHEN 1 THEN 'Заборонений вид діяльності'
                WHEN 3 THEN 'Скарги пошукачів'
                WHEN 6 THEN 'Офісники'
                ELSE ' '
            END AS reason,
            ebr.blocking_reason AS reason_Detail,
            sem.date_created AS date_blocking,
            j.title,
            j.region_list,
            j.date_updated,
            j.date_created,
            j.emails, 
            j.phones,
            REGEXP_REPLACE(j.html_desc, '<[^>]*>', ' ') AS job_text
        FROM job j
        JOIN employer e ON j.id_employer = e.id
        LEFT JOIN employer_cdp cdp ON e.id_cdp = cdp.id  
        JOIN statistics_employer_moderation sem ON sem.id_employer = e.id
        JOIN employer_blocking_reason ebr ON ebr.id_employer = e.id 
        WHERE e.country_code = 'ua'
          AND e.moderation_status IN (3)
          AND sem.date_created >= '2025-01-01' 
          AND ebr.id_reason IN (1,3,6)
        ORDER BY sem.date_created;
    """)
    result = session.execute(query)
    return result.fetchall()


# Compare existing and new data to find new rows
def find_new_rows(existing_data, new_data):
    """
    Compare existing data with new data and return only rows that do not already exist.
    Assumes the first column in both datasets is the unique identifier (e.g., 'ID Employer').
    """
    # Extract unique identifiers from existing data (skip header row)
    existing_ids = {str(row[1]).strip() for row in existing_data[1:]}  # Use index 1 for 'ID Employer'

    # Find new rows where the ID does not exist in the existing data
    new_rows = [row for row in new_data if str(row[1]).strip() not in existing_ids]  # Use index 1 for 'ID Employer'

    # Logging for debugging
    if new_rows:
        print(f"New rows found: {len(new_rows)}")
        for row in new_rows:
            print(f"New row ID: {row[1]}")  # Log the ID of new rows
    else:
        print("No new rows found.")

    return new_rows


# Main function
@op
def main(context):
    worksheet_hu = setup_google_sheets()
    session = setup_database()

    try:
        # Fetch data from the database
        data_hu = fetch_data(session)

        # Add a new column with the current date as the first column
        current_date = datetime.now().strftime('%Y-%m-%d')
        data_hu = [[current_date] + list(row) for row in data_hu]

        # Prepare headers with the new column
        headers = [
            'Дата передачі', 'ID Employer', 'company_name', 'reason', 'reason_Detail', 'date_blocking', 'title',
            'region_list', 'date_updated', 'date_created', 'emails', 'phones', 'job_text'
        ]

        # Convert data types for Google Sheets
        data_hu = [convert_data_types(row) for row in data_hu]

        # Fetch existing data from the worksheet
        existing_data = worksheet_hu.get_all_values()

        if existing_data:
            # If the worksheet is not empty, compare existing data with new data
            new_rows = find_new_rows(existing_data, data_hu)  # Pass existing_data and new data
            if new_rows:
                # Append only new rows
                worksheet_hu.append_rows(new_rows)
                context.log.info(f"Appended {len(new_rows)} new rows to Google Sheets.")
            else:
                context.log.info("No new rows to append.")
        else:
            # If the worksheet is empty, add headers and data
            worksheet_hu.update([headers] + data_hu)
            context.log.info("Data successfully added to Google Sheets.")

    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        session.close()


@job(
    name='service__' + 'google_sheet_ssu',
    description='nl89.jooble.com --> google sheet for SSU',
    metadata={
        "google_sheet": "https://docs.google.com/spreadsheets/d/1NGPIRNikiY1bQIzLhihqwooIv3KO3mIpZJ6qjahWQO8/edit?usp=sharing",
    },
)
def google_sheet_ssu_job():
    main()
