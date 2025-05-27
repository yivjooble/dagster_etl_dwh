import os
from datetime import datetime

import pandas as pd
from dagster import op, job, fs_io_manager, make_values_resource, Field, Failure, DynamicOut, DynamicOutput
from dotenv import load_dotenv
from pandas import json_normalize
from serpapi.google_search import GoogleSearch

from etl_api.utils.dwh_db_operations import save_to_dwh
from etl_api.utils.io_manager_path import get_io_manager_path
from etl_api.utils.utils import job_prefix
from utility_hub import DwhOperations

load_dotenv()

SCHEMA = "imp_api"
TABLE_NAME = "google_trends"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country"
JOB_PREFIX = job_prefix()
COUNTRY_LIST = ['GB', 'DE', 'PL', 'RS', 'NL', 'US', 'CH', 'IT', 'AT', 'BR', 'BE', 'ES', 'CA', 'CZ', 'FR']
TZ = "60"
API_KEY = os.getenv("API_SERP")

# https://serpapi.com/

# Timeframe options:
# UTC+0 (Greenwich Mean Time - GMT): 0
# UTC+1 (Central European Time - CET): 60
# UTC+2 (Eastern European Time - EET): 120


def generate_params(keyword, country, date):
    params = {
        "api_key": API_KEY,
        "engine": "google_trends",
        "q": f"{keyword}",
        "geo": f"{country}",
        "tz": TZ,
        "date": f"{date}"
    }
    return params


@op(out=DynamicOut(), required_resource_keys={'globals'})
def google_trends_get_instance(context):
    launch_countries = context.resources.globals["reload_countries"]

    # Define parameters
    country_keywords = {
        'GB': ['jobs'],
        'DE': ['stellenangebote', 'jobs'],
        'PL': ['praca'],
        'RS': ['posao'],
        'NL': ['vacatures'],
        'US': ['jobs'],
        'CH': ['jobs', 'emploi', 'lavoro'],
        'IT': ['lavoro'],
        'AT': ['jobs', 'stellenangebote'],
        'BR': ['vagas de emprego', 'emprego'],
        'BE': ['emploi', 'vacatures', 'jobs'],
        'ES': ['trabajo', 'empleo'],
        'CA': ['jobs', 'emploi'],
        'CZ': ['práce'],
        'FR': ['emploi'],
    }

    context.log.info('Getting instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     )

    for country in launch_countries:
        for internal_country in country_keywords:
            if country == internal_country:
                yield DynamicOutput(
                    value={'country': country,
                           'keywords': country_keywords[country]},
                    mapping_key='country_' + country
                )


@op(required_resource_keys={"globals"})
def google_trends_fetch_data(context, instance_country: dict):
    timeframe = context.resources.globals["timeframe"]
    country = instance_country['country']
    data_frames = []

    for keyword in instance_country['keywords']:
        try:
            # Build payload
            params = generate_params(keyword=keyword, country=country, date=timeframe)
            search = GoogleSearch(params)
            results = search.get_dict()
            context.log.info(f"Fetching data for {country} - {keyword}.")

            # Extract and flatten the interest over time data
            data = pd.DataFrame(results['interest_over_time']['timeline_data'])
            flattened_data = json_normalize(data['values'].explode().tolist())
            flattened_data['date'] = data['date']

            # Add country and keyword to the flattened data
            flattened_data['country'] = country
            flattened_data['keyword'] = keyword
            data_frames.append(flattened_data)

        except Exception as e:
            context.log.error(f"Error fetching data for {country} - {keyword}:\n{results}\n{e}")
            raise Failure(description=f"Error fetching data for {country} - {keyword}:\n{results}\n{e}")

    # Concatenate all data frames into one
    if data_frames:
        all_data = pd.concat(data_frames)
        all_data.reset_index(drop=True, inplace=True)
        all_data['date'] = pd.to_datetime(all_data['date'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')

        # Replace 'GB' with 'UK' in the country column if needed
        if country == 'GB':
            all_data['country'] = 'UK'
            country = 'UK'

        # Select only the required columns
        all_data = all_data[['date', 'country', 'keyword', 'value']]

        # Get today and 3 months ago dates for data cleanup
        today = datetime.today().strftime('%Y-%m-%d')
        three_months_ago = datetime.today() - pd.DateOffset(months=3)

        # Delete old data from the database and save new data
        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_COLUMN,
            country_column=COUNTRY_COLUMN,
            date_start=three_months_ago.strftime('%Y-%m-%d'),
            date_end=today,
            country=country
        )

        save_to_dwh(df=all_data, table_name=TABLE_NAME, schema=SCHEMA)
        context.log.info(f"|{country.upper()}|: Successfully saved data to DWH.")
    else:
        context.log.error("No data fetched.")
        raise Failure(description="No data fetched.")


@job(
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=COUNTRY_LIST),
                                                   timeframe=Field(str, default_value='today 3-m'), ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=JOB_PREFIX + TABLE_NAME,
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "dwh",
        "api": "google_trends",
    },
    description=f'Fetches google trends data and stores it in: {SCHEMA}.{TABLE_NAME}.\n'
                f'Fetches data for period: last 3 months. Deletes equal period in DWH.',
)
def google_trends_job():
    instances = google_trends_get_instance()
    instances.map(google_trends_fetch_data)
