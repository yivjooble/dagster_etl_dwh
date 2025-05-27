import json
import requests
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
from dagster import op, job, fs_io_manager, make_values_resource, Field, Failure, DynamicOut, DynamicOutput
from dotenv import load_dotenv

from utility_hub import DwhOperations, retry_policy
from utility_hub.core_tools import get_creds_from_vault
from etl_api.utils.dwh_db_operations import save_to_dwh
from etl_api.utils.io_manager_path import get_io_manager_path
from etl_api.utils.utils import job_prefix

load_dotenv()

SCHEMA = "imp_api"
TABLE_NAME = "google_trends_new"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country"
JOB_PREFIX = job_prefix()
COUNTRY_LIST = ['GB', 'DE', 'PL', 'RS', 'NL', 'US', 'CH', 'IT', 'AT', 'BR', 'BE', 'ES', 'CA', 'CZ', 'FR']

"""
References:
- DataForSEO Docs: https://docs.dataforseo.com/v3/keywords_data/google_trends/locations/?python
"""


def calculate_date_range(timeframe) -> Tuple[str, str]:
    today = datetime.today()

    if timeframe == 'past_day':
        date_start = today - timedelta(days=1)
    elif timeframe == 'past_30_days':
        date_start = today - timedelta(days=30)
    elif timeframe == 'past_90_days':
        date_start = today - pd.DateOffset(months=3)
    elif timeframe == 'past_180_days':
        date_start = today - pd.DateOffset(months=6)
    else:
        raise ValueError(
            f"Invalid timeframe: {timeframe}. Valid options: past_day, past_30_days, past_90_days, past_180_days")

    return date_start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def map_country_code(country) -> int:
    country_codes = {
        'GB': 2826,
        'DE': 2276,
        'PL': 2616,
        'RS': 2688,
        'NL': 2528,
        'US': 2840,
        'CH': 2756,
        'IT': 2380,
        'AT': 2040,
        'BR': 2076,
        'BE': 2056,
        'ES': 2724,
        'CA': 2642,
        'CZ': 2203,
        'FR': 2250
    }
    return country_codes[country]


def create_payload(keyword, location_code, timeframe) -> json:
    """
    Create the payload for the Google Trends API request.
    """
    return json.dumps([{
        "time_range": timeframe,
        "category_code": 0,
        "keywords": [keyword],
        "location_code": location_code,
        "language_code": "en",
    }])


def fetch_trends_from_api(payload, api_key) -> json:
    """
    Send a request to the Google Trends API and return the response.
    """
    url = "https://api.dataforseo.com/v3/keywords_data/google_trends/explore/live"
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response.raise_for_status()  # Raise HTTPError for bad responses
    return response.json()


def process_api_response(tasks, country, keyword) -> pd.DataFrame:
    """
    Process the API response and extract relevant trends data.
    """
    rows = []
    for task in tasks:
        results = task[0].get('result', [])
        for result in results:
            check_url = result.get('check_url')
            items = result.get('items', [])
            for item in items:
                data_entries = item.get('data', [])
                for trend_data in data_entries:
                    if trend_data.get('values', [None])[0] is not None:
                        rows.append({
                            "date": datetime.utcfromtimestamp(trend_data.get('timestamp')).strftime('%Y-%m-%d'),
                            "value": trend_data.get('values')[0] if trend_data.get('values') else None,
                            "country": country,
                            "keyword": keyword,
                            "check_url": check_url,
                        })
    return pd.DataFrame(rows)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def google_trends_new_get_instance(context):
    launch_countries = context.resources.globals["reload_countries"]

    country_keywords = {
        'GB': ['jobs'],
        'DE': ['stellenangebote', 'jobs', 'Indeed'],
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

    for country in launch_countries:
        for internal_country in country_keywords:
            if country.lower() == internal_country.lower():
                yield DynamicOutput(
                    value={'country': country, 'keywords': country_keywords[country]},
                    mapping_key=f'country_{country}'
                )


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def google_trends_new_fetch_data(context, instance_country: dict):
    timeframe = context.resources.globals["timeframe"]
    country = instance_country['country']
    rows = []
    context.log.info(f"Fetching data for {country}")

    try:
        if timeframe in ['past_day', 'past_30_days', 'past_90_days', 'past_180_days']:
            for keyword in instance_country['keywords']:
                mapped_location_code = map_country_code(country)

                context.log.debug(f"location_code: {mapped_location_code}\n"
                                  f"timeframe: {timeframe}\n"
                                  f"keyword: {keyword}")

                payload = create_payload(keyword, mapped_location_code, timeframe)

                results = fetch_trends_from_api(payload=payload,
                                                api_key=get_creds_from_vault('DATAFORSEO_API_KEY'))
                data = pd.json_normalize(results)

                # Extract relevant data
                tasks = data['tasks']

                # Iterate over the list of tasks
                rows.append(process_api_response(tasks, country, keyword))

            # Concatenate all dataframes
            df = pd.concat(rows)

            # Rename GB to UK
            if country == 'GB':
                df['country'] = df['country'].replace('GB', 'UK')

            # Get today and 3 months ago dates for data cleanup
            date_start, date_end = calculate_date_range(timeframe)

            if not df.empty:
                # Delete old data from the database and save new data
                DwhOperations.delete_data_from_dwh_table(
                    context=context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    date_column=DATE_COLUMN,
                    country_column=COUNTRY_COLUMN,
                    date_start=date_start,
                    date_end=date_end,
                    country='UK' if country == 'GB' else country
                )

                # Save data to DWH
                save_to_dwh(df=df, table_name=TABLE_NAME, schema=SCHEMA)
                context.log.info(f"Data saved to DWH for {country}")
            else:
                context.log.info(f"No data for: {country}")
        else:
            context.log.error(f"Invalid timeframe: {timeframe}\n"
                              f"Valid options: past_day, past_30_days, past_90_days, past_180_days")
    except Exception as e:
        raise Failure(description=f"Error fetching data for {country} - {keyword}:\n{e}")


resource_defs = {"globals": make_values_resource(reload_countries=Field(list, default_value=COUNTRY_LIST),
                                                 timeframe=Field(str, default_value='past_90_days')),
                 "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})}


@job(
    resource_defs=resource_defs,
    name=JOB_PREFIX + TABLE_NAME,
    tags={"data_model": f"{SCHEMA}"},
    metadata={"destination_db": "dwh",
              "api": "https://docs.dataforseo.com/v3/keywords_data/google_trends/"},
    description=f'Fetches google trends data and stores it in: {SCHEMA}.{TABLE_NAME}.\n'
                f'Default period: last 3 months. Deletes equal period in DWH.',
)
def google_trends_new_job():
    instances = google_trends_new_get_instance()
    instances.map(google_trends_new_fetch_data)
