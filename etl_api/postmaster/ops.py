from datetime import datetime, timedelta
import pandas as pd

from dagster import (
    op,
    job,
    make_values_resource,
    Field,
    fs_io_manager,
    DynamicOut,
    DynamicOutput
)

from etl_api.postmaster.postmaster_tools import PostmasterResource
from etl_api.postmaster.postmaster_data_processor import process_postmaster_data
from utility_hub.core_tools import generate_job_name
from utility_hub.db_operations import DwhOperations
from utility_hub.data_collections import map_country_code_to_id, all_countries_list
from etl_api.utils.api_jobs_config import job_config_postmaster, custom_retry_policy


TABLE_NAME = "postmaster"
SCHEMA = "imp_api"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country_code"
DEFAULT_DATE = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')


@op(out=DynamicOut(), required_resource_keys={'globals', 'postmaster'})
def postmaster_get_country_instance(context):
    """Creates instances for each country with date range configuration"""
    launch_countries = context.resources.globals["reload_countries"]
    launch_countries = {country.strip('_').lower() for country in launch_countries}

    # Get date range from config
    start_date = context.resources.globals.get("reload_date_start", DEFAULT_DATE)
    end_date = context.resources.globals.get("reload_date_end", DEFAULT_DATE)

    date_range = pd.date_range(
        start=start_date,
        end=end_date,
    )

    context.log.info('Getting instances...\n'
                     f'Date range: {start_date} - {end_date}\n'
                     f'Selected  countries: {launch_countries}\n'
                     )

    for country in launch_countries:
        yield DynamicOutput(
            value={
                'country': country,
                'date_range': date_range
            },
            mapping_key='country_' + country
        )


@op(required_resource_keys={'globals', 'postmaster'}, retry_policy=custom_retry_policy)
def postmaster_get_and_save_data(context, instance_country: dict):
    """
    Fetches data from Google Postmaster Tools API for a date range and saves to DWH
    """
    country = instance_country['country']
    destination_db = context.resources.globals['destination_db']
    api = context.resources.postmaster.get_api(context)
    context.log.info(f"Processing data for country: {country}")

    try:
        for date in instance_country['date_range']:
            try:
                # Convert date to required format (YYYYMMDD)
                check_date = date.strftime('%Y%m%d')
                formatted_date = date.strftime('%Y-%m-%d')
                context.log.info(f"Processing date: {formatted_date}")

                # Get stats for specific country
                stats = api.get_country_domains_stats(check_date, country)

                if not stats:
                    context.log.warning(f"No stats were retrieved for date: {formatted_date}\nCountry: {country}")
                    continue

                # Process data into DataFrame
                df = process_postmaster_data(
                    context=context,
                    stats=stats,
                    date=check_date,
                    map_country_code_to_id=map_country_code_to_id
                )

                if df is None or df.empty:
                    context.log.warning(f"No data to save for date: {formatted_date}\nCountry: {country}")
                    continue

                context.log.info(f"Successfully processed country: {country.upper()}\nDate: {formatted_date}")

                DwhOperations.delete_data_from_dwh_table(
                    context=context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    date_column=DATE_COLUMN,
                    date_start=formatted_date,
                    country_column=COUNTRY_COLUMN,
                    country=country,
                    destination_db=destination_db
                )

                DwhOperations.save_to_dwh_copy_method(
                    context=context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    df=df,
                    destination_db=destination_db
                )

            except Exception as e:
                context.log.error(f"Error processing date: {formatted_date}\nCountry: {country}\nError: {e}")
                raise

    except Exception as e:
        context.log.error(f"Error in postmaster operation: {e}")
        raise


@job(
    config=job_config_postmaster,
    resource_defs={
        "globals": make_values_resource(
            reload_countries=Field(list, default_value=all_countries_list),
            reload_date_start=Field(str, default_value=DEFAULT_DATE),
            reload_date_end=Field(str, default_value=DEFAULT_DATE),
            destination_db=Field(str, default_value='both')
        ),
        "io_manager": fs_io_manager,
        "postmaster": PostmasterResource()
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": SCHEMA},
    metadata={
        "input_date": f"{DEFAULT_DATE} - {DEFAULT_DATE}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description="Collects and stores Google Postmaster Tools domain statistics"
)
def postmaster_job():
    instances = postmaster_get_country_instance()
    instances.map(postmaster_get_and_save_data).collect()
