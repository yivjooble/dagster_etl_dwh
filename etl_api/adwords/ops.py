import os
import multiprocessing
import pyodbc
from typing import List, Dict, Any, Generator
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
import pandas as pd

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from utility_hub.connection_managers import DatabaseConnectionManager
from etl_api.utils.io_manager_path import get_io_manager_path
from etl_api.utils.dwh_connections import dwh_conn_psycopg2, cloudberry_conn_psycopg2
from dagster import (
    op, job, fs_io_manager, Failure, make_values_resource, Field
)
from utility_hub.core_tools import generate_job_name, fetch_gitlab_data
from utility_hub.job_configs import job_config
from utility_hub import DwhOperations

# Constants and configuration
DATE_FORMAT = "%Y-%m-%d"
YESTERDAY_DATE = (datetime.today() - timedelta(days=1)).strftime(DATE_FORMAT)

id_account = '9402478433'
client_id = 9425368410
googleAds_config = os.path.join(os.getcwd(), 'configs', 'etl_api', 'adwords', 'googleadsv8.yaml')
googleAds_config_version = "v17"

SCHEMA = "imp_statistic"
TABLE_NAME = "adwords_to_compare"
PROCEDURE_CALL = 'call imp_statistic.adwords_data_verification(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name="adwords_data_verification",
    manual_object_type="procedures"
)


# Google Ads API functions
def get_customers(client: GoogleAdsClient, customer_id: int) -> List[int]:
    """
    Get list of enabled customer IDs from Google Ads API.
    
    Args:
        client: Google Ads client instance
        customer_id: Manager account ID
        
    Returns:
        List of enabled customer IDs
    """
    googleads_service = client.get_service("GoogleAdsService")
    query = "SELECT customer_client.id, customer_client.status FROM customer_client where customer_client.manager = false"
    response = googleads_service.search(
        customer_id=str(customer_id), query=query
    )
    ids = []
    for row in response:
        if (row.customer_client.status.name == 'ENABLED'):
            ids.append(row.customer_client.id)
    return ids


def get_conversions_value(client: GoogleAdsClient, customer_id: int, date: str) -> Generator:
    """
    Get conversion values for a specific date from Google Ads API.
    
    Args:
        client: Google Ads client instance
        customer_id: Customer ID
        date: Date in format YYYY-MM-DD
        
    Returns:
        Generator yielding campaign data rows
    """
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT campaign.id, campaign.name, metrics.impressions, metrics.clicks, metrics.cost_micros FROM campaign
        WHERE segments.date = '{date}' and metrics.impressions > 0
        """
    # Issues a search request using streaming.
    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = str(customer_id)
    search_request.query = query
    response = ga_service.search_stream(search_request)
    for batch in response:
        for row in batch.results:
            yield row


# Data storage functions
def add_conversion_value(data_list: List[Dict[str, Any]], id: int, campaign_id: str, 
                         campaign: str, date: str, impressions: int, clicks: int, cost: float) -> List[Dict[str, Any]]:
    """
    Add a conversion value record to the data list.
    
    Args:
        data_list: List of data dictionaries
        id: Customer ID
        campaign_id: Campaign ID
        campaign: Campaign name
        date: Date in format YYYY-MM-DD
        impressions: Number of impressions
        clicks: Number of clicks
        cost: Cost in currency units
        
    Returns:
        Updated data list
    """
    data_list.append({
        "customer_id": id,
        "campaign_id": campaign_id,
        "campaign": campaign,
        "day": date,
        "impressions": impressions,
        "clicks": clicks,
        "cost": cost
    })
    return data_list


def delete_from_dwh(data_list: List[Dict[str, Any]], delete_conn, start_date: str, end_date: str) -> None:
    """
    Delete data from data warehouse for the most recent date in the data.
    
    Args:
        data_list: List of data dictionaries
        destination_db: Destination database ("dwh", "cloudberry", or "both")
    """
    df = pd.DataFrame(data_list)
    if not df.empty:

        with delete_conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {SCHEMA}.{TABLE_NAME} WHERE day BETWEEN '{start_date}'::date AND '{end_date}'::date")
            delete_conn.commit()


def save_data_to_dwh(data_list: List[Dict[str, Any]], engine, destination_db: str, start_date: str, end_date: str) -> None:
    """
    Delete existing data and save new data to data warehouse.
    
    Args:
        data_list: List of data dictionaries
        engine: SQLAlchemy engine
        destination_db: Destination database ("dwh", "cloudberry", or "both")
    """
    df = pd.DataFrame(data_list)
    
    # Save new data to data warehouse    
    if not df.empty:
        df.to_sql(name=TABLE_NAME, con=engine, schema=SCHEMA, if_exists='append', index=False)


# Parallel processing functions
def collect(date_range, client_id: int, google_repository: GoogleAdsClient, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Collect data from Google Ads API for a specific customer and date range.
    
    Args:
        date_range: Range of dates to collect data for
        client_id: Customer ID
        google_repository: Google Ads client
        data_list: List to store collected data
        
    Returns:
        Updated data list
    """
    try:
        for date in date_range:
            formatted_date = date.strftime(DATE_FORMAT)
            for value in get_conversions_value(google_repository, client_id, formatted_date):
                metrics = value.metrics
                add_conversion_value(
                    data_list,
                    client_id,
                    value.campaign.id,
                    value.campaign.name,
                    formatted_date,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros / 1000000
                )

    except GoogleAdsException as ex:
        if (ex.error.code().name != "PERMISSION_DENIED"):
            print(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
    except Exception as ex:
        print(f"Other exception {ex}")
    
    return data_list


def run_async(max_processes: int, date_range, client_id: int, google_repository: GoogleAdsClient) -> List[Dict[str, Any]]:
    """
    Run data collection in parallel for multiple customers.
    
    Args:
        max_processes: Maximum number of processes to use
        date_range: Range of dates to collect data for
        client_id: Manager account ID
        google_repository: Google Ads client
        
    Returns:
        List of collected data dictionaries
    """
    customer_ids = get_customers(google_repository, client_id)
    prms = []
    data_list = []
    
    for id in customer_ids:
        prms.append((date_range, id, google_repository, data_list))

    num_processes = min(len(customer_ids), max_processes)

    p = Pool(processes=num_processes)
    p.starmap(collect, prms)
    p.close()
    p.join()

    return data_list


# Main ETL functions
@op(required_resource_keys={'globals'})
def load_and_save_data_from_g_ads(context):
    """
    Load data from Google Ads and save to data warehouse.
    
    Args:
        context: Dagster context
        
    Returns:
        Collected data
    """
    start_date = context.resources.globals["reload_date_start"]
    end_date = context.resources.globals["reload_date_end"]
    destination_db = context.resources.globals["destination_db"]
    context.log.info("Script start")
    pyodbc.pooling = False
    
    # Validate destination_db value
    valid_destinations = ["dwh", "cloudberry", "both"]
    if destination_db not in valid_destinations:
        error_msg = f"Invalid destination_db value: '{destination_db}'. Must be one of {valid_destinations}"
        context.log.error(error_msg)
        raise Failure(error_msg)
        
    try:
        date_range = pd.date_range(
            start=start_date,
            end=end_date,
        )
        context.log.info(f"Date range: from {date_range[0].strftime('%Y-%m-%d')} to {date_range[-1].strftime('%Y-%m-%d')}")

        googleads_client = GoogleAdsClient.load_from_storage(googleAds_config, version=googleAds_config_version)
        postgres_engine = DatabaseConnectionManager().get_sqlalchemy_engine()
        cloudberry_engine = DatabaseConnectionManager().get_sqlalchemy_engine(
            credential_key="cloudberry", host="an-dwh.jooble.com", database="an_dwh", db_type="postgresql"
        )
        postgres_delete_conn = dwh_conn_psycopg2()
        cloudberry_delete_conn = cloudberry_conn_psycopg2()

        data_list = run_async(
            max_processes=multiprocessing.cpu_count() * 2,
            date_range=date_range,
            client_id=client_id,
            google_repository=googleads_client
        )

        if destination_db == "dwh":
            delete_from_dwh(data_list, postgres_delete_conn, start_date, end_date)
            save_data_to_dwh(data_list, postgres_engine, destination_db, start_date, end_date)
            context.log.info("Data loaded and saved to postgres")
        elif destination_db == "cloudberry":
            delete_from_dwh(data_list, cloudberry_delete_conn, start_date, end_date)
            save_data_to_dwh(data_list, cloudberry_engine, destination_db, start_date, end_date)
            context.log.info("Data loaded and saved to cloudberry")
        elif destination_db == "both":
            # run postgres
            delete_from_dwh(data_list, postgres_delete_conn, start_date, end_date)
            save_data_to_dwh(data_list, postgres_engine, destination_db, start_date, end_date)
            context.log.info("Data loaded and saved to postgres")

            # run cloudberry
            delete_from_dwh(data_list, cloudberry_delete_conn, start_date, end_date)
            save_data_to_dwh(data_list, cloudberry_engine, destination_db, start_date, end_date)
            context.log.info("Data loaded and saved to cloudberry")
        
    except Exception as ex:
        context.log.error(f"Critical exception caught: {ex}")
        raise Failure(f"Critical exception caught: {ex}")


@op(required_resource_keys={'globals'})
def compare_ads_and_dwh_data(context, data):
    """
    Compare data between Google Ads and data warehouse.
    
    Args:
        context: Dagster context
        data: Data from previous step
    """
    destination_db = context.resources.globals["destination_db"]
    context.log.info(f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    date_range = pd.date_range(
        start=context.resources.globals["reload_date_start"],
        end=context.resources.globals["reload_date_end"],
    )

    for date in date_range:
        date_string = date.strftime(DATE_FORMAT)
        DwhOperations.execute_on_dwh(
            context=context,
            query=PROCEDURE_CALL,
            ddl_query=GITLAB_DDL_Q,
            params=(date_string,),
            destination_db=destination_db
        )


@job(config=job_config,
     resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(
                        reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                        reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                        destination_db=Field(str, default_value="both"),
                    ),
                    },
     name=generate_job_name(TABLE_NAME) + "_yesterday",
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "destination_db": "dwh, cloudberry, both",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
     },
     description=f'{SCHEMA}.{TABLE_NAME}',
     )
def adwords_to_compare_yesterday_job():
    """
    Dagster job to load data from Google Ads and compare with data warehouse.
    """
    data = load_and_save_data_from_g_ads()
    compare_ads_and_dwh_data(data)
