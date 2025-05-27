import os
import requests
from datetime import datetime
from typing import Dict, List
import pandas as pd
from dagster import (
    fs_io_manager, 
    job, 
    make_values_resource, 
    op, 
    get_dagster_logger, 
    Field
)
from etl_api.utils.io_manager_path import get_io_manager_path

# Import from utility_hub
from utility_hub import DwhOperations
from utility_hub.job_configs import job_config
from utility_hub.core_tools import generate_job_name, get_creds_from_vault

# Constants and configuration
SCHEMA_NAME = "imp_api"
TABLE_NAME = "apple_search_ads"

# Apple Search Ads API configuration
CLIENT_ID = get_creds_from_vault("APPLE_CLIENT_ID")
ORG_ID = "8495890"  # Organization ID for X-AP-Context header
CLIENT_SECRET = get_creds_from_vault("APPLE_CLIENT_SECRET")
TOKEN_URL = "https://appleid.apple.com/auth/oauth2/token"
API_BASE_URL = "https://api.searchads.apple.com/api/v5"
SCOPE = "searchadsorg"

# =================================================================
# APPLE SEARCH ADS REFERENCE
# =================================================================
# 
# ACCESS TOKEN GENERATION WORKFLOW:
# 1. Generate public/private key pair using script:
#    $ bash utils/generate_keys.sh
# 
# 2. Upload the generated public key to Apple Search Ads account:
#    https://app-ads.apple.com/cm/app/8495890/settings/apicertificates
# 
# 3. Generate JWT token using the private key:
#    $ python utils/generate_token.py
# 
# 4. Store JWT token in vault as APPLE_CLIENT_SECRET
#    (The ETL uses this JWT token to get an OAuth access token)
#
# NOTE: If authentication fails, verify that:
#  - The public key was correctly uploaded to Apple
#  - The CLIENT_ID and ORG_ID values are correct
#  - The key_id in generate_token.py matches Apple's dashboard
# 
# USEFUL LINKS:
# - Account Settings: https://app-ads.apple.com/cm/app/8495890/settings/apicertificates
# - API Documentation: https://developer.apple.com/documentation/apple_search_ads
# - Authentication Guide: https://developer.apple.com/documentation/apple_search_ads/implementing-oauth-for-the-apple-search-ads-api
# 
# =================================================================



def get_access_token() -> str:
    """Get an OAuth access token by exchanging the client_secret JWT token.
    
    Returns:
        str: The OAuth access token for Apple Search Ads API
    """
    logger = get_dagster_logger()
    
    if not CLIENT_SECRET:
        raise ValueError("Client secret is empty. Please run generate_token.py to create a valid JWT token.")
    
    # Prepare request data
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    
    headers = {
        "Host": "appleid.apple.com",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(
            TOKEN_URL,
            headers=headers,
            data=payload
        )
        
        response.raise_for_status()
        
        # Extract token from response
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            raise ValueError(f"No access token in response: {token_data}")
        
        return access_token
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to obtain access token: {str(e)}")
        if hasattr(e, "response") and e.response:
            logger.error(f"Response: {e.response.text}")
        raise


def get_campaigns(access_token: str) -> List[Dict]:
    """Get all campaigns from Apple Search Ads API using the limit parameter.
    
    Args:
        access_token: OAuth access token for API authentication
        
    Returns:
        List[Dict]: List of all campaign data
    """
    logger = get_dagster_logger()
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-AP-Context": f"orgId={ORG_ID}"
    }
    
    try:
        # Use limit=1000 to get all campaigns in a single request
        response = requests.get(
            f"{API_BASE_URL}/campaigns?limit=1000",
            headers=headers
        )
        
        response.raise_for_status()
        data = response.json()
        
        # Extract campaigns from response
        campaigns = data.get("data", [])
        total_results = data.get("pagination", {}).get("totalResults", 0)
        
        logger.info(f"Successfully retrieved {len(campaigns)} campaigns (total expected: {total_results})")
        return campaigns
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get campaigns: {str(e)}")
        if hasattr(e, "response") and e.response:
            logger.error(f"Response: {e.response.text}")
        raise


def campaigns_to_dataframe(campaigns: List[Dict]) -> pd.DataFrame:
    """Convert campaigns data to a pandas DataFrame.
    
    Args:
        campaigns: List of campaign data from API
        
    Returns:
        pd.DataFrame: DataFrame with campaign data
    """
    records = []
    
    for campaign in campaigns:
        # Extract daily budget amount and currency if available
        daily_budget_amount = None
        currency = None
        if campaign.get("dailyBudgetAmount"):
            daily_budget_amount = campaign["dailyBudgetAmount"].get("amount")
            currency = campaign["dailyBudgetAmount"].get("currency")
        
        # Extract countries/regions as comma-separated string
        countries = ", ".join(campaign.get("countriesOrRegions", []))
        
        record = {
            "campaign_id": campaign.get("id"),
            "campaign_name": campaign.get("name"),
            "org_id": campaign.get("orgId"),
            "status": campaign.get("status"),
            "serving_status": campaign.get("servingStatus"),
            "display_status": campaign.get("displayStatus"),
            "countries": countries,
            "daily_budget": daily_budget_amount,
            "currency": currency,
            "creation_date": campaign.get("creationTime"),
            "start_date": campaign.get("startTime"),
            "modified_date": campaign.get("modificationTime"),
            "deleted": campaign.get("deleted", False),
            "extracted_date": datetime.now().strftime("%Y-%m-%d")
        }
        records.append(record)
    
    return pd.DataFrame(records)


@op(
    required_resource_keys={"globals"},
    description="Extract, transform and load Apple Search Ads data to Cloudberry"
)
def extract_apple_search_ads_data(context):
    """Extract data from Apple Search Ads API, transform it, and load to Cloudberry.
    
    Args:
        context: Dagster context object containing resources and configuration
        
    Returns:
        None
    """
    logger = get_dagster_logger()
    destination_db = context.resources.globals["destination_db"]
    
    try:
        # Step 1: Get OAuth access token
        access_token = get_access_token()
        
        # Step 2: Fetch campaigns data
        campaigns = get_campaigns(access_token)
        
        if not campaigns:
            logger.warning("No campaigns data returned from API")
        
        # Step 3: Transform data to DataFrame
        df = campaigns_to_dataframe(campaigns)

        # Truncate table before loading
        DwhOperations.truncate_dwh_table(
            context=context,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            destination_db=destination_db,
        )
        
        # Step 4: Save data to DWH
        DwhOperations.save_to_dwh_pandas(
            context=context,
            df=df,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            destination_db=destination_db,
        )
        
        logger.info(f"Successfully saved {len(df)} records to {destination_db}")
        
    except Exception as e:
        logger.error(f"Error in Apple Search Ads ETL: {str(e)}")
        raise


@job(config=job_config,
     resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(destination_db=Field(str, default_value="cloudberry")),},
     name=generate_job_name(table_name=TABLE_NAME),
     tags={"data_model": f"{SCHEMA_NAME}"},
     metadata={
         "destination_db": "cloudberry",
     },
     description=f'{SCHEMA_NAME}.{TABLE_NAME}',
     )
def extract_apple_search_ads_data_job():
    """
    Dagster job to load data from Apple Search Ads and compare with data warehouse.
    """
    extract_apple_search_ads_data()
