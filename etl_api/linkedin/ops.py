import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

import pandas as pd
import requests
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    Array,
    String
)

# module import
from service.utils.io_manager_path import get_io_manager_path
from utility_hub import DwhOperations
from utility_hub.core_tools import generate_job_name, get_creds_from_vault

# Constants
TABLE_NAME = "linkedin_paid_cost"
SCHEMA_NAME = "imp_api"
LINKEDIN_CLIENT_ID = get_creds_from_vault("LINKEDIN_CLIENT_ID")
LINKEDIN_CLIENT_SECRET = get_creds_from_vault("LINKEDIN_CLIENT_SECRET")

# LinkedIn API configuration
LINKEDIN_API_BASE_URL = "https://api.linkedin.com/rest/adAnalytics"
LINKEDIN_API_VERSION = "202505"

# LinkedIn OAuth tokens
LINKEDIN_REFRESH_TOKEN = get_creds_from_vault("LINKEDIN_REFRESH_TOKEN")

# Default date range for LinkedIn campaign analytics
DEFAULT_END_DATE = datetime.now().strftime("%Y-%m-%d")
DEFAULT_START_DATE = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")


class LinkedInAPI:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.token_expiry = None
        self.headers = {
            "LinkedIn-Version": LINKEDIN_API_VERSION,
            'X-Restli-Protocol-Version': '2.0.0',
        }

    def refresh_access_token(self) -> str:
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        token_data = response.json()

        self.access_token = token_data["access_token"]
        # LinkedIn tokens typically expire in 60 days (5184000 seconds)
        expires_in = token_data.get("expires_in", 5184000)
        self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        
        # Store the new refresh token if provided
        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]
            
        return self.access_token

    def get_access_token(self) -> str:
        if not self.access_token or not self.token_expiry or datetime.now() >= self.token_expiry:
            return self.refresh_access_token()
        return self.access_token

    def get_campaign_analytics(self, accounts: List[str], start_date: str, end_date: str, logger) -> List[Dict[str, Any]]:
        """
        Fetch campaign analytics data from LinkedIn API.

        Args:
            accounts: List of LinkedIn account IDs
            start_date: Start date in format YYYY-MM-DD
            end_date: End date in format YYYY-MM-DD
            logger: Logger instance for logging messages

        Returns:
            List of campaign analytics data
        """
        results = []
        
        # Parse dates and remove leading zeros
        start_year, start_month, start_day = [x.lstrip('0') for x in start_date.split('-')]
        end_year, end_month, end_day = [x.lstrip('0') for x in end_date.split('-')]
        
        # Format account URNs
        account_urns = [f"urn%3Ali%3AsponsoredAccount%3A{aid}" for aid in accounts]
        accounts_param = f"List({','.join(account_urns)})"
        
        # Build the base query parameters with properly formatted date range
        date_range = f'(start:(year:{start_year},month:{start_month},day:{start_day}),end:(year:{end_year},month:{end_month},day:{end_day}))'
        params = {
            'q': 'analytics',
            'pivot': 'CAMPAIGN',
            'dateRange': date_range,
            'timeGranularity': 'DAILY',
            'accounts': accounts_param
        }
        
        # Define fields separately to prevent URL-encoding of commas
        fields = 'costPerQualifiedLead,costInUsd,clicks,dateRange,impressions,landingPageClicks,likes,shares,costInLocalCurrency,approximateUniqueImpressions,pivotValues'
        
        try:
            # Ensure we have a valid access token and set the Authorization header
            access_token = self.get_access_token()
            self.headers["Authorization"] = f"Bearer {access_token}"
            
            # Build the URL with all parameters to ensure proper encoding
            query_params = {
                'q': 'analytics',
                'pivot': 'CAMPAIGN',
                'dateRange': f'(start:(year:{start_year},month:{start_month},day:{start_day}),end:(year:{end_year},month:{end_month},day:{end_day}))',
                'timeGranularity': 'DAILY',
                'accounts': accounts_param,
                'fields': fields
            }
            
            # Build URL manually to control the encoding
            query_string = '&'.join([f"{k}={v}" if k != 'dateRange' else f"dateRange={v}" 
                                  for k, v in query_params.items()])
            request_url = f"{LINKEDIN_API_BASE_URL}?{query_string}"
            
            # Use the full URL directly without params to prevent double-encoding
            response = requests.get(request_url, headers=self.headers)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            analytics_data = response.json()
            elements = analytics_data.get('elements', [])
            element_count = len(elements)
            logger.info(f"Found {element_count} records for {len(accounts)} accounts")
            
            return elements
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching analytics: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise


@op(required_resource_keys={'globals'})
def linkedin_campaign_costs(context):
    """
    Fetch campaign cost data from LinkedIn API and save to data warehouse.
    """
    # Get configuration from resources
    accounts = context.resources.globals["linkedin_accounts"]
    destination_db = context.resources.globals["destination_db"]
    start_date = context.resources.globals["start_date"]
    end_date = context.resources.globals["end_date"]

    context.log.info(f"Fetching LinkedIn campaign costs for {len(accounts)} accounts from {start_date} to {end_date}")

    # Initialize LinkedIn API client with stored refresh token
    linkedin_api = LinkedInAPI(
        client_id=str(LINKEDIN_CLIENT_ID),
        client_secret=str(LINKEDIN_CLIENT_SECRET),
        refresh_token=str(LINKEDIN_REFRESH_TOKEN)
    )

    # Fetch campaign analytics data
    context.log.info("Fetching campaign analytics from LinkedIn API")
    campaign_data = linkedin_api.get_campaign_analytics(
        accounts=accounts,
        start_date=start_date,
        end_date=end_date,
        logger=context.log
    )

    if not campaign_data:
        context.log.warning("No campaign data returned from LinkedIn API")
        return

    # Convert directly to DataFrame
    df = pd.DataFrame(campaign_data)
    
    # Rename and transform columns as needed
    if not df.empty:
        # Extract campaign_id from pivotValues
        df['campaign_id'] = df['pivotValues'].str[0].str.split(':').str[-1]
        
        # Convert date range to date string
        df['date'] = df['dateRange'].apply(
            lambda x: f"{x['start']['year']}-{str(x['start']['month']).zfill(2)}-{str(x['start']['day']).zfill(2)}"
        )
        
        # Rename columns to match expected schema
        df = df.rename(columns={
            'landingPageClicks': 'landing_page_clicks',
            'costInLocalCurrency': 'cost_in_local_currency',
            'approximateUniqueImpressions': 'approximate_unique_impressions',
            'costPerQualifiedLead': 'cost_per_qualified_lead',
            'costInUsd': 'cost_in_usd',
        })
        
        # Convert cost columns to float if they exist
        numeric_columns = ['cost_in_local_currency', 'cost_in_usd', 'cost_per_qualified_lead', 
                         'impressions', 'clicks', 'landing_page_clicks', 'likes', 'shares',
                         'approximate_unique_impressions']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Select and order columns
        columns = [
            'campaign_id', 'date', 'impressions', 'clicks', 'landing_page_clicks',
            'likes', 'shares', 'cost_in_local_currency', 'approximate_unique_impressions',
            'cost_per_qualified_lead', 'cost_in_usd'
        ]
        df = df[[col for col in columns if col in df.columns]]
    
    context.log.info(f"Transformed DataFrame shape: {df.shape}")

    if df.empty:
        context.log.warning("No data to save after transformation")
        return

    # Truncate table before saving
    DwhOperations.truncate_dwh_table(
        context=context,
        schema=SCHEMA_NAME,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )
    context.log.info(f"Truncated table {SCHEMA_NAME}.{TABLE_NAME}")

    # Save data to DWH using COPY method
    DwhOperations.save_to_dwh_pandas(
        context=context,
        df=df,
        schema=SCHEMA_NAME,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )

    context.log.info(f"Successfully saved LinkedIn data to: {SCHEMA_NAME}.{TABLE_NAME}")


@job(
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(
            linkedin_accounts=Field(Array(int), default_value=[513084275, 507098127, 507099110],
                                        description="List of LinkedIn account IDs to fetch"),
            destination_db=Field(String, default_value="cloudberry",
                                 description="Destination database (dwh, cloudberry, or both)"),
            start_date=Field(String, default_value=DEFAULT_START_DATE,
                             description="Start date in format YYYY-MM-DD"),
            end_date=Field(String, default_value=DEFAULT_END_DATE,
                           description="End date in format YYYY-MM-DD")
        )
    },
    name=generate_job_name('linkedin_campaign_costs'),
    description='Fetch campaign cost data from LinkedIn API and save to data warehouse',
)
def linkedin_campaign_costs_job():
    linkedin_campaign_costs()
