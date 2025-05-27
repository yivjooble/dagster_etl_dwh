from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dagster import op, job, fs_io_manager, make_values_resource, Field
from etl_api.salesforce.core.base_ops import BaseSalesforceOps
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub.db_operations import DwhOperations
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff


FILE_NAME = "sf_auction_campaign_metrics"
SCHEMA = "salesforce"
OBJECT_NAME = "AuctionCampaign__c"

def get_revenue_date_range():
    today = datetime.now().date()

    # If it's the first day of the month, we need data for the previous month
    if today.day == 1:
        # Last day of previous month
        end_date = today - timedelta(days=1)
        # First day of previous month
        start_date = end_date.replace(day=1)
    else:
        # First day of current month
        start_date = today.replace(day=1)
        # Yesterday
        end_date = today - timedelta(days=1)

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

RELOAD_DATE_START, RELOAD_DATE_END = get_revenue_date_range()

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=FILE_NAME,
)

# Field mapping for Revenue
FIELD_MAPPING = {
    'key': 'DWH_key__c',
    'last_revenue_date': 'LastRevenueDate__c',
    'revenue_usd': 'RevenueUSD__c',
    'revenue_currency': 'RevenueCurrency__c',
    'clicks': 'Clicks__c',
    'daily_cpa': 'DailyCPA__c',
    'daily_cr': 'DailyCR__c',
    'paid_job_count': 'PaidJobCount__c',
    'organic_job_count': 'OrganicJobCount__c',
    'min_cpc_job_count': 'MinCPCJobCount__c',
    'max_cpc_job_count': 'MaxCPCJobCount__c'
}


class AuctionCampaignMetrics(BaseSalesforceOps):
    def __init__(self, domain, source_db):
        super().__init__(domain=domain)
        self.field_mapping = FIELD_MAPPING
        self.source_db = source_db

    def update_auction_campaign_metrics(self, context, datediff_start, datediff_end, reload_date_end):
        """
        Update AuctionCampaign__c metrics and related fields in Salesforce AuctionCampaign__c object

        Args:
            context: Dagster execution context
        """
        try:
            context.log.info(f"Gitlab SQL query:\n{GITLAB_SQL_URL}")

            # Execute SQL query with date parameter
            results = DwhOperations.execute_on_dwh(
                context=context,
                query=GITLAB_SQL_Q,
                params=(reload_date_end, datediff_end, datediff_start, datediff_end, reload_date_end),
                fetch_results=True,
                destination_db=self.source_db
            )

            if not results:
                context.log.warning("No data retrieved from DWH")
                return

            # Create DataFrame for validation and field mapping
            df = pd.DataFrame.from_records(results)

            # Rename columns according to Salesforce field mapping
            df = df.rename(columns=self.field_mapping)

            # Clean numeric columns - replace inf values with None and convert NaN to None
            numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_columns:
                df[col] = df[col].replace([np.inf, -np.inf, np.nan], None)

            records = []
            for record in df.to_dict(orient='records'):
                clean_record = {}
                for key, value in record.items():
                    if isinstance(value, (np.floating, np.integer)):
                        clean_record[key] = float(value)
                    else:
                        clean_record[key] = value
                records.append(clean_record)

            # Upload to Salesforce
            job_result = self.upload_data_csv(
                records=records,
                object_name=OBJECT_NAME,
                operation='upsert',
                external_id_field=self.field_mapping['key'],
                context=context
            )

            job_name = context.dagster_run.job_name
            # Check for errors
            self.check_upload_errors(
                job_results=job_result,
                object_name=OBJECT_NAME,
                job_name=job_name,
                context=context
            )

        except Exception as e:
            context.log.error(f"Error updating auction campaign metrics: {str(e)}")
            raise


@op(required_resource_keys={"globals"})
def update_auction_campaign_metrics(context):
    reload_date_start = context.resources.globals["reload_date_start"]
    reload_date_end = context.resources.globals["reload_date_end"]
    context.log.info(f"Reload date range: {reload_date_start} - {reload_date_end}")

    datediff_start = get_datediff(reload_date_start)
    datediff_end = get_datediff(reload_date_end)

    ops = AuctionCampaignMetrics(
        domain=context.resources.globals['domain'],
        source_db=context.resources.globals['source_db']
    )
    ops.update_auction_campaign_metrics(context, datediff_start, datediff_end, reload_date_end)


@job(
    resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=RELOAD_DATE_START),
                                                   reload_date_end=Field(str, default_value=RELOAD_DATE_END),
                                                   domain=Field(str, default_value="login.salesforce.com"),
                                                   source_db=Field(str, default_value="cloudberry")),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(FILE_NAME),
    tags={"data_model": SCHEMA},
    metadata={
        "date": f"{RELOAD_DATE_START} - {RELOAD_DATE_END}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "source_db": "cloudberry",
        "destination": "salesforce",
        "target_sf_object": OBJECT_NAME,
    },
    description="""Updates AuctionCampaign__c metrics and related fields in Salesforce.
                   Revenue and clicks are accumulated values for the current month."""
)
def sf_auction_campaign_metrics_job():
    update_auction_campaign_metrics()
