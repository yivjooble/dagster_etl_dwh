import numpy as np
import pandas as pd
from dagster import op, job, fs_io_manager, make_values_resource, Field
from etl_api.salesforce.core.base_ops import BaseSalesforceOps
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub.db_operations import DwhOperations
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


FILE_NAME = "sf_auction_campaign"
SCHEMA = "salesforce"
OBJECT_NAME = "AuctionCampaign__c"

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=FILE_NAME,
)

# Field mapping for Auction Campaign
FIELD_MAPPING = {
    'key': 'DWH_key__c',
    'project_country_key': 'ProjectCountry__r.DWH_key__c',
    'id_campaign': 'DWH_Id__c',
    'name': 'Name',
    'cpc': 'CPC__c',
    'budget_value_currency': 'BudgetCurrency__c',
    'monthly_budget_enabled': 'MonthlyBudgetIsOn__c',
    'daily_budget_enabled': 'DailyBudgetIsOn__c',
    'daily_budget_value_currency': 'DailyBudgetCurrency__c',
    'daily_budget_time_start': 'DailyBudgetStartTime__c',
    'utm': 'UTM__c',
    'campaign_status': 'Status__c',
    'is_segmentation_on': 'IsSegmentationOn__c',
    'segmentation_type': 'SegmentationType__c',
    'force_jdp_percent': 'ForceJDPpercentage__c',
    'campaign_affiliate_allowed': 'AffiliateAllowed__c',
    # 'banned_affiliate_pubs': '',
    # 'banned_ppc_sources': '',
    'target_action': 'TargetAction__c',
    # 'target_metrics': 'TargetMetrics__c',
    'last_benchmark_cpa': 'LastCPA__c',
    'sensitive_to_deviations_max_value': 'SensitiveToDeviationsMaxValue__c',
    'last_date_benchmark_update': 'LastDateBenchmarkUpdate__c',
    'absence_of_target_action_metric_benchmark': 'TargetActionMetricBenchmarAbsence__c',
    'budget_value_usd': 'BudgetUSD__c',
    'daily_budget_value_usd': 'DailyBudgetUSD__c'
}


class AuctionCampaign(BaseSalesforceOps):
    def __init__(self, domain, source_db):
        super().__init__(domain=domain)
        self.field_mapping = FIELD_MAPPING
        self.source_db = source_db

    def update_auction_campaign(self, context):
        """
        Update contractor sizes and related fields in Salesforce Account object

        Args:
            context: Dagster execution context
        """
        try:
            context.log.info(f"Gitlab SQL query:\n{GITLAB_SQL_URL}")

            # Get last day of previous month
            # _, last_day_prev_month = get_previous_month_dates()
            # context.log.info(f"Using date for calculations: {last_day_prev_month}")

            # Execute SQL query with date parameter
            results = DwhOperations.execute_on_dwh(
                context=context,
                query=GITLAB_SQL_Q,
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
            context.log.error(f"Error updating auction campaign: {str(e)}")
            raise


@op(required_resource_keys={'globals'})
def update_auction_campaign(context):
    ops = AuctionCampaign(
        domain=context.resources.globals['domain'],
        source_db=context.resources.globals['source_db']
    )
    ops.update_auction_campaign(context)


@job(
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(
            domain=Field(str, default_value="login.salesforce.com"),
            source_db=Field(str, default_value="cloudberry")
        )
    },
    name=generate_job_name(FILE_NAME),
    tags={"data_model": SCHEMA},
    metadata={
        "date": f"Last day of previous month",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "source_db": "cloudberry",
        "destination": "salesforce",
        "target_sf_object": OBJECT_NAME,
    },
    description="""Updates Auction Campaign and related fields in Salesforce."""
)
def sf_auction_campaign_job():
    update_auction_campaign()
