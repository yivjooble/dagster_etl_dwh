import numpy as np
import pandas as pd
from dagster import op, job, fs_io_manager, make_values_resource, Field
from etl_api.salesforce.core.base_ops import BaseSalesforceOps
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub.db_operations import DwhOperations
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_previous_month_dates


FILE_NAME = "sf_project_country"
SCHEMA = "salesforce"
OBJECT_NAME = "ProjectCountry__c"

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=FILE_NAME,
)

# Field mapping for Account Project Country
FIELD_MAPPING = {
    'key': 'DWH_key__c',
    'country': 'Country__r.DWH_Id__c',
    'project_user_name': 'Name',
    'project_key': 'Account__r.DWH_key__c',
    'gap_time': 'GapTime__c',
    'frequency_feed_update': 'FrequencyFeedUpdate__c',
    'active_feed_links': 'ActiveFeedLinks__c',
    'feeds_cnt': 'FeedsCount__c',
    'is_affiliate_jobs_reassembling': 'IsAffiliateJobsReassembling__c',
    'is_optimus_prime_allowed': 'IsOptimusPrimeAllowed__c',
    'is_google_for_jobs_allowed': 'IsGoogleForJobsAllowed__c',
    'project_status': 'ProjectStatus__c',
    'is_easy_apply': 'IsEasyApply__c',
    'is_aoj': 'IsAOJ__c',
    'affiliate_allowed': 'AffiliateAllowed__c',
    'banned_affiliate_pubs': 'BannedAffiliatePubs__c',
    'banned_ppc_sources': 'BannedPPCSources__c',
    'utm_from_feed': 'UTMFromFeed__c',
    'organic_utm': 'OrganicUTM__c',
    'exclusions': 'Exclusions__c',
    'id_user': 'UserId__c',
    'user_status': 'UserStatus__c',
    'email_login': 'LoginEmail__c',
    'last_login_date': 'LastLoginDate__c',
    'communication_email': 'CommunicationEmail__c',
    'currency': 'Currency__c',
    'client_buying_model': 'ClientBuyingModel__c',
    'no_local_traffic': 'LocalTrafficAllowed__c',
    'is_automatic_external_statistic_upload': 'IsAutomaticExternalStatisticUpload__c',
    'budget_currency': 'BudgetCurrency__c',
    'monthly_budget_enabled': 'MonthlyBudgetIsOn__c',
    'daily_budget_enabled': 'DailyBudgetIsOn__c',
    'daily_budget_is_set': 'DailyBudgetIsSet__c',
    'daily_budget_value_currency': 'DailyBudgetValueCurrency__c',
    'daily_time_start': 'DailyBudgetStartTime__c',
    'scheduled_cpc_budget_changes': 'ScheduledCPCBudgetChanges__c',
    'period_for_unique_click': 'PeriodForUniqueClick__c',
    'is_card_payment_allowed': 'IsCardPaymentAllowed__c',
    'discount': 'Discount__c',
    'project_discount': 'ProjectDiscount__c',
    'id_site': 'SiteId__c',
    'site_status': 'SiteStatus__c',
    'is_price_per_job': 'PricePerJob__c',
    'tracking_is_on': 'IsTrackingOn__c',
    'tracking_type': 'TrackingType__c',
    'is_multitracking': 'IsMultitracking__c',
    'custom_utm_away': 'CustomUTMAway__c',
    'force_jdp_on': 'ForceJDPIsOn__c',
    'def_force_jdp_percent': 'ForceJDPPercent__c',
    'traffic_mode': 'TrafficMode__c',
    'min_cpc_value_currency': 'MinCPCValueCurrency__c',
    'max_cpc_value_currency': 'MaxCPCValueCurrency__c',
    'target_action': 'TargetAction__c',
    'last_cpa': 'LastCPA__c',
    'last_date_benchmark_update': 'LastDateBenchmarkUpdate__c',
    'sensitive_to_deviations_max_value': 'SensitiveToDeviationsMaxValue__c',
    'daily_budget_value_usd': 'DailyBudgetValueUSD__c',
    'budget_usd': 'BudgetUSD__c',
    'min_cpc_value_usd': 'MinCPCValueUSD__c',
    'max_cpc_value_usd': 'MaxCPCValueUSD__c',
    'is_unlimited_budget': 'IsUnlimitedBudget__c'
}


class ProjectCountry(BaseSalesforceOps):
    def __init__(self, domain, source_db):
        super().__init__(domain=domain)
        self.field_mapping = FIELD_MAPPING
        self.source_db = source_db

    def update_project_country(self, context):
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
                        clean_record[key] = int(value) if isinstance(value, np.integer) else float(value)
                    else:
                        clean_record[key] = value
                records.append(clean_record)

            context.log.debug(f"Records OPS: {records[:25]}")

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
            context.log.error(f"Error updating project country: {str(e)}")
            raise


@op(required_resource_keys={'globals'})
def update_project_country(context):
    ops = ProjectCountry(
        domain=context.resources.globals['domain'],
        source_db=context.resources.globals['source_db']
    )
    ops.update_project_country(context)


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
        "triggered_by_job": "dwh__budget_revenue_daily_agg",
    },
    description="""Updates Account project country and related fields in Salesforce."""
)
def sf_project_country_job():
    update_project_country()
