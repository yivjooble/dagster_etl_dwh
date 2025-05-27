import os

from dagster import define_asset_job, ScheduleDefinition, DefaultScheduleStatus

# IMPORT ASSETS
from .assets import *

# DEFINE JOBS
from etl_api.apple_search_ads.ops import extract_apple_search_ads_data_job
from etl_api.linkedin.ops import linkedin_campaign_costs_job
from etl_api.currency_source.ops import currency_source_from_api_job
# from etl_api.google_trends_new.ops import google_trends_new_job
from etl_api.adwords.ops import adwords_to_compare_yesterday_job
# aggregation
from .tableau_metadata_collection.ops import (
    insert_tableau_metadata_job,
    get_tableau_extract_refresh_job,
)

from .pentaho_metadata_collection.ops import pentaho_metadata_collection_job
# from .crm_data_collection.ops import crm_data_collection_job
# from etl_api.crm_data_collection.ops import crm_cases_agg_job
# from etl_api.crm_data_collection.ops import crm_cases_stage_log_job
from .cobra_get_traffic_info.ops import cobra_info_projects_nonlocal_job
from .es_sender_score.ops import es_sender_score
from .outreach.outreach_ahrefs_checked.job import outreach_ahrefs_checked_job
from .outreach.outreach_period.job import outreach_period_job
from .outreach.outreach_link_current.job import outreach_link_current_job
from .outreach.outreach_link_agg.job import outreach_link_agg_job
from .outreach.outreach_link_agg.job import outreach_link_cost_job
from .outreach.outreach_funnel_agg.job import outreach_funnel_agg_job
from .outreach.outreach_account_status.job import outreach_account_status_job
from .outreach.outreach_domain_tags.job import outreach_domain_tags_job
from .outreach.outreach_link_agg_closed.job import outreach_link_agg_closed_job
from .job_kaiju_category.ops import job_kaiju_category_job
# from .google_trends.ops import google_trends_job
from .unsent_letters.ops import unsent_letters_job
from .google_sheets.fin_model_google_sheet.ops import fin_model_google_sheet_job
from .google_sheets.dic_ppc_comments.ops import dic_ppc_comments_job
from .postmaster.ops import postmaster_job
from .salesforce.account.ops import sf_account_job
from .salesforce.contractor.ops import sf_contractor_job
from .salesforce.project_country.ops import sf_project_country_job
from .salesforce.auction_campaign.ops import sf_auction_campaign_job
from .salesforce.auction_campaign_metrics.ops import sf_auction_campaign_metrics_job


outreach_kpi_current_job = define_asset_job(
    name="api__outreach_kpi_current",
    selection=outreach_kpi_current_assets,
    description="aggregation.outreach_kpi_current",
)
outreach_kpi_closed_job = define_asset_job(
    name="api__outreach_kpi_closed",
    selection=outreach_kpi_closed_assets,
    description="aggregation.outreach_kpi_closed",
)


# DEFINE SCHEDULES
def schedule_definition(
    job_name,
    cron_schedule,
    execution_timezone="Europe/Kiev",
    default_status=(
        DefaultScheduleStatus.RUNNING
        if os.environ.get("INSTANCE") == "PRD"
        else DefaultScheduleStatus.STOPPED
    ),
):
    return ScheduleDefinition(
        job=job_name,
        cron_schedule=cron_schedule,
        execution_timezone=execution_timezone,
        default_status=default_status,
    )


# DEFINE SCHEDULES
schedule_apple_search_ads = schedule_definition(
    job_name=extract_apple_search_ads_data_job,
    cron_schedule="0 09 * * *"
)
schedule_currency_source_from_api_job = schedule_definition(
    job_name=currency_source_from_api_job,
    cron_schedule="15 09 * * *"
)
schedule_adwords_to_compare_yesterday = schedule_definition(
    job_name=adwords_to_compare_yesterday_job,
    cron_schedule="0 09,15,18 * * *"
)
schedule_dic_ppc_comments = schedule_definition(
    job_name=dic_ppc_comments_job,
    cron_schedule="50 14,15,18 * * *"
)

# api
schedule_fin_model_google_sheet = schedule_definition(
    job_name=fin_model_google_sheet_job,
    cron_schedule="0 20 * * 4"
)
# schedule_google_trends_new = schedule_definition(
#     job_name=google_trends_new_job,
#     cron_schedule="0 9 * * *"
# )
# schedule_google_trends = schedule_definition(
#     job_name=google_trends_job,
#     cron_schedule="0 21 5,15,25 * *"
# )
schedule_get_tableau_extract_refresh = schedule_definition(
    job_name=get_tableau_extract_refresh_job,
    cron_schedule="*/10 * * * *"
)
schedule_insert_tableau_metadata = schedule_definition(
    job_name=insert_tableau_metadata_job,
    cron_schedule="0 */6 * * *"
)
schedule_pentaho_metadata_collection = schedule_definition(
    job_name=pentaho_metadata_collection_job,
    cron_schedule="0 */12 * * *"
)
# schedule_crm_data_collection = schedule_definition(
#     job_name=crm_data_collection_job,
#     cron_schedule="40 04 * * *"
# )
# schedule_crm_cases_agg = schedule_definition(
#     job_name=crm_cases_agg_job,
#     cron_schedule="0 09 * * *"
# )
# schedule_crm_cases_stage_log = schedule_definition(
#     job_name=crm_cases_stage_log_job,
#     cron_schedule="0 09 * * *"
# )
schedule_cobra_info_projects_nonlocal = schedule_definition(
    job_name=cobra_info_projects_nonlocal_job,
    cron_schedule="30 13 * * *"
)
schedule_es_sender_score = schedule_definition(
    job_name=es_sender_score,
    cron_schedule="50 08 * * *"
)
schedule_job_kaiju_category = schedule_definition(
    job_name=job_kaiju_category_job,
    cron_schedule="45 08 * * *"
)
schedule_unsent_letters = schedule_definition(
    job_name=unsent_letters_job,
    cron_schedule="45 06 * * *"
)
schedule_postmaster = schedule_definition(
    job_name=postmaster_job,
    cron_schedule="0 12,14,17 * * *"
)
schedule_sf_contractor = schedule_definition(
    job_name=sf_contractor_job,
    cron_schedule="20 09 * * *"
)
# schedule_sf_account = schedule_definition(
#     job_name=sf_account_job,
#     cron_schedule="0 10 * * *"
# )
# schedule_sf_project_country = schedule_definition(
#     job_name=sf_project_country_job,
#     cron_schedule="0 10 * * *"
# )

# outreach api
schedule_outreach_ahrefs_checked = schedule_definition(
    job_name=outreach_ahrefs_checked_job,
    cron_schedule="0 07 * * *"
)
schedule_outreach_period = schedule_definition(
    job_name=outreach_period_job,
    cron_schedule="0 10 * * *"
)
schedule_outreach_link_current = schedule_definition(
    job_name=outreach_link_current_job,
    cron_schedule="30 9 * * *"
)
schedule_outreach_link_agg = schedule_definition(
    job_name=outreach_link_agg_job,
    cron_schedule="0 10,14 * * *"
)
schedule_outreach_link_cost = schedule_definition(
    job_name=outreach_link_cost_job,
    cron_schedule="0 10,14 * * *"
)
schedule_outreach_funnel_agg = schedule_definition(
    job_name=outreach_funnel_agg_job,
    cron_schedule="0 6 * * *"
)
schedule_outreach_account_status = schedule_definition(
    job_name=outreach_account_status_job,
    cron_schedule="30 6 * * 1"
)
schedule_outreach_kpi_current = schedule_definition(
    job_name=outreach_kpi_current_job,
    cron_schedule="0 10,13 * * *"
)
schedule_outreach_kpi_closed = schedule_definition(
    job_name=outreach_kpi_closed_job,
    cron_schedule="0 17 * * *"
)
schedule_outreach_domain_tags = schedule_definition(
    job_name=outreach_domain_tags_job,
    cron_schedule="0 08 * * *"
)
schedule_outreach_link_agg_closed = schedule_definition(
    job_name=outreach_link_agg_closed_job,
    cron_schedule="0 18 1 * *"
)
