from dagster import Definitions

# IMPORT SENSORS
from .sensors import *

# IMPORT SCHEDULES, ASSETS, JOBS
from .jobs_schedules import *


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_apple_search_ads,
        schedule_currency_source_from_api_job,
        # schedule_google_trends_new,
        schedule_adwords_to_compare_yesterday,
        schedule_dic_ppc_comments,
        schedule_outreach_period,
        schedule_outreach_link_current,
        schedule_outreach_link_agg,
        schedule_outreach_funnel_agg,
        schedule_outreach_account_status,
        schedule_outreach_kpi_current,
        schedule_outreach_kpi_closed,
        schedule_outreach_ahrefs_checked,
        schedule_outreach_domain_tags,
        schedule_outreach_link_agg_closed,
        schedule_outreach_link_cost,
        schedule_cobra_info_projects_nonlocal,
        # schedule_crm_data_collection,
        # schedule_crm_cases_agg,
        # schedule_crm_cases_stage_log,
        schedule_es_sender_score,
        schedule_pentaho_metadata_collection,
        schedule_insert_tableau_metadata,
        schedule_get_tableau_extract_refresh,
        schedule_job_kaiju_category,
        # schedule_google_trends,
        schedule_unsent_letters,
        schedule_fin_model_google_sheet,
        schedule_postmaster,
        schedule_sf_contractor,
    ],
    assets=[
        *outreach_kpi_current_assets,
        *outreach_kpi_closed_assets,
    ],
    jobs=[
        extract_apple_search_ads_data_job,
        linkedin_campaign_costs_job,
        currency_source_from_api_job,
        # google_trends_new_job,
        adwords_to_compare_yesterday_job,
        dic_ppc_comments_job,
        outreach_period_job,
        outreach_link_current_job,
        outreach_link_agg_job,
        outreach_funnel_agg_job,
        outreach_account_status_job,
        outreach_kpi_current_job,
        outreach_kpi_closed_job,
        outreach_ahrefs_checked_job,
        outreach_link_cost_job,
        outreach_domain_tags_job,
        outreach_link_agg_closed_job,
        cobra_info_projects_nonlocal_job,
        # crm_data_collection_job,
        # crm_cases_agg_job,
        # crm_cases_stage_log_job,
        es_sender_score,
        pentaho_metadata_collection_job,
        insert_tableau_metadata_job,
        get_tableau_extract_refresh_job,
        job_kaiju_category_job,
        # google_trends_job,
        unsent_letters_job,
        fin_model_google_sheet_job,
        postmaster_job,
        sf_contractor_job,
        sf_account_job,
        sf_project_country_job,
        sf_auction_campaign_job,
        sf_auction_campaign_metrics_job
    ],
    sensors=[
        monitor_all_jobs_sensor,
        sf_account_job_sensor,
        sf_auction_campaign_sensor,
        sf_auction_campaign_metrics_job_sensor
    ],
)
