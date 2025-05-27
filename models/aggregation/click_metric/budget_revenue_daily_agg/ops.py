import time
import pandas as pd
from time import timezone
from dataclasses import dataclass

from dagster import (
    op,
    SkipReason,
)

# module import
from utility_hub import (
    DwhOperations,
)
from utility_hub.core_tools import submit_external_job_run, run_tableau_object_refresh, fetch_gitlab_data
from models.aggregation.utils import unified_check_data_discrepancy
from utility_hub.data_collections import tableau_object_uid

@dataclass(frozen=True)
class BudgetRevenueConfig:
    table_name: str = 'budget_revenue_daily_agg'
    schema: str = 'aggregation'
    destination_db: str = "both"
    procedure_call: str = 'call aggregation.insert_budget_revenue_daily_agg(%s);'

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = BudgetRevenueConfig()

BUDGET_REVENUE_GITLAB_DDL_Q, BUDGET_REVENUE_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=config.schema,
    file_name=config.table_name,
)


@op(required_resource_keys={'globals'})
def budget_revenue_daily_agg_query_on_db(context, click_data_result=None):
    """
    Args:
        context: job execution context;
        click_data_result(list): previous step result.
    """
    destination_db = context.resources.globals["destination_db"]
    reload_date_start = context.resources.globals["reload_date_start"]
    reload_date_end = context.resources.globals["reload_date_end"]

    context.log.info(f'{config.procedure_call}\n'
                     f"Date: {reload_date_start}\n"
                     f"DDL run on dwh:\n{BUDGET_REVENUE_GITLAB_DDL_URL}")

    date_range = pd.date_range(start=reload_date_start, end=reload_date_end)

    try:
        for date in date_range:
            formatted_date = date.strftime('%Y-%m-%d')
            DwhOperations.execute_on_dwh(
                context=context,
                query=config.procedure_call,
                ddl_query=BUDGET_REVENUE_GITLAB_DDL_Q,
                params=(formatted_date,),
                destination_db=destination_db
            )
        return True
    except Exception as e:
        context.log.error(f"{e}")
        raise Exception


@op(required_resource_keys={'globals'})
def budget_revenue_daily_agg_check_data_discrepancy(context, budget_result=None):
    """
    Execute data discrepancy review code on destination db.
    """
    return unified_check_data_discrepancy(
        context=context,
        procedure_name="budget_revenue_daily_agg"
    )


@op
def launch_sf_project_country(previous_step_result=None):
    # Skip if it's not the 10:00 utc runtime zone
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 540:
        return SkipReason("Waiting for 12:00 run")
    else:
        submit_external_job_run(job_name_with_prefix='api__sf_project_country',
                                repository_location_name='etl_api')


@op
def v_de_goals_2023_submit_tableau_refresh(context, previous_step_result=None):
    """
    Refresh Tableau datasource: aggregation.v_de_goal_2023.
    """
    # Skip if it's not the 10:00 utc runtime zone
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 540:
        return SkipReason("Waiting for 12:00 run")
    else:
        datasource_id = tableau_object_uid['datasource_by_name']['aggregation_v_de_goal_2023']
        run_tableau_object_refresh(context=context, datasource_id=datasource_id)
    

@op
def v_budget_and_revenue_submit_tableau_refresh(context, previous_step_result=None):
    """
    Refresh Tableau datasource: aggregation.v_budget_and_revenue.
    """
    # Skip if it's not the 12:00 utc runtime zone
    current_time = time.localtime(time.mktime(time.gmtime()) - timezone)
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if current_minutes < 540:
        return SkipReason("Waiting for 12:00 run")
    else:
        datasource_id = tableau_object_uid['datasource_by_name']['aggregation_v_budget_and_revenue']
        run_tableau_object_refresh(context=context, datasource_id=datasource_id)
    