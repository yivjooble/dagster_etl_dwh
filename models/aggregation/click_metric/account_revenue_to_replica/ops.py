from copy import deepcopy
from dataclasses import dataclass

from dagster import (
    op,
    DynamicOut,
)

# module import
from utility_hub import (
    Operations,
    DbOperations,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, get_datediff, check_time_condition


@dataclass(frozen=True)
class RplAccountRevenueConfig:
    table_name: str = "account_revenue"
    schema: str = "an"
    procedure_call: str = "call an.prc_imp_account_revenue(%s);"
    time_check: float = 11.30

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = RplAccountRevenueConfig()
TIME_CONDITION = check_time_condition(time_to_check=config.time_check, comparison='>=')
RPL_ACCOUNT_REVENUE_GITLAB_DDL_Q, RPL_ACCOUNT_REVENUE_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name=config.proc_name_parsed,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def rpl_account_revenue_get_sqlinstance(context, click_metric_result=None):
    """Compute dictionary for DynamicOutput with params to run query on target db using Dagster multitasking

    Args:
        context (_type_): logs
        click_metric_result(list): pass result from previous step.

    Yields:
        dict: dict with params to start query
    """
    if TIME_CONDITION:
        launch_countries = context.resources.globals["reload_countries"]

        context.log.info(f'Selected countries: {launch_countries}\n'
                         f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                         f"DDL run on replica:\n{RPL_ACCOUNT_REVENUE_GITLAB_DDL_URL}")

        # iterate over sql instances
        for sql_instance in Operations.generate_sql_instance(
                context=context,
                instance_type="repstat",
                query=config.procedure_call,
                ddl_query=RPL_ACCOUNT_REVENUE_GITLAB_DDL_Q,):
            yield sql_instance
    else:
        context.log.info(f"Time check condition: {config.time_check}.")
        return


@op(retry_policy=retry_policy)
def rpl_account_revenue_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    for date in sql_instance_country_query['date_range']:
        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)

        operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
        context.log.info(f"--> Starting sql-script on: {date.strftime('%Y-%m-%d')}")

        local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff

        DbOperations.call_procedure(context, local_sql_instance_country_query)

    return True
