import pandas as pd
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
from utility_hub.core_tools import fetch_gitlab_data, get_datediff


@dataclass(frozen=True)
class ClickMetricConfig:
    table_name: str = "click_metric_agg"
    schema: str = "an"
    procedure_call: str = "call an.prc_click_metric_agg(%s);"

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = ClickMetricConfig()

CLICK_METRIC_GITLAB_DDL_Q, CLICK_METRIC_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name=config.proc_name_parsed,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def prc_click_metric_agg_get_sqlinstance(context):
    """Compute dictionary for DynamicOutput with params to run query on target db using Dagster multitasking

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'{config.proc_name_parsed}\n'
                     f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                     f"DDL run on replica:\n{CLICK_METRIC_GITLAB_DDL_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=config.procedure_call,
            ddl_query=CLICK_METRIC_GITLAB_DDL_Q, ):
        yield sql_instance


@op(retry_policy=retry_policy)
def prc_click_metric_agg_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    try:
        for date in sql_instance_country_query['date_range']:
            # create local copy of dict
            local_sql_instance_country_query = deepcopy(sql_instance_country_query)

            reload_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
            context.log.info(f"--> Starting sql-script on: {date.strftime('%Y-%m-%d')}")

            local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = reload_date_diff

            DbOperations.call_procedure(context, local_sql_instance_country_query)

        return True
    except Exception as e:
        context.log.error(f"{e}")
        raise Exception
