import pandas as pd
from dataclasses import dataclass

from dagster import (
    op,
)

# module import
from utility_hub import (
    DwhOperations,
)
from utility_hub.core_tools import fetch_gitlab_data, get_datediff


@dataclass(frozen=True)
class SessionAbTestMainMetricsConfig:
    table_name: str = 'session_abtest_main_metrics_agg'
    schema: str = 'aggregation'
    procedure_call: str = 'call aggregation.insert_session_abtest_main_metrics_agg(%s, %s);'

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = SessionAbTestMainMetricsConfig()

SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_Q, SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=config.schema,
    file_name=config.table_name,
)


@op(required_resource_keys={'globals'})
def session_abtest_main_metrics_agg_query_on_db(context, session_abtest_result=None):
    context.log.info(f"Operation date range:\n"
                     f"{context.resources.globals['reload_date_start']} - {context.resources.globals['reload_date_end']}\n"
                     f"DDL run on dwh:\n{SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_URL}")
    destination_db = context.resources.globals["destination_db"]

    date_range = pd.date_range(
        pd.to_datetime(context.resources.globals["reload_date_start"]),
        pd.to_datetime(context.resources.globals["reload_date_end"])
    )

    for date in date_range:
        operation_date = date.strftime('%Y-%m-%d')
        operation_date_diff = get_datediff(operation_date)

        DwhOperations.execute_on_dwh(
            context=context,
            query=config.procedure_call,
            ddl_query=SESSION_ABTEST_MAIN_METRICS_GITLAB_DDL_Q,
            params=(operation_date, operation_date_diff),
            destination_db=destination_db
        )
