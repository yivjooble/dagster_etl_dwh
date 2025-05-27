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
    DwhOperations,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, get_datediff
from models.aggregation.utils import unified_check_data_discrepancy


@dataclass(frozen=True)
class FinanceReportConfig:
    table_name: str = 'finance_report'
    schema: str = 'aggregation'
    procedure_call: str = 'call aggregation.insert_finance_report(1);'

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = FinanceReportConfig()

FINANCE_REPORT_GITLAB_DDL_Q, FINANCE_REPORT_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=config.schema,
    file_name=config.table_name,
)


@op(required_resource_keys={'globals'})
def insert_finance_report_query_on_db(context, click_data_result=None):
    """
    Args:
        context: job execution context;
        click_data_result(list): previous step result.
    """
    destination_db = context.resources.globals["destination_db"]
    DwhOperations.execute_on_dwh(
        context=context,
        query=config.procedure_call,
        ddl_query=FINANCE_REPORT_GITLAB_DDL_Q,
        destination_db=destination_db
    )


@op(required_resource_keys={'globals'})
def insert_finance_report_check_data_discrepancy(context, finance_result):
    """
    Execute data discrepancy review code on destination db.
    """
    return unified_check_data_discrepancy(
        context=context,
        procedure_name="finance_report"
    )
