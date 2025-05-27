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


@dataclass(frozen=True)
class AccountRevenueAbTestAgg:
    table_name: str = "account_revenue_abtest_agg"
    schema: str = "aggregation"
    date_column = "date_diff"
    country_column = "country_id"
    procedure_call: str = "call an.prc_account_revenue_abtest_agg(%s);"
    time_check: float = 11.30

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = AccountRevenueAbTestAgg()
ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_Q, ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name=config.proc_name_parsed,
)
ACCOUNT_REVENUE_ABTEST_GITLAB_SELECT_Q, ACCOUNT_REVENUE_ABTEST_GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name='create_rpl_df',
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def account_revenue_abtest_agg_get_sql_instance(context, rpl_account_revenue_result=None):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

    Args:
        context (_type_): logs
        rpl_account_revenue_result(list): previous step result of rpl_account_revenue.

    Yields:
        dict: dict with params to start query
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                     f"DDL run on replica:\n{ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=config.procedure_call,
            ddl_query=ACCOUNT_REVENUE_ABTEST_GITLAB_DDL_Q,
            select_query=ACCOUNT_REVENUE_ABTEST_GITLAB_SELECT_Q, ):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def account_revenue_abtest_agg_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    destination_db = context.resources.globals["destination_db"]
    country_id = sql_instance_country_query['country_id']

    DbOperations.create_procedure(context, sql_instance_country_query)

    for date in sql_instance_country_query['date_range']:
        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)

        operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
        context.log.info(f"--> Starting sql-script on: {date.strftime('%Y-%m-%d')}")

        local_sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff

        DbOperations.call_procedure(context, local_sql_instance_country_query)

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query,
            country_column=config.country_column,
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            continue

        DwhOperations.delete_data_from_dwh_table(context=context,
                                                 schema=config.schema,
                                                 table_name=config.table_name,
                                                 country_column=config.country_column,
                                                 date_column=config.date_column,
                                                 date_start=operation_date_diff,
                                                 country=country_id,
                                                 destination_db=destination_db)

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, config.schema, config.table_name, df=first_chunk,
                                              destination_db=destination_db)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, config.schema, config.table_name, df=chunk,
                                                  destination_db=destination_db)
