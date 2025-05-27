from copy import deepcopy
from dataclasses import dataclass

from dagster import (
    op,
    DynamicOut,
)

# module import
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    retry_policy,
)
from utility_hub.core_tools import (
    fetch_gitlab_data,
    get_datediff,
    run_tableau_object_refresh,
    check_time_condition
)

from utility_hub.data_collections import tableau_object_uid


@dataclass(frozen=True)
class MobileAppConfig:
    table_name: str = "business_metrics"
    schema: str = "mobile_app"
    date_diff_column: str = "load_datediff"
    country_column: str = "country_id"
    procedure_call: str = "call an.prc_mobile_app_business_metrics(%s);"
    time_check: float = 11.30

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]


config = MobileAppConfig()
TIME_CONDITION = check_time_condition(time_to_check=config.time_check, comparison='>=')
MOBILE_APP_GITLAB_DDL_Q, MOBILE_APP_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name=config.proc_name_parsed,
)
MOBILE_APP_GITLAB_SELECT_Q, MOBILE_APP_GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name='create_rpl_df',
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def mobile_app_business_metrics_get_sql_instance(context, click_metric_result=None):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

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
                         f"DDL run on replica:\n{MOBILE_APP_GITLAB_DDL_URL}")

        # iterate over sql instances
        for sql_instance in Operations.generate_sql_instance(
                context=context,
                instance_type="repstat",
                query=config.procedure_call,
                ddl_query=MOBILE_APP_GITLAB_DDL_Q,
                select_query=MOBILE_APP_GITLAB_SELECT_Q,):
            yield sql_instance
    else:
        context.log.info(f"Time check condition: {config.time_check}.")
        pass


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def mobile_app_business_metrics_query_on_db(context, sql_instance_country_query: dict):
    """
    Start procedure on rpl with input data.

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    destination_db = context.resources.globals["destination_db"]
    DbOperations.create_procedure(context, sql_instance_country_query)

    country_id = sql_instance_country_query['country_id']

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
            sql_instance_country_query=local_sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            continue

        DwhOperations.delete_data_from_dwh_table(context=context,
                                                 schema=config.schema,
                                                 table_name=config.table_name,
                                                 country_column=config.country_column,
                                                 date_column=config.date_diff_column,
                                                 date_start=operation_date_diff,
                                                 country=country_id,
                                                 destination_db=destination_db)

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context,
                                              config.schema,
                                              config.table_name,
                                              df=first_chunk,
                                              destination_db=destination_db)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context,
                                                  config.schema,
                                                  config.table_name,
                                                  df=chunk,
                                                  destination_db=destination_db)


@op
def mobile_app_business_metrics_refresh_tableau_object(context, results=None):
    """
    Refresh tableau object
    """
    if TIME_CONDITION:
        datasource_id = tableau_object_uid['datasource_by_name']['mobile_app_v_aggregated_mobile_metrics']
        run_tableau_object_refresh(context=context, datasource_id=datasource_id)
    else:
        context.log.info(f"Time check condition: {config.time_check}.")
        pass
