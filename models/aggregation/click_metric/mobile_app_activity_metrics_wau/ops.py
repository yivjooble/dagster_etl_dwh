from datetime import datetime, timedelta
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
from utility_hub.core_tools import fetch_gitlab_data, get_datediff, check_time_condition


@dataclass(frozen=True)
class ActivityMetricsWauConfig:
    table_name: str = "activity_metrics"
    schema: str = "mobile_app"
    date_column: str = "date"
    country_column: str = "country_id"
    procedure_call: str = "call an.prc_mobile_app_activity_metrics_wau(%s, %s);"
    time_check: float = 11.30

    @property
    def proc_name_parsed(self) -> str:
        return self.procedure_call.split('(')[0].split('.')[1]

    @property
    def get_week_date_range(self) -> tuple[str, str]:
        current_date = datetime.now().date()

        # If it's Monday (weekday=0), we need data for the previous week
        if current_date.weekday() == 0:
            # End date is previous Sunday
            end_date = current_date - timedelta(days=1)
            # Start date is previous Monday
            start_date = end_date - timedelta(days=6)
        else:
            # Start date is Monday of current week
            start_date = current_date - timedelta(days=current_date.weekday())
            # End date is upcoming Sunday
            end_date = start_date + timedelta(days=6)

        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


config = ActivityMetricsWauConfig()
WEEK_DATE_START, WEEK_DATE_END = config.get_week_date_range
TIME_CONDITION = check_time_condition(time_to_check=config.time_check, comparison='>=')
MOBILE_APP_WAU_GITLAB_DDL_Q, MOBILE_APP_WAU_GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name=config.proc_name_parsed,
)
MOBILE_APP_WAU_GITLAB_SELECT_Q, MOBILE_APP_WAU_GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name=config.proc_name_parsed,
    file_name='create_rpl_df',
)


@op(out=DynamicOut(),
    required_resource_keys={'globals'},
    description="""Job to process and store weekly active user metrics for the mobile app.
                   Date range: 1 week: first day of current week (monday) - last day of current week (sunday).
                   Countries: all.""",
    )
def activity_metrics_wau_get_sql_instance(context, mobile_app_dau_result=None):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

    Args:
        context (_type_): logs
        mobile_app_dau_result(list): pass result from previous step.

    Yields:
        dict: dict with params to start query
    """
    if TIME_CONDITION:
        launch_countries = context.resources.globals["reload_countries"]

        context.log.info(f'Selected countries: {launch_countries}\n'
                         f'Start procedures for: {WEEK_DATE_START} - {WEEK_DATE_END}\n'
                         f"DDL run on replica:\n{MOBILE_APP_WAU_GITLAB_DDL_URL}")

        # iterate over sql instances
        for sql_instance in Operations.generate_sql_instance(
                context=context,
                instance_type="repstat",
                query=config.procedure_call,
                ddl_query=MOBILE_APP_WAU_GITLAB_DDL_Q,
                select_query=MOBILE_APP_WAU_GITLAB_SELECT_Q,):
            yield sql_instance
    else:
        context.log.info(f"Time check condition: {config.time_check}.")
        pass


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def activity_metrics_wau_query_on_db(context, sql_instance_country_query: dict):
    """
    Start procedure on rpl with input data.

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    DbOperations.create_procedure(context, sql_instance_country_query)

    country_id = sql_instance_country_query['country_id']

    operation_date_start = WEEK_DATE_START
    operation_date_end = WEEK_DATE_END
    operation_datediff_start = get_datediff(operation_date_start)
    operation_datediff_end = get_datediff(operation_date_end)

    context.log.info(f"--> Starting sql-script on: {operation_date_start} - {operation_date_end}")

    sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_datediff_start
    sql_instance_country_query['to_sqlcode_date_or_datediff_end'] = operation_datediff_end

    DbOperations.call_procedure(context, sql_instance_country_query)

    # Generator for retrieving chunks
    chunk_generator = DbOperations.execute_query_and_return_chunks(
        context=context,
        sql_instance_country_query=sql_instance_country_query
    )

    # Check for the presence of data
    first_chunk = next(chunk_generator, None)
    if first_chunk is None:
        return

    query = """DELETE
               FROM mobile_app.activity_metrics
               WHERE date BETWEEN %s AND %s
               AND country_id = %s
               AND metric_type = 'WAU';"""

    DwhOperations.execute_on_dwh(
        context=context,
        query=query,
        params=(operation_date_start, operation_date_end, country_id)
    )

    # Save the first chunk
    DwhOperations.save_to_dwh_copy_method(context, config.schema, config.table_name, df=first_chunk)

    # Save the remaining chunks
    for chunk in chunk_generator:
        DwhOperations.save_to_dwh_copy_method(context, config.schema, config.table_name, df=chunk)

    return True
