from copy import deepcopy
from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field
)

# project import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    all_countries_list,
    job_config,
    retry_policy,
)
from utility_hub.core_tools import (
    fetch_gitlab_data,
    generate_job_name,
    get_datediff,
    get_first_day_of_month_as_int
)

TABLE_NAME = "account_contact"
SCHEMA = "imp"
COUNTRY_COLUMN = "country"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def account_contact_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.
    Args: sql_query.
    Output: sql_instance, db, query.
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Date range: [{context.resources.globals['reload_date_start']}"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL}"
    )

    for sql_instance in Operations.generate_sql_instance(context=context,
                                                         instance_type="prod",
                                                         query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={'globals'}, retry_policy=retry_policy)
def account_contact_query_on_db(context, sql_instance_country_query: dict):
    """
    Launch query on each instance.
    """
    try:
        country_code = sql_instance_country_query['country_code']

        date = sql_instance_country_query["date_range"]
        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)
        local_sql_instance_country_query['to_sql_country_id'] = sql_instance_country_query['country_id']
        local_sql_instance_country_query['to_sql_start_date'] = date

        context.log.info(f"--> Starting sql-script on: {date}")

        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query,
            country_column=COUNTRY_COLUMN,
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        # Save the first chunk
        DwhOperations.save_to_dwh_upsert(context=context,
                                         schema=SCHEMA,
                                         table_name=TABLE_NAME,
                                         df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_upsert(context=context,
                                             schema=SCHEMA,
                                             table_name=TABLE_NAME,
                                             df=chunk)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def account_contact_job():
    instances = account_contact_get_sqlinstance()
    instances.map(account_contact_query_on_db).collect()
