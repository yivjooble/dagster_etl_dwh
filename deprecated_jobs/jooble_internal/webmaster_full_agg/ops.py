import pandas as pd
from datetime import datetime, timedelta

from dagster import (
    DynamicOut,
    Field,
    fs_io_manager,
    job,
    make_values_resource,
    op,
)

from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    job_config,
    all_countries_list,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from etl_jooble_internal.utils.io_manager_path import get_io_manager_path


TABLE_NAME = "webmaster_full_agg"
SCHEMA = "traffic"
COUNTRY_COLUMN = "country_code"
DATE_COLUMN = "date"
# 4 days gap on source data
DATE_START = (datetime.now() - timedelta(days=8)).date().isoformat()
DATE_END = (datetime.now() - timedelta(days=1)).date().isoformat()
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(required_resource_keys={"globals"}, out=DynamicOut())
def webmaster_full_agg_get_sqlinstance(context):
    """
    Retrieves SQL instances and their corresponding database names for BigQuery.

    Args:
        context (Context): The context object for the operation.
        query (str): The SQL query to be executed.

    Yields:
        DynamicOutput: A dynamic output containing the SQL instance host, database name, and query.

    """
    launch_countries = context.resources.globals["reload_countries"]
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]),
                               pd.to_datetime(context.resources.globals["reload_date_end"]))

    context.log.info(f'I> Selected countries: {launch_countries}\n'
                     f'I> Date range: [{date_range}]\n'
                     f"I> Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="seo_server",
            db_name="BigQuery",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def webmaster_full_agg_query_on_db(context, sql_instance_country_query: dict):
    """
    Launches a query on the web BigQuery statistic database.

    Args:
        context (Context): The context object.
        sql_instance_country_query (dict): The SQL instance country query.
    """
    country_code = sql_instance_country_query["country_code"]

    to_sqlcode_tbl_stat = f"{country_code}_stat"
    to_sqlcode_reload_date_start = context.resources.globals["reload_date_start"]
    to_sqlcode_reload_date_end = context.resources.globals["reload_date_end"]

    formatted_query = sql_instance_country_query["query"].format(
        to_sqlcode_country_code=country_code,
        to_sqlcode_tbl_stat=to_sqlcode_tbl_stat,
        to_sqlcode_reload_date_start=to_sqlcode_reload_date_start,
        to_sqlcode_reload_date_end=to_sqlcode_reload_date_end,
    )

    sql_instance_country_query.update({"formatted_query": formatted_query})
    sql_instance_country_query.update({"country_code": country_code})

    try:
        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return

        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_COLUMN,
            country_column=COUNTRY_COLUMN,
            date_start=context.resources.globals["reload_date_start"],
            date_end=context.resources.globals["reload_date_end"],
            country=country_code
        )

        # Save the first chunk
        DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk)

        # Save the remaining chunks
        for chunk in chunk_generator:
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk)

        context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={
        "globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                        reload_date_start=Field(str, default_value=DATE_START),
                                        reload_date_end=Field(str, default_value=DATE_END),),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{DATE_START} - {DATE_END}",
        "gitlab_ddl_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def webmaster_full_agg_job():
    instances = webmaster_full_agg_get_sqlinstance()
    instances.map(webmaster_full_agg_query_on_db)
