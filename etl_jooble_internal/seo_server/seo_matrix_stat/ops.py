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
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from etl_jooble_internal.utils.io_manager_path import get_io_manager_path


TABLE_NAME = "seo_matrix_stat"
SCHEMA = "traffic"
DELETE_DATE_COLUMN = "action_date"
YESTERDAY_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def seo_matrix_stat_get_sqlinstance(context):
    """
    Retrieves SQL instances and their corresponding database names for BigQuery.

    Args:
        context (Context): The context object for the operation.
    Yields:
        DynamicOutput: A dynamic output containing the SQL instance host, database name, and query.

    """
    date_start = context.resources.globals["reload_date_start"]
    date_end = context.resources.globals["reload_date_end"]

    context.log.info(f"Start date: {date_start}\nend date: {date_end}\n"
                     f"Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="seo_server",
            db_name="SeoQueries",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def seo_matrix_stat_query_on_db(context, sql_instance_country_query: dict):
    """
    Launches a query on the web BigQuery statistic database.

    Args:
        context (Context): The context object.
        sql_instance_country_query (dict): The SQL instance country query.
    """
    destination_db = context.resources.globals["destination_db"]
    date_start = context.resources.globals["reload_date_start"]
    date_end = context.resources.globals["reload_date_end"]
    date_range = pd.date_range(date_start, date_end)

    try:
        for date in date_range:
            operation_date = date.strftime('%Y-%m-%d')
            sql_instance_country_query['to_sql_date_start'] = date.strftime('%Y-%m-%d')

            # Generator for retrieving chunks
            chunk_generator = DbOperations.execute_query_and_return_chunks(
                context=context,
                sql_instance_country_query=sql_instance_country_query
            )

            # Check for the presence of data
            first_chunk = next(chunk_generator, None)
            if first_chunk is None:
                return

            DwhOperations.delete_data_from_dwh_table(context=context,
                                                     schema=SCHEMA,
                                                     table_name=TABLE_NAME,
                                                     date_column=DELETE_DATE_COLUMN,
                                                     date_start=operation_date,
                                                     date_end=operation_date,
                                                     destination_db=destination_db
                                                     )

            # Save the first chunk
            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=first_chunk, destination_db=destination_db)

            # Save the remaining chunks
            for chunk in chunk_generator:
                DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=chunk, destination_db=destination_db)

            context.log.info(f'Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                   destination_db=Field(str, default_value="both")
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def seo_matrix_stat_job():
    db_instances = seo_matrix_stat_get_sqlinstance()
    db_instances.map(seo_matrix_stat_query_on_db).collect()
