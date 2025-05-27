from datetime import timedelta, datetime
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput
)
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# module import
from service.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name, fetch_gitlab_data
from utility_hub import TrinoOperations, trino_rpl_catalog, retry_policy, map_country_code_to_id
from utility_hub.db_operations import DwhOperations

SCHEMA_NAME = "yza"
TABLE_NAME = "seo_gfj_stat"

SQL_QUERIES = ['calendar_union', 'month_union', 'lifetime_union']


def get_sql_query(query: str, trino_db_name: str, country_id: int):
    today = datetime.today().date()

    SQL, URL = fetch_gitlab_data(
        config_key="default",
        dir_name="dwh_test",
        file_name=f"{query}",
    )

    modified_sql = SQL.replace('rpl_db', f"rpl_{trino_db_name}").replace('db_stat', f"{trino_db_name}_stat").replace(
        'dbname', f"{trino_db_name}")

    modified_sql = modified_sql.replace('country_db_id', str(country_id))

    if query == 'lifetime_union':
        # calculate and rewrite the lifetime values for all the jobs pushed in the last 45 days 
        # (their lifetime values)
        date_start = today - timedelta(days=45)
        date_end = today
        SQL = modified_sql.replace('date_start', f"'{date_start}'").replace('date_end', f"'{date_end}'")
    elif query == 'calendar_union':
        # calculate and rewrite lest 6 days
        date_start = today - timedelta(days=6)
        date_end = today
        SQL = modified_sql.replace('date_start', f"'{date_start}'").replace('date_end', f"'{date_end}'")
    elif query == 'month_union':
        # calculate and rewrite the monthly stat for current month
        date_start = today.replace(day=1)
        date_end = today
        SQL = modified_sql.replace('date_start', f"'{date_start}'").replace('date_end', f"'{date_end}'")

    return SQL, URL, date_start.strftime("%Y-%m-%d"), date_end.strftime("%Y-%m-%d")


@op(out=DynamicOut(), required_resource_keys={'globals'})
def seo_gfj_stat_prepare_trino_catalogs(context):
    """Prepare Trino catalogs for parallel execution

    Args:
        context: Dagster context with access to resources

    Yields:
        DynamicOutput: Individual Trino catalog configurations for parallel execution
    """
    # Get catalogs from resources (passed by user in web UI)
    trino_catalogs = context.resources.globals["trino_catalogs"]

    context.log.info(f"Running SQL queries on {len(trino_catalogs)} Trino catalogs: {trino_catalogs}\n"
                     f"SQL queries: {SQL_QUERIES}")

    # Yield each catalog as a dynamic output
    for catalog in trino_catalogs:
        yield DynamicOutput(
            value={
                'trino_catalog': catalog,
                'trino_db_name': catalog.split('_')[1]
            },
            mapping_key=f"catalog_{catalog}"
        )


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def seo_gfj_stat_execute_sql(context, catalog_info):
    """Execute SQL on a specific Trino catalog concurrently for all queries

    Args:
        context: Dagster context
        catalog_info: Trino catalog name as a string

    Returns:
        str: Execution result message
    """
    trino_catalog = catalog_info['trino_catalog']
    trino_db_name = catalog_info['trino_db_name']
    country_id = map_country_code_to_id.get(trino_db_name)
    context.log.info(f"Executing SQL on catalog: {trino_catalog}")

    def process_query(query):
        sql_query, url, date_start, date_end = get_sql_query(query, trino_db_name, country_id)

        context.log.info(f"Executing SQL on catalog: {trino_catalog}\n"
                         f"date_start: {date_start}, date_end: {date_end}\n"
                         f"query: {query}\n"
                         f"url: {url}\n")

        chunk_generator = TrinoOperations.fetch_data_from_trino(
            context=context,
            sql_code_to_execute=sql_query,
            catalog=trino_catalog,
        )

        chunks = []
        for chunk in chunk_generator:
            if chunk is not None and not chunk.empty:
                chunks.append(chunk)

        if not chunks:
            context.log.warning("No data returned from Trino query, skipping")
            return

        full_df = pd.concat(chunks, ignore_index=True)

        stat_type = None
        if query == "calendar_union":
            stat_type = 1
        elif query == "month_union":
            stat_type = 2
        elif query == "lifetime_union":
            stat_type = 3

        additional_where_clause = f"stat_type = {stat_type}" if stat_type is not None else None

        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            country_column="country_id",
            country=country_id,
            date_column="date",
            date_start=date_start,
            date_end=date_end,
            destination_db="cloudberry",
            additional_where_clause=additional_where_clause
        )

        DwhOperations.save_to_dwh_copy_method(
            context=context,
            schema=SCHEMA_NAME,
            table_name=TABLE_NAME,
            df=full_df,
            destination_db="cloudberry"
        )

    # Run all queries concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process_query, query) for query in SQL_QUERIES]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                context.log.error(f"Query execution generated an exception: {exc}")


@job(
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 10,
                }
            }
        }
    },
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(
            trino_catalogs=Field(list, default_value=trino_rpl_catalog,
                                 description="List of Trino catalogs to run SQL on")
        )
    },
    name=generate_job_name('seo_gfj_stat'),
    description=f"{SCHEMA_NAME}.{TABLE_NAME}",
    metadata={
        "month_union": "https://gitlab.jooble.com/an/dwh-sql/-/blob/master/dwh_test/tables/month_union.sql",
        "calendar_union": "https://gitlab.jooble.com/an/dwh-sql/-/blob/master/dwh_test/tables/calendar_union.sql",
        "lifetime_union": "https://gitlab.jooble.com/an/dwh-sql/-/blob/master/dwh_test/tables/lifetime_union.sql"
    },
)
def seo_gfj_stat_job():
    """
    Job to fetch data from Trino and load it into Cloudberry.
    Configure catalogs through the Dagster UI when triggering the job.
    """
    catalogs = seo_gfj_stat_prepare_trino_catalogs()
    catalogs.map(seo_gfj_stat_execute_sql)