import pandas as pd

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


TABLE_NAME = "dic_seo_search_category"
SCHEMA = "dimension"
COUNTRY_COLUMN = "country_code"

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)
CUSTOM_COUNTRY_LIST = ["uk", "de", "fr", "pl", "nl"]


@op(required_resource_keys={"globals"}, out=DynamicOut())
def dic_seo_search_category_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.

    Args:
        context (Context): The context object.

    Yields:
        dict: dict with params to start query
    '''
    launch_countries = context.resources.globals["reload_countries"]
    context.log.info(f'I> Selected countries: {launch_countries}\n'
                     f"I> Gitlab sql-code url: {GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="seo_server",
            db_name="SeoQueries",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def dic_seo_search_category_query_on_db(context, sql_instance_country_query: dict):
    """
    Launches a query on the SeoQueries database.

    Args:
        context (Context): The context object.
        sql_instance_country_query (dict): The SQL instance country query.

    Returns:
        pd.DataFrame: The result of the query.
    """

    destination_db = context.resources.globals["destination_db"]
    country_code = sql_instance_country_query["country_code"]
    country_id = sql_instance_country_query["country_id"]

    to_sqlcode_tbl_category = f"{country_code}_child_category_to_query"
    to_sqlcode_tbl_seo_query = f"{country_code}_seo_query"

    formatted_query = sql_instance_country_query["query"].format(
        to_sqlcode_country_id=country_id,
        to_sqlcode_country_code=country_code,
        to_sqlcode_tbl_category=to_sqlcode_tbl_category,
        to_sqlcode_tbl_seo_query=to_sqlcode_tbl_seo_query,
    )

    sql_instance_country_query.update({"formatted_query": formatted_query})
    sql_instance_country_query.update({"country_code": country_code})
    sql_instance_country_query.update({"country_id": country_id})

    try:
        # Generator for retrieving chunks
        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            return pd.DataFrame()

        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            country_column=COUNTRY_COLUMN,
            country=country_code,
            destination_db=destination_db
        )

        df_combined = pd.concat([first_chunk, *chunk_generator])

        return df_combined
    except Exception as e:
        context.log.error(f"Error executing query on db: {e}")
        raise e


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def dic_seo_search_category_save_to_dwh(context, seo_server_results):
    """
    Saves the combined DataFrame to the DWH.

    Args:
        context (Context): The context object.
        seo_server_results (pd.DataFrame): The combined DataFrame.
    """
    destination_db = context.resources.globals["destination_db"]
    # Filter out empty DataFrames and combine results from all countries
    df_combined = pd.concat([df for df in seo_server_results if not df.empty], ignore_index=True)

    if df_combined.empty:
        context.log.error("No data to process - all country queries returned empty results")
        return

    # Execute dwh data from aggregation.v_job_kaiju_category
    query = """
            SELECT
                child_category_id,
                child_category_name,
                parent_category_id,
                parent_category_name
            FROM aggregation.v_job_kaiju_category
            """
    results = DwhOperations.execute_on_dwh(
        context=context,
        query=query,
        fetch_results=True,
    )
    df_dwh = pd.DataFrame.from_records(results)

    try:
        context.log.info(f"Start merging with the category data")
        # Merge with the category data
        df_merged = pd.merge(
            df_combined,
            df_dwh,
            left_on='id_category',
            right_on='child_category_id',
            how='left'
        )
        context.log.info(f"Start deduplicating")
        # Deduplicate keeping first occurrence per country_id and id_seo_query
        df_merged['rn'] = df_merged.sort_values(['id_category']).groupby(['country_id', 'id_seo_query']).cumcount() + 1
        df_final = df_merged[df_merged['rn'] == 1].drop(columns=['rn'])
        df_final.reset_index(drop=True, inplace=True)

        context.log.info(f"Start converting to nullable integer type")
        # Convert integer columns to nullable integer type, preserving NULLs
        integer_columns = ['country_id', 'id_seo_query', 'child_category_id', 'parent_category_id']
        for col in integer_columns:
            df_final[col] = df_final[col].apply(
                lambda x: pd.NA if pd.isna(x) else int(x)
            ).astype('Int64')

        context.log.debug(f"Total rows after merging: {df_final.shape[0]}")
        context.log.debug(f"Countries processed: {df_final['country_code'].nunique()}")

        # Save the final DataFrame to the DWH
        DwhOperations.save_to_dwh_copy_method(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            df=df_final,
            destination_db=destination_db
        )
    except Exception as e:
        context.log.error(f"Error saving to dwh: {e}")
        raise


@job(
    config=job_config,
    resource_defs={
        "globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRY_LIST),
                                        destination_db=Field(str, default_value='both')),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "true"
    },
    description=f"""
        [{SCHEMA}.{TABLE_NAME}] >>> For creating the final DataFrame, two sources are merged:
        1 - query by the link in the metadata;
        2 - query view from DWH [aggregation.v_job_kaiju_category].
        """,
)
def dic_seo_search_category_job():
    db_instances = dic_seo_search_category_get_sqlinstance()
    seo_server_results = db_instances.map(dic_seo_search_category_query_on_db).collect()
    dic_seo_search_category_save_to_dwh(seo_server_results)
