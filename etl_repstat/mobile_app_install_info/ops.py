from datetime import datetime, timedelta
import pandas as pd

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    DynamicOutput,
    Field,
)

# module import
from utility_hub import (
    Operations,
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = "mobile_app_install_info"
SCHEMA = "an"
DATE_DIFF_COLUMN = "date_diff"
COUNTRY_COLUMN = "country_code"

YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')

GITLAB_SELECT_Q, GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="repstat",
    dir_name='prc_mobile_app_install_info',
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def mobile_app_install_info_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    '''
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}\n'
                     f"Query run on replica:\n{GITLAB_SELECT_Q_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=GITLAB_SELECT_Q,
            select_query=GITLAB_SELECT_Q,):
        yield sql_instance


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def mobile_app_install_info_query_on_db(context, sql_instance_country_query: dict):
    """Execute query on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        pd.DataFrame: dataframe with data
        sql_instance_country_query (dict): dict with params
    """
    df_combined = pd.DataFrame()

    operation_date_start = context.resources.globals["reload_date_start"]
    operation_date_end = context.resources.globals["reload_date_end"]

    operation_date_diff_start = get_datediff(operation_date_start)
    operation_date_diff_end = get_datediff(operation_date_end)
    context.log.info(f"--> Starting sql-script on: {operation_date_start}")

    sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff_start
    sql_instance_country_query['to_sqlcode_date_or_datediff_end'] = operation_date_diff_end

    # Generator for retrieving chunks
    chunk_generator = DbOperations.execute_query_and_return_chunks(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
    )

    # Check for the presence of data
    first_chunk = next(chunk_generator, None)
    if first_chunk is None:
        context.log.info(f"No data found for {sql_instance_country_query.get('db_name', 'unknown')}")
        return pd.DataFrame(), sql_instance_country_query

    df_combined = pd.concat([df_combined, first_chunk, *chunk_generator])

    # Replace country codes not in all_countries_list with sql_instance_country_query['db_name']
    if not df_combined.empty:
        # Convert country codes to uppercase for comparison
        valid_countries = [country.strip('_').upper() for country in all_countries_list]

        # Replace invalid country codes with the db_name
        invalid_mask = ~df_combined[COUNTRY_COLUMN].isin(valid_countries)
        if invalid_mask.any():
            context.log.warning(f"Found {invalid_mask.sum()} rows with invalid country codes. "
                                f"Replacing with {sql_instance_country_query['db_name'].upper()}")
            df_combined.loc[invalid_mask, COUNTRY_COLUMN] = sql_instance_country_query['db_name'].upper()

    return df_combined, sql_instance_country_query


@op(out=DynamicOut())
def mobile_app_install_info_process_results(context, results):
    '''
    Process results and split by country for parallel writing

    Args:
        context (_type_): logs
        results (pd.DataFrame): dataframe with data

    Yields:
        dict: dict with params
    '''
    instances_by_country = {}
    dataframes = []

    # Unpack results and build a map "country->connection parameters"
    for df, sql_instance in results:
        # Always add instance to dict, regardless of df content
        country_code = sql_instance.get('db_name', '').upper()
        if country_code:
            instances_by_country[country_code] = sql_instance
            # Only add non-empty dataframes to the list
            if not df.empty:
                dataframes.append(df)
                context.log.info(f"Added {df.shape[0]} rows from instance df: {country_code}")
            else:
                context.log.info(f"No data found for country: {country_code}")

    if not dataframes:
        context.log.error("No data to process - all queries returned empty results")
        return

    # Combine all dataframes
    df_combined = pd.concat(dataframes, ignore_index=True)

    # Set country code as index
    df_combined.set_index(COUNTRY_COLUMN, drop=False, inplace=True)

    # For each unique country in the dataframe
    for country_code in df_combined.index.unique():
        # Get the corresponding sql instance of this country
        sql_instance_country_query = instances_by_country.get(country_code)

        if sql_instance_country_query is None:
            context.log.warning(f"No sql instance for country: {country_code}")
            continue

        # Filter data by country and ensure we get a DataFrame
        country_df = df_combined.loc[[country_code]] if isinstance(df_combined.loc[country_code], pd.Series) else df_combined.loc[country_code]

        if country_df.empty:
            continue

        # Create a separate output for each country
        yield DynamicOutput(
            value={
                "country_code": country_code,
                "dataframe": country_df,
                "sql_instance_country_query": sql_instance_country_query
            },
            mapping_key=country_code
        )


@op(retry_policy=retry_policy)
def mobile_app_install_info_write_data_to_rpl(context, country_data: dict):
    '''
    Delete data from rpl table.
    Save data to rpl table.

    Args:
        context (_type_): logs
        country_data (dict): dict with params
    '''
    country_code = country_data['country_code']
    country_df = country_data['dataframe']
    sql_instance_country_query = country_data['sql_instance_country_query']

    date_diff_start = sql_instance_country_query['to_sqlcode_date_or_datediff_start']
    date_diff_end = sql_instance_country_query['to_sqlcode_date_or_datediff_end']

    # Delete data from rpl table
    DbOperations.delete_data_from_table(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        date_column=DATE_DIFF_COLUMN,
        date_start=date_diff_start,
        date_end=date_diff_end,
    )

    # Save the filtered results for country
    context.log.info(f"Saving {country_df.shape[0]} rows for country: {country_code}")
    DbOperations.save_to_pg_copy_method(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        df=country_df
    )


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_query_url": f"{GITLAB_SELECT_Q_URL}",
        "destination_db": "rpl",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'''Rewriting a single country is not possible!!!
                    To ensure data completeness for a single country,
                    the job must be run for all countries.''',
)
def mobile_app_install_info_job():
    instances = mobile_app_install_info_get_sqlinstance()
    results = instances.map(mobile_app_install_info_query_on_db).collect()
    country_data = mobile_app_install_info_process_results(results)
    country_data.map(mobile_app_install_info_write_data_to_rpl)
