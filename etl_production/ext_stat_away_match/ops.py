import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field,
    DynamicOutput
)

# project import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    job_config,
    retry_policy,
    all_countries_list
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name, get_datediff


TABLE_NAME = 'ext_stat_away_match'
SCHEMA = 'imp_statistic'
DATE_DIFF_COLUMN = 'date_diff'
COUNTRY_COLUMN = 'country'
YESTERDAY_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


GITLAB_SQL_Q_APPCAST_CLICKS, GITLAB_SQL_URL_APPCAST_CLICKS = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name='ext_stat_away_match_pt2_dwh',
)

GITLAB_SQL_Q_AWAY_CLICKS, GITLAB_SQL_URL_AWAY_CLICKS = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name='ext_stat_away_match_pt1_prod',
)


def modify_ip(ip):
    """
    Modify IP address by keeping only the first three octets.

    Args:
        ip (str): Original IP address

    Returns:
        str: Modified IP address with last octet set to 0
    """
    if not pd.isnull(ip):
        return '.'.join(ip.split('.')[:3]) + '.0'


def find_matches(
        left: pd.DataFrame,
        right: pd.DataFrame,
        left_time_col: str,
        right_time_col: str,
        left_keys: list[str],
        right_keys: list[str],
        right_id_col: str,
        left_id_col: str,
        direction: str = 'nearest',
        tolerance_seconds: int = 60
) -> pd.DataFrame:
    """
    Returns a dataframe with matched values in left dataframe with values in right dataframe. Values are matched by
    full correspondence between values in left and right dataframes of columns of left_keys and right_keys and
    absolute difference between values of left_time_col and right_time_col <= tolerance_seconds.
    The resulting dataframe contains all rows of left dataframe and matched rows of right dataframe.

    Args:
        left (pd.DataFrame): Left dataframe to match from
        right (pd.DataFrame): Right dataframe to match to
        left_time_col (str): Time column name in left dataframe
        right_time_col (str): Time column name in right dataframe
        left_keys (list[str]): List of key columns in left dataframe
        right_keys (list[str]): List of key columns in right dataframe
        right_id_col (str): ID column name in right dataframe
        left_id_col (str): ID column name in left dataframe
        direction (str): Direction of matching ('nearest', 'forward', 'backward')
        tolerance_seconds (int): Maximum time difference in seconds for matching

    Returns:
        pd.DataFrame: Combined dataframe with matched records
    """
    # Sort dataframes by time for merge_asof
    left_sorted = left.sort_values(left_time_col)
    right_sorted = right.sort_values(right_time_col)

    df = pd.merge_asof(
        left_sorted,
        right_sorted,
        left_by=left_keys,
        right_by=right_keys,
        left_on=[left_time_col],
        right_on=[right_time_col],
        suffixes=['_away', ''],
        direction=direction,
        tolerance=pd.Timedelta(seconds=tolerance_seconds)
    )

    # Split into matched and unmatched records
    df_na = df[df[right_id_col].isnull()].copy()
    df_not_na = df[df[right_id_col].notnull()].copy()

    # Calculate time difference between matched records
    df_not_na['diff'] = (df_not_na[left_time_col] - df_not_na[right_time_col]).dt.total_seconds().abs()

    # Find invalid matches (where multiple records match to the same target)
    df_invalid_matches = df_not_na.loc[
        (df_not_na.groupby(right_id_col)['diff'].rank(ascending=True) != 1) |
        (df_not_na.groupby(left_id_col)['diff'].rank(ascending=True) != 1)
    ].drop('diff', axis=1)
    df_invalid_matches = df_invalid_matches[left.columns]

    # Combine valid and invalid matches
    df_na = pd.concat([df_na, df_invalid_matches], axis=0, ignore_index=True)
    df_not_na = df_not_na.loc[
        (df_not_na.groupby(right_id_col)['diff'].rank(ascending=True) == 1) &
        (df_not_na.groupby(left_id_col)['diff'].rank(ascending=True) == 1)
    ]

    return pd.concat([df_na, df_not_na.drop('diff', axis=1)], axis=0, ignore_index=True)


@op(required_resource_keys={'globals'}, retry_policy=retry_policy)
def ext_stat_away_match_query_on_dwh(context):
    """
    Query DWH to get appcast clicks data.

    Returns:
        pd.DataFrame: DataFrame containing appcast clicks data
    """
    destination_db = context.resources.globals['destination_db']
    date_start = get_datediff(context.resources.globals['reload_date_start'])
    date_end = get_datediff(context.resources.globals['reload_date_end'])

    # get data from dwh
    context.log.info("Fetching data from imp_statistic.conversions_appcast...")
    records = DwhOperations.execute_on_dwh(
        context=context,
        query=GITLAB_SQL_Q_APPCAST_CLICKS,
        params=(date_start, date_end),
        fetch_results=True,
        destination_db=destination_db
    )
    df = pd.DataFrame.from_records(records)
    context.log.info(f"Data fetched successfully - {df.shape[0]} rows")

    if df.empty:
        context.log.warning("No appcast clicks data found in DWH")

    return df


@op(out=DynamicOut(), required_resource_keys={'globals'})
def ext_stat_away_match_get_sql_instance(context, dwh_records: pd.DataFrame):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.
     Args:
        dwh_records (pd.DataFrame): DataFrame with DWH data

    Yields:
        dict: SQL instance configuration for each country
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(
        "Getting SQL instances...\n"
        f"Selected countries: {launch_countries}\n"
        f"Date range: [{context.resources.globals['reload_date_start']} - {context.resources.globals['reload_date_end']}]\n"
        f"Gitlab sql-code link:\n{GITLAB_SQL_URL_AWAY_CLICKS}"
    )

    for sql_instance in Operations.generate_sql_instance(
        context=context,
        instance_type="prod",
        query=GITLAB_SQL_Q_AWAY_CLICKS
    ):
        instance_dict = sql_instance.value
        instance_with_dwh = {
            **instance_dict,
            'df_dwh': dwh_records
        }
        yield DynamicOutput(instance_with_dwh, mapping_key=f"instance_{instance_dict['country_code']}")


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def ext_stat_away_match_query_on_db(context, sql_instance_country_query: dict):
    """
    Execute queries on production database and process results.

    Args:
        sql_instance_country_query (dict): SQL instance configuration

    Returns:
        None: Data is saved directly to DWH
    """
    destination_db = context.resources.globals['destination_db']

    for date in sql_instance_country_query['date_range']:
        operation_date = date.strftime("%Y-%m-%d")
        operation_date_diff = get_datediff(operation_date)
        country = sql_instance_country_query['country_code'].upper()
        df_dwh = sql_instance_country_query['df_dwh']

        # Filter DWH data for the current country and current date
        df_dwh_filtered = df_dwh[(df_dwh['country'] == country) & (df_dwh['date_diff_prod'] == operation_date_diff)]
        # Get unique projects for the current date and country
        unique_id_projects = list(df_dwh_filtered['id_project'].unique())

        if not unique_id_projects:
            context.log.info(f"No projects found for country {country} on date {operation_date}")
            continue

        projects_string = ', '.join(str(int(p)) for p in unique_id_projects)
        original_query = sql_instance_country_query['query']
        modified_query = original_query.replace('IN :to_sqlcode_unique_id_projects', f'IN ({projects_string})')
        # create local copy of dict
        local_sql_instance_country_query = deepcopy(sql_instance_country_query)
        local_sql_instance_country_query['query'] = modified_query
        local_sql_instance_country_query['to_sqlcode_date_start'] = operation_date_diff

        chunk_generator = DbOperations.execute_query_and_return_chunks(
            context=context,
            sql_instance_country_query=local_sql_instance_country_query
        )

        # Check for the presence of data
        first_chunk = next(chunk_generator, None)
        if first_chunk is None:
            continue

        df_prod_combined = pd.concat([first_chunk, *chunk_generator])
        df_prod_combined['ip_mod'] = df_prod_combined['ip'].apply(modify_ip)
        df_prod_combined['date'] = pd.to_datetime(df_prod_combined['date'])
        df_prod_combined['id_project'] = df_prod_combined['id_project'].astype(np.int64)

        # Convert matching columns to string type
        for col in ['country', 'id_project', 'user_ip', 'conversions_appcast_id']:
            if col in df_dwh_filtered.columns:
                df_dwh_filtered[col] = df_dwh_filtered[col].astype(str)

        for col in ['country', 'id_project', 'ip_mod', 'id_away']:
            if col in df_prod_combined.columns:
                df_prod_combined[col] = df_prod_combined[col].astype(str)

        df_prod_combined['date'] = pd.to_datetime(df_prod_combined['date']).values.astype('datetime64[ns]')
        df_dwh_filtered['datetime_prod'] = pd.to_datetime(df_dwh_filtered['datetime_prod']).values.astype('datetime64[ns]')

        # Find matches between production and DWH data
        df = find_matches(
            left=df_prod_combined,
            right=df_dwh_filtered,
            left_time_col='date',
            right_time_col='datetime_prod',
            left_keys=['country', 'id_project', 'ip_mod'],
            right_keys=['country', 'id_project', 'user_ip'],
            right_id_col='conversions_appcast_id',
            left_id_col='id_away'
        )

        # Select and filter columns
        col_list = ['country', 'id_project', 'date_diff', 'id_away', 'date', 'uid_job', 'click_price',
                    'click_price_usd', 'is_duplicated', 'is_bot', 'is_mobile',
                    'id_traf_source', 'id_current_traf_source', 'affiliate_traf_source',
                    'ip_cc', 'ip', 'placement', 'conversions_appcast_id']

        final_df = df[df['id_away'].notnull()][col_list]

        if final_df.empty:
            context.log.info(f"No matches found for country {country} on date {date}")
            continue

        DwhOperations.delete_data_from_dwh_table(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            date_column=DATE_DIFF_COLUMN,
            date_start=operation_date_diff,
            country_column=COUNTRY_COLUMN,
            country=country,
            destination_db=destination_db
        )

        DwhOperations.save_to_dwh_copy_method(
            context=context,
            df=final_df,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            destination_db=destination_db
        )


@job(
    config=job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                   reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                   destination_db=Field(str, default_value="cloudberry")),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "gitlab_sql_url_1": f"{GITLAB_SQL_URL_APPCAST_CLICKS}",
        "gitlab_sql_url_2": f"{GITLAB_SQL_URL_AWAY_CLICKS}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "triggered_by_job": "internal__conversions_appcast"
    },
    description=f'{SCHEMA}.{TABLE_NAME}')
def ext_stat_away_match_job():
    dwh_records = ext_stat_away_match_query_on_dwh()
    instances = ext_stat_away_match_get_sql_instance(dwh_records)
    instances.map(ext_stat_away_match_query_on_db).collect()
