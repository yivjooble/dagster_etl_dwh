import pandas as pd
import numpy as np

import sqlalchemy.engine
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    Out,
    In,
    Field,
    make_values_resource,
    fs_io_manager
)
from ..utils.utils import job_prefix
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import get_creds_from_vault

CLUSTER_RANGE_SCHEMA = 'aggregation'
CLUSTER_RANGE_TABLE = 'cpc_cluster_range'
JOB_PREFIX = job_prefix()


def read_sql_file(file: str) -> str:
    """
    Reads .sql file and returns query as a string.
    :param file: path to the file.
    :return: SQL query string.
    """
    with open(file) as f:
        sql_query = f.read()
    return sql_query


def create_conn_dwh_cloudberry() -> sqlalchemy.engine.Engine:
    """
    Creates sqlalchemy engine to connect to the DWH database.
    :return: Engine for further connection to the DWH.
    """
    # create SQLAlchemy engine for bulk insert with pd.to_sql
    con = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        host="an-dwh.jooble.com",
        dbname="an_dwh",
        user=get_creds_from_vault('DWH_USER'),
        password=get_creds_from_vault('DWH_PASSWORD')))
    return con


def create_conn_dwh() -> sqlalchemy.engine.Engine:
    """
    Creates sqlalchemy engine to connect to the DWH database.
    :return: Engine for further connection to the DWH.
    """
    # create SQLAlchemy engine for bulk insert with pd.to_sql
    con = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        host=get_creds_from_vault('DWH_HOST'),
        dbname=get_creds_from_vault('DWH_DB'),
        user=get_creds_from_vault('DWH_USER'),
        password=get_creds_from_vault('DWH_PASSWORD')))
    return con


def exec_query_dwh(q: str) -> pd.DataFrame:
    """
    Runs the select SQL query to the DWH and returns result as a DataFrame.
    :param q: SQL query for reading data in DWH.
    :return: DataFrame with query results.
    """
    with create_conn_dwh().connect() as con:
        df = pd.read_sql(q, con)
    return df


def get_min_cpc_data(target_date: str) -> pd.DataFrame:
    """
    Collects affiliate publisher's min CPCs from DWH (aggregation.cpc_cluster_affiliate_price) on the given date
     and converts them in USD.
    :param target_date: date, on which data is selected in the 'YYYY-MM-DD' format.
    :return: DataFrame with results.
    """
    q = read_sql_file('sql/affiliate_price.sql').format(target_date=target_date)
    data = exec_query_dwh(q)

    currency_query = read_sql_file('sql/currency_rate.sql').format(target_date=target_date)
    currency_dict = exec_query_dwh(currency_query).set_index('name').to_dict()['value_to_usd']

    incorrect_currencies = list(data[~data['currency'].isin(currency_dict)]['currency'].unique())
    if len(incorrect_currencies) > 0:
        raise ValueError('Unknown currencies cannot be converted to USD: {}'.format(incorrect_currencies))
    data['min_cpc_usd'] = data['min_cpc'] * data['currency'].map(currency_dict)
    return data


def detect_outlier(arr: list, x: float) -> bool:
    """
    Determines if x is an outlier in the arr array using 1.5 * IQR method.
    :param arr: array of values.
    :param x: value from arr that is to be checked for being an outlier.
    :return: True if x is an outlier in the arr, False otherwise.
    """
    q1 = np.percentile(arr, 25)
    q3 = np.percentile(arr, 75)
    iqr = q3 - q1
    lower_bound = q1 - iqr * 1.5
    upper_bound = q3 + iqr * 1.5
    if (x >= lower_bound) and (x <= upper_bound):
        return False  # not outlier
    return True  # outlier


def get_decimal_places(min_cpc: float) -> int:
    """
    Given country recommended minimal CPC, determine to how many decimal places should CPCs be rounded (2 or 3).
    If country recommended minimal CPC <= 0.05, we should round CPCs to 3 decimal places.
    Otherwise, we should round CPCs to 2 decimal places.
    :param min_cpc: minimal CPC for the given country.
    :return: integer, 2 or 3
    """
    if min_cpc <= 0.05:
        return 3
    return 2


def get_min_cpc_dict(target_date: str) -> tuple:
    """
    Collects affiliate publisher's minimal CPCs, calculates average minimal CPC by country excluding outliers.
    Returns result as a dictionary with country as a key and minimal CPC as value.
    :param target_date: date, on which data on affiliate publisher's minimal CPCs is selected in the 'YYYY-MM-DD'
    format.
    :return: tuple of dictionaries:
        a dictionary with country id as a key and average minimal CPC as value,
        a dictionary with country id as a key and update type as value.
    """
    data = get_min_cpc_data(target_date)
    incorrect_countries = data[data['country_id'].isnull()]['country'].to_list()
    if len(incorrect_countries) > 0:
        raise ValueError('Countries from the cpc_cluster_affiliate_price are not found in database: {}'.format(
            incorrect_countries))

    data = data.merge(
        data.groupby('country_id')['min_cpc_usd'].agg(list),
        how='left',
        left_on='country_id',
        right_index=True,
        suffixes=('', '_list')
    ).copy()
    data['is_outlier'] = data.apply(lambda x: detect_outlier(x['min_cpc_usd_list'], x['min_cpc_usd']), axis=1)
    low_min_cpc_df = data[~data['is_outlier']].groupby(
        ['country_id', 'update_type'], as_index=False)['min_cpc_usd'].agg(['mean'])
    low_min_cpc_df = low_min_cpc_df.sort_values(['country_id', 'update_type'],
                                                ascending=False).drop_duplicates(subset=['country_id']).copy()

    low_min_cpc_dict = low_min_cpc_df.set_index(['country_id']).to_dict()['mean']
    update_type_dict = data[['country_id', 'update_type']].groupby(
        'country_id')['update_type'].max().to_dict()
    return {k: round(v, get_decimal_places(v)) for k, v in low_min_cpc_dict.items()}, update_type_dict


def collect_internal_data(date_start: str, date_end: str, min_cpc_dict: dict) -> pd.DataFrame:
    """
    Collects statistics on click counts and revenue by CPC from DWH in the specified date period. Filters data
    by country id to keep only countries mentioned in the min_cpc_dict. Rounds CPC to the 2 or 3 decimal places
    depending on country minimal CPC. Aggregates resulted data by rounded CPC.

    :param date_start: period start date in the 'YYYY-MM-DD' format.
    :param date_end: period end date in the 'YYYY-MM-DD' format.
    :param min_cpc_dict: a dictionary with country id as a key and minimal CPC as value.
    :return: DataFrame with clicks and revenue grouped by country id and rounded CPC.
    """
    clicks_q = read_sql_file('sql/clicks_by_cpc_data.sql').format(date_start=date_start, date_end=date_end)
    df = exec_query_dwh(clicks_q)

    df = df[df['country_id'].isin(min_cpc_dict.keys())]
    df['low_min_cpc_usd'] = df['country_id'].map(min_cpc_dict)
    df['click_price_usd'] = df.apply(lambda x: round(x['click_price_usd'], get_decimal_places(x['low_min_cpc_usd'])),
                                     axis=1)
    df_agg = df.groupby(['country_id', 'click_price_usd', 'low_min_cpc_usd'], as_index=False).sum()

    return df_agg


def get_segment(cumulative_click_percent: float, prev_cumulative_click_percent: float) -> str:
    """
    Determines which cluster current CPC belongs to based on two parameters.
    :param cumulative_click_percent: value between 0 and 100, percent of country paid clicks that have current or lower
    CPC.
    :param prev_cumulative_click_percent: value between 0 and 100, percent of country paid clicks that have previous
    CPC or lower.
    :return: string, one of the following values: ['low', 'medium', 'high'].
    """
    click_percent = cumulative_click_percent - prev_cumulative_click_percent
    if cumulative_click_percent <= 0.334:
        return 'low'  # 'low' segment
    elif cumulative_click_percent <= 0.667:
        # if prev campaign was in 'low' segment
        if prev_cumulative_click_percent <= 0.334:
            # if current campaign's >=50% clicks fall into 'low' segment
            if (cumulative_click_percent - 0.334) <= 0.5 * click_percent:
                return 'low'  # 'low' segment
        return 'medium'  # 'medium segment'
    else:
        # if prev campaign was in 'medium' segment
        if prev_cumulative_click_percent <= 0.667:
            # if current campaign's >=50% clicks fall into 'medium' segment
            if (cumulative_click_percent - 0.667) <= 0.5 * click_percent:
                return 'medium'  # 'medium' segment
    return 'high'  # 'high' segment


def get_min_cpc_by_cluster(
        df_acs: pd.DataFrame,
        low_min_cpc_dict: dict,
        update_type_dict: dict,
        target_date: str) -> pd.DataFrame:
    """
    For the given DataFrame with clicks grouped by country id and CPC determines ranges of clusters for each country
    in the DataFrame.
    :param df_acs: DataFrame with clicks grouped by country id and CPC.
    :param low_min_cpc_dict: a dictionary with country id as a key and minimal CPC as value.
    :param update_type_dict: a dictionary with country id as a key and update type as value.
    :param target_date: date to which cluster ranges will be assigned in the 'YYYY-MM-DD' format.
    :return:
    """
    df_acs = df_acs[df_acs['click_price_usd'] >= df_acs['low_min_cpc_usd']].copy()

    df_acs['country_total_click_count'] = df_acs['country_id'].map(
        df_acs.groupby('country_id')['click_count'].sum().to_dict())
    df_acs['cumulative_click_percent'] = df_acs.groupby(['country_id'])['click_count'].cumsum() / df_acs[
        'country_total_click_count']
    df_acs['prev_cumulative_click_percent'] = df_acs.groupby('country_id'
                                                             )['cumulative_click_percent'].shift(1).fillna(0)

    df_acs['cluster'] = df_acs.apply(
        lambda x: get_segment(x['cumulative_click_percent'], x['prev_cumulative_click_percent']), axis=1)

    df_cluster_min_cpc = df_acs.groupby(['country_id', 'cluster'], as_index=False).agg(
        {'click_price_usd': ['min', 'max']})
    df_cluster_min_cpc.rename(columns={'max': 'max_cpc_usd', 'min': 'min_cpc_usd'}, inplace=True)

    ranges_df = df_cluster_min_cpc.set_index(['country_id', 'cluster']).unstack()
    ranges_df.columns = [y + '_' + x for _, x, y in ranges_df.columns]
    ranges_df = ranges_df.reset_index()

    missing_countries = [x for x in low_min_cpc_dict if x not in ranges_df['country_id'].unique()]
    ranges_df = pd.concat([ranges_df, pd.DataFrame({'country_id': missing_countries})], ignore_index=True)

    ranges_df['date'] = target_date
    ranges_df['low_min_cpc_usd'] = ranges_df['country_id'].map(low_min_cpc_dict)
    ranges_df['update_type'] = ranges_df['country_id'].map(update_type_dict)
    ranges_df = ranges_df[[
        'date', 'country_id', 'update_type', 'low_min_cpc_usd', 'low_max_cpc_usd', 'medium_min_cpc_usd',
        'medium_max_cpc_usd', 'high_min_cpc_usd', 'high_max_cpc_usd'
    ]]
    return ranges_df


@op(out=Out(dict))
def prepate_params_to_cpc_run(context):
    """
     Prepare params for CPC run. This is called by prepare_cpc_run when it is time to run the test
     
     Args:
         context: context object passed by the test
     
     Returns:
         dict with prepared params for CPC run. See prep
    """
    target_date = context.resources.globals["target_date"]

    prev_month_date_end = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=1)).date()
    prev_month_date_start = prev_month_date_end.replace(day=1)

    low_min_cpc_dict, update_type_dict = get_min_cpc_dict(target_date)
    internal_df = collect_internal_data(
        prev_month_date_start.strftime('%Y-%m-%d'),
        prev_month_date_end.strftime('%Y-%m-%d'),
        low_min_cpc_dict
    )

    params = {
        'target_date': target_date,
        'low_min_cpc_dict': low_min_cpc_dict,
        'update_type_dict': update_type_dict,
        'internal_df': internal_df
    }
    context.log.info(f"prepared params:\n"
                     f"{params}"
                     )
    return params


@op(ins={'params': In(dict)})
def get_min_cpc_by_cluster(context, params: dict):
    """
     Get min CPC for each cluster. This is a wrapper for get_min_cpc_by_cluster that creates a new table
     in the cluster_range_table and appends it to the existing one
     
     Args:
         context: Information that identifies the user that has made the request
         params: Dictionary with the following keys :
                internal_df : Dataframe with cluster ids as keys 
                low_min_cpc_dict : Dictionary with cluster ids
                update_type_dict : dict
                target_date : str
    """
    min_df = get_min_cpc_by_cluster(params['internal_df'],
                                    params['low_min_cpc_dict'],
                                    params['update_type_dict'],
                                    params['target_date'])
    # Postgres dwh
    min_df.to_sql(
        name=CLUSTER_RANGE_TABLE,
        schema=CLUSTER_RANGE_SCHEMA,
        index=False,
        if_exists='append',
        con=create_conn_dwh()
    )

    # Cloudberry dwh
    min_df.to_sql(
        name=CLUSTER_RANGE_TABLE,
        schema=CLUSTER_RANGE_SCHEMA,
        index=False,
        if_exists='append',
        con=create_conn_dwh_cloudberry()
    )
    context.log.info("success")


@job(
    resource_defs={"globals": make_values_resource(
        target_date=Field(str, default_value=datetime.now().replace(day=1).strftime('%Y-%m-%d'))),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=JOB_PREFIX + 'cpc_clustering',
    description=''
)
def start_cpc_clustering_job():
    params = prepate_params_to_cpc_run()
    get_min_cpc_by_cluster(params)
