import pandas as pd
import numpy as np
import psycopg2
import json

from pathlib import Path
from contextlib import closing
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from dagster import (
    op, job
)

from ..utils.messages import send_dwh_alert_slack_message


CREDS_PATH = Path(__file__).parent / 'credentials'
SQL_PATH = Path(__file__).parent / 'sql'

with open(f'{CREDS_PATH}/dwh.json') as json_file:
    dwh_cred = json.load(json_file)
USER_DWH = dwh_cred['user']
PW_DWH = dwh_cred['password']
HOST_DWH = dwh_cred['host']
DB_DWH = dwh_cred['database']


def exec_query_dwh(q):
    with closing(psycopg2.connect(dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH)) as con:
        df = pd.read_sql(q, con)
    return df


def read_sql_file(file):
    sql_file = open(file)
    sql_query = sql_file.read()
    sql_file.close()
    return sql_query


def get_clusters(data, sort_column, click_percent_bins, labels):
    data_grouped = data.groupby(sort_column)['click_count'].sum().reset_index()
    data_grouped = data_grouped.sort_values(sort_column)
    data_grouped['cumulative_click_%'] = data_grouped['click_count'].cumsum() / data_grouped['click_count'].sum()
    data_grouped['cluster'] = pd.cut(data_grouped['cumulative_click_%'], click_percent_bins, labels=labels)
    cluster_dict = data_grouped[[sort_column, 'cluster']].set_index(sort_column)['cluster'].to_dict()
    return data[sort_column].map(cluster_dict)


def agg_median(x):
    return np.repeat(x['click_price_usd'], x['click_count']).median()


def collect_data(date_start, date_end):
    # collect click data
    clicks_q = read_sql_file(f'{SQL_PATH}/query_clicks.sql').format(date_start=date_start, date_end=date_end)
    df = exec_query_dwh(clicks_q)

    df = df.groupby(['country', 'id_project', 'id_campaign'])[
        ['click_count', 'paid_click_count', 'revenue_usd']].sum().reset_index()
    df['click_price_usd'] = df['revenue_usd'] / df['click_count']

    dfg = df.groupby('country')[['click_price_usd', 'click_count']].apply(agg_median).reset_index()
    dfg.columns = ['country', 'country_median_cpc']

    df = df.merge(dfg, how='left', on=['country'])

    # filter low-clicks projects
    df_rev_c = df.groupby('country')[['revenue_usd', 'click_count']].sum().reset_index()
    df_rev_c.columns = ['country', 'country_revenue_usd', 'country_click_count']
    df_rev_c['country_revenue_rank'] = df_rev_c['country_revenue_usd'].rank(ascending=False)
    df = df.merge(df_rev_c, how='left', on=['country'])

    df_rev_p = df.groupby(['country', 'id_project'])[['click_count', 'revenue_usd']].sum().reset_index()
    df_rev_p.columns = ['country', 'id_project', 'project_click_count', 'project_revenue_usd']
    df = df.merge(df_rev_p, how='left', on=['country', 'id_project'])

    df['%_project_country_clicks'] = 100 * df['project_click_count'] / df['country_click_count']
    df = df[df['%_project_country_clicks'] > 0.5].copy()

    # cluster data
    marketing_q = read_sql_file(f'{SQL_PATH}/query_marketing.sql').format(date_start=date_start, date_end=date_end)
    df_marketing = exec_query_dwh(marketing_q)

    data = df.merge(df_marketing, how='left', on=['country']).copy()
    data['record_date'] = date_end

    # collect jobs data
    jobs_q = read_sql_file(f'{SQL_PATH}/query_jobs.sql').format(date_start=date_start, date_end=date_end)
    jobs_df = exec_query_dwh(jobs_q)

    data = data.merge(
        jobs_df.groupby(['country', 'id_project', 'id_campaign'])['paid_job_count'].mean().reset_index(),
        how='left',
        on=['country', 'id_project', 'id_campaign']
    )
    return data


def cluster_data(data, click_percent_bins, cluster_labels):
    data['diff_marketing_cpc'] = data['click_price_usd'] - data['marketing_cpc']
    data['percent_diff_marketing_cpc'] = 100 * (data['click_price_usd'] - data['marketing_cpc']) / data['marketing_cpc']

    data['diff_median_cpc'] = data['click_price_usd'] - data['country_median_cpc']
    data['percent_diff_median_cpc'] = 100 * (data['click_price_usd'] - data['country_median_cpc']) / data[
        'country_median_cpc']

    data['marketing_cluster'] = get_clusters(data[data['marketing_cpc'].notnull()], 'percent_diff_marketing_cpc',
                                             click_percent_bins, cluster_labels)
    data['median_cluster'] = get_clusters(data, 'percent_diff_median_cpc', click_percent_bins, cluster_labels)
    return data


def insert_data(data, dwh_schema, country_table, campaign_table):
    country_q = '''
    select id as country_id, lower(alpha_2) as country
    from dimension.countries
    '''
    country_dict = exec_query_dwh(country_q).set_index('country').to_dict()['country_id']

    data['id_country'] = data['country'].map(country_dict)
    cols_by_campaign = [
        'record_date', 'id_country', 'id_project', 'id_campaign',
        'click_count', 'paid_click_count', 'revenue_usd', 'click_price_usd',
        'paid_job_count', 'project_revenue_usd', 'marketing_cluster', 'median_cluster'
    ]

    cols_by_country = [
        'record_date', 'id_country', 'marketing_cpc', 'country_median_cpc', 'country_revenue_rank'
    ]

    con_dwh = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH))

    camp_data = data[cols_by_campaign].copy()
    camp_data['revenue_usd'] = camp_data['revenue_usd'].round(2)
    camp_data['project_revenue_usd'] = camp_data['project_revenue_usd'].round(2)
    camp_data['click_price_usd'] = camp_data['click_price_usd'].round(6)
    camp_data['paid_job_count'] = camp_data['paid_job_count'].round()

    camp_data.to_sql(
        name=campaign_table,
        schema=dwh_schema,
        if_exists='append',
        con=con_dwh,
        index=False
    )

    data[cols_by_country].drop_duplicates().round(6).to_sql(
        name=country_table,
        schema=dwh_schema,
        if_exists='append',
        con=con_dwh,
        index=False
    )





@op
def insert_data_op():
    with open(f'{Path(__file__).parent}/config.json', 'r') as f:
        config = json.load(f)

    dwh_schema = config['dwh_schema']
    campaign_table = config['campaign_table']
    country_table = config['country_table']
    cluster_labels_list = [int(k) for k in config['clusters_start_percentage'].keys()]
    click_percent_bins = [int(v)/100 for v in config['clusters_start_percentage'].values()] + [1]

    date_start = ((datetime.now().date().replace(day=1) - timedelta(days=1)).replace(day=23)).strftime('%Y-%m-%d')
    date_end = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')

    data = collect_data(date_start, date_end)
    clustered_data = cluster_data(data, click_percent_bins, cluster_labels_list)
    insert_data(clustered_data, dwh_schema, country_table, campaign_table)

    send_dwh_alert_slack_message(f':white_check_mark: *cpc_segmentation_script DONE*')


@job(
    name='dwh__cpc_segmentation_script',
    description='aggregation.segment_campaign_data'
)
def cpc_segmentation_script_job():
    insert_data_op()

