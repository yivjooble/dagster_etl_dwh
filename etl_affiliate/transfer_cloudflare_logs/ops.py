from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
from urllib import parse
from datetime import datetime, timedelta
from dagster import op, job, fs_io_manager, Out, Failure, Field, make_values_resource
import inspect

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.job_xx_hosts import COUNTRY_CODE_TO_ID, COUNTRY_OFFSET
from etl_affiliate.utils.utils import log_written_data, create_conn, job_prefix, exec_query_pg, send_dwh_alert_slack_message, exec_select_query_pg
from utility_hub.core_tools import get_creds_from_vault

API_KEY = get_creds_from_vault('CF_LOGS_API_KEY')
JOB_PREFIX = job_prefix()
TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


def check_num(x):
    try:
        _x_num = int(x)
        return x
    except (ValueError, TypeError):
        return None


def get_elastic_client():
    index_template = '.ds-logs-cloudflare_logpush.http_request-default-*'
    client = Elasticsearch(
        ["https://10.0.2.89:9200", "https://10.0.2.87:9200", "https://10.0.2.88:9200"],
        api_key=API_KEY,
        verify_certs=False
    )

    idx_list = client.cat.indices(index=index_template, h='index', s='index:desc').split()
    return client, idx_list


def get_data_from_elastic(date_start, date_end):
    client, idx_list = get_elastic_client()
    query = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "range": {
                            "@timestamp": {
                                "format": "strict_date_optional_time",
                                "gte": f"{date_start}T06:00:00.000Z",
                                "lte": f"{date_end}T06:00:00.000Z"
                            }
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "regexp": {
                                        "url.path": "/external/.*"
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {
                                                "regexp": {
                                                    "url.path": "/desc/.*|/away/.*"
                                                }
                                            },
                                            {
                                                "regexp": {
                                                    "url.query": ".*extrlInt.*"
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "match_phrase": {
                            "data_stream.dataset": "cloudflare_logpush.http_request"
                        }
                    }
                ]
            }
        }
    }

    rel = scan(client=client,
               query=query,
               scroll='1m',
               index=idx_list,
               raise_on_error=True,
               preserve_order=False,
               clear_scroll=True)

    result = list(rel)
    temp = []

    for hit in result:
        temp.append([
            hit.get('_source', {}).get('user_agent', {}).get('original'),
            hit.get('_source', {}).get('@timestamp'),
            hit.get('_source', {}).get('source', {}).get('ip'),
            hit.get('_source', {}).get('url', {}).get('domain'),
            hit.get('_source', {}).get('url', {}).get('path'),
            hit.get('_source', {}).get('url', {}).get('query'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('bot', {}).get('score',
                                                                                                            {}).get(
                'src'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('bot', {}).get('score',
                                                                                                            {}).get(
                'value'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('bot', {}).get('tag'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('waf', {}).get('score',
                                                                                                            {}).get(
                'global'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('waf', {}).get('action'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('client', {}).get(
                'request', {}).get('referer'),
            date_start,
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('waf', {}).get(
                'rule', {}).get('id'),
            hit.get('_source', {}).get('cloudflare_logpush', {}).get('http_request', {}).get('firewall', {}).get('matches', 
                {}).get('action')
        ])

    df = pd.DataFrame(temp)
    df.columns = ['user_agent', 'click_datetime', 'ip', 'url_domain', 'url_path', 'url_query', 'bot_score_src',
                  'bot_score', 'bot_tags', 'waf_score', 'waf_action', 'referer', 'log_date', 'rule_id', 'firewall_action']

    return df


def transform_data(data):
    df = data.copy()
    df['bot_tags'] = df['bot_tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else None)
    df['uid_job'] = df['url_path'].apply(lambda x: x.split('/')[2])
    df['country'] = df['url_domain'].apply(lambda x: x.split('.')[0].upper() if x != 'jooble.org' else 'US')
    df['id_country'] = df['country'].map(COUNTRY_CODE_TO_ID)

    df['click_datetime'] = pd.to_datetime(df['click_datetime']).dt.tz_localize(None)
    df['tz_offset'] = df['country'].map(COUNTRY_OFFSET).fillna(0)
    df['click_datetime'] = df.apply(lambda x: x['click_datetime'] + pd.Timedelta(hours=x['tz_offset']), axis=1)
    df['date_diff'] = df['click_datetime'].dt.date.apply(lambda x: (x - datetime(1900, 1, 1).date()).days)

    df['url_params'] = df['url_query'].apply(lambda x: {k: v[0] for k, v in parse.parse_qs(x).items()})

    df['utm_source'] = df['url_params'].apply(lambda x: x.get('utm_source'))
    df['utm_medium'] = df['url_params'].apply(lambda x: x.get('utm_medium'))
    df['url_title_hash'] = df['url_params'].apply(lambda x: x.get('extra_title'))
    df['url_flags'] = df['url_params'].apply(lambda x: x.get('extra_flags'))
    df['url_prev_uid'] = df['url_params'].apply(lambda x: x.get('extra_prev_uid'))
    df['url_request_id'] = df['url_params'].apply(lambda x: x.get('extra_ars_request_id'))

    df['url_params'] = df['url_params'].apply(lambda x: {k: v for k, v in x.items() if
                                                         k not in ['utm_source', 'utm_medium', 'cpc', 'extra_title',
                                                                   'extra_flags', 'extra_search_by', 'extra_use_region',
                                                                   'extra_ars_request_id', 'extra_prev_uid',
                                                                   'extra_is_origin']})
    df['url_pub_params'] = df['url_params'].apply(lambda x: '&'.join([f'{k}={v}' for k, v in x.items()]))
    df['url_pub_params'] = df['url_pub_params'].apply(lambda x: None if x == '' else x)
    df['record_type'] = df['url_path'].apply(
        lambda x: 1 if 'external' in x else 
                  2 if 'away' in x else 
                  3 if 'desc' in x else 
                  None)

    for col in ['bot_score', 'waf_score', 'url_flags']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    df['url_flags'] = df['url_flags'].apply(lambda x: x if not pd.isna(x) and abs(x) <= 9223372036854775807 
                                            else None)
    
    for col in ['uid_job', 'url_prev_uid']:
        df[col] = df[col].apply(check_num)

    df.drop(columns=['url_path', 'url_domain', 'tz_offset', 'url_query', 'url_params', 'country'], inplace=True)
    return df


def insert_data(df, schema_name):
    # Postgres dwh
    df.to_sql(schema=schema_name,
              name="cloudflare_log_data",
              if_exists="append",
              con=create_conn("dwh"),
              index=False)

    # Cloudberry dwh
    df.to_sql(schema=schema_name,
              name="cloudflare_log_data",
              if_exists="append",
              con=create_conn("cloudberry"),
              index=False)


def check_unknown_rules(date_diff, schema_name):
    slack_message = exec_select_query_pg(
        """
        SELECT  'Unknown rule_ids: ' || STRING_AGG(rule_id, ', ') || CHR(10) || CHR(10) ||
                'Please, describe them.' || CHR(10) ||
                '<@U018GESMPDJ>, <@U06E7T8LP8X>' AS text
        FROM (SELECT rule_id
                FROM {schema}.cloudflare_log_data
                WHERE date_diff = {date_diff} AND rule_id IS NOT NULL AND rule_id <> 'badscore'
                EXCEPT
                SELECT REPLACE(id::text, '-', '')
                FROM {schema}.dic_cloudflare_rules) AS a
        GROUP BY TRUE
        HAVING COUNT(rule_id) > 0
        """.format(schema=schema_name, date_diff=date_diff), "dwh")
    
    if slack_message is not None and not slack_message.empty:
        if isinstance(slack_message, pd.DataFrame) or isinstance(slack_message, pd.Series):
            slack_message = slack_message.iloc[0, 0] if isinstance(slack_message, pd.DataFrame) else slack_message.iloc[0]
            
        send_dwh_alert_slack_message(slack_message)


@op(out=Out(bool),
    required_resource_keys={"globals"})
def prc_collect_cloudflare_log_data_op(context) -> bool:
    """
    Collects data from Cloudflare logs in Elastic and inserts them to the affiliate.cloudflare_log_data.
    """
    schema_name = context.resources.globals["schema_name"]
    op_name = inspect.currentframe().f_code.co_name
    start_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    send_slack_info = context.resources.globals["send_slack_info"]
    
    date_start = context.resources.globals["target_date_str"]
    date_end = (datetime.strptime(date_start, '%Y-%m-%d') + timedelta(1)).strftime('%Y-%m-%d')
    date_diff = (datetime.strptime(date_start, '%Y-%m-%d').date() - datetime(1900, 1, 1).date()).days

    try:
        context.log.info("deleting data from destination")
        exec_query_pg("delete from {schema}.cloudflare_log_data where log_date = '{log_date}'".format(
            schema=schema_name, log_date=date_start), "dwh")
        
        context.log.info("getting data from elastic")
        df = get_data_from_elastic(date_start, date_end)
        
        context.log.info("transforming and saving data")
        insert_data(transform_data(df), schema_name)

        context.log.info("checking unknown rules")
        check_unknown_rules(date_diff, schema_name)

        log_written_data(context=context, table_schema=schema_name, table_name="cloudflare_log_data",
                         date=date_start, start_dt=start_dt, end_dt=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         op_name=op_name, send_slack_message=send_slack_info)
        return True
    except:
        raise Failure(description=f"{op_name} error")


@job(resource_defs={"globals": make_values_resource(send_slack_info=Field(bool, default_value=True),
                                                    schema_name=Field(str, default_value='affiliate'),
                                                    target_date_str=Field(str, default_value=TARGET_DATE_STR)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'collect_cloudflare_log_data',
     description=f'Collects data from Cloudflare logs to the affiliate.cloudflare_log_data.')
def collect_cloudflare_log_data_job():
    prc_collect_cloudflare_log_data_op()
