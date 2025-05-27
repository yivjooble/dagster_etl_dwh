import pandas as pd
import socket
import time
import logging as logger

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from collections import defaultdict

from ..ga4_utils.map_columns import map_column_names
from .config import request_params, request_params_rs
from ..logs_messages_tools.project_logs import get_logger

timeout_in_sec = 180
socket.setdefaulttimeout(timeout_in_sec)


def get_request_body(start_date, end_date, table_name, requests_params):
    """
    builds request body for table_name

    :param table_name:
    :param requests_params:
    :param view_id:
    :param parse_date:
    :return:
    """
    params = requests_params.get(table_name)

    request_body = {
        'requests': {
            "dateRanges": [{
                "startDate": start_date,
                "endDate": end_date
                }],
            'dimensions': params.get('dimensions'),
            'metrics': params.get('metrics'),
            'limit': '600000'            
        }
    }     

    return request_body


def get_report(property_id, analytics, body, logger=None):
    """
    makes request to GA API. If API is unavailable, makes more attempts to connect to API
    returns GA Report as dict

    :param service:
    :param body:
    :param logger:
    :return:
    """
    max_num_of_call_attempts = 5
    num_of_call_attempts = 0
    call_sleep_seconds = 1

    while num_of_call_attempts < max_num_of_call_attempts:
        try:
            return analytics.properties().batchRunReports(property=property_id, body=body).execute()
        except HttpError as e:
            api_error = e
            if logger is not None:
                logger.exception('API is currently unavailable')
            time.sleep(call_sleep_seconds)
            call_sleep_seconds *= 2
            num_of_call_attempts += 1
            continue
    raise api_error


def report_to_dataframe(response, table_name: str):
    report_data = defaultdict(list)

    for report in response.get('reports', []):
        param = request_params_rs if table_name == 'ga4_dte_ea_sha_reg_rs' else request_params

        mapped_dimensions = [mapped for i, input_fields in enumerate(
            param[table_name]['dimensions']) for i, mapped in map_column_names.items() if i == input_fields['name']]
        mapped_metrics = [mapped for i, input_fields in enumerate(
            param[table_name]['metrics']) for i, mapped in map_column_names.items() if i == input_fields['name']]

        # get data from report and transform to pd.DataFrame
        rows = report.get('rows', [])

        for report in response.get('reports', []):
            rows = report.get('rows', [])
            for row in rows:
                for i, key in enumerate(mapped_dimensions):
                    report_data[key].append(row.get('dimensionValues', [])[i]['value'])  # Get dimensions
                for i, key in enumerate(mapped_metrics):
                    report_data[key].append(row.get('metricValues', [])[i]['value'])  # Get metrics

    df = pd.DataFrame(report_data)

    # format date
    if 'action_date' in df.columns:
        df['action_date'] = pd.to_datetime(df['action_date']).dt.date

    return df


def write_to_dwh(table_name, dataframe, engine, schema='imp_api'):
    dataframe.to_sql(table_name, 
                     con=engine, 
                     schema=schema, 
                     if_exists='append', 
                     index=False)
    logger.info(f'saved to {schema}."{table_name}"')
