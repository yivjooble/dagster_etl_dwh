import pandas as pd
import socket
import time

from googleapiclient.errors import HttpError
from collections import defaultdict

from etl_ga.ga4.ga4_utils.map_columns import map_column_names
from etl_ga.ga4.ga4_employer.config import request_params

timeout_in_sec = 360
socket.setdefaulttimeout(timeout_in_sec)


def get_request_body(context, date_start, date_end, table_name, requests_params, page_type):
    """
    builds request body for table_name

    :param table_name:
    :param requests_params:
    :param view_id:
    :param parse_date:
    :return:
    """
    params = requests_params.get(table_name)

    if params is None:
        raise Exception(f'Unknown table_name: {table_name}\n{requests_params}')

    request_body = {
        'requests': {
            "dateRanges": [{
                "startDate": date_start,
                "endDate": date_end
            }],
            'dimensions': params.get('dimensions'),
            'metrics': params.get('metrics'),
            'dimensionFilter': {
                'filter': {
                    'fieldName': 'landingPage',
                    'stringFilter': {
                        'matchType': 'CONTAINS',
                        'value': f"/{page_type}/"
                    }
                }
            },
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

        mapped_dimensions = [mapped for i, input_fields in enumerate(
            request_params[table_name]['dimensions']) for i, mapped in map_column_names.items() if
                             i == input_fields['name']]
        mapped_metrics = [mapped for i, input_fields in enumerate(
            request_params[table_name]['metrics']) for i, mapped in map_column_names.items() if
                          i == input_fields['name']]

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


def write_to_dwh(context, table_name, dataframe, engine, schema='imp_api'):
    dataframe.to_sql(table_name,
                     con=engine,
                     schema=schema,
                     if_exists='append',
                     index=False)
    context.log.info(f'saved to {schema}."{table_name}"')
