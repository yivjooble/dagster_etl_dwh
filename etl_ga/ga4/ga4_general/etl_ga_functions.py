import pandas as pd
import socket
import time
import logging as logger

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from collections import defaultdict

from ..ga4_utils.map_columns import map_column_names
from ..ga4_general.config import request_params
from ..logs_messages_tools.project_logs import get_logger
from .config import analytics

timeout_in_sec = 300  # Increased from 180 to 300 seconds (5 minutes)
socket.setdefaulttimeout(timeout_in_sec)


def get_session_custom_channel_group(property_id, domain):
    """
    Retrieves the custom channel group for a given GA4 property ID.

    Accesses GA4 metadata to find the custom channel group dimension and
    returns it in a format suitable for API requests.

    Args:
        property_id (str): The property ID for the GA4 account.
        domain (str): country domain for logging.

    Returns:
        dict: A dictionary with the custom channel group with ID or None if not found.
    """
    try:
        # Execute the API call to fetch metadata for the given property ID
        response = (
            analytics.properties().getMetadata(name=f"{property_id}/metadata").execute()
        )

        # Search for 'sessionCustomChannelGroup' in dimensions
        for dimension in response.get("dimensions", []):
            if "sessionCustomChannelGroup" in dimension.get("apiName", ""):
                return {"name": dimension["apiName"]}

        logger.warning(
            f"{domain} - Session Custom Channel Group not found in metadata. Used default parameter."
        )
        return None
    except Exception as e:
        logger.error("Error occurred while fetching metadata:", e)
        return None


def get_request_body(date_start, date_end, table_name, requests_params):
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
        "requests": {
            "dateRanges": [{"startDate": date_start, "endDate": date_end}],
            "dimensions": params.get("dimensions"),
            "metrics": params.get("metrics"),
            "limit": "600000",
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
    api_error = ''

    while num_of_call_attempts < max_num_of_call_attempts:
        try:
            return (
                analytics.properties()
                .batchRunReports(property=property_id, body=body)
                .execute()
            )
        except HttpError as e:
            api_error = e
            if logger is not None:
                logger.exception("API is currently unavailable")
            time.sleep(call_sleep_seconds)
            call_sleep_seconds *= 2
            num_of_call_attempts += 1
            continue
    raise api_error


def report_to_dataframe(response, table_name: str):
    report_data = defaultdict(list)

    for report in response.get("reports", []):
        mapped_dimensions = [
            mapped
            for i, input_fields in enumerate(request_params[table_name]["dimensions"])
            for i, mapped in map_column_names.items()
            if i == input_fields["name"]
        ]
        mapped_metrics = [
            mapped
            for i, input_fields in enumerate(request_params[table_name]["metrics"])
            for i, mapped in map_column_names.items()
            if i == input_fields["name"]
        ]

        # get data from report and transform to pd.DataFrame
        rows = report.get("rows", [])

        for report in response.get("reports", []):
            rows = report.get("rows", [])
            for row in rows:
                for i, key in enumerate(mapped_dimensions):
                    report_data[key].append(
                        row.get("dimensionValues", [])[i]["value"]
                    )  # Get dimensions
                for i, key in enumerate(mapped_metrics):
                    report_data[key].append(
                        row.get("metricValues", [])[i]["value"]
                    )  # Get metrics

    df = pd.DataFrame(report_data)

    # format date
    if "action_date" in df.columns:
        df["action_date"] = pd.to_datetime(df["action_date"]).dt.date

    return df
