from datetime import datetime, timedelta

import pandas as pd

from dagster import (
    op, job, Field, make_values_resource, Failure
)
from sqlalchemy import create_engine

# custom modules
from etl_ga.ga4.ga4_employer.config import (
    country_domain_to_property_id,
    country_domain_to_country_id,
    request_params,
    analytics,
    cloudberry_conn_psycopg2
)

from etl_ga.ga4.logs_messages_tools.messages import send_dwh_alert_slack_message
from etl_ga.ga4.ga4_employer.etl_ga_functions import (
    get_report, get_request_body, report_to_dataframe, write_to_dwh
)
from etl_ga.utils.db_config import PORT, USER, PASSWORD

# Define the schema and table names for storing GA4 API data
SCHEMA = 'imp_api'
TABLE_NAME = 'ga4_employer'

# Calculate the start and end dates for the data request
# The end date is yesterday, and the start date is two days ago
DATE_END = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
DATE_START = (datetime.now().date() - timedelta(2)).strftime('%Y-%m-%d')

# Send request to GA4 API for page_type: 'employer' and save into: imp_api.ga4_employer
PAGE_TYPES = ['employer',]


def dwh_engine():
    host = "an-dwh.jooble.com"
    database = "an_dwh"
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{host}:{PORT}/{database}')
    return engine


def delete_history_data(context, delete_date_start, delete_date_end, page, table_name):
    try:
        with cloudberry_conn_psycopg2() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''delete from imp_api.{table_name} 
                                where action_date::date between '{delete_date_start}' and '{delete_date_end}'
                                    and page_name like '%{page}%';''')
                conn.commit()
                cur.close()
            context.log.info(
                f"Deleted between '{delete_date_start}' and '{delete_date_end}' for {page} from {SCHEMA}.{table_name}\n")
    except Exception as e:
        context.log.info(f'deleting error: {e}')


def ga4_employer_request_handler(context, date_start, date_end, page_types):
    try:
        context.log.info(f'====== [{TABLE_NAME}] started - parse date: {date_start}/-/{date_end}')

        # create engine
        engine = dwh_engine()
        context.log.info('Connected to GA4 service')

        # get table names
        table_names = request_params.keys()

        for tn in table_names:
            for page_type in page_types:
                context.log.info(f"page_type: {page_type}")
                dataframes = {}
                rows_cnt_acc = 0

                property_ids = country_domain_to_property_id

                for domain in property_ids:
                    # get report for every domain
                    request_body = get_request_body(
                        context,
                        table_name=TABLE_NAME,
                        requests_params=request_params,
                        date_start=date_start,
                        date_end=date_end,
                        page_type=page_type
                    )

                    property_id = property_ids.get(domain)
                    report = get_report(property_id, analytics, body=request_body)

                    rows_cnt = report.get("reports")[0].get("rowCount") or 0
                    rows_cnt_acc += rows_cnt
                    context.log.info(f'{domain} for {TABLE_NAME}: {rows_cnt}')

                    df = report_to_dataframe(report, TABLE_NAME)
                    df['country_domain'] = domain
                    df['country_id'] = country_domain_to_country_id.get(domain)
                    # add column page_name
                    df['page_name'] = page_type

                    # add to df's list
                    dataframes[domain] = df

                # union dataframes for different domains
                result_df = pd.concat(dataframes.values())

                write_to_dwh(
                    context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    dataframe=result_df,
                    engine=engine
                )

            send_dwh_alert_slack_message(f":add: *EMPLOYER-PAGE: {TABLE_NAME}*\n"
                                         f">*[{date_start}/-/{date_end}]*")
    except Exception as e:
        raise Failure(description=f"error: {e}")


@op(required_resource_keys={'globals'})
def ga4_employer_delete_historical_data_op(context):
    """delete historical data

    Args:
        context (_type_): _description_

    Returns:
        _type_: _description_
    """
    date_start = context.resources.globals["date_start"]
    date_end = context.resources.globals["date_end"]
    page_names = context.resources.globals["page_types"]

    for page in page_names:
        context.log.info(f"delete from {SCHEMA}.{TABLE_NAME}, {page}")

        delete_history_data(context, date_start, date_end, page, TABLE_NAME)

    return True


@op(required_resource_keys={'globals'})
def ga4_employer_api_request_op(context, deletion_result):
    """save api request result df to dwh

    Args:
        context (_type_): _description_
        deletion_result (_type_): _description_
    """
    date_start = context.resources.globals["date_start"]
    date_end = context.resources.globals["date_end"]
    page_types = context.resources.globals["page_types"]

    ga4_employer_request_handler(context, date_start, date_end, page_types)


@job(
    name='ga4__employer',
    description=f'{SCHEMA}.{TABLE_NAME}',
    resource_defs={"globals": make_values_resource(date_start=Field(str, default_value=DATE_START),
                                                   date_end=Field(str, default_value=DATE_END),
                                                   page_types=Field(list, default_value=PAGE_TYPES)
                                                   )}
)
def ga4_employer_job():
    deletion_result = ga4_employer_delete_historical_data_op()
    ga4_employer_api_request_op(deletion_result)
