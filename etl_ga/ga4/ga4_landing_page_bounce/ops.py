from datetime import datetime, timedelta

import pandas as pd

from dagster import (
    op, job, Field, make_values_resource, Failure
    )
from sqlalchemy import create_engine

# custom modules
from etl_ga.ga4.ga4_landing_page_bounce.config import (
    country_domain_to_country_id,
    country_domain_to_property_id,
    blog_countries_property_id,
    career_advice_property_id,
    company_property_id,
    career_property_id,
    request_params,
    analytics,
    recruiting_property_list,
    ea_property_list,
)

from ..logs_messages_tools.messages import send_dwh_alert_slack_message
from .etl_ga_functions import get_report, get_request_body, report_to_dataframe, write_to_dwh
from ...utils.db_config import HOST, PORT, DATABASE, USER, PASSWORD
from .config import analytics, dwh_conn_psycopg2



# Define the schema and table names for storing GA4 API data
SCHEMA = 'imp_api'
TABLE_NAME = 'ga4_landing_page_bounce'
DTE = 'ga_dte_landing_page_bounce'

# Calculate the start and end dates for the data request
# The end date is yesterday, and the start date is two days ago
DATE_END = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
DATE_START = (datetime.now().date() - timedelta(2)).strftime('%Y-%m-%d')


# Send request to GA4 API for page_type: 'company', 'companies', 'blog', 'career-advice', 'career', 'salary' 
# and save into: imp_api.ga4_landing_page_bounce
# Send request to GA4 API for page_type: 'l' and save into: imp_api.ga_dte_landing_page_bounce
PAGE_TYPES = [
    'company', 'companies', 'blog', 'career-advice', 'career', 'salary', 'l', 'ea',
]




def dwh_engine():
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
    return engine


def delete_history_data(context, delete_date_start, delete_date_end, page, table_name):
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''delete from imp_api.{table_name} 
                                where action_date::date between '{delete_date_start}' and '{delete_date_end}'
                                    and page_name like '%{page}%';''')
                conn.commit()
                cur.close()
            context.log.info(f"Deleted between '{delete_date_start}' and '{delete_date_end}' for {page} from {SCHEMA}.{table_name}\n")
    except Exception as e:
        context.log.info(f'deleting error: {e}')


def ga4_landing_page_request_handler(context, date_start, date_end, page_types):
    try:        
        context.log.info(f'====== [{TABLE_NAME}] started - parse date: {date_start}/-/{date_end}')
        
        #create engine
        engine = dwh_engine()
        context.log.info('Connected to GA4 service')

        # get table names
        table_names = request_params.keys()

        for tn in table_names:

            for page_type in page_types:
                # set current table name
                current_table_name = TABLE_NAME

                context.log.info(f"page_type: {page_type}")
                dataframes = {}
                rows_cnt_acc = 0
                
                # if blog, connect to [fr, ca, ua] from Jooble Special Projects
                if page_type == 'blog':
                    property_ids = blog_countries_property_id
                elif page_type == 'career-advice':
                    property_ids = career_advice_property_id
                elif page_type == 'company' or page_type == 'companies':
                    property_ids = company_property_id
                elif page_type == 'career':
                    property_ids = career_property_id
                elif page_type == 'salary':
                    property_ids = country_domain_to_property_id
                elif page_type in ['l', 'ea']:
                    property_ids = recruiting_property_list if page_type == 'l' else ea_property_list
                else:
                    raise Exception(f'Unknown page_type: {page_type}')
                

                for domain in property_ids:
                    # get report for every domain
                    request_body = get_request_body(
                        context,
                        table_name=current_table_name,
                        requests_params=request_params,
                        date_start=date_start,
                        date_end=date_end,
                        page_type=page_type
                    )

                    property_id = property_ids.get(domain)
                    report = get_report(property_id, analytics, body=request_body)

                    # save report to json
                    # json.dump(report, open(f'logs/{table_name}_{domain}_{parse_date}.json', 'w'), indent=4)

                    rows_cnt = report.get("reports")[0].get("rowCount") or 0

                    rows_cnt_acc += rows_cnt
                    tn = current_table_name if page_type not in ['l', 'ea'] else DTE
                    context.log.info(f'{domain} for {tn}: {rows_cnt}')

                    df = report_to_dataframe(report, current_table_name)
                    df['country_domain'] = domain
                    df['country_id'] = country_domain_to_country_id.get(domain)
                    # add column page_name
                    df['page_name'] = f"{page_type}"
                    
                    if page_type == 'companies':
                        df['page_name'] = "company"
                    
                    if page_type == 'career' and not df.empty:
                        df.loc[df['landing_page'].str.contains(r'/career/.+/job-description'), 'page_name'] = 'career-job-description'
                        df.loc[df['landing_page'].str.contains(r'/career/.+/skills'), 'page_name'] = 'career-skills'
                    
                    # add to df's list
                    dataframes[domain] = df
                
                # union dataframes for different domains
                result_df = pd.concat(dataframes.values())
                
                if page_type in ['l', 'ea']:
                    current_table_name = DTE

                write_to_dwh(
                    context,
                    schema=SCHEMA,
                    table_name=current_table_name,
                    dataframe=result_df,
                    engine=engine
                )


            send_dwh_alert_slack_message(f":add: *SALARY-PAGE: {TABLE_NAME}*\n"
                                         f">*[{date_start}/-/{date_end}]*")
    except Exception as e:
        raise Failure (description=f"error: {e}")



@op(required_resource_keys={'globals'})
def delete_historical_data_op(context):
    """delete historical data

    Args:
        context (_type_): _description_

    Returns:
        _type_: _description_
    """
    date_start = context.resources.globals["date_start"]
    date_end = context.resources.globals["date_end"]
    page_names = context.resources.globals["page_types"]
    
    # В DTE з’явилась нова сторінка - https://ua.jooble.org/l/recruiting/#r2r (тільки для UA)
    # Створити табличку imp_api.ga_dte_r2r_landing_page_bounce, яка буде мати аналогічну структуру як і наша табличка
    # для дод сторінок АГГ imp_api.ga_landing_page_bounce. (Дані з ГА4)
    # imp_api.ga_dte_r2r_landing_page_bounce -> renamed to imp_api.ga_landing_page_bounce AND countries: ua, hu, ro
    for page in page_names:
        if page in ['l', 'ea']:
            table_name = DTE
        else:
            table_name = TABLE_NAME
        
        context.log.info(f"delete from {SCHEMA}.{table_name}, {page}")

        delete_history_data(context, date_start, date_end, page, table_name)
    
    return True


@op(required_resource_keys={'globals'})
def ga4_landing_page_api_request_op(context, deletion_result):
    """save api request result df to dwh

    Args:
        context (_type_): _description_
        deletion_result (_type_): _description_
    """
    date_start = context.resources.globals["date_start"]
    date_end = context.resources.globals["date_end"]
    page_types = context.resources.globals["page_types"]
    
    ga4_landing_page_request_handler(context, date_start, date_end, page_types)
    


@job(
    name='ga4__landing_page_bounce',
    description=f'{SCHEMA}.{TABLE_NAME}\n{SCHEMA}.{DTE}',
    resource_defs={"globals": make_values_resource(date_start=Field(str, default_value=DATE_START),
                                                    date_end=Field(str, default_value=DATE_END),
                                                    page_types=Field(list, default_value=PAGE_TYPES)
                                                                        )}
)
def ga4_landing_page_bounce_job():
    deletion_result = delete_historical_data_op()
    ga4_landing_page_api_request_op(deletion_result)
