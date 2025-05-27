# python modules
import datetime
import traceback
from datetime import datetime, timedelta

# third-party modules
import pandas as pd

from dagster import op, job, make_values_resource, Field
from sqlalchemy import create_engine

# custom modules
from .config import (
    country_domain_to_country_id,
    country_domain_to_country_id_rs,
    country_domain_to_property_id,
    country_domain_to_property_id_rs,
    request_params,
    request_params_rs,
    analytics
)

from ..logs_messages_tools.messages import send_dwh_alert_slack_message
from ...utils.dwh_db_operations import delete_data_from_dwh_table
from .etl_ga_functions import get_report, get_request_body, report_to_dataframe, write_to_dwh
from ...utils.db_config import HOST, PORT, DATABASE, USER, PASSWORD

START_DATE = (datetime.today() - timedelta(days=3)).strftime("%Y-%m-%d")
END_DATE = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


@op(required_resource_keys={'globals'})
def ga4_dte_delete_from_DWH(context):
    start_date_str = context.resources.globals["reload_date_start"]
    end_date_str = context.resources.globals["reload_date_end"]
    countries = context.resources.globals["countries_list"]

    date_end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1)

    if countries == '34':
        # RS, country_id = 34, imp_api.ga4_rs_dte_traffic_channel
        delete_data_from_dwh_table(context=context, SCHEMA='imp_api',
                                   TABLE_NAME='ga4_dte_ea_sha_reg_rs',
                                   DELETE_COUNTRY_COLUMN='country_id',
                                   DELETE_DATE_DIFF_COLUMN='action_date_time',
                                   launch_countries=countries,
                                   launch_datediff_start=start_date_str.replace('-', '') + '%',
                                   launch_datediff_end=date_end_date.strftime('%Y-%m-%d').replace('-', '') + '%',
                                   id_list=countries
                                   )
    else:
        delete_data_from_dwh_table(context=context, SCHEMA='imp_api',
                                   TABLE_NAME='ga4_dte_ea_sha_reg',
                                   DELETE_COUNTRY_COLUMN='country_id',
                                   DELETE_DATE_DIFF_COLUMN='action_date_time',
                                   launch_countries=countries,
                                   launch_datediff_start=start_date_str.replace('-', '') + '%',
                                   launch_datediff_end=date_end_date.strftime('%Y-%m-%d').replace('-', '') + '%',
                                   id_list=countries
                                   )
        delete_data_from_dwh_table(context=context, SCHEMA='imp_api',
                                   TABLE_NAME='ga4_dte_ea_reg',
                                   DELETE_COUNTRY_COLUMN='country_id',
                                   DELETE_DATE_DIFF_COLUMN='action_date',
                                   launch_countries=countries,
                                   launch_datediff_start=start_date_str,
                                   launch_datediff_end=end_date_str,
                                   id_list=countries
                                   )
        delete_data_from_dwh_table(context=context, SCHEMA='imp_api',
                                   TABLE_NAME='ga4_dte_traffic_channel',
                                   DELETE_COUNTRY_COLUMN='country_id',
                                   DELETE_DATE_DIFF_COLUMN='action_date',
                                   launch_countries=countries,
                                   launch_datediff_start=start_date_str,
                                   launch_datediff_end=end_date_str,
                                   id_list=countries
                                   )

    return True


@op(required_resource_keys={'globals'})
def ga4_dte_api_request(context, delete_from_dwh=None):
    try:
        # if countries == 1, 10, 11 (main dte countries), else get tablename for rs only
        params = request_params if context.resources.globals["countries_list"] == '1, 10, 11' else request_params_rs
        table_names = params.keys()
        get_property_id = country_domain_to_property_id if context.resources.globals[
                                                               "countries_list"] == '1, 10, 11' else country_domain_to_property_id_rs
        get_country_id = country_domain_to_country_id if context.resources.globals[
                                                             "countries_list"] == '1, 10, 11' else country_domain_to_country_id_rs

        start_date = context.resources.globals["reload_date_start"]
        end_date = context.resources.globals["reload_date_end"]

        engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

        context.log.info('connected to service')

        for table_name in table_names:
            context.log.info(f'[{table_name}] started - parse date: between {start_date} and {end_date}')

            dataframes = {}
            rows_cnt_acc = 0

            for domain in get_property_id:
                # get report for every domain
                request_body = get_request_body(
                    table_name=table_name,
                    requests_params=params,
                    start_date=start_date,
                    end_date=end_date
                )

                property_id = get_property_id.get(domain)
                report = get_report(property_id, analytics, body=request_body)

                # json.dump(report, open(f'logs/{table_name}_{domain}_{parse_date}.json', 'w'), indent=4)

                rows_cnt = report.get("reports")[0].get("rowCount") or 0

                rows_cnt_acc += rows_cnt
                context.log.info(f'Got rows from {domain} for {table_name}: {rows_cnt}')

                df = report_to_dataframe(report, table_name)
                df['country_domain'] = domain
                df['country_id'] = get_country_id.get(domain)

                dataframes[domain] = df
            # union dataframes for different domains
            result_df = pd.concat(dataframes.values())

            write_to_dwh(
                table_name=table_name,
                dataframe=result_df,
                engine=engine
            )

            context.log.info(f'{table_name} report with {rows_cnt_acc} rows\n\n')

            countries_count = int(result_df['country_id'].nunique())
            send_dwh_alert_slack_message(f":add: *DTE: {table_name}*\n"
                                         f">*{start_date}-{end_date}:* {countries_count} *countries* & {result_df.shape[0]} *rows*")
    except Exception as e:
        context.log.error(f'{e}\n'
                          f'{traceback.format_exc()}\n')
        print(traceback.format_exc())
        send_dwh_alert_slack_message("Error on GA4 integration", e)


@job(resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=START_DATE),
                                                    reload_date_end=Field(str, default_value=END_DATE),
                                                    countries_list=Field(str, default_value='1, 10, 11')
                                                    )},
     name='ga4__dte',
     description='imp_api.ga4_dte')
def ga4_dte_job():
    delete_from_dwh = ga4_dte_delete_from_DWH()
    ga4_dte_api_request(delete_from_dwh)


@job(resource_defs={"globals": make_values_resource(reload_date_start=Field(str, default_value=START_DATE),
                                                    reload_date_end=Field(str, default_value=END_DATE),
                                                    countries_list=Field(str, default_value='34'))},
     name='ga4__ga4_dte_ea_sha_reg_rs',
     description='imp_api.ga4_dte_ea_sha_reg_rs')
def ga4_dte_ea_sha_reg_rs_job():
    delete_from_dwh = ga4_dte_delete_from_DWH()
    ga4_dte_api_request(delete_from_dwh)
