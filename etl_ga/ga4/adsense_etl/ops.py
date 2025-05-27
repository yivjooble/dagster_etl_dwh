# python modules
import argparse

# third-party modules
import httplib2
import pandas as pd

from pathlib import Path
from datetime import timedelta, datetime
from oauth2client import client, file, tools
from googleapiclient.discovery import build
from sqlalchemy import create_engine

from dagster import (
    op, job, make_values_resource, Field, Failure
)

# custom modules
from ..logs_messages_tools.messages import send_dwh_alert_slack_message
from .config import request_params, CLIENT_ID, SCOPES, CLIENT_SECRETS_PATH
from ...utils.db_config import HOST, PORT, DATABASE, USER, PASSWORD
from ...utils.dwh_db_operations import execute_on_dwh
from utility_hub import DwhOperations

# Get today's date
today = datetime.now()

# Subtract one day to get yesterday's date
yesterday = today - timedelta(days=1)

# Extract the year, month, and day from the 'yesterday' date
YEAR = yesterday.year
MONTH = yesterday.month
YESTERDAY_DAY = yesterday.strftime("%d")
CREDS_PATH = Path(__file__).parent.parent / 'credentials'


def adsense_etl(context,
                start_date_year: int,
                start_date_month: int,
                start_date_day: int,
                end_date_year: int,
                end_date_month: int,
                end_date_day: int,
                destination_db: str
                ):
    """_summary_

    Args:
        context (_type_): _description_
        start_date_year (int): _description_
        start_date_month (int): _description_
        start_date_day (int): _description_
        end_date_year (int): _description_
        end_date_month (int): _description_
        end_date_day (int): _description_
        destination_db (str): _description_
    """
    tables_names = request_params.keys()

    start_date = f"{start_date_year}-{start_date_month}-{start_date_day}"
    end_date = f"{end_date_year}-{end_date_month}-{end_date_day}"
    context.log.info(f'adsense_etl started - parse date: {start_date}/-/{end_date}')

    service = get_service()
    context.log.info('connected to service')

    slack_msg = ':jooble: *Adsense aggregation*\n'

    # dict for final dataframes
    dataframes = {}

    for name in tables_names:

        if name in ('adsense_afc', 'adsense_afs', 'adsense_csa', 'adsense_adv'):
            # get 'adsense_afc', 'adsense_afs', 'adsense_csa', 'adsense_adv'  dataframes
            params = request_params.get(name)
            report = get_report(
                service=service,
                client_id=CLIENT_ID,
                currency_code='USD',
                start_date_year=start_date_year,
                start_date_month=start_date_month,
                start_date_day=start_date_day,
                end_date_year=end_date_year,
                end_date_month=end_date_month,
                end_date_day=end_date_day,
                **params
            )
            dataframes[name] = report_to_dataframe(report)
            rows_cnt = report.get("totalMatchedRows")
            context.log.info(f'Got rows for {name}: {rows_cnt}')

            # raise Failure if there is nothing from Adsense
            if rows_cnt is None or rows_cnt == 0:
                slack_msg = f':warning: *{name}* has *0* rows *({start_date}/-/{end_date})*\n\n'
                send_dwh_alert_slack_message(slack_msg + '<!subteam^S02ETK2JYLF|dwh.analysts>')
                raise Failure(description=f'{name} has 0 rows')
        else:
            # get dataframes for all tables from tables_names except 'adsense_afc' and 'adsense_afs' separately in USD and EUR
            dfs_diffs_by_currency = {}

            for currency in ('USD', 'EUR'):
                params = request_params.get(name)
                report = get_report(
                    service=service,
                    client_id=CLIENT_ID,
                    currency_code=currency,
                    start_date_year=start_date_year,
                    start_date_month=start_date_month,
                    start_date_day=start_date_day,
                    end_date_year=end_date_year,
                    end_date_month=end_date_month,
                    end_date_day=end_date_day,
                    **params
                )
                dfs_diffs_by_currency[currency] = report_to_dataframe(report)

                rows_cnt = report.get("totalMatchedRows")
                context.log.info(f'Got rows for {name} {currency}: {rows_cnt}')

            # join USD and EUR dataframes
            columns_for_join = list(dfs_diffs_by_currency['USD'].columns).remove('estimated_earnings_usd')

            dataframes[name] = pd.merge(
                dfs_diffs_by_currency.pop('USD'),
                dfs_diffs_by_currency.pop('EUR'),
                on=columns_for_join
            )

    # union 'adsense_product_revenue_afc' and 'adsense_product_revenue_afs' dataframes
    # dataframes['adsense_product_revenue'] = dataframes.pop('adsense_product_revenue_afc')._append(
    #     dataframes.pop('adsense_product_revenue_afs'), ignore_index=True
    # )

    def _execute(slack_msg: str, engine_creds: str):
        engine = create_engine(engine_creds)

        # write dataframes to dwh
        for table_name in dataframes:
            df = dataframes.get(table_name)
            slack_msg += f':white_check_mark: *{df.shape[0]}* rows are inserted into `{table_name}` *({start_date}/-/{end_date})*\n\n'
            write_to_dwh(table_name=table_name, dataframe=df, engine=engine)

        context.log.info(f'adsense report-{start_date}/-/{end_date} is written to DWH')
        send_dwh_alert_slack_message(slack_msg)

    if destination_db == "both":
        # dwh
        _execute(slack_msg=slack_msg, engine_creds=f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

        # cloudberry
        _execute(slack_msg=slack_msg, engine_creds=f'postgresql://{USER}:{PASSWORD}@10.0.1.83:{PORT}/an_dwh')

    if destination_db == "dwh":
        # dwh
        _execute(slack_msg=slack_msg, engine_creds=f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')

    if destination_db == "cludberry":
        # dwh
        _execute(slack_msg=slack_msg, engine_creds=f'postgresql://{USER}:{PASSWORD}@10.0.1.83:{PORT}/an_dwh')


# from google.oauth2 import service_account
# from google.auth.transport.requests import Request
# from google.oauth2.service_account import Credentials
# from google_auth_httplib2 import AuthorizedHttp
# from googleapiclient.discovery import build
# import httplib2


# def get_service():
#     """Initializes the adsense report service object.

#     Returns:
#         analytics an authorized adsense report service object.
#     """
#     # Load the service account credentials
#     credentials = service_account.Credentials.from_service_account_file(
#         SERVICE_ACCOUNT_FILE, scopes=SCOPES)

#     # Authorize the HTTP object
#     http = AuthorizedHttp(credentials, http=httplib2.Http())

#     # Build the service object
#     service = build("adsense", "v2", http=http)

#     return service


def get_service():
    """Initializes the adsense report service object.

  Returns:
    analytics an authorized adsense report service object.
  """
    # Parse command-line arguments.Setting flags.noauth_local_webserver to True allows to run script without web browser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])
    flags.noauth_local_webserver = True

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        CLIENT_SECRETS_PATH, scope=SCOPES,
        message=tools.message_if_missing(CLIENT_SECRETS_PATH))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage(f'{CREDS_PATH}/adsensereporting.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    service = build("adsense", "v2", http=http)

    return service


def get_report(service,
               client_id,
               dimensions,
               metrics,
               currency_code,
               start_date_year: int,
               start_date_month: int,
               start_date_day: int,
               end_date_year: int,
               end_date_month: int,
               end_date_day: int,
               filters=None):
    """
    makes request to Adsense API
    returns Adsense Report as dict

    more info https://developers.google.com/adsense/management/reference/rest/v2/accounts.reports/generate?hl=ru

    :param service:
    :param client_id:
    :param dimensions:
    :param metrics:
    :param currency_code:
    :param filters:
    :return:
    """

    return service.accounts().reports().generate(
        account=f"accounts/{client_id}",
        dateRange='CUSTOM',
        currencyCode=currency_code,
        dimensions=dimensions,
        metrics=metrics,
        filters=filters,
        startDate_year=start_date_year,
        startDate_month=start_date_month,
        startDate_day=start_date_day,
        endDate_year=end_date_year,
        endDate_month=end_date_month,
        endDate_day=end_date_day,
    ).execute()


def report_to_dataframe(report):
    data = []
    headers = []

    # get and rename headers from report
    for header in report.get('headers', []):
        currency_code = header.get('currencyCode')
        column_header = header.get('name') if currency_code is None else header.get('name') + '_' + currency_code

        if column_header == 'DATE':
            column_header = 'ACTION_DATE'

        headers.append(column_header.lower())

    # get data from report and transform to pd.DataFrame
    for row in report.get('rows', []):
        row_data = []
        for value in row.get('cells'):
            row_data.append(value.get('value'))
        data.append(row_data)

    return pd.DataFrame(data, columns=headers)


def write_to_dwh(table_name, dataframe, engine, schema='imp_api'):
    if table_name in ('adsense_csa', 'adsense_adv'):
        # Define column mappings
        column_mappings = {
            'adsense_csa': {
                'action_date': 'create_date',
                'custom_channel_name': 'custom_channel',
                'country_name': 'country',
                'impressions': 'impressions',
                'clicks': 'clicks',
                'impressions_rpm_usd': 'revenue_per_thousand_impressions_usd',
                'active_view_viewability': 'active_view_viewable',
                'estimated_earnings_usd': 'estimated_earnings_usd'
            },
            'adsense_adv': {
                'action_date': 'create_date',
                'impressions': 'impressions',
                'clicks': 'clicks',
                'impressions_rpm_usd': 'revenue_per_thousand_impressions_usd',
                'active_view_viewability': 'active_view_viewable',
                'estimated_earnings_usd': 'estimated_earnings_usd'
            }
        }

        # Rename columns based on the mapping
        dataframe = dataframe.rename(columns=column_mappings[table_name])

        # Select only the mapped columns
        dataframe = dataframe[list(column_mappings[table_name].values())]

    # Write the DataFrame to the database
    dataframe.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)


@op(required_resource_keys={'globals'})
def delete_from_adsense_tables(context):
    """Delete previous dates from imp_api.adsense_custom_channels_revenue

    Args:
        context (_type_): _description_

    Raises:
        Failure: _description_

    Returns:
        _type_: _description_
    """
    destination_db = context.resources.globals["destination_db"]
    start_date_year = context.resources.globals["start_date_year"]
    start_date_month = context.resources.globals["start_date_month"]
    start_date_day = context.resources.globals["start_date_day"]
    end_date_year = context.resources.globals["end_date_year"]
    end_date_month = context.resources.globals["end_date_month"]
    end_date_day = context.resources.globals["end_date_day"]

    start_date = f"{start_date_year}-{start_date_month}-{start_date_day}"
    end_date = f"{end_date_year}-{end_date_month}-{end_date_day}"

    tables = [
        'adsense_custom_channels_revenue',
        'adsense_afc',
        'adsense_afs',
        'adsense_csa',
        'adsense_adv',
    ]

    try:
        for table in tables:
            if table in ('adsense_csa', 'adsense_adv'):
                q = f'''delete from imp_api.{table}
                        where create_date between '{start_date}' and '{end_date}';'''
                DwhOperations.execute_on_dwh(
                    context=context,
                    query=q,
                    destination_db=destination_db
                )
                context.log.info(f'''deleted from imp_api.{table}
                                where create_date between '{start_date}' and '{end_date}';''')
            else:
                q = f'''delete from imp_api.{table}
                        where action_date between '{start_date}' and '{end_date}';'''
                DwhOperations.execute_on_dwh(
                    context=context,
                    query=q,
                    destination_db=destination_db
                )
                context.log.info(f'''deleted from imp_api.{table}
                                where action_date between '{start_date}' and '{end_date}';''')

        return True
    except:
        raise Failure


@op(required_resource_keys={'globals'})
def adsense_etl_op(context, deletion=None):
    """Start main op

    Args:
        context (_type_): _description_
        deletion (_type_, optional): _description_. Defaults to None.
    """
    destination_db = context.resources.globals["destination_db"]
    start_date_year = context.resources.globals["start_date_year"]
    start_date_month = context.resources.globals["start_date_month"]
    start_date_day = context.resources.globals["start_date_day"]
    end_date_year = context.resources.globals["end_date_year"]
    end_date_month = context.resources.globals["end_date_month"]
    end_date_day = context.resources.globals["end_date_day"]
    adsense_etl(context=context,
                start_date_year=start_date_year,
                start_date_month=start_date_month,
                start_date_day=start_date_day,
                end_date_year=end_date_year,
                end_date_month=end_date_month,
                end_date_day=end_date_day,
                destination_db=destination_db)


@job(
    resource_defs={"globals": make_values_resource(start_date_year=Field(int, default_value=int(YEAR)),
                                                   start_date_month=Field(int, default_value=int(MONTH)),
                                                   start_date_day=Field(int, default_value=int(YESTERDAY_DAY)),
                                                   end_date_year=Field(int, default_value=int(YEAR)),
                                                   end_date_month=Field(int, default_value=int(MONTH)),
                                                   end_date_day=Field(int, default_value=int(YESTERDAY_DAY)),
                                                   destination_db=Field(str, default_value='both')
                                                   )},
    name='ga__adsense',
    metadata={
        'destination_db': 'dwh, cloudberry, both',
        'target_tables': """['imp_api.adsense_custom_channels_revenue'],
                            ['imp_api.adsense_afc'],
                            ['imp_api.adsense_afs'],
                            ['imp_api.adsense_csa'],
                            ['imp_api.adsense_adv']""",
    },
    description='imp_api.adsense_')
def adsense_etl_job():
    deletion = delete_from_adsense_tables()
    adsense_etl_op(deletion)
