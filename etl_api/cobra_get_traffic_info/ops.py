import datetime
import json

import requests
import pandas as pd

from pathlib import Path
from sqlalchemy import create_engine

from ..utils.messages import send_dwh_alert_slack_message
from ..utils.io_manager_path import get_io_manager_path

from dagster import (
    op, job, fs_io_manager, make_values_resource, Field
)

# table columns
COUNTRY_CODE = 'country_code'
PROJECT_ID = 'project_id'
ALLOW_NONLOCAL_TRAFFIC = 'allow_nonlocal_traffic'
ALLOWED_COUNTRIES = 'allowed_countries'

DWH_SCHEMA = 'imp_api'
DWH_TABLE = 'cobra_info_projects_nonlocal'

creds_dir = Path(__file__).parent / 'credentials'

with open(f'{creds_dir}/cobra.json') as json_f:
    cobra_cred = json.load(json_f)

COBRA_URL = cobra_cred['cobra_url']

with open(f'{creds_dir}/dwh_cred.json') as json_file:
    dwh_cred = json.load(json_file)

USER = dwh_cred['user']
PASSWORD = dwh_cred['password']
HOST = dwh_cred['host']
PORT = dwh_cred['port']
DB = dwh_cred['database']

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')


def save_to_db(context, dataframe: pd.DataFrame, destination_db: str):
    if destination_db == 'dwh':
        dataframe.to_sql(DWH_TABLE, con=engine, schema=DWH_SCHEMA, if_exists='append', index=False)
        context.log.info(f'Data saved to dwh')
    elif destination_db == 'cloudberry':
        cloudberry_engine = create_engine(f'postgresql://{USER}:{PASSWORD}@an-dwh.jooble.com:{PORT}/an_dwh')
        dataframe.to_sql(DWH_TABLE, con=cloudberry_engine, schema=DWH_SCHEMA, if_exists='append', index=False)
        context.log.info(f'Data saved to cloudberry')
    elif destination_db == 'both':
        # save to dwh
        dataframe.to_sql(DWH_TABLE, con=engine, schema=DWH_SCHEMA, if_exists='append', index=False)
        context.log.info(f'Data saved to dwh')
        # save to cloudberry
        cloudberry_engine = create_engine(f'postgresql://{USER}:{PASSWORD}@an-dwh.jooble.com:{PORT}/an_dwh')
        dataframe.to_sql(DWH_TABLE, con=cloudberry_engine, schema=DWH_SCHEMA, if_exists='append', index=False)
        context.log.info(f'Data saved to cloudberry')


def main(context, destination_db: str):
    # get data from cobra
    response = requests.get(COBRA_URL, timeout=3000)
    result = {}
    for country, data in response.json().items():
        try:
            result[country] = data['desktop']['features']['projects']['localCountryProjects']
        except KeyError:
            result[country] = {}

    # transform data to pd.Dataframe
    table = []
    for country, data in result.items():
        if len(data) == 0:
            continue
        for project_id, allowed_countries in data.items():
            for allowed_country in allowed_countries:

                if len(allowed_countries) > 1 and allowed_country == country:
                    continue

                row = {}
                row[COUNTRY_CODE] = country
                row[PROJECT_ID] = project_id

                if len(allowed_countries) == 1:
                    row[ALLOW_NONLOCAL_TRAFFIC] = 'not allowed'
                    row[ALLOWED_COUNTRIES] = 'none'
                else:
                    row[ALLOW_NONLOCAL_TRAFFIC] = 'partially allowed'
                    row[ALLOWED_COUNTRIES] = allowed_country
                table.append(row)

    dataframe = pd.DataFrame(table, columns=[COUNTRY_CODE, PROJECT_ID, ALLOW_NONLOCAL_TRAFFIC, ALLOWED_COUNTRIES])
    dataframe['request_date'] = datetime.datetime.today().strftime("%Y-%m-%d")

    save_to_db(context, dataframe, destination_db)

    rows_cnt = dataframe.shape[0]

    today_date = datetime.datetime.today().strftime("%Y-%m-%d")
    send_dwh_alert_slack_message(
        f':white_check_mark: *{rows_cnt}* rows are inserted into `{DWH_SCHEMA}.{DWH_TABLE}` *({today_date})*\n\n'
    )
    pd.DataFrame.from_dict({
        'schema_name': [DWH_SCHEMA],
        'table_name': [DWH_TABLE],
        'log_date': [today_date],
        'last_updated': [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        'is_truncate': [0],
        'update_recurrence': ['daily'],
        'cron_string': ['5 9 * * *'],
        'job_name': ['cobra-get-traffic-info'],
        'job_start_time': ['09:05:00']
    }).to_sql('python_job_log', con=engine, schema='logs', if_exists='append', index=False)


@op(required_resource_keys={"globals"})
def start_cobra_info_projects_nonlocal_op(context):
    main(context, context.resources.globals["destination_db"])


@job(
    name='api__cobra_info_projects_nonlocal',
    description='Get info about projects that allow nonlocal traffic',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(
                       destination_db=Field(str, default_value="both")
                   )
                   }
)
def cobra_info_projects_nonlocal_job():
    start_cobra_info_projects_nonlocal_op()
