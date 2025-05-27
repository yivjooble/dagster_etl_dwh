import requests
import re
import pandas as pd
import numpy as np
import json
import traceback

from pathlib import Path
from dagster import Failure

CREDS_DIR = Path(__file__).parent / 'credentials'

with open(f'{CREDS_DIR}/credentials.json', 'r') as f:
    cred = json.load(f)
username = cred['username']
password = cred['password']
solution_name = cred['solution_name']


def get_crm_cookies():
    try:
        payload = {
            "UserName": username,
            "UserPassword": password,
            "SolutionName": solution_name,
            "TimeZoneOffset": -120,
            "Language": "Ru-ru"
        }
        r = requests.post('https://jooble.creatio.com/ServiceModel/AuthService.svc/Login', json=payload, timeout=3000)
        cookies = r.cookies
        str_cookie = r.headers.get('Set-Cookie')
        bpmcsrf = re.findall('BPMCSRF=(.+?);', str_cookie)[0]

        headers = {
            'BPMCSRF': bpmcsrf,
            'Accept': 'application/json;odata=verbose',
            'Content-Type': 'application/json;odata=verbose'
        }

        return cookies, headers
    except Exception:
        raise Failure(f"Failed to get CRM cookies: {traceback.format_exc()}")


def get_sql_query_results(query):
    try:
        cookies, headers = get_crm_cookies()
        resp = requests.post(
            url='https://jooble.creatio.com/0/rest/SqlConsoleService/ExecuteSqlScript',
            json={"sqlScript": query},
            cookies=cookies, headers=headers,
            timeout=3000)

        resp = resp.json()['ExecuteSqlScriptResult']['QueryResults'][0]

        columns = resp['Columns']
        rows = resp['Rows']

        df = pd.DataFrame(columns=columns, data=rows)
        df = df.replace('NULL', np.nan)
        return df
    except Exception:
        raise Failure(f"Failed to get SQL query results: {traceback.format_exc()}\n "
                      f"response:\n {resp.json()}")
