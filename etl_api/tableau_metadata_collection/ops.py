import xml.etree.ElementTree as ElementTree

import requests
import xmltodict
from dagster import (
    op, job, fs_io_manager
)

from .tableau_api_functions import *
from ..utils.io_manager_path import get_io_manager_path
from ..utils.messages import send_dwh_alert_slack_message
from ..utils.api_jobs_config import retry_policy

DWH_SCHEMA = 'dc'
CREDS_DIR = Path(__file__).parent / 'credentials'

with open(f'{CREDS_DIR}/dwh.json') as json_file:
    dwh_cred = json.load(json_file)

USER = dwh_cred['user']
PASSWORD = dwh_cred['password']
HOST = dwh_cred['host']
DB = dwh_cred['database']

postgres_engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}/{DB}')
cloudberry_engine = create_engine(f'postgresql://{USER}:{PASSWORD}@an-dwh.jooble.com/an_dwh')

xmlns = {'t': 'http://tableau.com/api'}
server_name = 'http://tableau.jooble.com'

with open(f'{CREDS_DIR}/api_credentials.json') as json_file:
    api_cred = json.load(json_file)
TOKEN_NAME = api_cred['token_name']
TOKEN_SECRET = api_cred['token_secret']


class ApiCallError(Exception):
    """ ApiCallError """
    pass


def _encode_for_display(text):
    """
    Encodes strings, so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.
    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.
    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ElementTree.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(server, token_name, token_secret, site=""):
    """
    Signs in to the server specified with the given credentials
    'server'   specified server address
    'username' is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format(3.14)

    # Builds the request
    xml_request = ElementTree.Element('tsRequest')
    credentials_element = ElementTree.SubElement(
        xml_request, 'credentials', personalAccessTokenName=token_name, personalAccessTokenSecret=token_secret)
    ElementTree.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ElementTree.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ElementTree.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    user_id = parsed_response.find('.//t:user', namespaces=xmlns).get('id')
    return token, site_id, user_id


def get_tableau_extract_refresh():
    auth_token, site_name, user = sign_in(server_name, token_name=TOKEN_NAME, token_secret=TOKEN_SECRET, site="")

    request_url = server_name + "/api/{0}/sites/{1}/tasks/extractRefreshes".format(3.14, site_name)
    response = requests.get(request_url, headers={'x-tableau-auth': auth_token})
    _check_status(response, 200)
    json_response = xmltodict.parse(_encode_for_display(response.text))

    tasks_data = []
    for task in json_response['tsResponse']['tasks']['task']:
        er = task.get('extractRefresh')
        schedule = er.get('schedule')
        tasks_data.append({
            'id': er.get('@id'),
            'datasource_id': er.get('datasource', {}).get('@id'),
            'workbook_id': er.get('workbook', {}).get('@id'),
            'type': er.get('@type'),
            'schedule_state': schedule.get('@state'),
            'update_frequency': schedule.get('@frequency'),
            'next_run_datetime': schedule.get('@nextRunAt')
        })

    df = pd.DataFrame(tasks_data)
    df['next_run_datetime'] = pd.to_datetime(df['next_run_datetime'], format='ISO8601')
    # Insert data into postgres
    df.to_sql(
        'tableau_extract_refresh',
        con=postgres_engine,
        schema='dc',
        if_exists='append',
        index=False
    )
    # Insert data into cloudberry
    df.to_sql(
        'tableau_extract_refresh',
        con=cloudberry_engine,
        schema='dc',
        if_exists='append',
        index=False
    )


@op
def truncate_destination_table(context):
    """
    Truncate destination table: dc.tableau_extract_refresh
    """
    ddl_query = f'TRUNCATE TABLE {DWH_SCHEMA}.tableau_extract_refresh'
    # Truncate table in postgres
    with postgres_engine.connect() as connection:
        cursor = connection.connection.cursor()
        cursor.execute(ddl_query)
        connection.connection.commit()
        cursor.close()
        context.log.debug(f'Table {DWH_SCHEMA}.tableau_extract_refresh truncated in postgres')

    # Truncate table in cloudberry
    with cloudberry_engine.connect() as connection:
        cursor = connection.connection.cursor()
        cursor.execute(ddl_query)
        connection.connection.commit()
        cursor.close()
        context.log.debug(f'Table {DWH_SCHEMA}.tableau_extract_refresh truncated in cloudberry')


@op
def get_tableau_extract_refresh_op(truncate):
    get_tableau_extract_refresh()


@op(retry_policy=retry_policy)
def insert_tableau_metadata_op(context):
    workbooks, data_sources, wb_ds, physical_tables, ds_pt, database_tables = split_data(context)
    clear_data(DWH_SCHEMA)
    insert_tableau_metadata(DWH_SCHEMA, workbooks, data_sources, wb_ds, physical_tables, database_tables, ds_pt)
    send_dwh_alert_slack_message(f':white_check_mark: *tableau-metadata-collection DONE*')


@job(
    name='api__insert_tableau_metadata',
    description='Insert tableau metadata',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    tags={"data_model": f"{DWH_SCHEMA}"},
)
def insert_tableau_metadata_job():
    insert_tableau_metadata_op()


@job(
    name='api__get_tableau_extract_refresh',
    description='Get tableau extract refresh',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    tags={"data_model": f"{DWH_SCHEMA}"},
)
def get_tableau_extract_refresh_job():
    truncate = truncate_destination_table()
    get_tableau_extract_refresh_op(truncate)
