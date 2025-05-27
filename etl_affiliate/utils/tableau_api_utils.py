import xml.etree.ElementTree as ElementTree
import requests
import pandas as pd
from etl_affiliate.utils.utils import create_conn
from utility_hub.core_tools import get_creds_from_vault

xmlns = {"t": "http://tableau.com/api"}
server_name = "http://tableau.jooble.com"

SQL_TASK_ID_BY_DATASOURCE = """
select ter.id
from dc.tableau_extract_refresh ter
join dc.tableau_datasource td
    on td.rest_api_id = ter.datasource_id
where td.datasource_name = '{name}'
order by next_run_datetime desc
limit 1;
"""

SQL_TASK_ID_BY_WORKBOOK = """
select ter.id
from dc.tableau_extract_refresh ter
join dc.tableau_workbook tw
    on tw.rest_api_id = ter.workbook_id
where tw.workbook_name = '{name}'
order by next_run_datetime desc
limit 1;
"""


class ApiCallError(Exception):
    """ ApiCallError """
    pass


def _encode_for_display(text):
    """
    Encodes strings, so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.
    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").

    Args:
        text (str): string to encode.

    Returns:
        str: encoded string.
    """
    return text.encode("ascii", errors="backslashreplace").decode("utf-8")


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.

    Args:
        server_response: the response received from the server.
        success_code: the expected success code for the response.

    Raises:
         ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ElementTree.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find("t:error", namespaces=xmlns)
        summary_element = parsed_response.find(".//t:summary", namespaces=xmlns)
        detail_element = parsed_response.find(".//t:detail", namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get("code", "unknown") if error_element is not None else "unknown code"
        summary = summary_element.text if summary_element is not None else "unknown summary"
        detail = detail_element.text if detail_element is not None else "unknown detail"
        error_message = "{0}: {1} - {2}".format(code, summary, detail)
        raise ApiCallError(error_message)
    return


def sign_in(server, token_name, token_secret, site=""):
    """
    Signs in to the server specified with the given credentials.

    Args:
        server (str): specified server address (e.g., "tableau.jooble.com").
        token_name (str): name of the Personal Access Token created in Tableau server.
        token_secret (str): Personal Access Token secret.
        site (str): is the ID (as a string) of the site on the server to sign in to. The default is "", which signs in
            to the default site.

    Returns:
        tuple of authentication token, site_id, user_id to use in API requests.
    """
    url = server + "/api/{0}/auth/signin".format(3.14)

    # Builds the request
    xml_request = ElementTree.Element("tsRequest")
    credentials_element = ElementTree.SubElement(
        xml_request, "credentials", personalAccessTokenName=token_name, personalAccessTokenSecret=token_secret)
    ElementTree.SubElement(credentials_element, "site", contentUrl=site)
    xml_request = ElementTree.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ElementTree.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find("t:credentials", namespaces=xmlns).get("token")
    site_id = parsed_response.find(".//t:site", namespaces=xmlns).get("id")
    user_id = parsed_response.find(".//t:user", namespaces=xmlns).get("id")
    return token, site_id, user_id


def get_task_id(workbook_name: str or None, datasource_name: str or None) -> str:
    """
    Finds the extract refresh task identifier using DWH tables dc.tableau_extract_refresh and dc.tableau_datasource or
    dc.tableau_workbook using the workbook name (to get extract refresh task identifier for an embedded datasource
    refresh) or the datasource name (to get extract refresh task identifier for a published datasource refresh).
    If workbook_name is specified, the task id for the embedded datasource is returned; otherwise the datasource_name
    is used in SQL query.

    Args:
        workbook_name (str or None): name of the workbook with an embedded datasource to launch extract refresh for.
        datasource_name (str or None): name of the published datasource to launch extract refresh for.

    Returns:
        str: the task_id to launch extract refresh task for the specified datasource or workbook.
    """
    if workbook_name is not None:
        query = SQL_TASK_ID_BY_WORKBOOK.format(name=workbook_name)
    else:
        query = SQL_TASK_ID_BY_DATASOURCE.format(name=datasource_name)
    task_id = pd.read_sql(query, con=create_conn("dwh")).values[0][0]
    return task_id


def run_extract_refresh(task_id: str) -> None:
    """
    Makes a post request to Tableau REST API to launch extract refresh task.

    Args:
        task_id (str): the identifier of the extract refresh task to launch.
    """
    xml_body = """<?xml version='1.0' encoding='utf-8'?><tsRequest></tsRequest>"""
    auth_token, site_name, user = sign_in(server_name, token_name=get_creds_from_vault("TOKEN_NAME"),
                                          token_secret=get_creds_from_vault("TOKEN_SECRET"), site="")

    request_url = server_name + "/api/{0}/sites/{1}/tasks/extractRefreshes/{2}/runNow".format(3.14, site_name, task_id)
    response = requests.post(request_url, headers={"x-tableau-auth": auth_token}, data=xml_body)
    _check_status(response, 200)
