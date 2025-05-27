from etl_freshdesk.freshdesk.utils.database_io import select_to_dataframe, execute_query
from etl_freshdesk.freshdesk.utils.sql_scripts import q_select_update_time, q_insert_update_time

from datetime import datetime
import pandas as pd
import requests
import re
from typing import Union, Tuple
from utility_hub.core_tools import get_creds_from_vault

INT_API_KEY = get_creds_from_vault('FD_INT_API_KEY')
EXT_API_KEY = get_creds_from_vault('FD_EXT_API_KEY')
INT_DOMAIN = get_creds_from_vault('FD_INT_DOMAIN')
EXT_DOMAIN = get_creds_from_vault('FD_EXT_DOMAIN')


def get_update_time(schema: str, update_type: int) -> str:
    """Collect most recent update time in the given database and for given update type
        (returned as string in YYYY-MM-DDThh:mm:ssTZD format).

    Args:
        schema (str): database schema to collect data from (either 'freshdesk_internal' or 'freshdesk_external').
        update_type (int): integer code for update type. Possible values:
            1: update tickets info;
            2: delete tickets marked as spam;
            3: delete deleted tickets;
            4: collect satisfaction surveys.
            5: update solution articles info.

    Returns:
        str: datetime of the last update in the YYYY-MM-DDThh:mm:ssTZD format.
    """
    q = q_select_update_time.format(schema=schema, type=update_type)
    ut = select_to_dataframe(q).iloc[0, 0]
    return datetime.strftime(ut, '%Y-%m-%dT%H:%M:%SZ')


def write_update_log(schema: str, update_time: datetime, status_code: int, update_type: int,
                     is_successful: int) -> None:
    """Write a record about data update to the update_history table in the given database.

    Args:
        schema (str): database schema to insert data in. Should be either 'freshdesk_internal' or 'freshdesk_external'.
        update_time (datetime): datetime of the current update.
        status_code (int): status code of the API request used to collect data
            (if several requests were made, the maximum status code is recorded).
        update_type (int): integer code for update type. Possible values:
            1: update tickets info;
            2: delete tickets marked as spam;
            3: delete deleted tickets;
            4: collect satisfaction surveys.
            5: update solution articles info.
        is_successful (int): 1 if launch was successful, 0 otherwise.
    """
    q = q_insert_update_time.format(schema=schema, ut=update_time, status=status_code, type=update_type,
                                    is_successful=is_successful)
    execute_query(q)


def convert_num(s: str) -> Union[int, str]:
    """Convert number stored as string to integer data type. If not possible, returns original string.

    Args:
        s (str): string to convert to integer.

    Returns:
        int or str: converted string to integer or the original string if data type conversion could not be completed.
    """
    try:
        return int(s)
    except ValueError:
        return s
    except TypeError:
        return s


def get_date_string(date_str: str) -> Union[datetime, None]:
    """Converts string datetime in YYYY-MM-DDThh:mm:ssTZD format into datetime object.

    Args:
        date_str (str): string datetime in YYYY-MM-DDThh:mm:ssTZD format.

    Returns:
        datetime: converted string to datetime (or None if None was passed as argument).
    """
    if pd.isnull(date_str):
        return None
    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')


def get_array_of_vals(s: str) -> str or None:
    """Converts string with numbers into string of array with numbers to insert into the database as array.
        Example: '1, 56, 424 5' -> '{1, 56, 424, 5}'

    Args:
        s (str): string with numbers.

    Returns:
        str: string of array with integers to insert into the database as array.
    """
    if isinstance(s, str):
        val_num_list = re.findall(r'\d+', s)
        str_array = '{' + ', '.join([str(i) for i in val_num_list if 0 < int(i) < 2 * 10 ** 9]) + '}'
        return str_array if len(str_array) > 2 else None


def validate_str(s: str) -> str or None:
    """Validates string input: string must be less than 100 characters long (otherwise current function returns None)
    and contain no single quotes (single quotes are removed by this function if any).

    Args:
        s (str): string to validate.

    Returns:
        str or None: validated string.
    """
    if isinstance(s, str):
        s_val = s.replace("'", "")
        return s_val if len(s_val) <= 100 else None


def get_credentials(fd_type: str) -> Tuple[str, str, str, str]:
    """For the given ticket system return tuple with its domain, api key, database name and api password.

    Args:
        fd_type (str): ticket system type (either 'internal' or 'external').

    Returns:
        (str, str, str, str):
            domain to use in API request,
            api key,
            name of the database schema for given ticket system (either 'freshdesk_internal' or 'freshdesk_external'),
            api password ('xxx' as currently Freshdesk API does not require password).
    """
    if fd_type == 'external':
        domain, api_key = EXT_DOMAIN, EXT_API_KEY
    else:
        domain, api_key = INT_DOMAIN, INT_API_KEY
    schema, api_pass = 'freshdesk_{}'.format(fd_type), 'xxx'
    return domain, api_key, schema, api_pass


def paginate_api_requests(request_url: str, fd_type: str) -> Tuple[list, list]:
    """Send API request and in case if number of results in request response is greater than 100, send request with
        next page number. Return list of results from all sent API requests.

    Args:
        request_url (str): URL of API request to send. Should end with 'page=' in order for function to iterate over
            pages of API requests results.
        fd_type (str): ticket system type (either 'internal' or 'external').

    Returns:
        tuple of two lists: list of API requests results and list of API requests status codes.
    """
    domain, api_key, schema, api_pass = get_credentials(fd_type)
    page = 1
    status_codes = [0]
    result = []
    for i in range(89):
        r = requests.get(request_url + str(page), auth=(api_key, api_pass))
        status_codes.append(r.status_code)
        if r.status_code == 200:
            result += r.json()
        else:
            raise UnsuccessfulRequest(r.status_code)
        if len(r.json()) == 100:
            page += 1
        else:
            break
    return result, status_codes


class UnsuccessfulRequest(Exception):
    """Raised when API request returns status code that is not 200."""
    def __init__(self, status_code):
        self.status_code = status_code
