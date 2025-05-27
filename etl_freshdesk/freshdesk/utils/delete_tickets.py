from etl_freshdesk.freshdesk.utils.utils import get_update_time, write_update_log, paginate_api_requests, \
    get_credentials, UnsuccessfulRequest
from etl_freshdesk.freshdesk.utils.database_io import execute_query
from etl_freshdesk.freshdesk.utils.sql_scripts import q_delete_tickets_referenced, q_delete_ids
from etl_freshdesk.freshdesk.utils.config import TABLES_W_FK

from datetime import datetime


def delete_tickets_from_db(ticket_ids: list, table_list: list, schema: str) -> None:
    """Delete info on specified ticket ids from database
        (in 'tickets' table and tables where ticket id is a foreign key).

    Args:
        ticket_ids (list): list of ticket ids to delete.
        table_list (list): list of tables where ticket id is a foreign key, so data needs to be deleted from them
            before deleting ticket from 'tickets' table.
        schema (str): database schema to delete tickets from (either 'freshdesk_internal' or 'freshdesk_external').
    """
    for table in table_list:
        del_q = q_delete_tickets_referenced.format(schema=schema, tickets=', '.join([str(x) for x in ticket_ids]),
                                                   table=table)
        execute_query(del_q)
    del_q = q_delete_ids.format(schema=schema, table='tickets', id_list=', '.join([str(x) for x in ticket_ids]))
    execute_query(del_q)


def update_deleted_tickets(filter_str: str, update_type: int, fd_type: str) -> None:
    """Collect data on deleted tickets from Freshdesk API and delete data on them from the database.

    Args:
        filter_str (str): filter string to use in the API request (should be either 'spam' or 'deleted').
        update_type (int): integer code for update type. Possible values for this function:
            2: delete tickets marked as spam;
            3: delete deleted tickets.
        fd_type (str): ticket system type (either 'internal' or 'external').
    """
    # update_type: 2 for spam, 3 for deleted
    table_list = TABLES_W_FK[fd_type]

    domain, api_key, schema, api_pass = get_credentials(fd_type)
    update_time = get_update_time(schema, update_type)
    ut = datetime.utcnow()
    req_url = f'https://{domain}.freshdesk.com//api/v2/tickets?filter={filter_str}&' \
              f'updated_since={update_time}&per_page=100&order_type=asc&order_by=updated_at&page='
    status_codes = [0]
    try:
        result, status_codes = paginate_api_requests(req_url, fd_type)
        ticket_ids = [x['id'] for x in result if x[filter_str] is True]
        if len(ticket_ids) > 0:
            delete_tickets_from_db(ticket_ids, table_list, schema)
        write_update_log(schema, ut, max(status_codes), update_type, 1)
    except UnsuccessfulRequest as e:
        write_update_log(schema, ut, e.status_code, update_type, 0)
    except Exception as e:
        write_update_log(schema, ut, max(status_codes), update_type, 0)
        raise e
