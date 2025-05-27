from etl_freshdesk.freshdesk.utils.database_io import execute_query, insert_to_db, select_to_dataframe
from etl_freshdesk.freshdesk.utils.utils import paginate_api_requests, get_update_time, write_update_log, \
    get_credentials, get_date_string, get_array_of_vals, UnsuccessfulRequest, validate_str
from etl_freshdesk.freshdesk.utils.parent_tables import update_parent_tables, get_parent_data, update_requesters
from etl_freshdesk.freshdesk.utils.sql_scripts import q_select_by_id, q_max_reply, q_update_ticket
from etl_freshdesk.freshdesk.utils.slack_logging import get_slack_logger, send_message
from etl_freshdesk.freshdesk.utils.config import SLACK_MESSAGE_TITLE, SLACK_CHANNEL, CUSTOM_TICKET_FIELDS, \
    TICKET_TABLE_COLS, PARENT_TABLE_FIELDS

from datetime import datetime
import pandas as pd


def collect_ticket_info(ticket: dict, parent_dictionaries: list, fd_type: str) -> dict:
    """Collect the necessary fields from the ticket dictionary obtained from Freshdesk API in the appropriate format.

    Args:
        ticket (dict): dictionary with ticket information obtained from Freshdesk API.
        parent_dictionaries (list): list of dictionaries with parent field values mapping from Freshdesk ids or names
            to the database ids.
        fd_type (str): ticket system type (either 'internal' or 'external').

    Returns:
        dict: dictionary with ticket fields in the format they are stored in the database.
    """
    ticket_info = {}
    common_fields_config = CUSTOM_TICKET_FIELDS[fd_type]
    if fd_type == 'internal':
        agents_dict, groups_dict, statuses_dict, channels_dict, priorities_dict, client_types_dict, categ_types_dict, \
            request_types_dict, ticket_types_dict, requesters_dict, vvt_flags_dict = parent_dictionaries

        ticket_info['id_project'] = get_array_of_vals(ticket['custom_fields']['cf_id_project'])
        ticket_info['id_user_auction'] = get_array_of_vals(ticket['custom_fields']['cf_id_user'])
        ticket_info['id_employer'] = get_array_of_vals(ticket['custom_fields']['cf_id_ea'])
        ticket_info['domain_project'] = validate_str(ticket['custom_fields']['cf_url'])
        ticket_info['login_auction'] = validate_str(ticket['custom_fields']['cf_email'])
        ticket_info['vvt_flags'] = sum([vvt_flags_dict[flag] for flag in vvt_flags_dict.keys()
                                        if ticket['custom_fields'][flag]])

        source_type = ticket['custom_fields']['cf_source_type']
        if source_type is not None:
            source_type = int(source_type.split(' - ')[0])
        ticket_info['source_type'] = source_type

        requester_fd_id = ticket['requester_id']
        if requester_fd_id not in requesters_dict:
            requesters_dict = update_requesters(requester_fd_id)
        ticket_info['requester_id'] = requesters_dict.get(requester_fd_id)

        if ticket['custom_fields']['cf_rand523360']:
            val_result = True if ticket['custom_fields']['cf_rand523360'] == 'Катить' else False
        else:
            val_result = None
        ticket_info['val_result'] = val_result

    else:
        agents_dict, groups_dict, statuses_dict, channels_dict, priorities_dict, client_types_dict, categ_types_dict, \
            request_types_dict, ticket_types_dict, job_complaint_action_dict = parent_dictionaries

        employer_project_id = ticket['custom_fields']['cf_id_employerid_project']
        ticket_info['employer_project_id'] = validate_str(employer_project_id)

        session_installation_id = ticket['custom_fields']['cf_session_idinstallation_id_new']
        ticket_info['session_installation_id'] = validate_str(session_installation_id)

        job_complaint_action_name = ticket['custom_fields']['cf_actions_on_job_complaints']
        ticket_info['job_complaint_action_id'] = job_complaint_action_dict.get(job_complaint_action_name)

    ticket_info['id'] = ticket['id']
    ticket_info['status_id'] = ticket['status']
    ticket_info['channel_id'] = ticket['source']
    ticket_info['priority_id'] = ticket['priority']
    ticket_info['tags'] = ', '.join(ticket['tags']) if len(ticket['tags']) > 0 else None
    ticket_info['created_at'] = get_date_string(ticket['created_at'])
    ticket_info['resolved_at'] = get_date_string(ticket['stats']['resolved_at'])
    ticket_info['updated_at'] = get_date_string(ticket.get('updated_at'))
    ticket_info['agent_response_at'] = get_date_string(ticket['stats']['agent_responded_at'])
    ticket_info['requester_response_at'] = get_date_string(ticket['stats']['requester_responded_at'])
    ticket_info['status_updated_at'] = get_date_string(ticket['stats']['status_updated_at'])
    ticket_info['first_reply_at'] = get_date_string(ticket['stats']['first_responded_at'])
    ticket_info['ticket_type'] = ticket_types_dict.get(ticket['type'])
    ticket_info['country'] = ticket['custom_fields'][common_fields_config["country"]]

    agent_fd_id = ticket['responder_id']
    ticket_info['agent_id'] = agents_dict.get(agent_fd_id)

    group_fd_id = ticket['group_id']
    ticket_info['group_id'] = groups_dict.get(group_fd_id)

    client_type_s = ticket['custom_fields'][common_fields_config["type_client"]]
    category_type_s = ticket['custom_fields'][common_fields_config["type_category"]]
    request_type_s = ticket['custom_fields'][common_fields_config["type_request"]]

    client_type = client_types_dict.get(client_type_s)
    category_type = None
    if categ_types_dict.get(client_type) is not None:
        category_type = categ_types_dict.get(client_type).get(category_type_s)
    request_type = None
    if request_types_dict.get(category_type) is not None:
        request_type = request_types_dict.get(category_type).get(request_type_s)

    ticket_info['type_client'] = client_type
    ticket_info['type_category'] = category_type
    ticket_info['type_request'] = request_type

    ticket_info = {k: v[:100] if type(v) == str and len(v) > 100 else v for k, v in ticket_info.items()}
    return ticket_info


def divide_tickets_new_existing(api_tickets_df: pd.DataFrame, db_tickets_df: pd.DataFrame) -> tuple:
    """Divides dataframe with updated tickets obtained from API into the new tickets dataframe and existing tickets
        dataframe.

    Args:
        api_tickets_df (pd.DataFrame): dataframe with updated tickets obtained from API.
        db_tickets_df (pd.DataFrame): dataframe with tickets existing in the database.

    Returns:
        tuple: two dataframes: one with tickets not existing in the database (new tickets) and one with existing
            tickets in the database.
    """
    existing_tickets_df = api_tickets_df.loc[db_tickets_df.index].copy()
    new_tickets_df = api_tickets_df.loc[list(set(api_tickets_df.index) - set(db_tickets_df.index))].copy()
    return new_tickets_df, existing_tickets_df


def update_ticket_row(column: str, value, ticket_id: int, schema: str) -> None:
    """Update ticket field value in the database.

    Args:
        column (str): the tickets table column name which needs to be updated.
        value: new value for the given column and ticket.
        ticket_id (int): id of the ticket to be updated.
        schema (str): database schema to run query in (either 'freshdesk_internal' or 'freshdesk_external').
    """
    if isinstance(value, str) or isinstance(value, pd.Timestamp):
        value = '\'{}\''.format(value)
    if pd.isnull(value):
        value = 'NULL'
    q = q_update_ticket.format(schema=schema, column=column, ticket_id=ticket_id, value=value)
    execute_query(q)


def update_tickets_table(api_tickets_df: pd.DataFrame, db_tickets_df: pd.DataFrame, columns_list: list,
                         schema: str) -> None:
    """Using data from API, insert new tickets to the tickets table; update changed fields for the existing updated
    tickets.

    Args:
        api_tickets_df (pd.DataFrame): dataframe with updated tickets obtained from API.
        db_tickets_df (pd.DataFrame): dataframe with tickets existing in the database.
        columns_list (list): list of column names in the database to be updated.
        schema (str): database schema to update data in (either 'freshdesk_internal' or 'freshdesk_external').
    """
    new_tickets_df, existing_tickets_df = divide_tickets_new_existing(api_tickets_df, db_tickets_df)

    insert_to_db(schema, new_tickets_df.reset_index()[columns_list], 'tickets')
    for col in columns_list[1:]:
        for ticket_id in existing_tickets_df.index:
            current_value = existing_tickets_df.loc[ticket_id, col]
            db_value = db_tickets_df.loc[ticket_id, col]

            if current_value != db_value and not (pd.isnull(current_value) and pd.isnull(db_value)):
                update_ticket_row(col, current_value, ticket_id, schema)


def update_status_changes_table(api_tickets_df: pd.DataFrame, db_tickets_df: pd.DataFrame, schema: str) -> None:
    """Compare status in the updated tickets obtained from API and data in the database; add records with status changes
        to the status_changes table.

    Args:
        api_tickets_df (pd.DataFrame): dataframe with updated tickets obtained from API.
        db_tickets_df (pd.DataFrame): dataframe with tickets existing in the database.
        schema (str): database schema to update data in (either 'freshdesk_internal' or 'freshdesk_external').
    """
    new_tickets_df, existing_tickets_df = divide_tickets_new_existing(api_tickets_df, db_tickets_df)

    new_status_changes = new_tickets_df[['status_id', 'status_updated_at']].reset_index().rename(
        columns={'id': 'ticket_id', 'status_updated_at': 'updated_at', 'status_id': 'updated_status'})
    ex_status_changes = existing_tickets_df[existing_tickets_df['status_id'] != db_tickets_df['status_id']][
        ['status_id', 'status_updated_at']].reset_index().rename(
        columns={'id': 'ticket_id', 'status_updated_at': 'updated_at', 'status_id': 'updated_status'})
    total_status_changes = pd.concat([new_status_changes, ex_status_changes])
    insert_to_db(schema, total_status_changes, 'status_changes')


def update_assignment_changes_table(api_tickets_df: pd.DataFrame, db_tickets_df: pd.DataFrame, schema: str) -> None:
    """Compare assigned agent and group in the updated tickets obtained from API and data in the database; add records
        with assignment changes to the assignment_changes table.

    Args:
        api_tickets_df (pd.DataFrame): dataframe with updated tickets obtained from API.
        db_tickets_df (pd.DataFrame): dataframe with tickets existing in the database.
        schema (str): database schema to update data in (either 'freshdesk_internal' or 'freshdesk_external').
    """
    new_tickets_df, existing_tickets_df = divide_tickets_new_existing(api_tickets_df, db_tickets_df)

    new_tickets_df['changed_at'] = new_tickets_df['updated_at'].fillna('created_at')
    new_assign_changes = new_tickets_df[
        (new_tickets_df['agent_id'].notnull()) | (new_tickets_df['group_id'].notnull())][
        ['agent_id', 'group_id', 'changed_at']].reset_index().rename(
        columns={'id': 'ticket_id', 'changed_at': 'updated_at', 'group_id': 'updated_group',
                 'agent_id': 'updated_agent'})
    ex_assign_api = existing_tickets_df[['agent_id', 'group_id']]
    ex_assign_db = db_tickets_df[['agent_id', 'group_id']]
    ex_assign_changes = ex_assign_api[ex_assign_api != ex_assign_db].dropna(how='all').merge(
        existing_tickets_df['updated_at'], how='left', left_index=True, right_index=True).reset_index().rename(
        columns={'id': 'ticket_id', 'group_id': 'updated_group', 'agent_id': 'updated_agent'})
    total_assign_changes = pd.concat([ex_assign_changes, new_assign_changes])
    insert_to_db(schema, total_assign_changes, 'assignment_changes')


def update_replies_table(api_tickets_df: pd.DataFrame, db_tickets_df: pd.DataFrame, schema: str) -> None:
    """Identify new agent replies in the updated tickets obtained from API; add records with replies to the database.

    Args:
        api_tickets_df (pd.DataFrame): dataframe with updated tickets obtained from API.
        db_tickets_df (pd.DataFrame): dataframe with tickets existing in the database.
        schema (str): database schema to update data in (either 'freshdesk_internal' or 'freshdesk_external').
    """
    new_tickets_df, existing_tickets_df = divide_tickets_new_existing(api_tickets_df, db_tickets_df)
    new_first_replies_df = new_tickets_df[new_tickets_df['first_reply_at'].notnull()][
        ['agent_id', 'first_reply_at']].reset_index().rename(
        columns={'id': 'ticket_id', 'first_reply_at': 'created_at'})
    new_replies_df = new_tickets_df[(new_tickets_df['agent_response_at'].notnull()) & (
            new_tickets_df['agent_response_at'] != new_tickets_df['first_reply_at'])][
        ['agent_id', 'agent_response_at', 'requester_response_at']].reset_index().rename(
        columns={'id': 'ticket_id', 'agent_response_at': 'created_at'})
    q = q_max_reply.format(schema=schema, id_list=', '.join([str(x) for x in existing_tickets_df.index]+['0']))
    db_replies_df = select_to_dataframe(q).set_index('ticket_id')

    ex_first_replies = existing_tickets_df.merge(db_replies_df, how='left', left_index=True, right_index=True)[
        ['agent_id', 'first_reply_at', 'last_reply_at']]
    ex_first_replies = ex_first_replies[(ex_first_replies['first_reply_at'].notnull()) &
                                        (ex_first_replies['last_reply_at'].isnull())
                                        ].drop('last_reply_at', axis=1).reset_index().rename(
        columns={'id': 'ticket_id', 'first_reply_at': 'created_at'})

    ex_replies = existing_tickets_df[(existing_tickets_df['agent_response_at'].notnull()) & (
            existing_tickets_df['agent_response_at'] != existing_tickets_df['first_reply_at'])][
        ['agent_id', 'agent_response_at', 'requester_response_at']].merge(db_replies_df, how='left',
                                                                          left_index=True, right_index=True)

    ex_replies = ex_replies[(ex_replies['last_reply_at'].isnull()) | (
            ex_replies['agent_response_at'] > ex_replies['last_reply_at'])].drop(
        'last_reply_at', axis=1).reset_index().rename(
        columns={'id': 'ticket_id', 'agent_response_at': 'created_at'})

    total_replies = pd.concat([new_first_replies_df, new_replies_df, ex_first_replies, ex_replies])
    insert_to_db(schema, total_replies, 'replies')


def update_tickets_info(fd_type: str, job_name: str) -> None:
    """Collect data on updated tickets from Freshdesk API since last data update, update ticket fields for updated
        tickets in the database, insert new records for new tickets, new replies, assignment and status changes.

    Args:
        fd_type (str): ticket system type (either 'internal' or 'external').
        job_name (str): dagster job name (used for logging).
    """
    domain, api_key, schema, api_pass = get_credentials(fd_type)
    columns = TICKET_TABLE_COLS[fd_type]
    field_list = PARENT_TABLE_FIELDS[fd_type]

    update_time = get_update_time(schema, 1)
    ut = datetime.utcnow()
    update_parent_tables(fd_type)
    parent_dictionaries = get_parent_data(schema, field_list)

    request_url = f'https://{domain}.freshdesk.com//api/v2/tickets?updated_since={update_time}&per_page=100&order_by' \
                  f'=updated_at&order_type=asc&include=stats&page='
    status_codes = [0]

    try:
        result, status_codes = paginate_api_requests(request_url, fd_type)
        ticket_id_list = [str(x['id']) for x in result]
        if len(ticket_id_list) == 0:
            write_update_log(schema, ut, max(status_codes), 1, 1)
        else:
            q = q_select_by_id.format(schema=schema, id_list=', '.join(ticket_id_list))
            db_tickets_df = select_to_dataframe(q).set_index('id').sort_index()

            if fd_type == 'internal':
                db_tickets_df = db_tickets_df.applymap(
                    lambda i: '{' + ', '.join([str(x) for x in i]) + '}' if type(i) == list else i)

            ticket_info_list = [collect_ticket_info(x, parent_dictionaries, fd_type) for x in result]
            api_tickets_df = pd.DataFrame.from_records(ticket_info_list).drop_duplicates(
                subset=['id'], keep='last').set_index('id').sort_index()

            update_tickets_table(api_tickets_df, db_tickets_df, columns, schema)
            update_status_changes_table(api_tickets_df, db_tickets_df, schema)
            update_assignment_changes_table(api_tickets_df, db_tickets_df, schema)
            update_replies_table(api_tickets_df, db_tickets_df, schema)

            write_update_log(schema, ut, max(status_codes), 1, 1)

        seconds_since_update = (ut - datetime.strptime(update_time, '%Y-%m-%dT%H:%M:%SZ')).total_seconds()
        if seconds_since_update > 600:
            logger = get_slack_logger(job_name)
            _ = send_message(
                logger,
                SLACK_CHANNEL,
                SLACK_MESSAGE_TITLE,
                description='{:.2f} min occurred since last update of the *{}* data'.format(
                    seconds_since_update / 60, fd_type
                )
            )
    except UnsuccessfulRequest as e:
        write_update_log(schema, ut, e.status_code, 1, 0)
    except Exception as e:
        write_update_log(schema, ut, max(status_codes), 1, 0)
        raise e
