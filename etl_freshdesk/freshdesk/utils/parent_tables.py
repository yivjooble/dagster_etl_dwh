from etl_freshdesk.freshdesk.utils.database_io import select_to_dataframe, insert_to_db, execute_query
from etl_freshdesk.freshdesk.utils.utils import convert_num, get_credentials, UnsuccessfulRequest
from etl_freshdesk.freshdesk.utils.config import DEFAULT_TICKET_FILED_IDS
from etl_freshdesk.freshdesk.utils.sql_scripts import q_select_parent_dic, q_select_class_dic, q_select_class_tree, \
    q_select_2_cols, q_upd_to_deleted, q_select_ticket_types, q_select_job_complaint_actions

import pandas as pd
import requests


def get_parent_data(schema: str, field_list: list) -> list:
    """Collect data from v_parent_dictionaries view and return list of dictionaries with id and one another variable
        (id in Freshdesk, name etc.) for each field in field_list that can be used to determine the ID of the entity
        in the corresponding parent table.

    Args:
        schema (str): name of the database schema to collect data from.
            Should be either 'freshdesk_internal' or 'freshdesk_external'.
        field_list (list): list of fields to collect data for.

    Returns:
        list: list of dictionaries for fields in field_list.
    """

    df = select_to_dataframe(q_select_parent_dic.format(schema=schema))
    parent_dict = df.groupby('table_name')[['key', 'value']].apply(
        lambda r: {convert_num(k): convert_num(v) for k, v in r.set_index('key').to_dict()['value'].items()}
    ).to_dict()

    df_types = select_to_dataframe(q_select_class_dic.format(schema=schema))
    parent_types_dict = df_types.groupby(['table_name', 'key1'])[['key2', 'value']].apply(
        lambda r: {convert_num(k): convert_num(v) for k, v in r.set_index('key2').to_dict()['value'].items()}
    ).reset_index().groupby('table_name').apply(
        lambda r: {convert_num(k): convert_num(v) for k, v in r.set_index('key1').to_dict()[0].items()}
    ).to_dict()

    parent_dict_list = []
    for field in field_list:
        if field in parent_dict:
            parent_dict_list.append(parent_dict[field])
        else:
            parent_dict_list.append(parent_types_dict[field])
    return parent_dict_list


def get_classification_tree(schema: str, api_tree: dict) -> pd.DataFrame:
    """Collect classification types (client, category, request) from database and merge with data received from API.

    Args:
        schema (str): name of the database schema to collect data from.
            Should be either 'freshdesk_internal' or 'freshdesk_external'.
        api_tree (dict): dictionary with types from Freshdesk API.

    Returns:
        pd.DataFrame: dataframe with merged classification tree from database and API.
    """
    db_classification = select_to_dataframe(q_select_class_tree.format(schema=schema))
    classification_df = pd.json_normalize(api_tree).T.reset_index()
    classification_df.columns = ['index', 'request']
    classification_df['client'], classification_df['category'] = zip(
        *classification_df['index'].apply(lambda x: x.split('.')))
    classification_df = classification_df[['client', 'category', 'request']].explode('request').reset_index(drop=True)

    clients_without_categories = set(api_tree.keys()) - set(classification_df['client'])
    if len(clients_without_categories) > 0:
        for c in clients_without_categories:
            classification_df.loc[classification_df.shape[0]] = [c, None, None]

    classification_df = classification_df.merge(
        db_classification[['client', 'client_id']].drop_duplicates(), how='left', on=['client'])
    classification_df = classification_df.merge(
        db_classification[['client_id', 'category', 'category_id']].drop_duplicates(), how='left',
        on=['client_id', 'category'])
    classification_df = classification_df.merge(
        db_classification[['client_id', 'category_id', 'request', 'request_id']].drop_duplicates(), how='left',
        on=['client_id', 'category_id', 'request'])
    return classification_df


def get_missing_types(df: pd.DataFrame, id_col: str, name_col: str, col_list: list) -> pd.DataFrame:
    """Get records from merged classification types that are missing in the database.

    Args:
        df (pd.DataFrame): dataframe with merged data on classification types from database and API.
        id_col (str): column representing id in the database.
        name_col (str): column representing name of the classification type received from API.
        col_list (list): list of columns to add in the returned dataframe.

    Returns:
        pd.DataFrame: dataframe with classification types that are missing in the database.
    """
    return df[(df[id_col].isnull()) & (df[name_col].notnull())][col_list].rename(columns={name_col: 'name'})


def update_types(schema: str, api_types: dict) -> None:
    """Update data in the client_types, category_types, request_types tables with the new types received from API.

    Args:
        schema (str): name of the database schema to collect data from.
            Should be either 'freshdesk_internal' or 'freshdesk_external'.
        api_types (dict): dictionary with classification type tree obtained from API.
    """
    classification_df = get_classification_tree(schema, api_types)
    missing_clients = get_missing_types(classification_df, 'client_id', 'client', ['client'])
    missing_categories = get_missing_types(classification_df, 'category_id', 'category', ['category', 'client_id'])
    missing_requests = get_missing_types(classification_df, 'request_id', 'request', ['request', 'category_id'])
    if missing_clients.shape[0] > 0:
        insert_to_db(schema, missing_clients.drop_duplicates(), 'client_types')
        update_types(schema, api_types)
    elif missing_categories.shape[0] > 0:
        insert_to_db(schema, missing_categories.drop_duplicates(), 'category_types')
        update_types(schema, api_types)
    elif missing_requests.shape[0] > 0:
        insert_to_db(schema, missing_requests.drop_duplicates(), 'request_types')


def update_requesters(id_fd: int) -> dict:
    """Update the requesters table using id of requester from ticket API data.
        Obtain name and email of the requester from API contacts and insert data in the requesters table.
        Return updated dictionary with available requesters.

    Args:
        id_fd (int): id of the requester in the Freshdesk API.

    Returns:
        dict: updated dictionary with requesters, where id in Freshdesk API is key and id in the database is value.
    """
    domain, api_key, schema, api_pass = get_credentials('internal')

    req = requests.get('https://{domain}.freshdesk.com//api/v2/contacts/{id}'.format(id=id_fd, domain=domain),
                       auth=(api_key, api_pass))
    if req.status_code == 404:
        req = requests.get('https://{domain}.freshdesk.com//api/v2/agents/{id}'.format(id=id_fd, domain=domain),
                           auth=(api_key, api_pass))
        if req.status_code != 200:
            raise UnsuccessfulRequest(req.status_code)
        email = req.json()['contact']['email']
        name = req.json()['contact']['name']

    elif req.status_code != 200:
        raise UnsuccessfulRequest(req.status_code)

    else:
        email = req.json()['email']
        name = req.json()['name']
    df = pd.DataFrame.from_dict({'name': [name], 'email': [email], 'id_freshdesk': [id_fd]})
    try:
        insert_to_db(schema, df, 'requesters')
    finally:
        req_df = select_to_dataframe(q_select_2_cols.format(
            col1='id_freshdesk', col2='id', table_name='requesters', schema=schema))
        return req_df.set_index('id_freshdesk').to_dict()['id']


def update_parent_table(api_dict: dict, schema: str, table_name: str, id_col: str = 'id', name_col: str = 'name',
                        to_delete: bool = False) -> None:
    """Update dictionary-tables with ticket fields values in the database using data obtained using API.

    Args:
        api_dict (dict): dictionary with ticket field values obtained from API.
        schema (str): name of the database schema to collect data from.
            Should be either 'freshdesk_internal' or 'freshdesk_external'.
        table_name (str): name of the table in the database, where values for current ticket field are stored.
        id_col (str): column in the database table, which represents unique identifier of the field value.
            These identifiers will be compared to ids in the API data.
        name_col (str): column in the database table, which represents the name of the field value.
        to_delete (bool): if True, missing values in the API data will be marked with is_deleted=True in the database.
    """
    df_api = pd.DataFrame.from_dict(api_dict, orient='index', columns=[name_col])

    q = q_select_2_cols.format(col1=id_col, col2=name_col, table_name=table_name, schema=schema)
    df_db = select_to_dataframe(q).set_index(id_col)

    df_to_insert = df_api.loc[[x for x in df_api.index if x not in df_db.index]]
    df_to_insert.index.rename(id_col, inplace=True)
    insert_to_db(schema, df_to_insert.reset_index(), table_name)

    ids_to_del = [x for x in df_db.index if x not in df_api.index]
    if to_delete and len(ids_to_del) > 0:
        q = q_upd_to_deleted.format(schema=schema, table_name=table_name, id_col=id_col,
                                    ids=', '.join([str(x) for x in ids_to_del]))
        execute_query(q)


def update_parent_tables(fd_type: str) -> None:
    """Update ticket fields values in the database using data from API. The following field values are updated:
        channels, priorities, statuses, agents, groups, ticket types, classification types. When updating data for
        internal freshdesk, source types are also updated.

    Args:
        fd_type (str): ticket system type, ticket fields for which need to be updated.
            Should be either 'external' or 'internal'.
    """
    domain, api_key, schema, api_pass = get_credentials(fd_type)
    r = requests.get("https://{d}.freshdesk.com/api/v2/ticket_fields".format(d=domain),
                     auth=(api_key, api_pass))
    if r.status_code != 200:
        raise UnsuccessfulRequest(r.status_code)

    fields = DEFAULT_TICKET_FILED_IDS[fd_type]

    channel_dict = {x[1]: x[0] for x in [i['choices'] for i in r.json() if i['id'] == fields['channel']][0].items()}
    update_parent_table(channel_dict, schema, 'channels', id_col='id', name_col='name')

    priorities_dict = {x[1]: x[0] for x in [i['choices'] for i in r.json() if i['id'] == fields['priority']][0].items()}
    update_parent_table(priorities_dict, schema, 'priorities', id_col='id', name_col='name')

    statuses_dict = {int(x[0]): x[1][1] for x in
                     [i['choices'] for i in r.json() if i['id'] == fields['status']][0].items()}
    update_parent_table(statuses_dict, schema, 'statuses', id_col='id', name_col='name', to_delete=True)

    groups_dict = {x[1]: x[0] for x in [i['choices'] for i in r.json() if i['id'] == fields['group']][0].items()}
    update_parent_table(groups_dict, schema, 'groups', id_col='id_freshdesk', name_col='name', to_delete=True)

    agents_dict = {x[1]: x[0] for x in [i['choices'] for i in r.json() if i['id'] == fields['agent']][0].items()}
    update_parent_table(agents_dict, schema, 'agents', id_col='id_freshdesk', name_col='name', to_delete=True)

    ticket_type_list = [i['choices'] for i in r.json() if i['id'] == fields['ticket_type']][0]
    df_db = select_to_dataframe(q_select_ticket_types.format(schema=schema))
    missing_types = [x for x in ticket_type_list if x not in df_db['name'].to_list()]
    if len(missing_types) > 0:
        missing_types_df = pd.DataFrame(missing_types, columns=['name'])
        insert_to_db(schema, missing_types_df, 'ticket_types')

    classification_types = [i['choices'] for i in r.json() if i['id'] == fields['client_type']][0]
    update_types(schema, classification_types)

    if fd_type == 'internal':
        source_type_dict = {int(x.split('-')[0].strip()): x.split('-')[1].strip() for x in [
            i['choices'] for i in r.json() if i['id'] == fields['source_type']][0]}
        update_parent_table(source_type_dict, schema, 'source_types', id_col='id', name_col='name')
    else:
        complaint_action_list = [i['choices'] for i in r.json() if i['name'] == 'cf_actions_on_job_complaints'][0]
        df_db = select_to_dataframe(q_select_job_complaint_actions.format(schema=schema))
        missing_actions = [x for x in complaint_action_list if x not in df_db['name'].to_list()]
        if len(missing_actions) > 0:
            missing_actions_df = pd.DataFrame(missing_actions, columns=['name'])
            insert_to_db(schema, missing_actions_df, 'job_complaint_actions')
