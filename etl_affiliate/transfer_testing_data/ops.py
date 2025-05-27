from dagster import (
    op,
    Out,
    Failure,
    job,
    Field,
    make_values_resource,
    fs_io_manager
)
import os
from etl_affiliate.utils.utils import exec_select_query_pg, exec_query_pg
from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import job_prefix, read_sql_file

JOB_PREFIX = job_prefix()
ORIGINAL_SCHEMA = 'affiliate'
STOP_TABLES = [
    'project_cpc_ratio_change_log',
    'partner_settings_change_log'
]


def transfer_tables(target_schema: str, set_owner_to: str, target_users_permissions: list) -> list:
    """
    Transfers tables mentioned in affiliate.dev_test_objects from the affiliate schema to the target_schema.
    If is_empty = True in the dictionary, no data is transferred, empty table is created.
    If data_filter_string is not an empty string, the filtered data is transferred to the target_schema.
    Also, creates constrains in tables.
    In addition, table modification permissions are granted to the pyapi user.

    Args:
        target_schema (str): the schema to create tables in.
        set_owner_to (str): table owner to set.
        target_users_permissions (list): list of DWH users to grant permissions to.

    Returns:
        list: list of dictionaries with tables transferred to affiliate schema.
    """
    q = read_sql_file(os.path.join(os.path.dirname(__file__), os.path.join('sql', 'get_tables.sql')))
    df_tables = exec_select_query_pg(q, host='dwh')

    s_all_constrains = """
        select constraint_name, table_name from information_schema.table_constraints 
        where constraint_schema = '{}' and constraint_type != 'CHECK'""".format(target_schema)
    constraints_list = exec_select_query_pg(s_all_constrains, host='dwh')

    for constraints_data in constraints_list.to_dict('records'):
        contr_table_name = constraints_data['table_name']
        constraint_name = constraints_data['constraint_name']
        q_delete_constraints = "alter table {}.{} drop constraint {}".format(target_schema, contr_table_name,
                                                                             constraint_name)
        exec_query_pg(q_delete_constraints, host='dwh')

    tables_list = df_tables.to_dict('records')
    for table_dict in tables_list:
        table_name = table_dict['table_name']
        is_empty = table_dict['is_empty']
        filter_str = table_dict['data_filter_string']
        table_constraints = table_dict['table_constraints']

        q_drop = 'drop table if exists {}.{}'.format(target_schema, table_name)
        exec_query_pg(q_drop, host='dwh')

        limit_str = 'limit 1' if is_empty else ''

        q_create = '''
        select * into {sc_target}.{table_name}
        from {sc_orig}.{table_name} {filter_str} {limit_str}
        '''.format(sc_target=target_schema, sc_orig=ORIGINAL_SCHEMA, table_name=table_name, filter_str=filter_str,
                   limit_str=limit_str)

        exec_query_pg(q_create, host='dwh')

        if table_constraints is not None:
            exec_query_pg(table_constraints.format(schema=target_schema), host='dwh')

        list_users_permissions = target_users_permissions + ['pyapi']
        for user in list_users_permissions:
            q_table_perm = '''grant select, update, delete, insert, truncate 
                    on {sc_target}.{table_name} to {user}'''.format(
                sc_target=target_schema, table_name=table_name, user=user)
            exec_query_pg(q_table_perm, host='dwh')

        q_set_owner = 'alter table {sc_target}.{table_name} owner to {owner}'.format(
            sc_target=target_schema, table_name=table_name, owner=set_owner_to)
        exec_query_pg(q_set_owner, host='dwh')

        if is_empty:
            q_truncate = 'truncate {}.{}'.format(target_schema, table_name)
            exec_query_pg(q_truncate, host='dwh')

    return tables_list


def transfer_routines(target_schema: str, tables_list: list, set_owner_to: str) -> bool:
    """
    Transfers routines mentioned in the affiliate.dev_test_objects from the affiliate schema to the target_schema.
    Routine definition is modified so that routine selects or inserts data to the tables of the target_schema in case
    if table is mentioned in the tables_list. In case if table name is in the STOP_TABLES list, its schema is not
    modified.

    Args:
        target_schema (str): the schema to create routines in.
        tables_list (list): list of dictionaries with table info.
        set_owner_to (str): routine owner to set.
    """
    q = read_sql_file(os.path.join(os.path.dirname(__file__), os.path.join('sql', 'get_routines.sql')))
    df_routines = exec_select_query_pg(q, host='dwh')
    set_owner_q = 'alter routine {schema}.{name} owner to {owner}'

    df_routines['definition_mod'] = df_routines['definition']

    for r in df_routines['name'].to_list():
        df_routines['definition_mod'] = df_routines.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(ORIGINAL_SCHEMA, r),
            '{}.{}'.format(target_schema, r)), axis=1)

    for t in [x['table_name'] for x in tables_list]:
        df_routines['definition_mod'] = df_routines.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(ORIGINAL_SCHEMA, t),
            '{}.{}'.format(target_schema, t)), axis=1)
    for t in STOP_TABLES:
        df_routines['definition_mod'] = df_routines.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(target_schema, t),
            '{}.{}'.format(ORIGINAL_SCHEMA, t)), axis=1)

    affiliate_schema_mod_check = df_routines['definition_mod'].str.contains('create.*affiliate\\.', case=False).sum()
    if affiliate_schema_mod_check > 0:
        raise Exception('Attempt to modify affiliate schema was detected')
    else:
        df_routines['definition_mod'].apply(lambda x: exec_query_pg(x, host='dwh'))
        df_routines['name_arg'].apply(lambda x: exec_query_pg(
            set_owner_q.format(schema=target_schema, name=x, owner=set_owner_to),
            host='dwh'
        ))

    return True


def transfer_views(target_schema: str, tables_list: list, set_owner_to: str, target_users_permissions: list) -> None:
    """
    Transfers views mentioned in the affiliate.dev_test_objects from the affiliate schema to the target_schema.
    View definition is modified so that a view selects data from the tables of the target_schema in case if table
    is mentioned in the tables_list. In case if table name is in the STOP_TABLES list, its schema is not modified.

    Args:
        target_schema (str): the schema to create views in.
        tables_list (list): list of dictionaries with table info.
        set_owner_to (str): routine owner to set.
        target_users_permissions (list): list of DWH users to grant permissions to.
    """
    q = read_sql_file(os.path.join(os.path.dirname(__file__), os.path.join('sql', 'get_views.sql')))
    df_views = exec_select_query_pg(q, host='dwh')
    set_owner_q = 'alter view {schema}.{name} owner to {owner}'
    permissions_q = 'grant select on {schema}.{name} to {user}'

    df_views['definition_mod'] = df_views['definition']

    for r in df_views['name'].to_list():
        df_views['definition_mod'] = df_views.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(ORIGINAL_SCHEMA, r),
            '{}.{}'.format(target_schema, r)), axis=1)

    for t in [x['table_name'] for x in tables_list]:
        df_views['definition_mod'] = df_views.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(ORIGINAL_SCHEMA, t),
            '{}.{}'.format(target_schema, t)), axis=1)
    for t in STOP_TABLES:
        df_views['definition_mod'] = df_views.apply(lambda x: x['definition_mod'].replace(
            '{}.{}'.format(target_schema, t),
            '{}.{}'.format(ORIGINAL_SCHEMA, t)), axis=1)

    affiliate_schema_mod_check = df_views['definition_mod'].str.contains('create.*affiliate\\.', case=False).sum()
    if affiliate_schema_mod_check > 0:
        raise Exception('Attempt to modify affiliate schema was detected')
    else:
        df_views['definition_mod'].apply(lambda x: exec_query_pg(x, host='dwh'))
        df_views['name'].apply(lambda x: exec_query_pg(
            set_owner_q.format(schema=target_schema, name=x, owner=set_owner_to),
            host='dwh'
        ))

        for view_name in df_views['name']:
            for user in target_users_permissions:
                exec_query_pg(permissions_q.format(schema=target_schema,
                                                   name=view_name,
                                                   user=user),
                              host='dwh')


def delete_views(target_schema: str) -> bool:
    """
    This function deletes views for the target schema.

    Args:
        target_schema (str): the schema to create views in.

    """
    q = read_sql_file(os.path.join(os.path.dirname(__file__), os.path.join('sql', 'get_views.sql')))
    df_views = exec_select_query_pg(q, host='dwh')

    for r in df_views['name'].to_list():
        exec_query_pg(('drop view if exists ' + target_schema + '.' + r), host='dwh')
    return True


@op(required_resource_keys={'globals'})
def delete_views_op(context) -> bool:
    target_schema = context.resources.globals['target_schema']
    if target_schema == 'affiliate':
        raise Failure(description='target schema cannot be affiliate')
    try:
        return delete_views(target_schema)
    except:
        raise Failure(description='delete_views_op failure')


@op(out=Out(list),
    required_resource_keys={'globals'})
def transfer_tables_op(context, prev_result) -> list:
    target_schema = context.resources.globals['target_schema']
    target_owner = context.resources.globals['set_owner_to']
    target_users_permissions = context.resources.globals['add_permissions_to']
    if target_schema == 'affiliate':
        raise Failure(description='target schema cannot be affiliate')
    try:
        return transfer_tables(target_schema, target_owner, target_users_permissions)
    except:
        raise Failure(description='transfer_tables_op failure')


@op(required_resource_keys={'globals'})
def transfer_routines_op(context, tables_list) -> bool:
    target_schema = context.resources.globals['target_schema']
    target_owner = context.resources.globals['set_owner_to']
    if target_schema == 'affiliate':
        raise Failure(description='target schema cannot be affiliate')
    try:
        return transfer_routines(target_schema, tables_list, target_owner)
    except:
        raise Failure(description='transfer_routines_op failure')


@op(required_resource_keys={'globals'})
def transfer_views_op(context, tables_list, prev_result2) -> None:
    target_schema = context.resources.globals['target_schema']
    target_owner = context.resources.globals['set_owner_to']
    target_users_permissions = context.resources.globals['add_permissions_to']
    if target_schema == 'affiliate':
        raise Failure(description='target schema cannot be affiliate')
    try:
        transfer_views(target_schema, tables_list, target_owner, target_users_permissions)
    except:
        raise Failure(description='transfer_views_op failure')


@job(resource_defs={'globals': make_values_resource(target_schema=Field(str, default_value='affiliate_dev'),
                                                    set_owner_to=Field(str, default_value='user_agg_team'),
                                                    add_permissions_to=Field(list, default_value=[])),
                    'io_manager': fs_io_manager.configured({'base_dir': f'{get_io_manager_path()}'})},
     name=JOB_PREFIX + 'transfer_testing_data',
     description=f'Allows to create necessary tables, routines, views in the affiliate_dev schema for testing changes '
                 f'in the aff_collect_click_statistics job.'
     )
def transfer_testing_data_job():
    dvo = delete_views_op()
    res = transfer_tables_op(dvo)
    tro = transfer_routines_op(res)
    transfer_views_op(res, tro)
