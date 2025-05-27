import json
from contextlib import closing
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from sql_metadata import Parser
from sqlalchemy import create_engine
from tableau_api_lib import TableauServerConnection

CREDS_DIR = Path(__file__).parent / 'credentials'
API_QUERIES = Path(__file__).parent / 'api_queries'

with open(f'{CREDS_DIR}/dwh.json') as json_file:
    dwh_cred = json.load(json_file)
USER_DWH = dwh_cred['user']
PW_DWH = dwh_cred['password']
HOST_DWH = dwh_cred['host']
DB_DWH = dwh_cred['database']

with open(f'{CREDS_DIR}/api_credentials.json') as json_file:
    api_cred = json.load(json_file)
TOKEN_NAME = api_cred['token_name']
TOKEN_SECRET = api_cred['token_secret']


def api_authenticate(token_name, token_secret):
    ts_config = {
        'my_env': {
            'server': 'http://tableau.jooble.com',
            'api_version': '3.19',
            'personal_access_token_name': token_name,
            'personal_access_token_secret': token_secret,
            'site_name': '',
            'site_url': ''
        }
    }

    conn = TableauServerConnection(ts_config, env='my_env')
    conn.sign_in()
    return conn


def get_server_info(conn):
    with open(f'{API_QUERIES}/getDatabaseServers.txt', 'r') as f:
        database_servers_query = f.read()
    response = conn.metadata_graphql_query(query=database_servers_query)
    servers_df = pd.DataFrame(response.json()['data']['databaseServers'])
    return servers_df


def replace_punc(x):
    if not pd.isnull(x):
        return x.replace('"', '').replace("'", '').replace("[", '').replace("]", '')


def extract_schema(full_table_name, table_schema=None):
    if len(full_table_name.split('.')) > 1:
        table_schema = replace_punc(full_table_name.split('.')[-2])
    elif table_schema == '':
        table_schema = None
    else:
        table_schema = table_schema
    table_name = replace_punc(full_table_name.split('.')[-1])
    return table_name, table_schema


def parse_tables(query):
    try:
        tables_list = Parser(query).tables
    except AttributeError:
        tables_list = []
    return [replace_punc(x) for x in tables_list]


def extract_ds_list(x):
    entries = [r['datasource'] for r in [y for x in [col['referencedByFields'] for col in x] for y in x]
               if not pd.isnull(r['datasource'])]

    published_ds_list = list(set([d['publishedDatasourceId'] for d in entries if 'publishedDatasourceId' in d]))
    embedded_ds_list = list(set([d['embeddedDatasourceId'] for d in entries if 'embeddedDatasourceId' in d]))

    datasources_list = embedded_ds_list + published_ds_list
    is_published_list = [int(x in published_ds_list) for x in datasources_list]

    return list(zip(datasources_list, is_published_list))


def extract_wb_list(wbs):
    workbook_ids = [x['id'] for x in wbs]
    workbook_luids = [x['luid'] for x in wbs]
    workbook_url_ids = [x['vizportalUrlId'] for x in wbs]
    workbook_names = [x['name'] for x in wbs]
    workbook_owners = [x['owner']['name'] for x in wbs]
    workbook_project_names = [x['projectName'] for x in wbs]
    workbook_project_url_ids = [x['projectVizportalUrlId'] for x in wbs]
    return list(zip(workbook_ids, workbook_luids, workbook_url_ids, workbook_names, workbook_owners,
                    workbook_project_names, workbook_project_url_ids))


def get_db_tables_data(context, conn):
    with open(f'{API_QUERIES}/getDatabaseTables.txt', 'r') as f:
        db_tables_query = f.read()

    response = conn.metadata_graphql_query(query=db_tables_query)
    context.log.info(f"{json.dumps(response.json(), indent=4)}")
    db_tables = response.json()['data']['databaseTables']

    df_dbt = pd.json_normalize(db_tables)
    df_dbt['datasources_list'] = df_dbt['columns'].apply(extract_ds_list)
    df_dbt = df_dbt.explode('datasources_list')
    df_dbt[['datasource_id', 'is_published_datasource']] = df_dbt['datasources_list'].apply(pd.Series)
    df_dbt['query_id'] = df_dbt['queries'].apply(lambda x: [y['queryID'] for y in x])
    df_dbt = df_dbt.explode('query_id').drop(['queries', 'columns', 'datasources_list'], axis=1)

    df_dbt.rename(columns={
        'id': 'physical_table_id',
        'name': 'table_name',
        'fullName': 'db_table_name',
        'schema': 'db_table_schema',
        'connectionType': 'connection_type',
        'database.id': 'database_id',
        'database.__typename': 'database_type',
        'database.name': 'database_name'
    }, inplace=True)

    for col in ['table_name', 'db_table_name', 'db_table_schema']:
        df_dbt[col] = df_dbt[col].apply(replace_punc)

    df_dbt['query'] = None
    df_dbt['is_custom_sql'] = False
    df_dbt.dropna(subset=['datasource_id'], inplace=True)
    df_dbt.drop('query_id', inplace=True, axis=1)
    df_dbt[['db_table_name', 'db_table_schema']] = df_dbt.apply(
        lambda x: extract_schema(x['db_table_name'], x['db_table_schema']), axis=1).apply(pd.Series)

    return df_dbt


def get_sql_tables_data(conn):
    with open(f'{API_QUERIES}/getSQLTables.txt', 'r') as f:
        sql_tables_query = f.read()

    response = conn.metadata_graphql_query(query=sql_tables_query)
    sql_tables = response.json()['data']['customSQLTables']

    df_sqlt = pd.json_normalize(sql_tables)
    df_sqlt['tables'] = df_sqlt['tables'].apply(lambda x: [tuple([replace_punc(t) for t in d.values()]) for d in x])
    df_sqlt['tables'] = df_sqlt['tables'].apply(lambda x: [extract_schema(a[0], a[1]) for a in x])
    df_sqlt['parsed_tables'] = df_sqlt['query'].apply(lambda x: [extract_schema(t) for t in parse_tables(x)])

    df_sqlt['missing_tables'] = df_sqlt.apply(lambda row: [x for x in row['parsed_tables'] if x not in row['tables']
                                                           and x[0] not in ['nolock', 'registration_datediff']], axis=1)

    df_sqlt['tables'] = df_sqlt['tables'] + df_sqlt['missing_tables']
    df_sqlt = df_sqlt.explode('tables')
    df_sqlt[['table_name', 'table_schema']] = df_sqlt['tables'].apply(pd.Series)

    df_sqlt['datasources_list'] = df_sqlt['columns'].apply(extract_ds_list)
    df_sqlt = df_sqlt.explode('datasources_list')
    df_sqlt[['datasource_id', 'is_published_datasource']] = df_sqlt['datasources_list'].apply(pd.Series)
    df_sqlt = df_sqlt.drop(['database', 'tables', 'columns', 'datasources_list', 'parsed_tables', 'missing_tables'],
                           axis=1)

    df_sqlt['is_custom_sql'] = True
    df_sqlt.rename(columns={
        'id': 'physical_table_id',
        'name': 'table_name',
        'connectionType': 'connection_type',
        'database.id': 'database_id',
        'database.__typename': 'database_type',
        'database.name': 'database_name',
        'table_name': 'db_table_name',
        'table_schema': 'db_table_schema'
    }, inplace=True)

    return df_sqlt


def get_published_datasources(conn):
    with open(f'{API_QUERIES}/getPublishedDatasources.txt', 'r') as f:
        pub_ds_query = f.read()

    response = conn.metadata_graphql_query(query=pub_ds_query)
    pub_ds = response.json()['data']['publishedDatasources']
    pub_ds_df = pd.json_normalize(pub_ds)

    pub_ds_df['wb_list'] = pub_ds_df['downstreamWorkbooks'].apply(extract_wb_list)
    pub_ds_df = pub_ds_df.explode('wb_list')
    pub_ds_df[[
        'workbook_id',
        'workbook_rest_api_id',
        'workbook_url_id',
        'workbook_name',
        'workbook_owner',
        'workbook_project',
        'workbook_project_url_id'
    ]] = pub_ds_df['wb_list'].apply(pd.Series)
    pub_ds_df.drop(['wb_list', 'downstreamWorkbooks'], axis=1, inplace=True)
    pub_ds_df['is_published_datasource'] = 1

    pub_ds_df.rename(columns={
        'id': 'datasource_id',
        'name': 'datasource_name',
        'luid': 'datasource_rest_api_id',
        'owner.name': 'datasource_owner',
        'vizportalUrlId': 'datasource_url_id',
        'projectName': 'datasource_project',
        'projectVizportalUrlId': 'datasource_project_url_id',
        'extractLastRefreshTime': 'last_refresh_datetime',
        'extractLastIncrementalUpdateTime': 'last_incremental_update_datetime',
        'extractLastUpdateTime': 'last_update_time'
    }, inplace=True)

    return pub_ds_df


def get_embedded_datasources(conn):
    with open(f'{API_QUERIES}/getEmbeddedDatasources.txt', 'r') as f:
        emb_ds_query = f.read()

    response = conn.metadata_graphql_query(query=emb_ds_query)
    emb_ds = response.json()['data']['embeddedDatasources']
    emb_ds_df = pd.json_normalize(emb_ds)

    emb_ds_df = emb_ds_df[
        emb_ds_df.apply(lambda x: len([y['name'] for y in x['upstreamDatasources']]) == 0, axis=1)].copy()
    emb_ds_df.drop(['upstreamDatasources'], axis=1, inplace=True)

    emb_ds_df[['datasource_owner',
               'datasource_url_id',
               'datasource_project',
               'datasource_project_url_id',
               'datasource_rest_api_id']] = None

    emb_ds_df['is_published_datasource'] = 0

    emb_ds_df.rename(columns={
        'workbook.id': 'workbook_id',
        'workbook.name': 'workbook_name',
        'workbook.luid': 'workbook_rest_api_id',
        'workbook.projectName': 'workbook_project',
        'workbook.owner.name': 'workbook_owner',
        'workbook.projectVizportalUrlId': 'workbook_project_url_id',
        'workbook.vizportalUrlId': 'workbook_url_id',
        'id': 'datasource_id',
        'name': 'datasource_name',
        'extractLastRefreshTime': 'last_refresh_datetime',
        'extractLastIncrementalUpdateTime': 'last_incremental_update_datetime',
        'extractLastUpdateTime': 'last_update_time'
    }, inplace=True)

    return emb_ds_df


def collect_data(context):
    conn = api_authenticate(TOKEN_NAME, TOKEN_SECRET)

    servers_df = get_server_info(conn)
    sqlt_df = get_sql_tables_data(conn)
    dbt_df = get_db_tables_data(context, conn)
    emb_ds_df = get_embedded_datasources(conn)
    pub_ds_df = get_published_datasources(conn)

    tables_df = pd.concat([sqlt_df, dbt_df], ignore_index=True)

    servers_dict = servers_df.set_index('database_id').to_dict('index')
    tables_df['database_host'] = tables_df['database_id'].apply(
        lambda x: servers_dict[x]['database_hostname'] if x in servers_dict else None)
    tables_df['is_dwh'] = tables_df['database_host'].isin(['31.28.172.21', 'dwh.jooble.com'])

    ds_df = pd.concat([emb_ds_df, pub_ds_df], ignore_index=True)
    ds_df['datasource_owner'] = ds_df.apply(
        lambda x: x['workbook_owner'] if x['is_published_datasource'] == 0 else x['datasource_owner'], axis=1)

    for col in ['last_refresh_datetime', 'last_incremental_update_datetime', 'last_update_time']:
        ds_df[col] = pd.to_datetime(ds_df[col], format='%Y-%m-%dT%H:%M:%SZ')

    df = ds_df.merge(tables_df, how='outer', on=['datasource_id', 'is_published_datasource'])

    return df


def split_data(context):
    df = collect_data(context)

    workbooks = df[[
        'workbook_id',
        'workbook_rest_api_id',
        'workbook_url_id',
        'workbook_name',
        'workbook_owner',
        'workbook_project',
        'workbook_project_url_id'
    ]].dropna(subset=['workbook_url_id']).drop_duplicates().rename(columns={
        'workbook_id': 'id',
        'workbook_rest_api_id': 'rest_api_id',
        'workbook_url_id': 'url_id',
        'workbook_owner': 'owner_name',
        'workbook_project': 'project_name',
        'workbook_project_url_id': 'project_url_id'
    })

    data_sources = df[[
        'datasource_id',
        'datasource_name',
        'datasource_owner',
        'is_published_datasource',
        'datasource_rest_api_id',
        'datasource_url_id',
        'datasource_project',
        'datasource_project_url_id',
        'last_refresh_datetime',
        'last_incremental_update_datetime',
        'last_update_time'
    ]].drop_duplicates().rename(columns={
        'datasource_id': 'id',
        'datasource_owner': 'owner_name',
        'is_published_datasource': 'is_published',
        'datasource_url_id': 'url_id',
        'datasource_project': 'project_name',
        'datasource_project_url_id': 'project_url_id',
        'datasource_rest_api_id': 'rest_api_id'
    }).dropna(subset=['id'])

    wb_ds = df[['workbook_id', 'datasource_id']].dropna().drop_duplicates()

    physical_tables = df[[
        'physical_table_id',
        'is_custom_sql',
        'query',
        'table_name',
        'database_name',
        'connection_type',
        'database_host',
        'is_dwh'
    ]].drop_duplicates().rename(columns={
        'physical_table_id': 'id',
        'is_dwh': 'is_in_dwh'
    }).dropna(subset=['id'])

    ds_pt = df[['physical_table_id', 'datasource_id']].dropna().drop_duplicates()

    database_tables = df[~df['connection_type'].isin(
        ['excel-direct', 'textscan', 'google-sheets', 'cloudfile:googledrive-excel-direct'])][[
        'physical_table_id',
        'db_table_name',
        'db_table_schema'
    ]].drop_duplicates().rename(columns={
        'db_table_name': 'table_name',
        'db_table_schema': 'table_schema'
    })

    for col in ['url_id', 'project_url_id']:
        workbooks[col] = workbooks[col].apply(lambda x: int(x) if not pd.isnull(x) else np.nan)

    for col in ['url_id', 'project_url_id', 'is_published']:
        data_sources[col] = data_sources[col].apply(lambda x: int(x) if not pd.isnull(x) else np.nan)

    for col in ['is_in_dwh', 'is_custom_sql']:
        physical_tables[col] = physical_tables[col].apply(lambda x: int(x) if not pd.isnull(x) else np.nan)

    return workbooks, data_sources, wb_ds, physical_tables, ds_pt, database_tables


def insert_tableau_metadata(dwh_schema, workbooks, data_sources, wb_ds, physical_tables, database_tables, ds_pt):
    # Insert data into postgres
    con = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH))

    # Insert data into cloudberry
    con_cloudberry = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname="an_dwh", user=USER_DWH, password=PW_DWH, host="an-dwh.jooble.com"))

    for dataframe, table_name in zip(
            [workbooks, data_sources, wb_ds, physical_tables, database_tables, ds_pt],
            ['tableau_workbook', 'tableau_datasource', 'tableau_workbook_datasource', 'tableau_physical_table',
             'tableau_database_table', 'tableau_datasource_phys_table']
    ):
        # Insert data into postgres
        dataframe.to_sql(
            name=table_name,
            schema=dwh_schema,
            if_exists='append',
            con=con,
            index=False
        )

        # Insert data into cloudberry
        dataframe.to_sql(
            name=table_name,
            schema=dwh_schema,
            if_exists='append',
            con=con_cloudberry,
            index=False
        )

def clear_data(dwh_schema):
    # Clear data in postgres
    with closing(psycopg2.connect(dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH)) as con:
        cur = con.cursor()
        for table in ['tableau_workbook', 'tableau_datasource', 'tableau_workbook_datasource', 'tableau_physical_table',
                      'tableau_database_table', 'tableau_datasource_phys_table'][::-1]:
            q = 'delete from {}.{}'.format(dwh_schema, table)
            cur.execute(q)
        con.commit()
        cur.close()

    # Clear data in cloudberry
    with closing(psycopg2.connect(dbname="an_dwh", user=USER_DWH, password=PW_DWH, host="an-dwh.jooble.com")) as con:
        cur = con.cursor()
        for table in ['tableau_workbook', 'tableau_datasource', 'tableau_workbook_datasource', 'tableau_physical_table',
                      'tableau_database_table', 'tableau_datasource_phys_table'][::-1]:
            q = 'delete from {}.{}'.format(dwh_schema, table)
            cur.execute(q)
        con.commit()
        cur.close()