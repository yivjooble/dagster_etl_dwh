import os
import pandas as pd
import gzip
import pickle
import pathlib

from datetime import datetime, timedelta
from typing import List

from dagster import (
    op,
    job,
    fs_io_manager,
    In,
    Out,
    DynamicOut,
    DynamicOutput
)

# project import
from ...utils.io_manager_path import get_io_manager_path
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.job_config import retry_policy, job_config
from ...utils.db_config import mssql_warehouse_jo_info
from ...utils.db_operations import start_query_mssql_db
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table
from utility_hub.db_operations import Operations
from utility_hub.core_tools import generate_job_name


TABLE_NAME = "clicks_campaign"
SCHEMA = "vnov"
# DELETE_DATE_COLUMN = 'created_at'
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')


@op(out=Out(str))
def yiv_to_dwh_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to start procedure')
    return q


@op(out=DynamicOut())
def yiv_to_dwh_get_sqlinstance(context, query):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    Operations.delete_files(context, PATH_TO_DATA)

    context.log.info('Getting SQL instances...\n'
                     )

    # iterate over sql instances
    for cluster_info in mssql_warehouse_jo_info.values():
        for db_name in cluster_info['dbs']:
            if db_name == 'Marketing':
                #  'to_sqlcode' > will pass any value to .sql file which starts with it
                yield DynamicOutput(
                    value={'sql_instance_host': cluster_info['host'],
                            'db_name': str(db_name).lower().strip(),
                            'query': query,
                            },
                    mapping_key='db_name_'+db_name
                )


@op(out=Out(str), retry_policy=retry_policy)
def yiv_to_dwh_launch_query_on_db(context, sql_instance_country_query: dict) -> str:
    '''
    Launch query on each instance.
    '''
    file_path = start_query_mssql_db(context, PATH_TO_DATA, TABLE_NAME, sql_instance_country_query)
    return file_path


@op(ins={"file_paths": In(List[str])})
def yiv_to_dwh_save_df_to_dwh(context, file_paths):
    try:
        all_dfs_list = []
        for file_path in file_paths:
            if file_path.endswith(".pkl"):
                with gzip.open(file_path, 'rb') as f:
                    country_df = pickle.load(f)
                    all_dfs_list.append(country_df)
                    context.log.info(f'df created for: {os.path.basename(file_path)}')

        # save to dwh table
        # truncate_dwh_table(TABLE_NAME, SCHEMA)

        result_df = pd.concat(all_dfs_list)
        save_to_dwh(result_df, TABLE_NAME, SCHEMA)

        send_dwh_alert_slack_message(f":add: *{SCHEMA}.{TABLE_NAME}*\n"
                                        )

        context.log.info('Successfully saved df to dwh.')
    except Exception as e:
        send_dwh_alert_slack_message(f":error_alert: saving to dwh error: *{TABLE_NAME}* <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'saving to dwh error: {e}')
        raise e



@job(config=job_config,
     resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=generate_job_name(TABLE_NAME),
     description=f'{SCHEMA}.{TABLE_NAME}')
def yiv_to_dwh_job():
    # start procedure on replica
    query = yiv_to_dwh_get_sql_query()
    db_instances = yiv_to_dwh_get_sqlinstance(query)
    file_paths = db_instances.map(yiv_to_dwh_launch_query_on_db).collect()
    # save df to dwh
    yiv_to_dwh_save_df_to_dwh(file_paths)
