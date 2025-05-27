import os
import pandas as pd
import gzip
import pickle

from datetime import datetime, timedelta
from typing import List

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    In,
    Out,
    DynamicOut,
    DynamicOutput
    )

# project import
from ..utils.io_manager_path import get_io_manager_path
# module import
from ..utils.messages import send_dwh_alert_slack_message
from ..utils.prod_job_config import retry_policy, job_config
from ..utils.prod_sqlinstances import SqlInstanceList, map_country_code_to_id
from ..utils.prod_db_operations import start_query_on_prod_db
from ..utils.date_format_settings import get_datediff
from ..utils.dwh_db_operations import delete_data_from_dwh_table, save_to_dwh, execute_on_dwh
from ..utils.utils import delete_pkl_files, map_country_to_id



TABLE_NAME = "session_action"
SCHEMA = "imp"
DELETE_DATE_DIFF_COLUMN = "date_diff"
DELETE_COUNTRY_COLUMN = "country"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

CUSTOM_COUNTRIES_LIST = [
    'ua', 'hu', 'ro'
]



@op(out=Out(str))
def session_action_get_sql_query(context) -> str:
    """
     Get sql query from sql file. This is a function to be called by session_action_get_sql_query
     
     Args:
     	 context: Information about the user session.
     
     Returns: 
     	 The sql query as a string. Example usage. code - block ::
    """
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('Loaded SQL query')
    return q


@op(out=DynamicOut(), 
    required_resource_keys={'globals'})
def session_action_get_sqlinstance(context, query):
    """
     Get SQL instances and save them to the database. This is called when we want to reload the data
     
     Args:
     	 context: The context of the session
     	 query: The SQL query to be executed on the DB
    """
    # delete previously stored data
    delete_pkl_files(context, PATH_TO_DATA) 

    launch_countries = context.resources.globals["reload_countries"]
    # if 'datediff' or 'date' format
    is_datediff = context.resources.globals["is_datediff"]
    launch_datediff_start = get_datediff(context.resources.globals["reload_date_start"]) if is_datediff else context.resources.globals["reload_date_start"]
    launch_datediff_end = get_datediff(context.resources.globals["reload_date_end"]) if is_datediff else context.resources.globals["reload_date_end"]

    context.log.info('Getting SQL instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     f'Start loading data for: [between {context.resources.globals["reload_date_start"]} and {context.resources.globals["reload_date_end"]}]')
    
    # iterate over sql instances
    for sql_instance in SqlInstanceList:
        for country_db in sql_instance['CountryList']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(country_db).lower() in str(launch_country).strip('_').lower():
                    # add country_id
                    for country_name, country_id in map_country_code_to_id.items():
                        if str(launch_country).strip('_').lower() in country_name:
                            #  'to_sqlcode' > will pass any value to .sql file which starts with it
                            yield DynamicOutput(
                                value={'sql_instance': sql_instance, 
                                       'country_db': country_db, 
                                       'country_id': country_id, 
                                       'query': query,
                                       'to_sqlcode_date_or_datediff_start': launch_datediff_start,
                                       'to_sqlcode_date_or_datediff_end': launch_datediff_end,},
                                mapping_key='Job_'+country_db
                            )


@op(out=Out(str), retry_policy=retry_policy)
def session_action_launch_query_on_db(context, sql_instance_country_query: dict) -> str:

    file_path = start_query_on_prod_db(context, PATH_TO_DATA, TABLE_NAME, sql_instance_country_query, DELETE_COUNTRY_COLUMN)
    return file_path


@op(ins={"file_paths": In(List[str])},
    required_resource_keys={'globals'})
def session_action_delete_history_data(context, **kwargs):

    launch_countries = context.resources.globals["reload_countries"]
    # if 'datediff' or 'date' format
    is_datediff = context.resources.globals["is_datediff"]
    launch_datediff_start = get_datediff(context.resources.globals["reload_date_start"]) if is_datediff else context.resources.globals["reload_date_start"]
    launch_datediff_end = get_datediff(context.resources.globals["reload_date_end"]) if is_datediff else context.resources.globals["reload_date_end"]

    launch_countries_id_list = map_country_to_id(map_country_code_to_id, launch_countries)
    # Convert list of integers to string
    id_list = ', '.join(map(str, launch_countries_id_list))  

    delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_COUNTRY_COLUMN, DELETE_DATE_DIFF_COLUMN, launch_countries, launch_datediff_start, launch_datediff_end, id_list)


@op(required_resource_keys={'globals'})
def session_action_save_df_to_dwh(context, file_paths: list, delete: bool) -> bool:
    """
    Save the df to dwh.

    Parameters
    ----------
    context:
        Context of the session.
    file_paths: list
        List of file paths.
    delete: bool
        Delete the file path.

    Returns
    -------
    bool
        True if the save is successful.

    """
    try:
        all_dfs_list = []
        for file_path in file_paths:
            if file_path.endswith(".pkl"):
                with gzip.open(file_path, 'rb') as f:
                    country_df = pickle.load(f)
                    all_dfs_list.append(country_df)
                    context.log.info(f'df created for: {os.path.basename(file_path)}')
        
        # save to dwh table
        result_df = pd.concat(all_dfs_list)
        save_to_dwh(result_df, TABLE_NAME, SCHEMA)
        
        countries_count = int(result_df[f'{DELETE_COUNTRY_COLUMN}'].nunique())
        rows = int(result_df.shape[0])
        send_dwh_alert_slack_message(f":add: *{SCHEMA}.{TABLE_NAME}*\n"
                                        f">*[{context.resources.globals['reload_date_start']}/-/{context.resources.globals['reload_date_end']}]:* {countries_count} *countries* & {rows} *rows*\n")
        context.log.info('Successfully saved df to dwh.')
        return True
    except Exception as e:
        send_dwh_alert_slack_message(f":error_alert: saving to dwh error: *{TABLE_NAME}* <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'saving to dwh error: {e}')
        raise e


@op(description='Delete old data due to sql script inside.')
def session_action_delete_old_data(context, save_result):
    # Get sql query from .sql file.
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", "delete_old_data.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
        context.log.info('Loaded SQL query to delete')
    # pass to execute
    execute_on_dwh(q)
    context.log.info('Successfully deleted.')



@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRIES_LIST),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                    is_datediff=Field(bool, default_value=True)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name='prd__'+TABLE_NAME,
    description=f'{SCHEMA}.{TABLE_NAME}')
def session_action_job():
    query = session_action_get_sql_query()
    instances = session_action_get_sqlinstance(query)
    file_paths = instances.map(session_action_launch_query_on_db).collect()
    delete = session_action_delete_history_data(file_paths)
    save_result = session_action_save_df_to_dwh(file_paths, delete)
    # session_action_delete_old_data(save_result)