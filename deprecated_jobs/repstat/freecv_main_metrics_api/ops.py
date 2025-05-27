import gzip
import os
import pickle
from datetime import datetime, timedelta
from typing import List

import pandas as pd
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

from etl_repstat.utils.date_format_settings import get_datediff
from etl_repstat.utils.dwh_db_operations import delete_data_from_dwh_table, save_to_dwh
# project import
from etl_repstat.utils.io_manager_path import get_io_manager_path
# module import
from etl_repstat.utils.messages import send_dwh_alert_slack_message
from etl_repstat.utils.rplc_config import clusters, map_country_code_to_id, all_countries_list
from etl_repstat.utils.rplc_db_operations import start_query_on_rplc_db, create_rplc_df
from etl_repstat.utils.rplc_job_config import retry_policy, job_config
from etl_repstat.utils.utils import delete_pkl_files, map_country_to_id, job_prefix

TABLE_NAME = "freecv_main_metrics_api"
SCHEMA = "kpav"
DELETE_DATE_DIFF_COLUMN = "query_request_date_diff"
DELETE_COUNTRY_COLUMN = "country_id"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
TODAY_DATE = datetime.now().date().strftime('%Y-%m-%d')
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
CALLED_PROC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", f"{TABLE_NAME}.sql")).read()


@op(out=Out(str))
def freecv_main_metrics_api_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to start procedure')
    return q


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def freecv_main_metrics_api_get_sqlinstance(context, query):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    launch_countries = context.resources.globals["reload_countries"]
    # if 'datediff' or 'date' format
    is_datediff = context.resources.globals["is_datediff"]
    launch_datediff_start = get_datediff(context.resources.globals["reload_date_start"]) if is_datediff else \
    context.resources.globals["reload_date_start"]
    launch_datediff_end = get_datediff(context.resources.globals["reload_date_end"]) if is_datediff else \
    context.resources.globals["reload_date_end"]

    context.log.info('Getting SQL instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     f'Start procedures for: [{launch_datediff_start}]/-/[{launch_datediff_end}]'
                     )

    # iterate over sql instances
    for cluster_info in clusters.values():
        for country in cluster_info['dbs']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(country).lower() in str(launch_country).strip('_').lower():
                    #  'to_sqlcode' > will pass any value to .sql file which starts with it
                    yield DynamicOutput(
                        value={'sql_instance_host': cluster_info['host'],
                               'country_db': str(country).lower().strip(),
                               'query': query,
                               'to_sqlcode_date_or_datediff_start': launch_datediff_start,
                               'to_sqlcode_date_or_datediff_end': launch_datediff_end
                               },
                        mapping_key='procedure_' + country
                    )


@op(out=Out(str), retry_policy=retry_policy)
def freecv_main_metrics_api_launch_query_on_db(context, sql_instance_country_query: dict) -> str:
    '''
    Launch query on each instance.
    '''
    file_path = start_query_on_rplc_db(context, sql_instance_country_query)
    return file_path


@op(out=Out(str))
def freecv_main_metrics_api_get_sql_to_create_rplc_df(context, result) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", "create_rplc_df.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to create dataframe from procedure result')
    return q


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def freecv_main_metrics_api_launch_df_creation(context, query):
    delete_pkl_files(context, PATH_TO_DATA)

    launch_countries = context.resources.globals["reload_countries"]
    # if 'datediff' or 'date' format
    is_datediff = context.resources.globals["is_datediff"]
    launch_datediff_start = get_datediff(context.resources.globals["reload_date_start"]) if is_datediff else \
    context.resources.globals["reload_date_start"]

    context.log.info('Getting SQL instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     f'Start dataframe creation: [{context.resources.globals["reload_date_start"]}]')

    # iterate over sql instances
    for cluster_info in clusters.values():
        for country in cluster_info['dbs']:
            # filter if custom countries
            for launch_country in launch_countries:
                if str(country).lower() in str(launch_country).strip('_').lower():
                    #  'to_sqlcode' > will pass any value to .sql file which starts with it
                    yield DynamicOutput(
                        value={'sql_instance_host': cluster_info['host'],
                               'country_db': str(country).lower().strip(),
                               'query': query,
                               'to_sqlcode_date_or_datediff_start': launch_datediff_start},
                        mapping_key='df_rplc_' + country
                    )


@op(out=Out(str), retry_policy=retry_policy)
def freecv_main_metrics_api_launch_query_to_create_df(context, sql_instance_country_query: dict) -> str:
    '''
    Launch query on each instance.
    '''
    file_path = create_rplc_df(context, PATH_TO_DATA, sql_instance_country_query)
    return file_path


@op(ins={"file_paths": In(List[str])},
    required_resource_keys={'globals'})
def freecv_main_metrics_api_delete_history_data(context, **kwargs):
    launch_countries = context.resources.globals["reload_countries"]
    # if 'datediff' or 'date' format
    is_datediff = True
    launch_datediff_start = get_datediff(context.resources.globals["reload_date_start"]) if is_datediff else \
    context.resources.globals["reload_date_start"]
    launch_datediff_end = get_datediff(context.resources.globals["reload_date_end"]) if is_datediff else \
    context.resources.globals["reload_date_end"]

    launch_countries_id_list = map_country_to_id(map_country_code_to_id, launch_countries)
    # Convert list of integers to string
    id_list = ', '.join(map(str, launch_countries_id_list))

    delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_COUNTRY_COLUMN, DELETE_DATE_DIFF_COLUMN,
                               launch_countries, launch_datediff_start, id_list, launch_datediff_end)


@op(required_resource_keys={'globals'})
def freecv_main_metrics_api_save_df_to_dwh(context, file_paths, delete):
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

        countries_count = int(result_df['country_id'].nunique())
        rows = int(result_df.shape[0])
        send_dwh_alert_slack_message(f":add: *{SCHEMA}.{TABLE_NAME}*\n"
                                     f">*[{context.resources.globals['reload_date_start']}]/-/[{context.resources.globals['reload_date_end']}]:* {countries_count} *countries* & {rows} *rows*\n")
        context.log.info('Successfully saved df to dwh.')
    except Exception as e:
        send_dwh_alert_slack_message(
            f":error_alert: saving to dwh error: *{TABLE_NAME}* <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'saving to dwh error: {e}')
        raise e


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    reload_date_end=Field(str, default_value=TODAY_DATE),
                                                    is_datediff=Field(bool, default_value=False)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{YESTERDAY_DATE}",
         "called_proc": f"{CALLED_PROC}",
         "destination_db": "dwh",
         "target_table": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def freecv_main_metrics_api_job():
    # start procedure on replica
    call_procedure_query = freecv_main_metrics_api_get_sql_query()
    replica_instances = freecv_main_metrics_api_get_sqlinstance(call_procedure_query)
    collect_result = replica_instances.map(freecv_main_metrics_api_launch_query_on_db).collect()
    # create df from procedure result table
    sql_create_df = freecv_main_metrics_api_get_sql_to_create_rplc_df(collect_result)
    df_creation_instances = freecv_main_metrics_api_launch_df_creation(sql_create_df)
    file_paths = df_creation_instances.map(freecv_main_metrics_api_launch_query_to_create_df).collect()
    # delete history from dwh
    delete = freecv_main_metrics_api_delete_history_data(file_paths)
    # save df to dwh
    freecv_main_metrics_api_save_df_to_dwh(file_paths, delete)
