import os
import pandas as pd
import gzip
import pickle

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
from ...utils.io_manager_path import get_io_manager_path
# module import
from ...utils.job_config import retry_policy, job_config
from ...utils.db_config import history_db_info
from ...utils.db_operations import start_query_on_history_db
from utility_hub.core_tools import get_previous_month_dates, generate_job_name
from utility_hub.data_collections import map_country_code_to_id, abroad_regions
from utility_hub.db_operations import Operations, DwhOperations


TABLE_NAME = "vacancy_job_history_agg_new"
SCHEMA = "aggregation"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CALLED_PROC = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", f"{TABLE_NAME}.sql")).read()
PREVIOUS_MONTH_START, PREVIOUS_MONTH_END = get_previous_month_dates()
# Custom countries list
ALL_COUNTRIES_LIST = ['uk', 'de', 'at', 'be', 'ch', 'nl', 'pl', 'ro', 'hu', 'ca', 'fr', 'rs', 'us']


@op(out=Out(str))
def vacancy_job_history_get_sql_query(context) -> str:
    """Get sql query from .sql file."""
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info(f'===== Loaded SQL query to start procedure\n{path_to_query}')
    return q


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def vacancy_job_history_get_sqlinstance(context, query):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    """
    Operations.delete_files(context, PATH_TO_DATA)

    launch_countries = context.resources.globals["reload_countries"]
    previous_month_start = context.resources.globals["previous_month_start"]
    previous_month_end = context.resources.globals["previous_month_end"]

    context.log.info('Getting SQL instances...\n'
                     f'{previous_month_start}/-/{previous_month_end}')

    # iterate over sql instances
    for cluster_info in history_db_info.values():
        for db_name in cluster_info['dbs']:
            for region_country, region_codes in abroad_regions.items():
                if str(region_country).lower() in str(db_name).lower():
                    # add country_id
                    for country_name, country_id in map_country_code_to_id.items():
                        if str(db_name).strip('_').lower() in country_name:
                            # if custom countries list
                            for launch_country in launch_countries:
                                if str(db_name).lower() in str(launch_country).strip('_').lower():
                                    yield DynamicOutput(
                                        value={'sql_instance_host': cluster_info['host'],
                                               'db_name': db_name,
                                               'country_id': country_id,
                                               'query': query,
                                               'previous_month_start': previous_month_start,
                                               'previous_month_end': previous_month_end,
                                               'abroad_region': region_codes
                                               },
                                        mapping_key='db_name_' + db_name
                                    )


@op(out=Out(str), retry_policy=retry_policy)
def vacancy_job_history_launch_query_on_db(context, sql_instance_country_query: dict) -> str:
    """
    Launch query on each instance.
    """
    file_path = start_query_on_history_db(context, PATH_TO_DATA, sql_instance_country_query)
    return file_path


@op(ins={"file_paths": In(List[str])}, required_resource_keys={'globals'})
def vacancy_job_history_save_df_to_dwh(context, file_paths, **kwargs):
    destination_db = context.resources.globals["destination_db"]
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

        if result_df.empty:
            context.log.info('No data to save to dwh.')
            return

        DwhOperations.save_to_dwh_pandas(context,
                                         schema=SCHEMA,
                                         table_name=TABLE_NAME,
                                         df=result_df,
                                         destination_db=destination_db)

        context.log.info('Successfully saved df to dwh.')
    except Exception as e:
        context.log.error(f'saving to dwh error: {e}')
        raise e


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=ALL_COUNTRIES_LIST),
                                                    previous_month_start=Field(str, default_value=PREVIOUS_MONTH_START),
                                                    previous_month_end=Field(str, default_value=PREVIOUS_MONTH_END),
                                                    destination_db=Field(str, default_value="both")),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=generate_job_name(TABLE_NAME),
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "destination_db": "dwh, cloudberry, both",
         "target_table": f"{SCHEMA}.{TABLE_NAME}",
     },
     description=f'{SCHEMA}.{TABLE_NAME}')
def vacancy_job_history_job():
    """
    Job to aggregate vacancy_job_history data.
    """
    # start procedure on replica
    query = vacancy_job_history_get_sql_query()
    db_instances = vacancy_job_history_get_sqlinstance(query)
    file_paths = db_instances.map(vacancy_job_history_launch_query_on_db).collect()
    # save df to dwh
    vacancy_job_history_save_df_to_dwh(file_paths)
