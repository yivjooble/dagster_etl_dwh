import gzip
import os
import pickle
from datetime import datetime, timedelta

import pandas as pd
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput
)

# Import custom modules and functions
from ..utils.date_format_settings import get_datediff
from ..utils.dwh_db_operations import delete_data_from_dwh_table, save_to_dwh, check_if_db_table_exists
from ..utils.io_manager_path import get_io_manager_path
from ..utils.rplc_config import clusters, map_country_code_to_id, all_countries_list
from ..utils.rplc_db_operations import start_query_on_rplc_db, create_rplc_df, run_ddl_replica
from ..utils.rplc_job_config import retry_policy, job_config
from ..utils.utils import delete_pkl_files, job_prefix, get_gitlab_file_content, get_project_id, get_file_path

# Define constants
TABLE_NAME = "jdp_away_clicks_agg"
SCHEMA = "ono"
DELETE_DATE_DIFF_COLUMN = "load_datediff"
DELETE_COUNTRY_COLUMN = "country_id"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
PROCEDURE_CALL = "call an.prc_jdp_away_clicks_agg(%s);"
PROC_NAME_PARSED = PROCEDURE_CALL.split('(')[0].split()[-1].strip('an.')
GITLAB_DDL_Q, GITLAB_DDL_URL = get_gitlab_file_content(
    project_id=get_project_id(),
    file_path=get_file_path(dir_name=PROC_NAME_PARSED, file_name=PROC_NAME_PARSED),
)


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def jdp_away_clicks_agg_get_sql_instance(context):
    """
    Compute dictionary for DynamicOutput with params to run query on target db using Dagster multitasking.

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    # Delete any existing pickle files from the data directory
    delete_pkl_files(context, PATH_TO_DATA)

    # Get the list of countries to launch the procedure for
    launch_countries = context.resources.globals["reload_countries"]

    # Get the date range for which to launch the procedure
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]),
                               pd.to_datetime(context.resources.globals["reload_date_end"]))

    # Log some information about the selected countries and date range
    context.log.info(f'Selected countries: {launch_countries}\n'
                     f'Start procedures for: [{context.resources.globals["reload_date_start"]}]\n'
                     f"DDL run on replica:\n{GITLAB_DDL_URL}")

    # Iterate over the available clusters and databases
    for cluster_info in clusters.values():
        for country in cluster_info['dbs']:
            for launch_country in launch_countries:
                if str(country).lower() in str(launch_country).strip('_').lower():
                    for country_name, country_id in map_country_code_to_id.items():
                        if str(launch_country).strip('_').lower() in country_name:
                            # Yield a dictionary with the parameters needed to launch the procedure for this
                            # country and date range
                            yield DynamicOutput(
                                value={'sql_instance_host': cluster_info['host'],
                                       'country_db': str(country).lower().strip(),
                                       'query': PROCEDURE_CALL,
                                       'date_range': date_range,
                                       'ddl_query': GITLAB_DDL_Q,
                                       'country_id': country_id
                                       },
                                mapping_key='procedure_' + country
                            )


@op(retry_policy=retry_policy,
    required_resource_keys={'globals'})
def jdp_away_clicks_agg_launch_query_on_db(context, sql_instance_country_query: dict):
    """
    Start procedure on rpl with input data.

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    # Extract the necessary parameters from the input dictionary
    params = {
        'country_db': sql_instance_country_query['country_db'],
        'sql_instance_host': sql_instance_country_query['sql_instance_host'],
        'ddl_query': sql_instance_country_query['ddl_query'],
    }

    # Run the DDL query on the replica database
    run_ddl_replica(context, params)
    context.log.info(f'DDL run: {sql_instance_country_query["country_db"]}')

    # Get the country database and country ID
    country_db = sql_instance_country_query['country_db']
    country_id = sql_instance_country_query['country_id']

    # Iterate over the date range
    for date in sql_instance_country_query['date_range']:
        # Calculate the date difference between the current date and the reference date
        operation_date_diff = get_datediff(date.strftime('%Y-%m-%d'))
        context.log.info(f"--> Starting sql-script on: {date.strftime('%Y-%m-%d')}")

        # Set the query and date difference parameters for the procedure call
        sql_instance_country_query['query'] = PROCEDURE_CALL
        sql_instance_country_query['to_sqlcode_date_int'] = operation_date_diff

        # Start the procedure on the replica database
        start_query_on_rplc_db(context, sql_instance_country_query)

        # Remove the query and date difference parameters from the input dictionary
        try:
            del sql_instance_country_query['query']
            del sql_instance_country_query['to_sqlcode_date_int']
        except KeyError:
            context.log.error("Some key was not found")
            raise KeyError

        # Create a Pandas DataFrame from the procedure result table
        select_q, select_q_url = get_gitlab_file_content(
            project_id=get_project_id(),
            file_path=get_file_path(dir_name=PROC_NAME_PARSED, file_name='create_rpl_df'),
        )

        sql_instance_country_query['query'] = select_q
        sql_instance_country_query['to_sqlcode_date_or_datediff_start'] = operation_date_diff
        file_path = create_rplc_df(context, PATH_TO_DATA, sql_instance_country_query)

        try:
            del sql_instance_country_query['query']
            del sql_instance_country_query['to_sqlcode_date_or_datediff_start']
        except KeyError:
            context.log.error("Some key was not found")
            raise KeyError

        # Load the DataFrame from the pickle file
        if file_path.endswith(".pkl"):
            with gzip.open(file_path, 'rb') as f:
                country_df = pickle.load(f)

                # If the DataFrame is empty, log a message
                if country_df.empty:
                    context.log.info(f'Empty df for: {country_db}')
                else:
                    # Delete any existing data from the destination table in the data warehouse
                    delete_data_from_dwh_table(context,
                                               SCHEMA,
                                               TABLE_NAME,
                                               DELETE_COUNTRY_COLUMN,
                                               DELETE_DATE_DIFF_COLUMN,
                                               country_db,
                                               operation_date_diff,  # launch date_diff start
                                               country_id)
                    # Save the DataFrame to the data warehouse
                    save_to_dwh(country_df, TABLE_NAME, SCHEMA)
                    context.log.info(f'|{country_db.upper()}|: Successfully saved df to dwh.\n'
                                     f'---------------------------------')


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    reload_date_end=Field(str, default_value=YESTERDAY_DATE),
                                                    is_datediff=Field(bool, default_value=True)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     description=f'''{SCHEMA}.{TABLE_NAME}''',
     tags={"data_model": f"{SCHEMA}"},
     metadata={
         "input_date": f"{YESTERDAY_DATE}",
         "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
         "destination_db": "dwh",
         "target_table": f"{SCHEMA}.{TABLE_NAME}",
     }
     )
def jdp_away_clicks_agg_job():
    """
    Orchestrate the execution of the jdp_away_clicks_agg pipeline.

    Returns:
        None
    """
    # Start the procedure on the replica database for each country and date range
    replica_instances = jdp_away_clicks_agg_get_sql_instance()
    replica_instances.map(jdp_away_clicks_agg_launch_query_on_db).collect()
