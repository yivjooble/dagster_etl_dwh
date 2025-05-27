import os

from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Out,
    DynamicOut,
    Field,
    DynamicOutput
)

# project import
from ..utils.io_manager_path import get_io_manager_path
# module import
from ..utils.rplc_job_config import retry_policy, job_config
from ..utils.rplc_config import clusters, all_countries_list
from ..utils.rplc_db_operations import start_query_on_rplc_db
from ..utils.date_format_settings import get_datediff
from ..utils.utils import job_prefix

TABLE_NAME = "sync_session_click_primary_click_with_mssql"
SCHEMA = "public"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
JOB_PREFIX = job_prefix()


@op(out=Out(str))
def sync_primary_click_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to start procedure')
    return q


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def sync_primary_click_get_sqlinstance(context, query):
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

    context.log.info('Getting SQL instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     f'Start procedures for: [{context.resources.globals["reload_date_start"]}]')

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
                        mapping_key='procedure_' + country
                    )


@op(retry_policy=retry_policy)
def sync_primary_click_launch_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    file_path = start_query_on_rplc_db(context, sql_instance_country_query)


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    is_datediff=Field(bool, default_value=True)),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}')
def sync_session_click_primary_click_with_mssql_job():
    # start procedure on replica
    call_procedure_query = sync_primary_click_get_sql_query()
    replica_instances = sync_primary_click_get_sqlinstance(call_procedure_query)
    replica_instances.map(sync_primary_click_launch_query_on_db)
