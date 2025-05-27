import os

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
from etl_repstat.utils.io_manager_path import get_io_manager_path
from etl_repstat.utils.rplc_config import clusters
from etl_repstat.utils.rplc_db_operations import start_query_on_rplc_db
# module import
from etl_repstat.utils.rplc_job_config import retry_policy, snapshot_job_config
from etl_repstat.utils.utils import job_prefix

TABLE_NAME = "snap_api_query_full_reload"
SCHEMA = "an"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()
CUSTOM_COUNTRIES = ['us']


@op(out=Out(str))
def snap_api_query_full_reload_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to start procedure')
    return q


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def snap_api_query_full_reload_get_sqlinstance(context, query):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info('Getting SQL instances...\n'
                     f"query: {query}\n"
                     f'Selected  countries: {launch_countries}\n'
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
                               },
                        mapping_key='procedure_' + country
                    )


@op(retry_policy=retry_policy)
def snap_api_query_full_reload_launch_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    start_query_on_rplc_db(context, sql_instance_country_query)


@job(config=snapshot_job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=CUSTOM_COUNTRIES),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + TABLE_NAME,
     tags={"job_type": "rpl_snap"},
     description=f'{SCHEMA}.{TABLE_NAME}')
def snap_api_query_full_reload_job():
    # start procedure on replica
    call_procedure_query = snap_api_query_full_reload_get_sql_query()
    replica_instances = snap_api_query_full_reload_get_sqlinstance(call_procedure_query)
    replica_instances.map(snap_api_query_full_reload_launch_query_on_db)
