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
from ..utils.io_manager_path import get_io_manager_path
# module import
from ..utils.rplc_job_config import retry_policy, job_config, snapshot_job_config
from ..utils.rplc_config import clusters, first_part_countries_list, second_part_countries_list
from ..utils.rplc_db_operations import start_query_on_rplc_db
from ..utils.utils import job_prefix



TABLE_NAME = "prc_insert_email_alert_snapshot"
SCHEMA = "an"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
JOB_PREFIX = job_prefix()



@op(out=Out(str))
def prc_insert_email_alert_snapshot_get_sql_query(context) -> str:
    '''Get sql query from .sql file.'''
    path_to_query = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(path_to_query, 'r') as query:
        q = query.read()
    context.log.info('===== Loaded SQL query to start procedure')
    return q


@op(out=DynamicOut(), 
    required_resource_keys={'globals'})
def prc_insert_email_alert_snapshot_get_sqlinstance(context, query):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    launch_countries = context.resources.globals["reload_countries"]
    
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
                        mapping_key='procedure_'+country
                    )


@op(retry_policy=retry_policy)
def prc_insert_email_alert_snapshot_launch_query_on_db(context, sql_instance_country_query: dict):
    '''
    Launch query on each instance.
    '''
    result = start_query_on_rplc_db(context, sql_instance_country_query)



@job(config=snapshot_job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=first_part_countries_list),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=JOB_PREFIX+TABLE_NAME+'_first_part',
    description=f'{SCHEMA}.{TABLE_NAME}')
def prc_insert_email_alert_snapshot_first_part_job():
    # start procedure on replica
    call_procedure_query = prc_insert_email_alert_snapshot_get_sql_query()
    replica_instances = prc_insert_email_alert_snapshot_get_sqlinstance(call_procedure_query)
    replica_instances.map(prc_insert_email_alert_snapshot_launch_query_on_db)
    
    
@job(config=snapshot_job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=second_part_countries_list),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=JOB_PREFIX+TABLE_NAME+'_second_part',
    description=f'{SCHEMA}.{TABLE_NAME}')
def prc_insert_email_alert_snapshot_second_part_job():
    # start procedure on replica
    call_procedure_query = prc_insert_email_alert_snapshot_get_sql_query()
    replica_instances = prc_insert_email_alert_snapshot_get_sqlinstance(call_procedure_query)
    replica_instances.map(prc_insert_email_alert_snapshot_launch_query_on_db)