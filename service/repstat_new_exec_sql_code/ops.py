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
from ..utils.rplc_config import stat_new_clusters, all_countries_list
from .repstat_exec_sql_code_utils.repstat_db_operations import start_query_on_rplc_db
from ..utils.utils import job_prefix

JOB_NAME = "repstat_new_exec_sql_code"
JOB_PREFIX = job_prefix()
DESCRIPTION = 'Execute custom sql code over repstat dbs.'


@op(out=DynamicOut(),
    required_resource_keys={'globals'})
def repstat_new_exec_sql_code_get_sqlinstance(context):
    '''
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    '''
    launch_countries = context.resources.globals["reload_countries"]
    query = context.resources.globals["query"]
    # if 'datediff' or 'date' format

    # iterate over sql instances
    for cluster_info in stat_new_clusters.values():
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


@op(out=Out(str), retry_policy=retry_policy)
def repstat_new_exec_sql_code_launch_query_on_db(context, sql_instance_country_query: dict) -> str:
    '''
    Launch query on each instance.
    '''
    file_path = start_query_on_rplc_db(context, sql_instance_country_query)
    return file_path


@job(config=job_config,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    query=Field(str, default_value='')),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + JOB_NAME,
     description=DESCRIPTION)
def repstat_new_exec_sql_code_job():
    # start procedure on replica
    replica_instances = repstat_new_exec_sql_code_get_sqlinstance()
    replica_instances.map(repstat_new_exec_sql_code_launch_query_on_db).collect()



job_config_custom = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 14,
            }
        }
    }
}


@job(config=job_config_custom,
     resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                    query=Field(str, default_value='call an.load_data_from_link_tables_custom_date(45668);')),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + JOB_NAME + '_load_data_from_link',
     description=DESCRIPTION)
def repstat_new_exec_load_data_from_link():
    # start procedure on replica
    replica_instances = repstat_new_exec_sql_code_get_sqlinstance()
    replica_instances.map(repstat_new_exec_sql_code_launch_query_on_db).collect()

