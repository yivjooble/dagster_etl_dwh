import inspect
from datetime import datetime
from dagster import op, job, Field, make_values_resource, fs_io_manager

from etl_affiliate.utils.io_manager_path import get_io_manager_path
from etl_affiliate.utils.utils import exec_query_pg, log_written_data, job_prefix, call_dwh_procedure

SCHEMA = 'affiliate'
JOB_PREFIX = job_prefix()


@op(required_resource_keys={'globals'})
def refresh_tracking_project_list_op(context) -> bool:
    """
    Refresh materialized view mv_tracking_project_list in target schema.
    """

    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    exec_query_pg(('refresh materialized view {target_schema}.mv_tracking_project_list;'.format(
        target_schema=schema_name)), host='dwh', destination_db="dwh")
    
    log_written_data(context=context, table_schema=schema_name, table_name='mv_tracking_project_list',
                      date=datetime.now().strftime('%Y-%m-%d'), start_dt=start_dt,
                      end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name, send_slack_message=False)

    return True


@op(required_resource_keys={'globals'})
def insert_tracking_project_list_op(context) -> bool:
    """
    Cloudberry: materialise -> affiliate.tracking_project_list.
    """

    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    call_dwh_procedure(schema_name, "insert_tracking_project_list()", destination_db="cloudberry")

    log_written_data(context=context, table_schema=schema_name, table_name='tracking_project_list',
                      date=datetime.now().strftime('%Y-%m-%d'), start_dt=start_dt,
                      end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), op_name=op_name, send_slack_message=True)
    
    return True


@job(resource_defs={"globals": make_values_resource(schema_name=Field(str, default_value='affiliate')),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'refresh_tracking_project_list',
     description=f'Refreshes materialized view affiliate.mv_tracking_project_list.'
     )
def refresh_tracking_project_list_job():
    refresh_tracking_project_list_op()
    insert_tracking_project_list_op()
