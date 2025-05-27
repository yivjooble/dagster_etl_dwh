import inspect
import json
import pandas as pd

from dagster import job, op, Field, make_values_resource, fs_io_manager, Failure
from datetime import datetime, timedelta

from etl_affiliate.utils.utils import get_gitlab_file_content, create_conn, \
    log_written_data, job_prefix, call_dwh_procedure, exec_query_pg, save_to_dwh
from etl_affiliate.utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import get_creds_from_vault

JOB_PREFIX = job_prefix()

TARGET_DATE = datetime.today() - timedelta(1)
TARGET_DATE_DIFF = (TARGET_DATE - datetime(1900, 1, 1)).days
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


@op(required_resource_keys={'globals'})
def refresh_pub_conv_by_category_op(context, prev_result) -> bool:
    """
    Refresh materialized view pub_conv_by_category in target schema.
    
    Args:
        context: Dagster run context object.
        prev_result: result of the previous operation.
    """

    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    exec_query_pg(('refresh materialized view {target_schema}.mv_pub_conv_by_category;'.format(
        target_schema=schema_name)), host='dwh', destination_db="dwh")

    exec_query_pg(('call {target_schema}.insert_pub_conv_by_category();'.format(
        target_schema=schema_name)), host='dwh', destination_db="cloudberry")

    log_written_data(context=context, table_schema=schema_name, table_name='mv_pub_conv_by_category',
                     date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     op_name=op_name, send_slack_message=True)
    return prev_result


@op(required_resource_keys={'globals'})
def refresh_mv_project_jobs_by_category_op(context, prev_result) -> bool:
    """
    Refresh materialized view mv_project_jobs_by_category in target schema.
    
    Args:
        context: Dagster run context object.
        prev_result: result of the previous operation.
    """

    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    exec_query_pg(('refresh materialized view {target_schema}.mv_project_jobs_by_category;'.format(
        target_schema=schema_name)), host='dwh')

    log_written_data(context=context, table_schema=schema_name, table_name='mv_project_jobs_by_category',
                     date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     op_name=op_name, send_slack_message=True)
    return prev_result


@op(required_resource_keys={'globals'})
def prc_insert_project_conversions_delay_op(context) -> bool:
    """
    Refreshes the project_conversions_delay table in the specified schema with data on project delays.

    This function retrieves delay data from a JSON file stored in GitLab, filters and enriches it 
    with additional project information, and then saves it to the project_conversions_delay
    table in the dwh.  
    
    Args:
        context (obj): Dagster run context object.
    """
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    exec_query_pg("TRUNCATE {}.project_conversions_delay".format(schema_name), "dwh")

    a = get_gitlab_file_content(file_path='modules/applies_and_conversions/hand_gather_conversion/appcast/delay.json',
                                api_token=get_creds_from_vault('GITLAB_PRIVATE_TOKEN_MULTITOOL'),
                                project_id='887')
    a = json.loads(a)

    df = pd.DataFrame(columns=['country', 'project_id', 'delay'])

    for country, items in a.items():
        for project_id, delay in items.items():
            new_row = pd.DataFrame([{'country': country, 'project_id': project_id, 'delay': delay}])
            df = pd.concat([df, new_row], ignore_index=True)

    multi_projects = df[df['country'].str.lower() == 'multi']
    
    if not multi_projects.empty:
        project_ids = "', '".join(multi_projects['project_id'].astype(str))
        dwh_multi_projects = pd.read_sql(f"""
                                            SELECT ip.id, c.alpha_2 as country
                                            FROM dimension.info_project as ip
                                            JOIN dimension.countries as c
                                            ON c.id = ip.country
                                            WHERE ip.id IN ('{project_ids}')
                                        """, con=create_conn("dwh"))

        multi_projects['project_id'] = multi_projects['project_id'].astype('Int32')
        dwh_multi_projects['id'] = dwh_multi_projects['id'].astype('Int32')
        merged_multi_projects = multi_projects.merge(dwh_multi_projects, how='inner', left_on='project_id', right_on='id')

        for index, row in merged_multi_projects.iterrows():
            new_row = pd.DataFrame([{
                'country': row['country_y'],
                'project_id': row['project_id'],
                'delay': row['delay']
            }])
            df = pd.concat([df, new_row], ignore_index=True)

    df = df[df['country'].str.lower() != 'multi']

    save_to_dwh(df, table_name='project_conversions_delay', schema=schema_name)

    log_written_data(context=context, table_schema=schema_name, table_name='project_conversions_delay',
                     date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                     op_name=op_name, send_slack_message=True)
    return True


@op(required_resource_keys={'globals'})
def prc_insert_additional_revenue_agg_op(context, prev_result) -> bool:
    """
    Calls insert_additional_revenue_agg procedure.

    Args:
        context: Dagster run context object.
        prev_result: Result of the previous operation.
    """
    target_date_str = context.resources.globals["target_date_str"]
    schema_name = context.resources.globals["schema_name"]
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name

    try:
        call_dwh_procedure(schema_name, 'insert_additional_revenue_agg(\'{}\')'.format(target_date_str))
        log_written_data(context=context, table_schema=schema_name, table_name='additional_revenue_agg',
                         date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         op_name=op_name, send_slack_message=True)
        return prev_result
    except:
        raise Failure(description=f"{op_name} error")


@op(required_resource_keys={'globals'})
def prc_insert_cloudflare_internal_data_mapping(context, prev_result):
    """
    Calls DWH procedure affiliate.insert_cloudflare_internal_data_mapping() to map 
    affiliate.cloudflare_log_data and internal data.
    """
    target_date_str = context.resources.globals["target_date_str"]
    date_diff = (datetime.strptime(target_date_str, '%Y-%m-%d').date() - datetime(1900, 1, 1).date()).days
    schema_name = context.resources.globals["schema_name"]
    
    start_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    op_name = inspect.currentframe().f_code.co_name
    
    try:
        call_dwh_procedure(schema_name, 'insert_cloudflare_internal_data_mapping({})'.format(date_diff))
        log_written_data(context=context, table_schema=schema_name, table_name='cloudflare_internal_data_mapping',
                         date=target_date_str, start_dt=start_dt, end_dt=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         op_name=op_name, send_slack_message=True)
        return prev_result
    except:
        raise Failure(description=f"{op_name} error")



@job(resource_defs={"globals": make_values_resource(target_date_diff=Field(int, default_value=TARGET_DATE_DIFF),
                                                    target_date_str=Field(str, default_value=TARGET_DATE_STR),
                                                    schema_name=Field(str, default_value='affiliate')),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=JOB_PREFIX + 'collect_additional_statistics',
     description=f'Collects data to the DWH tables of the affiliate schema: project_conversions_delay,'
                 f'additional_revenue_agg, cloudflare_internal_data_mapping; '
                 f'refreshes materialized views: mv_project_jobs_by_category,'
                 f'pub_conv_by_category.'
     )
def collect_additional_statistics_job():
    insert_delay_result = prc_insert_project_conversions_delay_op()
    mv_project_jobs_by_category = refresh_mv_project_jobs_by_category_op(insert_delay_result)
    insert_additional_revenue_result = prc_insert_additional_revenue_agg_op(mv_project_jobs_by_category)
    pub_conv_by_category = refresh_pub_conv_by_category_op(insert_additional_revenue_result)
    prc_insert_cloudflare_internal_data_mapping(pub_conv_by_category)