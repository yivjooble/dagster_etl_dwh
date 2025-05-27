import os
import inspect

from sqlalchemy import text
from dagster import asset

# import modules
import pandas as pd

# project import
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table, dwh_conn_sqlalchemy
# module import
from ...utils.outreach_connections import ltf_conn_sqlalchemy
from ...utils.messages import send_dwh_alert_slack_message

TABLE_NAME = "outreach_kpi_closed"
SCHEMA = "aggregation"



@asset(group_name="outreach_kpi_closed", key_prefix="period_check")
def get_max_period_id_outreach_period(context):
    '''get the max period_id from the aggregation.outreach_period table'''
    query = '''SELECT max(pp.id) as period_id FROM aggregation.outreach_period pp WHERE pp.closed=1;'''
    with dwh_conn_sqlalchemy().connect() as connection:
        result = connection.execute(text(query))
        max_period_id = result.fetchone()[0]
    context.log.info(f"Max period_id in aggregation.outreach_period: {max_period_id}")
    return max_period_id


@asset(group_name="outreach_kpi_closed", key_prefix="period_check")
def check_outreach_kpi_closed(context, get_max_period_id_outreach_period):
    '''check if the max_period_id exists in the aggregation.outreach_kpi_closed table'''
    query = f'''SELECT 1 FROM aggregation.outreach_kpi_closed WHERE period_id = {get_max_period_id_outreach_period};'''
    with dwh_conn_sqlalchemy().connect() as connection:
        result = connection.execute(text(query))
        exists = result.fetchone() is not None
    context.log.info(f"Max period_id exists in aggregation.outreach_kpi_closed: {exists}")
    return exists


@asset(group_name="outreach_kpi_closed", key_prefix="period_check")
def check_link_lead_salary(context, get_max_period_id_outreach_period):
    '''check if the max_period_id exists in the dbo.link_lead_salary table'''
    query = f'''SELECT 1 FROM dbo.link_lead_salary WHERE id_period = {get_max_period_id_outreach_period};'''
    with ltf_conn_sqlalchemy().connect() as connection:
        result = connection.execute(text(query))
        exists = result.fetchone() is not None
    context.log.info(f"Max period_id exists in dbo.link_lead_salary: {exists}")
    return exists


@asset(group_name="outreach_kpi_closed", key_prefix="main_pipeline")
def get_sql_query(context, check_outreach_kpi_closed, check_link_lead_salary):
    '''load sql query to perform a request'''
    if not check_outreach_kpi_closed and check_link_lead_salary:
        context.log.info(f'====== There is a new period to add.')
        script_dir = os.path.dirname(__file__)
        relative_path = os.path.join("sql", "outreach_kpi_closed.sql")
        sql_file = os.path.join(script_dir, relative_path)

        with open(sql_file, 'r') as file:
            sql_query = file.read()
        context.log.info(f"Got sql_query for {TABLE_NAME}")
        return sql_query
    else:
        context.log.info(f'====== There is no such period OR the period is already exists:\n'
                    f'-- check_outreach_kpi_closed: {check_outreach_kpi_closed}\n'
                    f'-- check_link_lead_salary: {check_link_lead_salary}')
        return None


@asset(group_name="outreach_kpi_closed", key_prefix="main_pipeline")
def create_df(context, get_sql_query):
    '''perform sql request to Outreach server'''
    if get_sql_query is not None:
        context.log.info("start to create dataframe")
        df = pd.read_sql_query(get_sql_query, 
                            con=ltf_conn_sqlalchemy())
        return df
    else:
        context.log.info(f'{inspect.currentframe().f_code.co_name}: pass the step.')
        return None


@asset(group_name="outreach_kpi_closed", key_prefix="main_pipeline")
def truncate_target_table(context, get_sql_query):
    '''truncate target table'''
    if get_sql_query is not None:
        context.log.info(f"start to truncate {SCHEMA}.{TABLE_NAME}")
        truncate_dwh_table(context,
                           table_name=TABLE_NAME, 
                            schema=SCHEMA)
    else:
        context.log.info(f'{inspect.currentframe().f_code.co_name}: pass the step.')


@asset(group_name="outreach_kpi_closed", 
       key_prefix="main_pipeline", 
       non_argument_deps={'truncate_target_table'})
def save_to_target_table(context, create_df):
    '''save result dataframe'''
    if create_df is not None:
        context.log.info(f"save [{TABLE_NAME}]")
        save_to_dwh(df=create_df, 
                    table_name=TABLE_NAME, 
                    schema=SCHEMA)
        send_dwh_alert_slack_message(f":white_check_mark: *{TABLE_NAME}.{TABLE_NAME}* executed successfully")
    else:
        context.log.info(f'{inspect.currentframe().f_code.co_name}: pass the step.')