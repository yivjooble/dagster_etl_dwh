from dagster import (
    op, 
    Out, 
    In, 
    Field,
    job, 
    fs_io_manager,
    make_values_resource
)

# import modules
import pandas as pd
import logging
import os

from datetime import datetime, timedelta
from sqlalchemy import text
# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy
from ...utils.dwh_db_operations import delete_data_from_dwh_table


SCHEMA = 'aggregation'
TABLE_NAME = 'outreach_domain_tags'
DELETE_DATE_DIFF_COLUMN = "date_created"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')



@op(out=Out(str))
def outreach_domain_tags_get_sql_query_op():
    """
    Retrieves the SQL query for fetching domain tags from the specified SQL file.

    Returns:
        str: The SQL query for fetching domain tags.
    """
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info(f"Got sql_query for {TABLE_NAME}")
    return sql_query


@op(ins={"sql_query": In(str)}, out=Out(pd.DataFrame), 
    required_resource_keys={'globals'})
def outreach_domain_tags_create_df_op(context, sql_query):
    """
    Create a dataframe by executing the given SQL query.

    Args:
        context (dagster.core.execution.contexts.system.SystemExecutionContext): The execution context.
        sql_query (str): The SQL query to execute.

    Returns:
        pandas.DataFrame: The resulting dataframe.
    """
    context.log.info("start to create dataframe")
    
    df = pd.read_sql_query(text(sql_query), 
                           con=ltf_conn_sqlalchemy(),
                           params={'to_sqlcode_date_or_datediff_start': context.resources.globals['date_start'],
                                   'to_sqlcode_date_or_datediff_end': context.resources.globals['date_end']})
    return df


@op(required_resource_keys={'globals'})
def outreach_domain_tags_delete_history_data(context, df):
    """
    Deletes historical data from the outreach domain tags table.

    Args:
        context (dagster.core.execution.contexts.system.SystemExecutionContext): The execution context.
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The modified DataFrame.
    """
    delete_data_from_dwh_table(context, SCHEMA, TABLE_NAME, DELETE_DATE_DIFF_COLUMN, context.resources.globals['date_start'], context.resources.globals['date_end'])
    return df


@op(ins={"df": In(pd.DataFrame)}, 
    required_resource_keys={'globals'})
def outreach_domain_tags_save_to_target_table_op(context, df):
    """
    Saves the given DataFrame to the target table in the data warehouse.

    Args:
        context (Context): The context object containing resources and information about the execution.
        df (DataFrame): The DataFrame to be saved.

    Returns:
        None
    """
    save_to_dwh(df=df, table_name=TABLE_NAME, schema=SCHEMA)
    context.log.info(f"saved: [{TABLE_NAME}]")

    rows = int(df.shape[0])
    send_dwh_alert_slack_message(f":done-1: *{SCHEMA}.{TABLE_NAME}*\n"
                                 f">*{context.resources.globals['date_start']}/-/{context.resources.globals['date_end']}*: {rows}")


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                    "globals": make_values_resource(date_start=Field(str, default_value=YESTERDAY_DATE),
                                                    date_end=Field(str, default_value=YESTERDAY_DATE))},
     name='api__'+TABLE_NAME,
     description=f'{SCHEMA}.{TABLE_NAME}')
def outreach_domain_tags_job():
    '''
    This function executes the ETL process for the outreach domain tags.
    It retrieves the SQL query, creates a DataFrame, deletes historical data,
    and saves the result to the target table.
    '''
    sql_query = outreach_domain_tags_get_sql_query_op()
    df = outreach_domain_tags_create_df_op(sql_query)
    df = outreach_domain_tags_delete_history_data(df)
    outreach_domain_tags_save_to_target_table_op(df)