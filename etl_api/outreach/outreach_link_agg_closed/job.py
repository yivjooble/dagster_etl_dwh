from dagster import (
    op, 
    job, 
    In, 
    Out, 
    fs_io_manager,
    Failure,
    )

# built in modules
import pandas as pd
import traceback
import logging
import os

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy


SCHEMA = "aggregation"
TABLE_NAME = "outreach_link_agg_closed"


@op(out=Out(str))
def outreach_link_agg_closed_get_sql():
    """
    Retrieves the SQL query for the outreach link aggregation closed job.

    Returns:
        str: The SQL query for the job.
    """
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", f"{TABLE_NAME}.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info(f"GOT sql_query for {SCHEMA}.{TABLE_NAME}")
    return sql_query


@op(ins={'sql': In(str)}, out=Out(pd.DataFrame))
def outreach_link_agg_closed_create_df(context, sql):
    """
    Create a dataframe for the outreach link aggregation closed job.

    Args:
        context (dagster.core.execution.contexts.system.SystemExecutionContext): The execution context.
        sql (str): The SQL query to fetch data from the database.

    Returns:
        pandas.DataFrame: The dataframe containing the fetched data.
    """
    context.log.info(f"Creating a df for: [{SCHEMA}.{TABLE_NAME}]")
    df_outreach_link_agg = pd.read_sql_query(sql,
                                             con=ltf_conn_sqlalchemy())
    return df_outreach_link_agg


@op(ins={'df': In(pd.DataFrame)})
def outreach_link_agg_closed_save_to_dwh(context, df):
    """
    Save the result table to the aggregation schema in the data warehouse.

    Args:
        context (dagster.core.execution.contexts.system.SystemExecutionContext): The execution context.
        df (pandas.DataFrame): The DataFrame containing the result table.

    Raises:
        Failure: If the saving process fails.

    Returns:
        None
    """
    try:
        # write result table to aggregation schema
        save_to_dwh(
            df,
            table_name=TABLE_NAME,
            schema=SCHEMA,
        )
        context.log.info(f"Wrote {SCHEMA}.{TABLE_NAME} table")
        send_dwh_alert_slack_message(f":white_check_mark: {SCHEMA}.{TABLE_NAME} executed successfully")
    except Exception:
        send_dwh_alert_slack_message(f":error_alert: {SCHEMA}.{TABLE_NAME} failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')
        raise Failure(f"Failed to save {SCHEMA}.{TABLE_NAME}")
    
    
@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name=f'api__{TABLE_NAME}',
     description=f'{SCHEMA}.{TABLE_NAME}')
def outreach_link_agg_closed_job():
    sql =  outreach_link_agg_closed_get_sql()
    df = outreach_link_agg_closed_create_df(sql)
    outreach_link_agg_closed_save_to_dwh(df)