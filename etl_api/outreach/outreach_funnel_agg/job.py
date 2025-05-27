from dagster import op, job, In, Out, fs_io_manager, Failure
import os

# built in modules
import pandas as pd
import traceback
import logging
import pickle
import gzip

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy
from ...utils.utils import delete_pkl_files


PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TABLE_NAME = "outreach_funnel_agg"
SCHEMA = "aggregation"


@op(out=Out(str))
def get_sql_query_outreach_funnel_agg():
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "outreach_funnel_agg.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info("GOT sql_query for outreach_funnel_agg")
    return sql_query


@op(ins={'sql_query_outreach_funnel_agg': In(str)}, 
    out=Out(str))
def create_df_outreach_funnel_agg(context, sql_query_outreach_funnel_agg):
    delete_pkl_files(context, PATH_TO_DATA)
    context.log.info("Creating a df: [aggregation.outreach_funnel_agg]")

    df_outreach_funnel_agg = pd.read_sql_query(sql_query_outreach_funnel_agg,
                                              con=ltf_conn_sqlalchemy())
    file_path = f"{PATH_TO_DATA}/api_funnel_agg.pkl"
    with gzip.open(file_path, 'wb') as file:
        pickle.dump(df_outreach_funnel_agg, file)
        
    context.log.info(f"{file_path}")
    return file_path
    

@op(ins={'file_path_outreach_funnel_agg': In(str)}, 
    out=Out(str))
def truncate_outreach_funnel_agg(context, file_path_outreach_funnel_agg):
    truncate_dwh_table(
        context,
        table_name=TABLE_NAME,
        schema=SCHEMA
    )
    return file_path_outreach_funnel_agg


@op(ins={'file_paths': In(str)})
def save_outreach_funnel_agg(context, file_paths: str):
    try:
        with gzip.open(file_paths, 'rb') as f:
            df = pickle.load(f)
            save_to_dwh(
                df=df,
                table_name=TABLE_NAME,
                schema=SCHEMA
            )
            rows = int(df.shape[0])
            context.log.info('Successfully saved df to dwh.\n'
                                f"rows: {rows}")
            send_dwh_alert_slack_message(f":add: *{SCHEMA}.{TABLE_NAME}*\n"
                                            f">{rows} *rows*")
    except Exception:
        send_dwh_alert_slack_message(":error_alert: outreach_funnel_agg failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')
        raise Failure from Exception


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_funnel_agg',
     description=f"{SCHEMA}.{TABLE_NAME}")
def outreach_funnel_agg_job():
    '''aggregation.outreach_funnel_agg'''
    sql_query_outreach_funnel_agg =  get_sql_query_outreach_funnel_agg()
    file_path_outreach_funnel_agg = create_df_outreach_funnel_agg(sql_query_outreach_funnel_agg)
    file_paths = truncate_outreach_funnel_agg(file_path_outreach_funnel_agg)
    save_outreach_funnel_agg(file_paths)