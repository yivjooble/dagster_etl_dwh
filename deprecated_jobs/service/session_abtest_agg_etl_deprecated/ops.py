import psycopg2
import os
import pandas as pd
import logging
import inspect

from dagster import (
    op,
    job,
    Config)

# multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import text
from typing import Dict
from psycopg2 import errors as psycopg2_errors

# custom modules
from .config import clusters, procedures, map_country_code_to_id as code_to_id
from .utils.messages import send_dwh_alert_slack_message
from .utils.db_connections import dwh_conn_sqlalchemy, dwh_conn_psycopg2
from ..utils.utils import delete_pkl_files, map_country_to_id, job_prefix

load_dotenv()

logging.getLogger('alembic').setLevel(logging.ERROR)
logger = logging.getLogger('root')

# PAST_DAYS_COUNT = 1
TABLE_NAME = "email_abtest_agg_tmp"
SCHEMA = "aggregation"
JOB_PREFIX = job_prefix()


class RecoveryConfig(Config):
    reload_days: list


def connect_to_replica(host, dbname):
    return psycopg2.connect(
        f"host={host} dbname={dbname} user={os.environ.get('REPLICA_USER')} password={os.environ.get('REPLICA_PASSWORD')}"
    )


@op
def reload_datediff_from_dwh(day: int) -> int:
    '''get date_diff from dwh to pass further'''
    try:
        with dwh_conn_psycopg2() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""select public.fn_get_date_diff(current_date - (%s))""", (day,))
                datediff = [y for x in cursor.fetchall() for y in x]
                return datediff[0]
    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise


def connect_and_execute_procedure(info: Dict, retry_cnt=0):
    '''Connect to a database and execute a procedure.'''
    try:
        conn = connect_to_replica(info['host'], info['dbname'])
        cursor = conn.cursor()
        start_time = datetime.now()

        # Prepare the procedure call statement
        procedure_call = f"""call an.{info['procedure']}({info['date_diff']});"""
        
        cursor.execute(procedure_call) # launch procedure on replica's db
        conn.commit()

        end_time = datetime.now()

        logger.info(f"success for {info['dbname']}({info['date_diff']}), result time: {end_time - start_time}")
    except Exception as e:
        logger.error(f"error for: {info['dbname']}: {info['procedure']}\n{e}")
        cursor.close()
        conn.close()

        # Retry if count is less than 5
        if retry_cnt < 5:
            connect_and_execute_procedure(info, retry_cnt + 1)
        else:
            logger.error(f"[retry FAILED {retry_cnt}], ERROR for [{info['dbname']}: {info['procedure']}]")
            # send_dwh_alert_slack_message(f":bangbang: *RPL: dredd procedures failed for [{info['dbname']}]* <@U02CHDSB5P0>")
    finally:
        cursor.close()
        conn.close()


def execeture_replica_procedure(process_info):
    '''This method is executed as a separate process.'''
    connect_and_execute_procedure(process_info)


@op
def reload_session_abtest_agg_procedure(datediff: int):
    '''Launch email_abtest_agg_tmp procedure over all countries.'''
    try:
        start_time = datetime.now()
        with ThreadPoolExecutor(4) as executor:
            futures = []
            for procedure in procedures:
                logging.info(f"==== Datediff: {datediff}, procedure_name: {procedure}")
                for cluster_info in clusters.values():
                    for country in cluster_info['dbs']:
                            info = {"host": cluster_info['host'],
                                    "dbname": country,
                                    "procedure": procedure,
                                    "date_diff": datediff}

                            future = executor.submit(execeture_replica_procedure, info)
                            futures.append(future)

        # start all processes
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # Handle your exceptions heres
                logging.error(f"Task resulted in an error: {e}")

        end_time = datetime.now()
        rtime = end_time - start_time
        rdt = str(rtime).split('.', 1)[0]

        logger.info(f"*Done:* {inspect.currentframe().f_code.co_name}: [{rdt}]")
        # send_dwh_alert_slack_message(f':done-1: *RPL: {TABLE_NAME}* [{rdt}]')
        return True
    except Exception as e:
        logger.error(f"{inspect.currentframe().f_code.co_name}: {e}")
        raise



def create_dataframe_from_replica(info: Dict[str, str]):
    try:
        start_time = datetime.now()
        final_df = pd.read_sql_query(sql=info['code'],
                               con=connect_to_replica(info['host'],
                                                      info['country']))
        end_time = datetime.now()
        logger.info(f"dataframe created for: {info['country']}({info['date_diff']}), result time: {end_time - start_time}")
        return final_df
    except psycopg2_errors.Error as e:
        logger.error(f"ERROR: {e}")


def get_sql_to_create_dataframe(datediff: int) -> str:
    sql_query = ('''select country_id,
                        group_num,
                        action_datediff,
                        is_returned,
                        device_type_id,
                        is_local,
                        session_create_page_type,
                        traffic_source_id,
                        current_traffic_source_id,
                        is_anomalistic,
                        attribute_name,
                        attribute_value,
                        metric_name,
                        metric_value
                    from an.rpl_session_abtest_agg
                    where action_datediff = {};''').format(datediff)
    return sql_query


@op
def delete_history_from_dwh_db_table(datediff: int):
    with dwh_conn_psycopg2() as conn:
        with conn.cursor() as cursor:
            cursor.execute("delete from {SCHEMA}.{TABLE_NAME} where action_datediff = {date}".format(SCHEMA=SCHEMA,
                                                                                                         TABLE_NAME=TABLE_NAME,
                                                                                                         date=datediff))
            conn.commit()
    logger.info(f"===== Deleted from dwh: [{datediff}].")


@op
def reload_copy_data_from_replica_to_dwh(datediff: int):
    try: 
        start_time = datetime.now()
        results_list = []
        with ThreadPoolExecutor(4) as executor:
            futures = []
            logger.info(f"===== Copy data from replica to dwb-db: [{datediff}].")
            for cluster_info in clusters.values():
                for country in cluster_info['dbs']:                   
                    info = {"host": cluster_info['host'],
                            "country": country,
                            "date_diff": datediff,
                            "code": get_sql_to_create_dataframe(datediff)}
                    future = executor.submit(create_dataframe_from_replica, info)
                    futures.append(future)

        # start all processes
        for future in as_completed(futures):
            result = future.result()

            if result is not None:
                results_list.append(result)

        # create a dataframe with result
        if len(results_list) == 0:
            return pd.DataFrame()
        else:

            result_df = pd.concat(results_list, ignore_index=True)
            result_df.to_sql(
                TABLE_NAME,
                con=dwh_conn_sqlalchemy(),
                schema=SCHEMA,
                index=False,
                if_exists="append",
                chunksize=50000)

        end_time = datetime.now()
        countries_count = int(result_df['country_id'].nunique())
        rows = int(result_df.shape[0])

        if countries_count < 31:
            send_dwh_alert_slack_message(f":error_alert: *{TABLE_NAME}* <!subteam^S02ETK2JYLF|dwh.analysts>\n"
                                         f">*{datediff}:* {countries_count} *countries* & {rows} *rows*\n")
        else:
            send_dwh_alert_slack_message(f":add: *{TABLE_NAME}*\n"
                                        f">*{datediff}:* {countries_count} *countries* & {rows} *rows*\n")
        logger.info(f"*Done:* {inspect.currentframe().f_code.co_name}: [{end_time - start_time}]")
    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

@op
def reload_process_for_day(config: RecoveryConfig):
    for day in config.reload_days:
        datediff = reload_datediff_from_dwh(day)
        reload_session_abtest_agg_procedure(datediff)
        delete_history_from_dwh_db_table(datediff)
        reload_copy_data_from_replica_to_dwh(datediff)

@job(name=JOB_PREFIX+TABLE_NAME+'_old_rec',
     description=f'{SCHEMA}.{TABLE_NAME}')
def launch_reload_process():
    reload_process_for_day()