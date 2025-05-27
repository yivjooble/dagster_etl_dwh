import logging
import os
import pandas as pd
import traceback

from dotenv import load_dotenv
from sqlalchemy import text
from datetime import datetime

# multiprocessing
from multiprocessing import Process
from multiprocessing import Queue, Manager

# custom modeles
from messages import send_dwh_alert_slack_message
from .utils import connect_sqlalchemy, get_sql
from config import job_language_config 



# load creds
load_dotenv()

# define destination path in dwh
TABLE_NAME = "job_language"
SCHEMA = "new_stat"

# get sql query from .sql
str_sql_query = get_sql(f"{TABLE_NAME}")


# Step 1
def create_replica_df(conn_info, results_list):
    '''
    connect to replica
    create df of yesterday's data
    '''
    get_conn_info = conn_info.get() # get info date from "for loop" of countries
    engine = connect_sqlalchemy(get_conn_info['host'], get_conn_info['dbname'])
    # define the parameter
    try:
        sql_query = text(f"""{str_sql_query}""")
        df = pd.read_sql_query(sql_query, 
                               con=engine) # create df with select result from replica per country
        logging.info(f"df created for {get_conn_info['dbname']}")
        results_list.append(df)
    except Exception:
        logging.error(f"for [{get_conn_info['dbname']}]: {traceback.format_exc()}")


def job_language():
    # collect processes
    processes = []
    result = Manager()
    errors = Manager()
    results_list = result.list() # stores all dfs
    for cluster, cluster_info in job_language_config.items():
        for country in cluster_info['dbs']:
                '''start multiprocessing
                '''
                logging.info(f"Start processing for: {country}")
                conn_info = Queue()
                info = {"host": cluster_info['host'],
                        "dbname": country,}
                conn_info.put(info)
                process = Process(target=create_replica_df, args=(conn_info, results_list,)) # create a process to run on a db
                processes.append(process)
    try: 
        # start all processes
        for process in processes:
            process.start()
        for process in processes:
            process.join()
    except Exception as e:
        logging.error(f"Proccessing ERROR: {traceback.format_exc()}")
    finally:
        try:
            # put all dfs to dwh table
            logging.info(f"Start processing to save to dwh table")
            res_df = pd.concat(results_list, ignore_index=True)
            res_df.to_sql(
                TABLE_NAME,
                con=connect_sqlalchemy(os.environ.get('DWH_HOST'), os.environ.get('DWH_DB')),
                schema=SCHEMA,
                if_exists='append',
                index=False,
                chunksize=100000
            )
            logging.info(f"Successfully saved to {SCHEMA}.{TABLE_NAME}")
            '''end multiprocessing
            '''
        except Exception:
            logging.error(f"Can't save df to dwh: {traceback.format_exc()}")


if __name__=="__main__":
    try:
        # job_language
        logging.info("START: [job_language]")
        start_time = datetime.now() # time tracking
        job_language()
        delta = datetime.now() - start_time
        rt = str(delta).split('.', 1)[0] # format r-time
        logging.info(f"END: [job_language]: [{rt}]\n\n")
        send_dwh_alert_slack_message(f":white_check_mark: job_language executed successfully [{rt}]")
    except Exception:
        logging.error(f"[job_language] {traceback.format_exc()}")
        send_dwh_alert_slack_message(":x: job_language failed")
