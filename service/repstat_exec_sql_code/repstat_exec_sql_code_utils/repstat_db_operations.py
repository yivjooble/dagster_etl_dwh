import pandas as pd
import gzip
import pickle

from sqlalchemy import text
from ...utils.rplc_connections import connect_to_replica
from ...utils.messages import send_dwh_alert_slack_message

def start_query_on_rplc_db(context, sql_instance_country_query: dict):
    try:
        conn = connect_to_replica(sql_instance_country_query['sql_instance_host'], sql_instance_country_query['country_db'])
        cursor = conn.cursor()
        
        # launch procedure on replica's db
        cursor.execute(sql_instance_country_query['query']) 
        conn.commit()

        context.log.info(f"success for {sql_instance_country_query['country_db']}")
        cursor.close()
        conn.close()
        return 'success'
    except Exception as e:
        context.log.error(f"error for: {sql_instance_country_query['country_db']}\n{e}")
        raise e
