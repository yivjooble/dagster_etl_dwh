import pandas as pd
import gzip
import pickle

from sqlalchemy import text
from ...utils.rplc_connections import connect_to_replica, conn_to_repstat_sqlalchemy
from ...utils.messages import send_dwh_alert_slack_message


def start_query_on_repstat_db(context, sql_instance_country_query: dict):
    try:
        with connect_to_replica(sql_instance_country_query['sql_instance_host'], sql_instance_country_query['country_db']) as con:
            with con.cursor() as cur:
                
                cur.execute(sql_instance_country_query['query']) 
                cur.commit()

                context.log.info(f"success for {sql_instance_country_query['country_db']}")
                return 'success_'+sql_instance_country_query['country_db']
    except Exception as e:
        context.log.error(f"error for: {sql_instance_country_query['country_db']}\n{e}")
        raise e


def save_to_repstat_db(df, table_name, schema, host, db_name):
    engine = conn_to_repstat_sqlalchemy(host, db_name)

    df.to_sql(
        table_name,
        con=engine,
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=50000
    )