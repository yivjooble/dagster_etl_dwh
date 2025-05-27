import pandas as pd
import gzip
import pickle

from sqlalchemy import text
from .prod_db_connections import prod_conn_sqlalchemy
from .messages import send_dwh_alert_slack_message

def save_pkl_files(context, df, PATH_TO_DATA, sql_instance_country_query: dict):
    file_path = f"{PATH_TO_DATA}/{sql_instance_country_query['country_db']}_DB.pkl"
    with gzip.open(file_path, 'wb') as f:
        pickle.dump(df, f)

    context.log.info(f"Data loaded for: {sql_instance_country_query['country_db']}")
    return file_path

def start_query_on_prod_db(context, PATH_TO_DATA, TABLE_NAME, sql_instance_country_query: dict):
    try:
        engine = prod_conn_sqlalchemy(sql_instance_country_query['sql_instance']['LocalAddress'], 'Job_'+sql_instance_country_query['country_db'])
        df = pd.read_sql(sql=text(sql_instance_country_query['query']), 
                         con=engine, 
                         params={key: val for key, val in sql_instance_country_query.items() if key.startswith('to_sqlcode')})
        # add country_id to df
        df['country_id'] = sql_instance_country_query['country_id']

        file_path = save_pkl_files(context, df, PATH_TO_DATA, sql_instance_country_query)

        return file_path
    except Exception as e:
        context.log.error(f'query execution error: {e}')
        raise e