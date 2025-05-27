import pandas as pd
import gzip
import pickle

from typing import Optional
from sqlalchemy import text
from datetime import datetime
from dagster import (
    Failure
)
from .dbs_con import get_prod_conn_sqlalchemy
from .rplc_config import clusters
from .rplc_db_operations import truncate_rpl_table, save_to_rpl_table
from ..utils.utils import delete_pkl_files



def save_pkl_files(context, df, PATH_TO_DATA, sql_instance_country_query: dict):
    """
    Save a DataFrame as a pickle file.

    Args:
        context (object): The context object provided by Dagster.
        df (pandas.DataFrame): The DataFrame to be saved.
        PATH_TO_DATA (str): The path to the directory where the pickle file will be saved.
        sql_instance_country_query (dict): A dictionary containing SQL query related information.

    Returns:
        str: The file path of the saved pickle file.
    """
    date_int = sql_instance_country_query.get('to_sqlcode_date_int', sql_instance_country_query.get('to_sqlcode_date_or_datediff_start'))
    date = date_int if date_int else datetime.now().strftime('%Y_%m_%d')
    country = sql_instance_country_query['country_db']
    
    file_path = f"{PATH_TO_DATA}/{country}_DB_{date}.pkl"
    
    with gzip.open(file_path, 'wb') as f:
        pickle.dump(df, f)

    context.log.info(f"Data loaded for: {sql_instance_country_query['country_db']}, {date}, {df.shape[0]}")
    return file_path


def start_query_on_prod_db(context, 
                           PATH_TO_DATA, 
                           TABLE_NAME, 
                           sql_instance_country_query: dict, 
                           DELETE_COUNTRY_COLUMN: Optional[str] = None):
    try:
        engine = prod_conn_sqlalchemy(sql_instance_country_query['sql_instance']['LocalAddress'], 'Job_'+sql_instance_country_query['country_db'])
        df = pd.read_sql(sql=text(sql_instance_country_query['query']), 
                         con=engine, 
                         params={key: val for key, val in sql_instance_country_query.items() if key.startswith('to_sqlcode')})
        
        # add country_id to df
        country_field_name = DELETE_COUNTRY_COLUMN if DELETE_COUNTRY_COLUMN else 'country_id'
        df[f"{country_field_name}"] = sql_instance_country_query['country_id']      

        file_path = save_pkl_files(context, df, PATH_TO_DATA, sql_instance_country_query)

        return file_path
    except Exception as e:
        context.log.error(f'query execution error: {e}')
        raise e
    
    
def copy_from_rpd_to_rpl_save_pkl_files(context, df, path_to_pkl_file, sql_instance_country_query: dict, i: int):
    date = datetime.now().strftime('%Y_%m_%d')
    country = str(sql_instance_country_query['country_db']).lower()
    
    file_path = f"{path_to_pkl_file}/{country}_DB_{date}_{i}.pkl"
    
    with gzip.open(file_path, 'wb') as f:
        pickle.dump(df, f)

    return file_path
    
    
def copy_from_prod_to_rpl(context, path_to_pkl_file: str, table_name: str, schema: str, sql_instance_country_query: dict):
    try:
        engine = prod_conn_sqlalchemy(sql_instance_country_query['sql_instance']['LocalAddress'], 'Job_'+sql_instance_country_query['country_db'])
        chunks = pd.read_sql(sql=text(sql_instance_country_query['query']), 
                         con=engine, 
                         params={key: val for key, val in sql_instance_country_query.items() if key.startswith('to_sqlcode')},
                         chunksize=100000)
        
        file_pathes = []
        for i, chunk in enumerate(chunks):
            file_path = copy_from_rpd_to_rpl_save_pkl_files(context, chunk, path_to_pkl_file, sql_instance_country_query, i)
            file_pathes.append(file_path)
            context.log.info(f"chunk {i} saved to pkl: {file_path}")

        # Looping through clusters to map production country name to replica country name
        for cluster_info in clusters.values():
            for country in cluster_info['dbs']:
                # Case-insensitive comparison for country names
                if country.lower() == sql_instance_country_query['country_db'].lower():

                    # truncate rpl table
                    truncate_rpl_table(
                        host=cluster_info['host'],
                        db_name=country.lower(),
                        table_name=table_name,
                        schema=schema
                    )
                    context.log.info(f"{country}: Table truncated: {schema}.{table_name}")
                    
                    # save to rpl table
                    for file_path in file_pathes:
                        with gzip.open(file_path, 'rb') as f:
                            country_df = pickle.load(f)
                            save_to_rpl_table(
                                host=cluster_info['host'],
                                db_name=country.lower(),
                                table_name=table_name,
                                schema=schema,
                                df=country_df)
                            context.log.info(f"df saved to rpl: {file_path}")
                        
        
    except Exception as e:
        context.log.error(f'query execution error: {e}')
        raise e