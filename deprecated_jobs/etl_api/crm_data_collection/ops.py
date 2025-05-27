import datetime
import psycopg2
import io
import pyarrow as pa
import pyarrow.parquet as pq

from pathlib import Path
from contextlib import closing
from sqlalchemy import create_engine

from .crm_requests import *
from dagster import (
    op, job, fs_io_manager, Failure
)

# custom
from ..utils.messages import send_dwh_alert_slack_message
from etl_api.utils.io_manager_path import get_io_manager_path



CREDS_DIR = Path(__file__).parent / 'credentials'
SQL_DIR = Path(__file__).parent / 'sql'

with open(f'{CREDS_DIR}/dwh.json') as json_file:
    dwh_cred = json.load(json_file)
USER_DWH = dwh_cred['user']
PW_DWH = dwh_cred['password']
HOST_DWH = dwh_cred['host']
DB_DWH = dwh_cred['database']


def read_sql_file(file):
    sql_file = open(file)
    sql_query = sql_file.read()
    sql_file.close()
    return sql_query


def connect_to_dwh():
    return create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH))


def dataframe_to_dwh(df, table_name, table_schema):
    con_dwh = connect_to_dwh()

   # Convert the DataFrame to a Parquet file-like object
    parquet_buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_buffer)

    # Read the Parquet file-like object back into a DataFrame
    parquet_buffer.seek(0)
    df_from_parquet = pd.read_parquet(parquet_buffer)

    try:
        # append to existing table
        df_from_parquet.to_sql(
            name=table_name,
            schema=table_schema,
            if_exists='append',
            con=con_dwh,
            index=False
        )
    except Exception as e:
        raise Failure(f"Failed to write to DWH: {e}")


def exec_query(q):
    with closing(psycopg2.connect(dbname=DB_DWH, user=USER_DWH, password=PW_DWH, host=HOST_DWH)) as con:
        cur = con.cursor()
        cur.execute(q)
        con.commit()
        cur.close()


def update_data(context, sql_path, dwh_schema, dwh_table):
    q = read_sql_file(sql_path)
    df = get_sql_query_results(q)

    exec_query('truncate table {}.{}'.format(dwh_schema, dwh_table))
    dataframe_to_dwh(df, dwh_table, dwh_schema)

    context.log.info(f'Wrote [{dwh_schema}.{dwh_table}] table')

    send_dwh_alert_slack_message(
        f':white_check_mark: insertion into *{dwh_schema}.{dwh_table} DONE*\n'
    )


@op
def update_crm_account_opportunity(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_account_opportunity.sql',
        dwh_schema='aggregation',
        dwh_table='crm_account_opportunity'
    )


@op
def update_crm_client_account(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_client_account.sql',
        dwh_schema='aggregation',
        dwh_table='crm_client_account'
    )


@op
def update_crm_client_contact(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_client_contact.sql',
        dwh_schema='aggregation',
        dwh_table='crm_client_contact'
    )


@op
def update_crm_test_data(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_test_data.sql',
        dwh_schema='aggregation',
        dwh_table='crm_test_data'
    )


@op
def update_crm_cases_agg(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_cases_agg.sql',
        dwh_schema='aggregation',
        dwh_table='crm_cases_agg'
    )

@op
def update_crm_cases_stage_log(context):
    update_data(
        context,
        sql_path=f'{SQL_DIR}/crm_cases_stage_log.sql',
        dwh_schema='aggregation',
        dwh_table='crm_cases_stage_log'
    )


@job(
    name='api__crm',
    description='aggregation.crm_*',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})}
)
def crm_data_collection_job():
    update_crm_account_opportunity()
    update_crm_client_account()
    update_crm_client_contact()
    update_crm_test_data()


@job(
    name='api__crm_cases_agg',
    description='aggregation.crm_cases_agg',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})}
)
def crm_cases_agg_job():
    update_crm_cases_agg()


@job(
    name='api__crm_cases_stage_log',
    description='aggregation.crm_cases_stage_log',
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})}
)
def crm_cases_stage_log_job():
    update_crm_cases_stage_log()
