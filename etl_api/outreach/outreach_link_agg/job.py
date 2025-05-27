from dagster import op, job, In, Out, fs_io_manager

# built in modules
import requests
import pandas as pd
import traceback
import logging
import os

# project import
from ...utils.io_manager_path import get_io_manager_path
from ...utils.dwh_db_operations import save_to_dwh, truncate_dwh_table, dwh_conn_sqlalchemy, dwh_conn_psycopg2
# module import
from ...utils.messages import send_dwh_alert_slack_message
from ...utils.outreach_connections import ltf_conn_sqlalchemy


@op(out=Out(str))
def get_sql_query_outreach_link_agg():
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "outreach_link_agg.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info("GOT sql_query for outreach_link_agg")
    return sql_query


@op(out=Out(str))
def get_sql_query_outreach_link_cost():
    sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "outreach_link_cost.sql"))
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    logging.info("GOT sql_query for outreach_link_cost")
    return sql_query


@op(ins={'sql_query_outreach_link_agg': In(str)}, out=Out(pd.DataFrame))
def create_df_outreach_link_agg(context, sql_query_outreach_link_agg):
    context.log.info("Creating a df for: [aggregation.outreach_link_agg]")
    df_outreach_link_agg = pd.read_sql_query(sql_query_outreach_link_agg,
                                             con=ltf_conn_sqlalchemy())
    return df_outreach_link_agg


@op(ins={'sql_query_outreach_link_cost': In(str)}, out=Out(pd.DataFrame))
def create_df_outreach_link_cost_raw(context, sql_query_outreach_link_cost):
    context.log.info("Creating a df for a raw table: [dwh_test.outreach_link_cost_raw]")
    df_outreach_link_cost = pd.read_sql_query(sql_query_outreach_link_cost,
                                              con=ltf_conn_sqlalchemy())
    context.log.info("Wrote [dwh_test.outreach_link_cost_raw] table")
    return df_outreach_link_cost


@op(ins={'status': In(str)}, out=Out(pd.DataFrame), 
    description='add column [cost] to outreach_link_cost_raw and save as new table')
def add_cost_column_to_target_table(context, status):
    if status == 'success':
        context.log.info("START: add_final_column to outreach_link_cost")
        conn = dwh_conn_psycopg2()
        cur = conn.cursor()
        cur.execute("""select distinct cast(link_created_date::date as text)
                        from dwh_test.outreach_link_cost_raw
                        where link_created_date is not null
                            and link_created_date::date >= '2022-01-01'
                            and link_created_date::date not in ('2023-05-05')
                        order by cast(link_created_date::date as text) desc;
                    """)
        result_dates = cur.fetchall()
        conn.close()
        dates = [str(y) for x in result_dates for y in x]
        
        currency = "eur"

        # select * from dwh_test.outreach_link_agg_raw + add column [cost]
        sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "add_cost_column.sql"))
        with open(sql_file, 'r') as file:
            sql_query = file.read()

        result_list = []

        for date in dates:
            if date is not None:
                context.log.info(f"date: {date}")
                # url = f"https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/{date}/currencies/{currency}.json"

                # context.log.info(f"url: {url}")
                # response = requests.get(url).json()
                
                # currency_rate_eur_to_usd = response.get("eur").get("usd")

                context.log.info(f"creating df for: {date}")
                df = pd.read_sql_query(sql_query,
                                        params={"date": date},
                                        con=dwh_conn_sqlalchemy())
                result_list.append(df)
            else:
                context.log.info(f"date is None: {date}")
                pass
            
        df = pd.concat(result_list)
        return df
    else:
        context.log.info("error")


@op(ins={'df': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def truncate_link_cost_raw(context, df):
    truncate_dwh_table(
        context,
        table_name="outreach_link_cost_raw",
        schema="dwh_test"
    )
    return df


@op(ins={'df_outreach_link_cost': In(pd.DataFrame)}, out=Out(str))
def save_link_cost_raw(context, df_outreach_link_cost):
    try:
        save_to_dwh(
            df_outreach_link_cost,
            table_name="outreach_link_cost_raw",
            schema="dwh_test"
        )
        context.log.info("Wrote [dwh_test.outreach_link_cost_raw] table")
        status = 'success'
    except Exception:
        status = 'failed'
    return status


@op(ins={'df': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def truncate_link_cost(context, df):
    truncate_dwh_table(
        context,
        table_name="outreach_link_cost",
        schema="aggregation"
    )
    context.log.info("Truncated [aggregation.outreach_link_cost] table")
    return df


@op(ins={'df': In(pd.DataFrame)})
def save_link_cost(context, df):
    try:
        # write result table to aggregation schema
        save_to_dwh(
            df,
            table_name="outreach_link_cost",
            schema="aggregation"
        )
        context.log.info("Wrote [aggregation.outreach_link_cost] table")
        send_dwh_alert_slack_message(f":white_check_mark: outreach_link_cost executed successfully")
    except Exception:
        send_dwh_alert_slack_message(f":error_alert: outreach_link_cost failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')


@op(ins={'df': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def truncate_link_agg(context, df):
    truncate_dwh_table(
        context,
        table_name="outreach_link_agg",
        schema="aggregation"
    )
    context.log.info("Truncated [aggregation.outreach_link_agg] table")
    return df


@op(ins={'df_outreach_link_agg': In(pd.DataFrame)})
def save_link_agg(context, df_outreach_link_agg):
    try:
        # write result table to aggregation schema
        save_to_dwh(
            df_outreach_link_agg,
            table_name="outreach_link_agg",
            schema="aggregation"
        )
        context.log.info("Wrote [aggregation.outreach_link_agg] table")
        send_dwh_alert_slack_message(f":white_check_mark: outreach_link_agg executed successfully")
    except Exception:
        send_dwh_alert_slack_message(f":error_alert: outreach_link_agg failed <!subteam^S02ETK2JYLF|dwh.analysts>")
        context.log.error(f'{traceback.format_exc()}')
        raise Exception


@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_link_cost',
     description='aggregation.outreach_link_cost')
def outreach_link_cost_job():
    # calculate outreach_link_cost: create raw table
    sql_query_outreach_link_cost =  get_sql_query_outreach_link_cost()
    df_outreach_link_cost = create_df_outreach_link_cost_raw(sql_query_outreach_link_cost)
    df = truncate_link_cost_raw(df_outreach_link_cost)
    status = save_link_cost_raw(df)
    # add final column [cost] to outreach_link_cost_raw and save as new table
    result_df = add_cost_column_to_target_table(status)
    df = truncate_link_cost(result_df)
    save_link_cost(df)
    
    
@job(resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='api__outreach_link_agg',
     description='aggregation.outreach_link_agg')
def outreach_link_agg_job():
    sql_query_outreach_link_agg =  get_sql_query_outreach_link_agg()
    df_link_agg = create_df_outreach_link_agg(sql_query_outreach_link_agg)
    df = truncate_link_agg(df_link_agg)
    save_link_agg(df)