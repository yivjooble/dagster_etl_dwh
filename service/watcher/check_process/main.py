import os
import pandas as pd
from tabulate import tabulate

from datetime import datetime
from dotenv import load_dotenv
from dagster import (
    op,
    job,
    fs_io_manager
)

# custom modules
from service.watcher.utils.database import create_dwh_psycopg2_connection, create_dwh_sqlalchemy_engine, \
    repstat_psycopg2_connection
from service.utils.utils import job_prefix, get_project_id, get_file_path, get_gitlab_file_content
from service.utils.io_manager_path import get_io_manager_path

load_dotenv()

JOB_NAME = "watcher_check_processes"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def execute_sql_on_pgstat_rpl(context, sql_query, is_select=False):
    rpl_config = {
        'nl_host': {'host_name': '10.0.1.65',
                    'db': 'ua'},
        'us_host': {'host_name': '192.168.1.207',
                    'db': 'us'}
    }

    if not is_select:
        for host_info in rpl_config.values():
            with repstat_psycopg2_connection(host_info['host_name'], host_info['db']) as con:
                with con.cursor() as cur:
                    cur.execute(sql_query)
                    con.commit()
                    context.log.info(f"Active processes were dropped on {host_info['host_name']}")
    elif is_select:
        # select active processes and return df
        dfs = []
        for host_info in rpl_config.values():
            with repstat_psycopg2_connection(host_info['host_name'], host_info['db']) as con:
                with con.cursor() as cur:
                    cur.execute(sql_query)
                    result = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    df = pd.DataFrame.from_records(result, columns=column_names)
                    dfs.append(df)

        return pd.concat(dfs)


@op
def save_process_to_dwh(context):
    # select current processes to save to dwh table
    sql_drop_logs = """
            select datname,
                usename,
                application_name,
                client_hostname,
                state,
                query,
                (now() - pg_stat_activity.query_start)::time as exec_time
            from pg_catalog.pg_stat_activity
            where state in (
                            'active',
                            'idle in transaction',
                            'idle'
                )
            and pid <> pg_backend_pid()
            and EXTRACT(EPOCH FROM (now() - pg_stat_activity.query_start)::interval) >= 3600
            and query !~~ '%cat_login_creds%'
            ;
             """
    # connect to dwh db
    conn = create_dwh_psycopg2_connection()
    # execute query
    cur = conn.cursor()
    cur.execute(sql_drop_logs)
    # fetch and commit result
    result = cur.fetchall()
    # close connection
    cur.close()
    conn.close()

    column_names = [
        'datname', 'usename', 'application_name', 'client_hostname', 'state',
        'query', 'exec_time'
    ]
    df = pd.DataFrame.from_records(result, columns=column_names)
    df['action_date'] = datetime.today().strftime("%Y-%m-%d")
    df.to_sql('killed_process',
              con=create_dwh_sqlalchemy_engine(),
              schema='dwh_system',
              index=False,
              if_exists='append')

    context.log.info(f"Dropped processes were saved to dwh: dwh_system.killed_process: {df}")


@op
def drop_process_on_dwh(context, save):
    """
    This app are going to kill dwh processes in state: 'active', 'idle in transaction' at 23:30 (Kyiv time)
    """
    sql_drop = """
                SELECT pg_terminate_backend(pid)
                from pg_catalog.pg_stat_activity
                where state in (
                                'active',
                                'idle in transaction',
                                'idle'
                    )
                and pid <> pg_backend_pid()
                and EXTRACT(EPOCH FROM (now() - pg_stat_activity.query_start)::interval) >= 3600
                and query !~~ '%cat_login_creds%'
                ;
               """

    # connect to dwh db
    conn = create_dwh_psycopg2_connection()

    # execute query
    cur = conn.cursor()
    cur.execute(sql_drop)

    # fetch and commit result
    conn.commit()

    # close connection
    cur.close()
    conn.close()

    context.log.info("Active processes were dropped")


@op
def drop_process_on_pgstat_rpl(context):
    """
    This app is going to kill rpl processes in state: 'active', 'idle in transaction' at 23:30 (Kyiv time)
    """
    # get select-query from gitlab
    gitlab_select_q, gitlab_select_q_url = get_gitlab_file_content(
        project_id=get_project_id(),
        file_path=get_file_path(file_name='check_proc_on_pgstat_rpl'),
    )
    context.log.info(f'Gitlab select-query url:\n{gitlab_select_q_url}')
    # check active processes on pgstat_rpl
    df = execute_sql_on_pgstat_rpl(context, gitlab_select_q, is_select=True)

    if df.empty:
        context.log.info("There are no active processes on pgstat_rpl")
    else:
        # context.log.info(
        #     f"Active processes on pgstat_rpl:\n{tabulate(df, headers='keys', tablefmt='psql', showindex=False, maxcolwidths=50)}")

        # get drop-query from gitlab
        gitlab_drop_q, gitlab_drop_q_url = get_gitlab_file_content(
            project_id=get_project_id(),
            file_path=get_file_path(file_name='drop_proc_on_pgstat_rpl'),
        )
        context.log.info(f'Gitlab drop-query url:\n{gitlab_drop_q_url}')
        execute_sql_on_pgstat_rpl(context, gitlab_drop_q)


@job(
    name=JOB_PREFIX + JOB_NAME,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    tags={"data_model": "watcher"},
    metadata={"gitlab_sql_url": "https://gitlab.jooble.com/an/separate-statistic-db-nl-us/-/tree/master/dwh_team"},
)
def check_processes():
    """
    Review and kill active processes on: DWH and RPL-nl/us
    """
    save = save_process_to_dwh()
    drop_process_on_dwh(save)
    drop_process_on_pgstat_rpl()
