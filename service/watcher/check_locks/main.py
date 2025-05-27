import os
import inspect

from datetime import datetime
from dotenv import load_dotenv
from dagster import (
    op,
    job,
    fs_io_manager
)

# custom modules
from service.watcher.utils.database import create_dwh_psycopg2_connection, Session, repstat_psycopg2_connection
from service.watcher.utils.logger import update_watcher_log_entry
from service.watcher.utils.models import LogWatcher
from service.watcher.utils.messages  import send_dwh_alert_slack_message
from service.utils.utils import job_prefix
from service.utils.io_manager_path import get_io_manager_path

load_dotenv()

JOB_NAME = "watcher_check_locks"
JOB_PREFIX = job_prefix()
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@op
def check_locks_dwh(context):
    '''
    This app are going to check existing locks in dwh db via defined view: dwh_test.lock_monitor
    compare a result with a previous result saved in table: dwh_system.watcher_logs 
    do appropriate action: notify or pass
    '''

    # select count of locks from db view
    sql = """select count(*) from dwh_test.lock_monitor;"""

    # connect to dwh db
    conn = create_dwh_psycopg2_connection()

    # execute query
    cur = conn.cursor()
    cur.execute(sql)

    # fetch result
    locks_cnt = cur.fetchone()

    # close connection
    cur.close()
    conn.close()

    if locks_cnt[0] >= 1:

        session = Session()
        previous_lock_cnt = session.query(LogWatcher).filter(LogWatcher.module_name == inspect.currentframe().f_code.co_name).one()

        # check if last saved date of cnt of locks is today
        if previous_lock_cnt.update_date.strftime("%Y-%m-%d") != datetime.now().strftime("%Y-%m-%d"):
            send_dwh_alert_slack_message(f':lock_alert: *The Watcher:* [dwh] detected *[{locks_cnt[0]}]* locked_items')
            context.log.info(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
            update_watcher_log_entry(previous_lock_cnt.id, locks_cnt)
        else:
            is_more_than_one_hour = datetime.now() - previous_lock_cnt.update_date

            # compare if previous locks cnt the same and a period of time no more than 1 hour
            if locks_cnt[0] == previous_lock_cnt.module_check_count and str(is_more_than_one_hour).split(':', 1)[0] < "1":
                pass
            else:
                send_dwh_alert_slack_message(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
                context.log.info(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
                update_watcher_log_entry(previous_lock_cnt.id, locks_cnt)

        session.close()

    else:
        context.log.info('No locks')

@op
def check_locks_rpl(context):
    '''
    This app are going to check existing locks in dwh db via defined view: dwh_test.lock_monitor
    compare a result with a previous result saved in table: dwh_system.watcher_logs
    do appropriate action: notify or pass
    '''

    # select count of locks from db view
    sql = """
            SELECT COALESCE(blockingl.relation::regclass::text, blockingl.locktype) AS locked_item,
               blockeda.datname,
               now() - blockeda.query_start                                     AS waiting_duration,
               blockeda.pid                                                     AS blocked_pid,
               blockeda.query                                                   AS blocked_query,
               blockeda.usename                                                 AS blocked_user,
               blockedl.mode                                                    AS blocked_mode,
               blockinga.pid                                                    AS blocking_pid,
               blockinga.query                                                  AS blocking_query,
               blockingl.mode                                                   AS blocking_mode,
               blockinga.usename                                                AS blocked_by
        FROM pg_locks blockedl
                 JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
                 JOIN pg_locks blockingl ON (blockingl.transactionid = blockedl.transactionid OR
                                             blockingl.relation = blockedl.relation AND
                                             blockingl.locktype = blockedl.locktype) AND blockedl.pid <> blockingl.pid
                 JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid AND blockinga.datid = blockeda.datid
        WHERE NOT blockedl.granted
        ;
    """

    rpl_config = {
        'nl_host': {'host_name': '10.0.1.65',
                    'db': 'ua'},
        'us_host': {'host_name': '192.168.1.207',
                    'db': 'us'}
    }

    for host_info in rpl_config.values():
        # connect to dwh db
        conn = repstat_psycopg2_connection(
            host_info['host_name'],
            host_info['db']
        )

        # execute query
        cur = conn.cursor()
        cur.execute(sql)

        # fetch result
        locks_cnt = cur.fetchone()

        # close connection
        cur.close()
        conn.close()

        if locks_cnt:

            if host_info['host_name'] == '10.0.1.65':
                db_name = 'nl'
            else:
                db_name = 'us'

            # session = Session()
            # previous_lock_cnt = session.query(LogWatcher).filter(LogWatcher.module_name == inspect.currentframe().f_code.co_name).one()

            # check if last saved date of cnt of locks is today
            # if previous_lock_cnt.update_date.strftime("%Y-%m-%d") != datetime.now().strftime("%Y-%m-%d"):
            send_dwh_alert_slack_message(f':lock_alert: *The Watcher:* [rpl-{db_name}] detected locked_items')
            context.log.info(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
                # update_watcher_log_entry(previous_lock_cnt.id, locks_cnt)
            # else:
            #     is_more_than_one_hour = datetime.now() - previous_lock_cnt.update_date
            #
            #     # compare if previous locks cnt the same and a period of time no more than 1 hour
            #     if locks_cnt[0] == previous_lock_cnt.module_check_count and str(is_more_than_one_hour).split(':', 1)[0] < "1":
            #         pass
            #     else:
            #         send_dwh_alert_slack_message(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
            #         context.log.info(f':lock_alert: *The Watcher:* detected *[{locks_cnt[0]}]* locked_items')
            #         update_watcher_log_entry(previous_lock_cnt.id, locks_cnt)
            #
            # session.close()

    else:
        context.log.info('No locks')


@job(
    name=JOB_PREFIX + JOB_NAME,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
)
def watcher_check_locks():
    """
    Review locks from: select count(*) from dwh_test.lock_monitor
    """
    check_locks_dwh()
    check_locks_rpl()
