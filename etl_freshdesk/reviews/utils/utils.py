import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import closing
from sqlalchemy import create_engine
import pandas as pd
from slack_logger import SlackLogger
from utility_hub.core_tools import get_creds_from_vault

USER = get_creds_from_vault('DWH_USER')
PASSWORD = get_creds_from_vault('DWH_PASSWORD')
HOST = get_creds_from_vault('DWH_HOST')
DB = get_creds_from_vault('DWH_DB')

SLACK_TOKEN = get_creds_from_vault('DWH_ALERTS_TOKEN')


def execute_query(q: str) -> None:
    """Performs SQL query in the 'external_freshdesk' database.

    Args:
        q (str): query to run.
    """
    with closing(psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        cursor.execute(q)


def insert_to_db(schema: str, df: pd.DataFrame, table_name: str) -> None:
    """Inserts DataFrame in the specified table of the 'external_freshdesk' database.

    Args:
        schema (str): schema to insert data into.
        df (pd.DataFrame): data to insert.
        table_name (str): table to insert data into.
    """
    conn = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
            dbname=DB, user=USER, password=PASSWORD, host=HOST))
    df.to_sql(
        name=table_name,
        schema=schema,
        if_exists='append',
        con=conn,
        index=False
    )


def select_to_dataframe(q: str) -> pd.DataFrame:
    """Performs SQL select query in the 'external_freshdesk' database and returns a DataFrame with query results.

    Args:
        q (str): select query to run.

    Returns:
        pd.DataFrame: pandas DataFrame with query results.
    """
    conn = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname=DB, user=USER, password=PASSWORD, host=HOST))

    df = pd.read_sql(q, conn)
    return df


def list_to_db_array(value_list: list) -> str:
    """Convert list of values into a string to insert into the database as array.

    Args:
        value_list (list): list of values to convert.

    Returns:
        str: string, which starts and ends with curly braces and contains original list elements separated by commas.
    """
    return '{' + ', '.join([str(y) for y in value_list]) + '}'


def get_slack_logger(job_name: str) -> SlackLogger:
    """Create SlackLogger to send messages to Slack.

    Args:
        job_name (str): Dagster job name.

    Returns:
        slack_logger.SlackLogger: instantiated logger
    """
    options = {
        "service_name": job_name,
        "service_environment": "Dagster",
        "display_hostname": True,
        "default_level": "info",
    }
    logger = SlackLogger(token=SLACK_TOKEN, **options)
    return logger
