import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import closing
from sqlalchemy import create_engine
import pandas as pd
from utility_hub.core_tools import get_creds_from_vault

USER = get_creds_from_vault('DWH_USER')
PASSWORD = get_creds_from_vault('DWH_PASSWORD')
HOST = get_creds_from_vault('DWH_HOST')
DB = get_creds_from_vault('DWH_DB')


def execute_query(q: str) -> None:
    """Performs SQL query in the DWH.

    Args:
        q (str): query to run.
    """
    with closing(psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)) as con:
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        cursor.execute(q)


def select_to_dataframe(q: str) -> pd.DataFrame:
    """Performs SQL select query in the DWH and returns a DataFrame with query results.

    Args:
        q (str): select query to run.

    Returns:
        pd.DataFrame: pandas DataFrame with query results.
    """
    conn = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
        dbname=DB, user=USER, password=PASSWORD, host=HOST))

    df = pd.read_sql(q, conn)
    return df


def insert_to_db(schema: str, df: pd.DataFrame, table_name: str) -> None:
    """Inserts DataFrame in the specified table of the specified database.

    Args:
        schema (str): schema name to insert the data in. Should be either 'freshdesk_internal' or 'freshdesk_external'.
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


def delete_old_logs(schema: str) -> None:
    """Calls delete_old_logs procedure to clear old records in update_history table.

    Args:
        schema (str): schema name to run procedure in. Should be either 'freshdesk_internal' or 'freshdesk_external'.
    """
    q = 'call {}.delete_old_logs()'.format(schema)
    execute_query(q)
