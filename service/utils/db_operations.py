import psycopg2
from utility_hub.core_tools import get_creds_from_vault


def db_conn_psycopg2(user: str = 'DWH_USER',
                     password: str = 'DWH_PASSWORD',
                     database: str = 'an_dwh',
                     host: str = 'DWH_HOST',
                     port: str = 'DWH_PORT'):
    """
    Connect to the database using psycopg2
    """
    try:
        conn = psycopg2.connect(
            host=get_creds_from_vault(host),
            port=int(get_creds_from_vault(port)),
            database=database,
            user=get_creds_from_vault(user),
            password=get_creds_from_vault(password),)
        return conn
    except Exception as e:
        raise Exception(f"Failed to connect to the database: {e}")


def execute_on_db(sql):
    """
    Execute SQL query on the database
    """
    try:
        conn = db_conn_psycopg2()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Failed to execute query on the database: {e}")


def select_from_db(query: str) -> list:
    """
    Select data from the database
    """
    try:
        conn = db_conn_psycopg2()
        cur = conn.cursor()
        cur.execute(f"{query}")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise Exception(f"Failed to select data from the database: {e}")
