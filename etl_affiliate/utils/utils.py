import psycopg2
import pandas as pd
import sqlparse
import gitlab

from contextlib import closing
from sqlalchemy import create_engine

from etl_affiliate.utils.messages import send_dwh_alert_slack_message
from etl_affiliate.utils.job_xx_hosts import HOST_BY_COUNTRY
from utility_hub.core_tools import get_creds_from_vault


def job_prefix():
    return 'aff__'


def execute_psycopg2_connect(db_name,
                             username,
                             password,
                             host,
                             query):
    """
    Args:
        db_name (str): The name of the database to connect to.
        username (str): The username for database authentication.
        password (str): The password for database authentication.
        host (str): The hostname or address of the database server.
        query (str): The SQL query to execute (e.g., INSERT, UPDATE, DELETE).

    Returns:
        None: This function does not return any value.

    Raises:
        psycopg2.Error: If a database connection or query execution error occurs.
    """
    with closing(psycopg2.connect(dbname=db_name, user=username, password=password, host=host)) as con:
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        cur.close()


def log_written_data(context, date: str, start_dt: str, end_dt: str, op_name: str, table_schema: str or None = None,
                     table_name: str or None = None, send_slack_message: bool = True, write_to_context: bool = True,
                     dagster_log_schema: str or None = None) -> None:
    """
    Log executed step to the dagster context and send message to Slack.

    Args:
        context: Dagster run context object.
        table_schema (str): schema of the table data was inserted into.
        table_name (str): name of the table data was inserted into.
        date (str): date string in the YYYY-MM-DD format of the date data was collected for.
        start_dt (str): datetime string of table recording execution start.
        end_dt (str): datetime string of table recording execution finish.
        op_name (str): the operation name.
        send_slack_message (bool): if True, send a message to Slack.
        write_to_context (bool): if True, write a message to dagster log.
        dagster_log_schema (str): dwh schema to insert dagster_job_log data (if table_schema is not None, it's valid
            for this purpose also).
    """
    if write_to_context:
        context.log.info(
            f'{table_schema}.{table_name} is updated ({date})')

    if send_slack_message:
        send_dwh_alert_slack_message(
            f':done-1: `{table_schema}.{table_name}` is updated *({date})*'
        )

    df = pd.DataFrame.from_dict({
        'run_id': [context.run.run_id],
        'job_name': [context.run.job_name],
        'operation_name': [op_name],
        'op_start_datetime': [start_dt],
        'op_end_datetime': [end_dt],
        'record_date': [date],
        'schema_name': [table_schema],
        'table_name': [table_name]
    })
    save_to_dwh(df, table_name='dagster_job_log', schema=dagster_log_schema or table_schema)


def read_sql_file(file: str) -> str:
    """
    Reads sql file.
    Args:
        file (str): path to sql file.

    Returns:
        str: sql query.
    """
    sql_file = open(file)
    sql_query = sql_file.read()
    sql_file.close()
    return sql_query


def call_dwh_procedure(procedure_schema: str, procedure_name: str, destination_db: str = "both") -> None:
    """
    Calls the specified procedure in DWH.

    Args:
        procedure_schema (str): the schema of a procedure to execute.
        procedure_name (str): the name of a procedure to execute.
        destination_db (str): postgres dwh or cloudberry dwh or both.
    """

    exec_query_pg("call {}.{};".format(procedure_schema, procedure_name),
                  host="dwh",
                  destination_db=destination_db)


def call_aff_procedure(procedure_schema: str, procedure_name: str, cluster: str) -> None:
    """
    Calls the specified procedure in affiliate database.

    Args:
        procedure_schema (str): the schema of a procedure to execute.
        procedure_name (str): the name of a procedure to execute.
        cluster (str): 'nl' or 'us' - cluster to execute procedure on.
    """

    exec_query_pg("call {}.{};".format(procedure_schema, procedure_name), cluster)


def create_conn(host: str, country=None):
    """
    Creates SQLAlchemy engine for executing SQL queries in different databases.

    Args:
        host (str): shortcut of the name of the database host. Possible values:
            'dwh' - dwh.jooble.com
            'cloudberry' - an-dwh.jooble.com
            'nl'/'us' - affiliate databases of the NL/US cluster
            'dwh2' - 'dwh' database on the '10.0.0.137'
            'soska' - database 'Soska' on 'lager.jooble.com'
            'prod' - Job_XX databases
        country (str): 2-letter country code in case if connecting to the Job_XX.

    Returns:
        sqlalchemy.engine.Engine: engine to use to connect to the database.
    """
    sql_server_driver = 'ODBC Driver 17 for SQL Server'
    if host in ('dwh', 'dwh2', 'us', 'nl', 'cloudberry'):
        if host == 'dwh':
            user = get_creds_from_vault('DWH_USER')
            pw = get_creds_from_vault('DWH_PASSWORD')
            host = get_creds_from_vault('DWH_HOST')
            db = get_creds_from_vault('DWH_DB')
        elif host == 'cloudberry':
            user = get_creds_from_vault('DWH_USER')
            pw = get_creds_from_vault('DWH_PASSWORD')
            host = "an-dwh.jooble.com"
            db = "an_dwh"
        elif host == 'us':
            user = get_creds_from_vault('AFF_USER')
            pw = get_creds_from_vault('AFF_PASSWORD')
            db = get_creds_from_vault('AFF_DB')
            host = get_creds_from_vault('AFF_HOST_US')
        elif host == 'nl':
            user = get_creds_from_vault('AFF_USER')
            pw = get_creds_from_vault('AFF_PASSWORD')
            db = get_creds_from_vault('AFF_DB')
            host = get_creds_from_vault('AFF_HOST_NL')
        else:
            user = get_creds_from_vault('JOB_EXPORTER_USER')
            pw = get_creds_from_vault('JOB_EXPORTER_PASSWORD')
            host = get_creds_from_vault('JOB_EXPORTER_HOST')
            db = get_creds_from_vault('JOB_EXPORTER_DB')
        con = create_engine('postgresql://{user}:{password}@{host}/{dbname}'.format(
            dbname=db, user=user, password=pw, host=host))

    else:
        if host == 'soska':
            user = get_creds_from_vault('S_USER')
            pw = get_creds_from_vault('S_PASSWORD')
            host = get_creds_from_vault('S_HOST')
            db = get_creds_from_vault('S_DB')
        elif host == 'prod':
            host = HOST_BY_COUNTRY[country] + '.jooble.com'
            db = 'Job_' + country
            user = get_creds_from_vault('JOBXX_USER')
            pw = get_creds_from_vault('JOBXX_PASSWORD')
        else:
            print('Invalid host.')
            return None
        con = create_engine(
            url="mssql+pyodbc://{0}:{1}@{2}:{3}/{4}?driver={5}".format(user, pw, host, 1433, db, sql_server_driver),
            connect_args={'connect_timeout': 14400},
            pool_pre_ping=True
        )

    return con


def exec_query_ms(query: str, host: str, country=None) -> pd.DataFrame:
    """
    Executes select query in the SQL Server database and returns result as a pandas DataFrame
    using pandas.read_sql_query.

    Args:
        query (str): select query to run.
        host (str): shortcut of the name of the database host. Possible values:
            'soska' - database 'Soska' on 'lager.jooble.com'
            'prod' - Job_XX databases
        country (str): 2-letter country code in case if connecting to the Job_XX.

    Returns:
        pd.DataFrame: query result.
    """
    conn = create_conn(host, country=country)
    conn.timeout = 600
    sql_query = pd.read_sql_query(query, conn)
    return sql_query


def exec_query_pg(q: str, host: str, destination_db: str = "both") -> None:
    """
    Executes not select query in the Postgres database using psycopg2 cursor.

    Args:
        q (str): sql query to execute.
        host (str): shortcut of the name of the database host. Possible values:
            'dwh' - dwh.jooble.com
            'nl'/'us' - affiliate databases of the NL/US cluster
            'destination_db' (str): postgres dwh or cloudberry dwh or both.
    """
    user = get_creds_from_vault('AFF_USER')
    pw = get_creds_from_vault('AFF_PASSWORD')
    db = get_creds_from_vault('AFF_DB')
    if host == 'us':
        host_address = get_creds_from_vault('AFF_HOST_US')
    elif host == 'nl':
        host_address = get_creds_from_vault('AFF_HOST_NL')
    elif host == 'dwh':
        if destination_db == "cloudberry":
            # Cloudberry dwh
            cb_db = "an_dwh"
            cb_user = get_creds_from_vault('DWH_USER')
            cb_pw = get_creds_from_vault('DWH_PASSWORD')
            cb_host_address = "an-dwh.jooble.com"

            execute_psycopg2_connect(db_name=cb_db,
                                     username=cb_user,
                                     password=cb_pw,
                                     host=cb_host_address,
                                     query=q)
            print("executed on cloudberry")
            return

        elif destination_db == "dwh":
            # Postgres dwh
            db = get_creds_from_vault('DWH_DB')
            user = get_creds_from_vault('DWH_USER')
            pw = get_creds_from_vault('DWH_PASSWORD')
            host_address = get_creds_from_vault('DWH_HOST')

            execute_psycopg2_connect(db_name=db,
                                     username=user,
                                     password=pw,
                                     host=host_address,
                                     query=q)
            print("executed on postgres")
            return

        elif destination_db == "both":
            # Postgres dwh
            db = get_creds_from_vault('DWH_DB')
            user = get_creds_from_vault('DWH_USER')
            pw = get_creds_from_vault('DWH_PASSWORD')
            host_address = get_creds_from_vault('DWH_HOST')

            execute_psycopg2_connect(db_name=db,
                                     username=user,
                                     password=pw,
                                     host=host_address,
                                     query=q)
            print("executed on postgres")

            # Cloudberry dwh
            cb_db = "an_dwh"
            cb_user = get_creds_from_vault('DWH_USER')
            cb_pw = get_creds_from_vault('DWH_PASSWORD')
            cb_host_address = "an-dwh.jooble.com"

            execute_psycopg2_connect(db_name=cb_db,
                                     username=cb_user,
                                     password=cb_pw,
                                     host=cb_host_address,
                                     query=q)
            print("executed on cloudberry")
            return
    else:
        print('Invalid host.')
        return None

    # Default execution
    with closing(psycopg2.connect(dbname=db, user=user, password=pw, host=host_address)) as con:
        cur = con.cursor()
        cur.execute(q)
        con.commit()
        cur.close()


def exec_select_query_pg(q: str, host: str):
    """
    Executes select query in the Postgres database using psycopg2 cursor. This is useful in case of calling a function
    that first updates the table and then returns some result. If pandas.read_sql_query is used for such purpose, any
    changes made with update/insert/delete statements are not committed.

    Args:
        q (str): sql query to execute.
        host (str): shortcut of the name of the database host. Possible values:
            'dwh' - dwh.jooble.com
            'nl'/'us' - affiliate databases of the NL/US cluster

    Returns:
        pd.DataFrame: query result.
    """
    user = get_creds_from_vault('AFF_USER')
    pw = get_creds_from_vault('AFF_PASSWORD')
    db = get_creds_from_vault('AFF_DB')
    if host == 'us':
        host_address = get_creds_from_vault('AFF_HOST_US')
    elif host == 'nl':
        host_address = get_creds_from_vault('AFF_HOST_NL')
    elif host == 'dwh':
        # Postgres dwh
        db = get_creds_from_vault('DWH_DB')
        user = get_creds_from_vault('DWH_USER')
        pw = get_creds_from_vault('DWH_PASSWORD')
        host_address = get_creds_from_vault('DWH_HOST')
    else:
        print('Invalid host.')
        return None

    with closing(psycopg2.connect(dbname=db, user=user, password=pw, host=host_address)) as con:
        cur = con.cursor()
        cur.execute(q)
        records = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        con.commit()
        cur.close()
    return pd.DataFrame(records, columns=colnames)


def save_to_dwh(df, table_name, schema):
    # Postgres dwh
    df.to_sql(
        table_name,
        con=create_conn('dwh'),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=50000
    )

    # Cloudberry dwh
    df.to_sql(
        table_name,
        con=create_conn('cloudberry'),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=50000
    )


def get_file_path(dir_name: str, file_name: str):
    """
    Get file path from gitlab
    """
    return f'{dir_name}/tables/{file_name}'


def get_gitlab_file_content(file_path: str, ref: str = 'master',
                            api_token: str = get_creds_from_vault('GITLAB_PRIVATE_TOKEN_PROD'),
                            project_id: str = '864'):
    try:
        url = 'https://gitlab.jooble.com'
        gl = gitlab.Gitlab(url, private_token=api_token)
        project = gl.projects.get(project_id)
        file_content = project.files.get(file_path=file_path, ref=ref).decode()
        formatted_ddl = sqlparse.format(file_content, reindent=True, keyword_case='upper')
    except Exception as e:
        raise Exception(f'Error while getting file content from gitlab: {e}\n{project_id}\n{file_path}\n{ref}')

    return formatted_ddl
