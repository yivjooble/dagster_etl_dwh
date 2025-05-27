from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    # DynamicOut,
    Field,
)

# module import
from utility_hub import (
    repstat_job_config,
)
from utility_hub.core_tools import get_creds_from_vault
from ...utils.io_manager_path import get_io_manager_path
import psycopg2


TABLE_NAME = "dwh_cloudberry_exec_sql_code"


@op(required_resource_keys={"globals"})
def dwh_cloudberry_exec_sql_code_op(context):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """

    def db_conn_psycopg2(user, password, database, host, port):
        """
        Connect to the database using psycopg2
        """
        try:
            conn = psycopg2.connect(
                host=host,
                port=int(port),
                database=database,
                user=user,
                password=password
            )
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to the database: {e}")

    db = context.resources.globals['db']
    sql = context.resources.globals['sql']

    conn = db_conn_psycopg2(
        user=get_creds_from_vault("REPLICA_USER"),
        password=get_creds_from_vault("REPLICA_PASSWORD"),
        database=db,
        host="10.0.1.83",
        port=5432,
    )

    def execute_on_db(sql, conn):
        """
        Execute SQL query on the database
        """
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Failed to execute query on the database: {e}")

    # execute query
    execute_on_db(sql, conn)
    context.log.info(f"Query executed")


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(sql=Field(str, default_value='select 1'),
                                                   db=Field(str, default_value='an_dwh'),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name='service__dwh_cloudberry_exec_sql_code',
)
def dwh_cloudberry_exec_sql_code_job():
    dwh_cloudberry_exec_sql_code_op()
