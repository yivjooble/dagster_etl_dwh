import psycopg2
from io import StringIO
from dagster import (
    op,
    job,
    fs_io_manager,
    Failure,
    make_values_resource,
    Field,
)
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import get_creds_from_vault


@op(required_resource_keys={'globals'})
def copy_table(context):

    # Define the source and target tables
    source_table = context.resources.globals['source_table']
    target_table = context.resources.globals['target_table']
    dwh_db_name = context.resources.globals['dwh_db_name']
    cloud_berry_db_name = context.resources.globals['cloud_berry_db_name']

    def _connect_db(db_name: str):
        name = get_creds_from_vault('DWH_USER')
        password = get_creds_from_vault('DWH_PASSWORD')

        db_params = {
            'dbname': dwh_db_name,
            'user': name,
            'password': password,
            'host': '10.0.1.61',
            'port': '5432'
        }

        if db_name == 'cloudberry':
            db_params = {
                'dbname': cloud_berry_db_name,
                'user': name,
                'password': password,
                'host': '10.0.2.151',
                'port': '5432'
            }

        # Establish connection to PostgreSQL database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        return cursor, connection

    try:
        # Create an in-memory buffer for data transfer
        data_buffer = StringIO()

        cursor_dwh, conn_dwh = _connect_db('dwh')
        cursor_cloudberry, conn_cloudberry = _connect_db('cloudberry')

        try:
            # Step 1: Copy data from source table to the in-memory buffer
            copy_from_source = f"COPY {source_table} TO STDOUT WITH CSV HEADER"
            cursor_dwh.copy_expert(copy_from_source, data_buffer)

            # Reset buffer's cursor to the start
            data_buffer.seek(0)

            # Step 2: Copy data from the buffer to the target table
            copy_to_target = f"COPY {target_table} FROM STDIN WITH CSV HEADER"
            cursor_cloudberry.copy_expert(copy_to_target, data_buffer)

            # Commit the transaction
            conn_cloudberry.commit()
            print(f"Data copied successfully from {source_table} to {target_table}.")

        except Exception as e:
            # Roll back in case of an error
            conn_cloudberry.rollback()
            print(f"Error occurred: {e}")

    except Exception as e:
        raise Failure(f"Failed to copy data from {source_table} to {target_table}:\n{e}")


@job(
    resource_defs={"globals": make_values_resource(source_table=Field(str, default_value='schema.table'),
                                                   target_table=Field(str, default_value='schema.table'),
                                                   dwh_db_name=Field(str, default_value='postgres'),
                                                   cloud_berry_db_name=Field(str, default_value='dwh'),
                                                   ),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    description='Copy data from one table to another',
    name='service__copy_table_job',
)
def copy_table_job():
    copy_table()
