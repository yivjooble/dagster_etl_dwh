import os
import pandas as pd

from dagster import (
    op,
    job,
    fs_io_manager,
    Failure
)

from utility_hub.core_tools import generate_job_name
from service.utils.io_manager_path import get_io_manager_path
from service.utils.dwh_db_operations import select_from_dwh, execute_on_dwh

JOB_NAME = "vacuum_full"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

"""
Vacuum full
Source: tables listed in dwh table: dwh_system.vacuum_full
"""


@op
def get_vacuum_full_tables(context) -> pd.DataFrame:
    """
    Retrieves the list of tables to be vacuumed.

    Args:
        context (Context): The context object.

    Returns:
        List[str]: The list of tables to be vacuumed.
    """
    # Get list of tables to be vacuumed
    try:
        df = select_from_dwh("select schema, table_name from dwh_system.vacuum_full")
        context.log.info(f'===== Got list of tables to be vacuumed:\n{df.to_markdown()}')
        return df
    except Exception as e:
        raise Failure(f'Error while getting list of tables to be vacuumed: {e}')


@op
def vacuum_full(context, df: pd.DataFrame):
    """
    Performs vacuum full on the specified table.

    Args:
        context (Context): The context object.
        df (pd.DataFrame): The DataFrame containing schema and table_name.
    """
    try:
        for index, row in df.iterrows():
            execute_on_dwh(f'VACUUM FULL {row["schema"]}.{row["table_name"]}')
            context.log.info(f'===== Performed vacuum full on {row["schema"]}.{row["table_name"]}')
    except Exception as e:
        raise Failure(f'Error while performing vacuum full: {e}')


@job(
    name=generate_job_name(JOB_NAME),
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
)
def vacuum_full_job():
    """
    Performs vacuum full on the specified tables from dwh table: dwh_system.vacuum_full.
    """
    # Get list of tables to be vacuumed
    tables = get_vacuum_full_tables()
    vacuum_full(tables)
