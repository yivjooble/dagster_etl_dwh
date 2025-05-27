import os
import random
import string
from typing import List

from dagster import DynamicOut, DynamicOutput, Out, fs_io_manager, job, op, Field, make_values_resource

# module import
from etl_jooble_internal.utils.io_manager_path import get_io_manager_path
from utility_hub import (
    DwhOperations,
    Operations,
    DbOperations,
    job_config,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name


TABLE_NAME = "domain_responsible_snapshot"
SCHEMA = "imp"
PATH_TO_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut())
def domain_responsible_snapshot_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictinary with data to start on separate instance.
    Args: sql_query.
    Output: sqlinstance, db, query.
    """
    Operations.delete_files(context, PATH_TO_DATA)

    context.log.info(f"Gitlab SQL query:\n{GITLAB_SQL_URL}")

    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="internal",
            instance_name="warehouse_137",
            db_name="dwh",
            query=GITLAB_SQL_Q):
        yield sql_instance


@op(out=Out(List[str]))
def domain_responsible_snapshot_launch_query_on_db(
    context, sql_instance_country_query: dict
) -> list:
    """
    Launch query on each instance.
    """
    file_path = DbOperations.execute_query_and_save_to_parquet(
        context, PATH_TO_DATA, sql_instance_country_query
    )
    return file_path


@op(required_resource_keys={"globals"})
def domain_responsible_snapshot_truncate_table(context, query_result_file_paths):
    """Truncate table in dwh."""
    destination_db = context.resources.globals["destination_db"]
    DwhOperations.truncate_dwh_table(
        context=context,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )
    return query_result_file_paths


@op(out=DynamicOut())
def domain_responsible_snapshot_collect_file_paths(context, file_paths_after_truncation):
    """
    Args:
        context (Context): The context object provided by Dagster.
        file_paths (List[str]): List of file paths.

    Returns:
        DynamicOutput: The list of file paths.
    """
    for f in file_paths_after_truncation:
        for file_path in f:
            if file_path.endswith(".parquet"):
                yield DynamicOutput(
                    value={'file_path': file_path,
                           },
                    mapping_key='db_name_' + ''.join(random.choices(string.ascii_letters, k=5))
                )


@op(required_resource_keys={"globals"})
def domain_responsible_snapshot_save_to_dwh(context, params: dict):
    try:
        file_path = params['file_path']
        destination_db = context.resources.globals["destination_db"]
        DwhOperations.save_to_dwh_copy_method(
            context=context,
            schema=SCHEMA,
            table_name=TABLE_NAME,
            file_path=file_path,
            destination_db=destination_db
        )
    except Exception as e:
        context.log.error(f"saving to dwh error: {e}")
        raise e


@job(
    config=job_config,
    resource_defs={
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
        "globals": make_values_resource(destination_db=Field(str, default_value="both")),
    },
    name=generate_job_name(TABLE_NAME),
    description=f"{SCHEMA}.{TABLE_NAME}",
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_SQL_URL}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "truncate": "True"
    },
)
def domain_responsible_snapshot_job():
    db_instances = domain_responsible_snapshot_get_sqlinstance()
    query_result_file_paths = db_instances.map(domain_responsible_snapshot_launch_query_on_db).collect()
    file_paths_after_truncation = domain_responsible_snapshot_truncate_table(query_result_file_paths)
    file_paths_dynamic = domain_responsible_snapshot_collect_file_paths(file_paths_after_truncation)
    file_paths_dynamic.map(domain_responsible_snapshot_save_to_dwh)
