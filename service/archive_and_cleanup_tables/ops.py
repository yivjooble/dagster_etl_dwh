from dagster import (
    op,
    job,
    fs_io_manager,
)

from utility_hub.core_tools import generate_job_name
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.db_operations import DwhOperations


maintenance_config = {
    'session_abtest_agg': {
        "retention_days": 365,
        'schema': 'aggregation',
        'table_name': 'session_abtest_agg',
        "archive_schema": "archive",
        'archive_table': 'session_abtest_agg',
        'date_column': 'action_datediff',
        'date_type': 'datediff',
        'archive_enabled': True,
    },
    'project_job_apply_agg': {
        "retention_days": 365,
        'schema': 'aggregation',
        'table_name': 'project_job_apply_agg',
        "archive_schema": "archive",
        'archive_table': 'project_job_apply_agg',
        'date_column': 'job_view_datediff',
        'date_type': 'datediff',
        'archive_enabled': True,
    },
    'action_agg': {
        "retention_days": 365,
        'schema': 'aggregation',
        'table_name': 'action_agg',
        "archive_schema": "archive",
        'archive_table': 'action_agg',
        'date_column': 'date_diff',
        'date_type': 'datediff',
        'archive_enabled': True,
    },
    'scroll_click_position_agg': {
        "retention_days": 365,
        'schema': 'aggregation',
        'table_name': 'scroll_click_position_agg',
        "archive_schema": "archive",
        'archive_table': 'scroll_click_position_agg',
        'date_column': 'date_diff',
        'date_type': 'datediff',
        'archive_enabled': True,
    },
    'search_agg': {
        "retention_days": 365,
        'schema': 'aggregation',
        'table_name': 'search_agg',
        "archive_schema": "archive",
        'archive_table': 'search_agg',
        'date_column': 'date_diff',
        'date_type': 'datediff',
        'archive_enabled': True,
    },
    # 'seo_ab_prod_metrics': { # TODO: uncomment this config - 2025-12-25
    #     "retention_days": 365,
    #     'schema': 'traffic',
    #     'table_name': 'seo_ab_prod_metrics',
    #     'date_column': 'date_diff',
    #     'date_type': 'datediff',
    #     'archive_enabled': False,
    # },
}


@op
def archive_and_cleanup_tables(context):
    for config in maintenance_config.values():
        DwhOperations.maintain_table_retention(
            context=context,
            **config
        )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name('archive_and_cleanup_tables'),
    tags={"service": "archive_and_cleanup_tables"},
    metadata={
        "maintenance_tables": f"{', '.join(maintenance_config.keys())}",
    },
    description="""
        This job is used to archive and cleanup tables.
        For each table, it can either archive old records to an archive table
        or delete them directly based on the archive_enabled configuration.
    """,
)
def archive_and_cleanup_tables_job():
    archive_and_cleanup_tables()
