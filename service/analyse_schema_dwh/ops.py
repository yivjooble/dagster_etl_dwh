from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from utility_hub import DwhOperations


TABLE_NAME = 'dwh_analyze_schema'
SCHEMA = 'dwh_team'

PROCEDURE_CALL = 'call dwh_test.analyze_schema(%s);'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name='analyze_schema_dwh',
)


@op(required_resource_keys={'globals'})
def analyze_schema_dwh_query_on_db(context):
    schema = context.resources.globals["schema"]

    context.log.info(f"schema: {schema}\n"
                     f"DDL run on dwh:\n{GITLAB_DDL_URL}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=PROCEDURE_CALL,
        ddl_query=GITLAB_DDL_Q,
        params=(schema,)
    )


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='aggregation'))
                  },
    name=generate_job_name(TABLE_NAME, '_aggregation'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_aggregation_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='imp'))
                  },
    name=generate_job_name(TABLE_NAME, '_imp'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_imp_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='imp_api'))
                  },
    name=generate_job_name(TABLE_NAME, '_imp_api'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_imp_api_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='imp_employer'))
                  },
    name=generate_job_name(TABLE_NAME, '_imp_employer'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_imp_employer_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='imp_statistic'))
                  },
    name=generate_job_name(TABLE_NAME, '_imp_statistic'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_imp_statistic_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='mobile_app'))
                   },
    name=generate_job_name(TABLE_NAME, '_mobile_app'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_mobile_app_job():
    analyze_schema_dwh_query_on_db()


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(schema=Field(str, default_value='affiliate'))
                   },
    name=generate_job_name(TABLE_NAME, '_affiliate'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def analyze_schema_dwh_affiliate_job():
    analyze_schema_dwh_query_on_db()