from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput,
)

# module import
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import generate_job_name
from utility_hub import DwhOperations
from utility_hub.job_configs import job_config


JOB_NAME = 'analyse_schema_tables_cb'
FUNCTION_NAME = 'fn_analyze_schema_tables'
SCHEMA = 'db_health'

SCHEMA_LIST = [
    'affiliate',
    'affiliate_dev',
    'aggregation',
    'auction',
    'balkans',
    'dc',
    'default',
    'dimension',
    'employer',
    'imp',
    'imp_api',
    'imp_employer',
    'imp_statistic',
    'mobile_app',
    'public',
    'traffic',
]


@op(required_resource_keys={'globals'}, out=DynamicOut())
def analyse_schema_tables_cb_get_sqlinstance(context):
    """
    Operation to get the sql instance to analyse the schema tables
    """
    schemas = context.resources.globals["include_schemas"]
    # Create a separate output for each schema in the list
    for schema in schemas:
        yield DynamicOutput(
            value={
                "schema": schema,
            },
            mapping_key=schema
        )


@op(required_resource_keys={'globals'})
def analyse_schema_tables_cb(context, schema_str):
    """
    Operation to analyse the schema tables in the database and send alert to Slack.

    This operation:
    1. Calls the database function to analyse the schema tables

    Args:
        context: The Dagster execution context
    """
    # Get configuration from context
    destination_db = context.resources.globals["destination_db"]

    try:
        # Construct function call
        f_call = f"SELECT {SCHEMA}.{FUNCTION_NAME}('{schema_str['schema']}');"
    except Exception as e:
        context.log.error(f"Error calling function: {e}")
        raise e

    context.log.info(f"Executing function: {f_call}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=f_call,
        destination_db=destination_db
    )



@job(
    config=job_config,
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(
                       destination_db=Field(str, default_value='cloudberry'),
                       include_schemas=Field(list, default_value=SCHEMA_LIST, is_required=False,
                                           description="List of schemas to include, e.g. ['imp', 'traffic']"),
                   )
                  },
    name=generate_job_name(JOB_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "cloudberry",
    },
    description=f'Run {SCHEMA}.{FUNCTION_NAME} for each schema in the list',
)
def analyse_schema_tables_cb_job():
    instances = analyse_schema_tables_cb_get_sqlinstance()
    instances.map(analyse_schema_tables_cb).collect()
