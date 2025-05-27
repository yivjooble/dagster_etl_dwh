import pandas as pd
from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    DynamicOut,
    Field,
)

# module import
from utility_hub import (
    Operations,
    DwhOperations,
    DbOperations,
    all_countries_list,
    repstat_job_config,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, generate_job_name
from ..utils.io_manager_path import get_io_manager_path

TABLE_NAME = "email_abtest_account_agg"
SCHEMA = "aggregation"
CLICKHOUSE_SCHEMA = 'dwh'

GITLAB_SELECT_Q, GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def email_abtest_account_agg_get_sqlinstance(context):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

    Args:
        context (_type_): logs

    Yields:
        dict: dict with params to start query
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f"SQL run on replica:\n{GITLAB_SELECT_Q_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=GITLAB_SELECT_Q,
            select_query=GITLAB_SELECT_Q):
        yield sql_instance


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def email_abtest_account_agg_query_on_db(context, sql_instance_country_query: dict):
    """Start procedure on rpl with input data

    Args:
        context (_type_): logs
        sql_instance_country_query (dict): dict with params to start

    Returns:
        _type_: None
    """
    def convert_arrow_to_pandas(df):
        """
        Converts ArrowExtensionArray columns in the DataFrame to standard Pandas dtypes.
        """
        for column in df.columns:
            if isinstance(df[column].array, pd.core.arrays.arrow.array.ArrowExtensionArray):
                df[column] = df[column].astype(object)
        return df

    def save_chunk(df_chunk):
        """
        Save chunks to follow DRY.
        """
        reload_dbs = context.resources.globals["reload_dbs"]

        if 'dwh' not in reload_dbs and 'clickhouse' not in reload_dbs:
            context.log.warning('No database.')
            return

        for db in reload_dbs:
            # Save to DWH
            if 'dwh' in db:
                DwhOperations.save_to_dwh_upsert(
                    context=context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    df=df_chunk
                )
                context.log.info(f"Saved for {db}")

            # Save to ClickHouse
            if 'clickhouse' in db:
                df = convert_arrow_to_pandas(df_chunk)
                DbOperations.save_to_clickhouse(
                    context=context,
                    database=CLICKHOUSE_SCHEMA,
                    table_name=TABLE_NAME,
                    df=df
                )
                context.log.info(f"Saved for {db}")

    # Generator for retrieving chunks
    chunk_generator = DbOperations.execute_query_and_return_chunks(
        context=context,
        sql_instance_country_query=sql_instance_country_query,
    )

    # Check for the presence of data
    first_chunk = next(chunk_generator, None)
    if first_chunk is None:
        return

    # Save the first chunk
    save_chunk(first_chunk)

    # Save the remaining chunks
    for chunk in chunk_generator:
        save_chunk(chunk)


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                                   reload_dbs=Field(list, default_value=['dwh', 'clickhouse'])),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}, {CLICKHOUSE_SCHEMA}"},
    metadata={
        "gitlab_sql_url": f"{GITLAB_SELECT_Q_URL}",
        "destination_db": "dwh, clickhouse",
        "dwh_table": f"{SCHEMA}.{TABLE_NAME}",
        "clickhouse_table": f"{CLICKHOUSE_SCHEMA}.{TABLE_NAME}"
    },
    description=f' {SCHEMA}.{TABLE_NAME}',
)
def email_abtest_account_agg_job():
    instances = email_abtest_account_agg_get_sqlinstance()
    instances.map(email_abtest_account_agg_query_on_db).collect()
