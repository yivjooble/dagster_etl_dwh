import pandas as pd
from dataclasses import dataclass, field
from typing import List

from dagster import (
    op,
    DynamicOut,
)

# module import
from utility_hub import (
    Operations,
    DbOperations,
    DwhOperations,
    retry_policy,
)
from utility_hub.core_tools import fetch_gitlab_data, submit_external_job_run, check_time_condition


@dataclass(frozen=True)
class EmailAbtestAccountConfig:
    table_name: str = "email_abtest_account_agg"
    schema: str = "aggregation"
    schema_clickhouse: str = 'dwh'
    reload_dbs: List[str] = field(default_factory=lambda: ['dwh', 'clickhouse'])
    time_check: float = 09.00


config = EmailAbtestAccountConfig()
TIME_CONDITION = check_time_condition(time_to_check=config.time_check, comparison='>=')
EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q, EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=config.schema,
    file_name=config.table_name,
)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def email_abtest_account_agg_get_sqlinstance(context, email_abtest_agg_by_account_job=None):
    """
    Loop over prod sql instances and create output dictionary with data to start on separate instance.

    Args:
        context (_type_): logs
        email_abtest_agg_by_account_job(list): email_abtest_agg_by_account_job result.

    Yields:
        dict: dict with params to start query
    """
    launch_countries = context.resources.globals["reload_countries"]

    context.log.info(f'Selected countries: {launch_countries}\n'
                     f"SQL run on replica:\n{EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q_URL}")

    # iterate over sql instances
    for sql_instance in Operations.generate_sql_instance(
            context=context,
            instance_type="repstat",
            query=EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q,
            select_query=EMAIL_ACCOUNT_AGG_GITLAB_SELECT_Q):
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
        dbs = config.reload_dbs

        if 'dwh' not in dbs and 'clickhouse' not in dbs:
            context.log.warning('No database.')
            return

        for db in dbs:
            # Save to DWH
            if 'dwh' in db:
                DwhOperations.save_to_dwh_upsert(
                    context=context,
                    schema=config.schema,
                    table_name=config.table_name,
                    df=df_chunk
                )
                context.log.info(f"Saved for {db}")

            # Save to ClickHouse
            if 'clickhouse' in db:
                df = convert_arrow_to_pandas(df_chunk)
                DbOperations.save_to_clickhouse(
                    context=context,
                    database=config.schema_clickhouse,
                    table_name=config.table_name,
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

    return True


@op
def launch_cabacus(context, previous_ste_result=None):
    # Skip cabacus if current time is 09:00 or later
    if TIME_CONDITION:
        context.log.info(f"Current time isn't fit {config.time_check}, skip cabacus")
        return
    else:
        submit_external_job_run(job_name_with_prefix='main_job',
                                repository_location_name='cabacus',
                                instance='cabacus')
