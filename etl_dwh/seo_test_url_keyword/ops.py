import pandas as pd

from .url_parser import UrlParser
from dagster import (
    Field,
    fs_io_manager,
    job,
    make_values_resource,
    op,
)
from utility_hub import (
    DwhOperations,
    retry_policy,
)

from utility_hub.core_tools import generate_job_name
from ..utils.io_manager_path import get_io_manager_path


TABLE_NAME = 'seo_test_url_keyword'
SCHEMA = 'traffic'


def wrapper(row):
    parser_obj = UrlParser(country=row['country'])
    res = parser_obj.parse(row['keys'])
    if res['type_id'] != 1:
        value = (None, None)
    else:
        value = (res['keyword'], res['region'])
    return pd.Series(value, index=['keyword', 'region'])


@op(required_resource_keys={'globals'}, retry_policy=retry_policy)
def seo_test_url_keyword_query_on_db(context):

    destination_db = context.resources.globals['destination_db']

    # get data from dwh
    query = "select country, keys from aggregation.seo_test_groups"
    context.log.info("Fetching data from aggregation.seo_test_groups...")
    records = DwhOperations.execute_on_dwh(
        context=context,
        query=query,
        fetch_results=True,
        destination_db=destination_db
    )
    df = pd.DataFrame.from_records(records)
    context.log.info(f"Data fetched successfully - {df.shape[0]} rows")

    # truncate target table
    DwhOperations.truncate_dwh_table(
        context=context,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )

    # result_df = df.drop(columns=['country']).join(df.apply(wrapper, axis=1, result_type='expand'))

    # create empty DataFrame for results
    result_df = pd.DataFrame(columns=['keys', 'keyword', 'region'])

    # group by countries for vectorized processing
    for country, group in df.groupby('country'):
        parser_obj = UrlParser(country=country)

        # vectorized processing for each group
        results = group['keys'].apply(parser_obj.parse)

        # convert results to DataFrame
        parsed_results = pd.DataFrame([
            {
                'keys': row.keys,
                'keyword': result['keyword'] if result['type_id'] == 1 else None,
                'region': result['region'] if result['type_id'] == 1 else None
            }
            for row, result in zip(group.itertuples(), results)
        ])

        result_df = pd.concat([result_df, parsed_results], ignore_index=True)

    # insert data into target table
    DwhOperations.save_to_dwh_copy_method(
        context=context,
        df=result_df,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        destination_db=destination_db
    )


@job(
    resource_defs={
        "globals": make_values_resource(destination_db=Field(str, default_value="both")),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
        "destination_db": "dwh, cloudberry, both",
        "truncate": "True",
        "triggered_by_job": "internal__seo_test_groups",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def seo_test_url_keyword_job():
    seo_test_url_keyword_query_on_db()
