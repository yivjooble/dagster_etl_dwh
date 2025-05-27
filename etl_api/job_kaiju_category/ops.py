import requests
import pandas as pd

from etl_api.utils.utils import job_prefix
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub import (
    DwhOperations,
    retry_policy,
)

from dagster import op, job, fs_io_manager, Field, make_values_resource


SCHEMA = "dimension"
TABLE_NAME = "job_kaiju_category"
URL = "http://nl-internal-nginx1.jooble.com/jaeger-user-query-service/prod/api/sentinel/categories/uk"
JOB_PREFIX = job_prefix()


def flatten_json(item, parent_id=None, parent_name=None, results=None, level=0) -> list:
    """Recursively flatten the JSON data into a list of dictionaries suitable for DataFrame.
       Skip adding self-referential parent item, only add parent to child references."""

    if results is None:
        results = []
    children = item.get('children', [])
    for child in children:
        results.append({
            'id_parent': item['id'] if 'id' in item else None,
            'parent_name': item.get('name', ''),
            'id_child': child['id'],
            'child_name': child.get('name', '')
        })
        flatten_json(child, item['id'], item.get('name', ''), results, level + 1)
    return results


@op(retry_policy=retry_policy)
def job_kaiju_category_fetch_json_data(context) -> list:
    """Fetch JSON data from the given URL with error handling."""

    try:
        response = requests.get(URL)
        response.raise_for_status()
        json_data = response.json()
        if not json_data:
            raise ValueError("Received an empty JSON response.")
        return json_data
    except requests.RequestException as e:
        context.log.error(f"Error fetching data: {e}")
        raise e


@op(required_resource_keys={'globals'})
def job_kaiju_category_save_df_to_dwh(context, json_data):
    """Convert the JSON data from a URL into a pandas DataFrame without self-references."""
    destination_db = context.resources.globals["destination_db"]
    flat_data = []

    for item in json_data:
        flatten_json(item, results=flat_data)

    df_combined = pd.DataFrame(flat_data)
    try:
        if not df_combined.empty:
            DwhOperations.truncate_dwh_table(
                context=context,
                schema=SCHEMA,
                table_name=TABLE_NAME,
                destination_db=destination_db
            )

            DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=df_combined, destination_db=destination_db)

            context.log.info(f"Successfully saved df to dwh. Rows: {len(df_combined)}.")
        else:
            context.log.warning("No records to save to dwh.")
    except Exception as e:
        context.log.error(f"Error saving to dwh: {e}")
        raise e


@job(
    resource_defs={"globals": make_values_resource(destination_db=Field(str, default_value='both')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=JOB_PREFIX + TABLE_NAME,
    description=f"Job for '{SCHEMA}.{TABLE_NAME}': Fetches job kaiju categories from api - "
                f"'{URL}', and stores it in the DWH. "
                f"This job also handles data truncation for the target table.",
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
)
def job_kaiju_category_job():
    json_data = job_kaiju_category_fetch_json_data()
    job_kaiju_category_save_df_to_dwh(json_data)
