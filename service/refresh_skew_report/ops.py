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
import pandas as pd
from utility_hub.utils import send_dwh_alert_slack_message


TARGET_TABLE = 'segment_skew_report'
SCHEMA = 'db_health'

PROCEDURE_CALL = 'call db_health.prc_refresh_segment_skew_report();'
GITLAB_DDL_Q, GITLAB_DDL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name='dwh_team',
    file_name='prc_refresh_segment_skew_report',
)

SCHEMA_LIST = [
    'affiliate',
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


def format_df_to_ascii_table(df):
    """
    Create a text-based ASCII table from a DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame to format into ASCII table

    Returns:
        str: Formatted ASCII table representation of the DataFrame
    """
    if df.empty:
        return "No data to display"

    # Get column widths
    col_widths = {}
    for col in df.columns:
        col_widths[col] = max(
            len(str(col)),
            df[col].astype(str).apply(len).max()
        ) + 2  # Add padding

    # Create header separator
    top_separator = "+"
    mid_separator = "|"
    for col in df.columns:
        top_separator += "-" * col_widths[col] + "+"
        mid_separator += "-" * col_widths[col] + "|"

    # Create header row
    header = "|"
    for col in df.columns:
        header += f" {str(col):{col_widths[col]-2}} |"

    # Create rows
    rows = []
    for _, row in df.iterrows():
        row_str = "|"
        for col in df.columns:
            value = str(row[col])
            row_str += f" {value:{col_widths[col]-2}} |"
        rows.append(row_str)

    # Combine parts
    table_parts = [top_separator, header, mid_separator]
    for row in rows:
        table_parts.append(row)
    table_parts.append(top_separator)

    return "\n".join(table_parts)


def create_html_report(content):
    """
    Create HTML content for a report with ASCII table inside code tags.

    Args:
        content (str): ASCII table content to be embedded in HTML

    Returns:
        str: HTML content as a string
    """
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<code>

{content}

</code>
</html>
"""
    return html_content


@op(required_resource_keys={'globals', 'io_manager'})
def refresh_skew_report_on_db(context):
    """
    Operation to refresh the segment skew report in the database and send alert to Slack.

    This operation:
    1. Calls the database procedure to refresh the skew report
    2. Retrieves the results
    3. Formats the data into an HTML report
    4. Sends the report to Slack as an alert

    Args:
        context: The Dagster execution context
    """
    # Get configuration from context
    destination_db = context.resources.globals["destination_db"]
    include_schemas = context.resources.globals.get("include_schemas")
    threshold = context.resources.globals.get("threshold")

    # Construct procedure call with or without parameters
    if include_schemas or threshold is not None:
        # Format include_schemas as an array if provided
        schemas_str = f"ARRAY{include_schemas}" if include_schemas else "NULL::text[]"
        # Use provided threshold or default to NULL
        threshold_str = str(threshold) if threshold is not None else "NULL::numeric"

        procedure_call = f"CALL {SCHEMA}.prc_refresh_segment_skew_report({schemas_str}, {threshold_str});"
    else:
        procedure_call = PROCEDURE_CALL

    context.log.info(f"Executing procedure: {procedure_call}")

    DwhOperations.execute_on_dwh(
        context=context,
        query=procedure_call,
        ddl_query=GITLAB_DDL_Q,
        destination_db=destination_db
    )

    # Query to retrieve the skew report data
    q = f"SELECT schema_name, full_table_name, skew_percentage FROM {SCHEMA}.{TARGET_TABLE};"
    result = DwhOperations.execute_on_dwh(
        context=context,
        query=q,
        fetch_results=True,
        destination_db=destination_db
    )

    # Convert result to DataFrame and generate report
    if result:
        df = pd.DataFrame.from_records(result, columns=['schema_name', 'full_table_name', 'skew_percentage'])

        if not df.empty:
            # Sort by schema_name alphabetically and then by skew percentage in descending order
            df.sort_values(by=['schema_name', 'skew_percentage'], ascending=[True, False], inplace=True)
            df.drop(columns=['schema_name'], inplace=True)

            # Format the dataframe as ASCII table
            ascii_table = format_df_to_ascii_table(df)

            # Generate HTML report content
            html_content = create_html_report(ascii_table)
            msg = f"⚠️ *SEGMENT SKEW REPORT* ⚠️\nFound {df.shape[0]} tables with significant skew."

            # Send alert to Slack with the in-memory HTML content
            send_dwh_alert_slack_message(
                message=msg,
                tag_dwh=True,
                as_file=True,
                filename="skew_report.html",
                file_content=html_content
            )

            context.log.info(f"Found {df.shape[0]} tables with significant skew. Alert sent to Slack.")
        else:
            context.log.info("No significant skew detected in tables.")
    else:
        context.log.info("No data returned from skew report query.")


@job(
    resource_defs={"io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"}),
                   "globals": make_values_resource(
                       destination_db=Field(str, default_value='cloudberry'),
                       include_schemas=Field(list, default_value=SCHEMA_LIST, is_required=False,
                                           description="List of schemas to include, e.g. ['imp', 'traffic']"),
                       threshold=Field(int, default_value=9, is_required=False,
                                     description="Threshold for skew percentage, default is 9")
                   )
                  },
    name=generate_job_name(TARGET_TABLE),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "gitlab_ddl_url": f"{GITLAB_DDL_URL}",
        "destination_db": "cloudberry",
        "target_table": f"{SCHEMA}.{TARGET_TABLE}",
        "truncate": "True"
    },
    description=f'{SCHEMA}.{TARGET_TABLE}',
)
def refresh_skew_report_job():
    refresh_skew_report_on_db()
