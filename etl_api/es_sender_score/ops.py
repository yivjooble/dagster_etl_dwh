import os
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from etl_api.utils.messages import send_dwh_alert_slack_message
from etl_api.utils.api_jobs_config import retry_policy
from etl_api.utils.io_manager_path import get_io_manager_path
from utility_hub import DwhOperations
from utility_hub.core_tools import generate_job_name

from dagster import op, job, make_values_resource, Field, fs_io_manager
from elasticsearch import Elasticsearch


load_dotenv()

es = Elasticsearch(
    [
        {
            "host": os.getenv("ES_HOST"),
            "port": int(os.getenv("ES_PORT")),
            "scheme": os.getenv("ES_SCHEME"),
        }
    ]
)

ES_INDEX_ALL_EMAILS = "email_hash_index-202*-*"
SENDERSCORE_URL = "https://nl-service.jooble.com/monitoring-senderscore"

SCHEMA = "imp"
TABLE_NAME = "sender_score"
DATE_COLUMN = "log_date"
YESTERDAY_DATE = (datetime.now() - timedelta(days=1)).date().isoformat()


def fetch_and_parse_senderscore_data(url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            raw_data = response.text
            senderscore_lines = [
                line for line in raw_data.split("\n") if line.startswith("senderscore")
            ]
            data = []
            for line in senderscore_lines:
                # Extracting the domain and sender score from the line
                match = re.search(r'senderscore{domain="([^"]+)".*"} (\d+\.\d+)', line)
                if match:
                    data.append(
                        {
                            "sender_host_name": match.group(1),
                            "sender_score": float(match.group(2)),
                        }
                    )
            return pd.DataFrame(data)
        else:
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error during data fetch and parsing: {e}")


@op(required_resource_keys={"globals"}, retry_policy=retry_policy)
def sender_score_fetch_and_save_data(context, delete):
    """
    Fetches email sending and delivery metrics from Elasticsearch and merges them with sender scores
    fetched from a specified URL. The merged data is then sent to a data warehouse.
    """
    date_range = pd.date_range(
        start=context.resources.globals["reload_date_start"],
        end=context.resources.globals["reload_date_end"],
    )
    destination_db = context.resources.globals["destination_db"]

    for date in date_range:
        date_start_time = date.strftime("%Y-%m-%d 00:00:00")
        date_end_time = date.strftime("%Y-%m-%d 23:59:59")

        try:
            # Elasticsearch queries to fetch all and bounced emails within the date range
            query_all_emails = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "range": {
                                    "Timestamp": {
                                        "gte": date_start_time,
                                        "lte": date_end_time,
                                    }
                                }
                            }
                        ]
                    }
                },
                "aggs": {
                    "by_hostname": {
                        "terms": {"field": "hostname", "size": 100},
                        "aggs": {
                            "bounced_emails": {
                                "filter": {"term": {"status": "bounced"}},
                            },
                        },
                    }
                },
            }

            # Fetch data using predefined query
            response_all_emails = es.search(index=ES_INDEX_ALL_EMAILS, body=query_all_emails)
            # Parse sender scores from a remote URL
            df_senderscore = fetch_and_parse_senderscore_data(SENDERSCORE_URL)

            # Process responses into DataFrames and merge them
            buckets_sent = response_all_emails["aggregations"]["by_hostname"]["buckets"]
            df_sent = pd.DataFrame.from_records(buckets_sent)
            df_sent.rename(columns={"doc_count": "sent", "key": "sender_host_name"}, inplace=True)
            df_sent.set_index("sender_host_name", inplace=True)

            df_sent["bounced"] = df_sent["bounced_emails"].apply(lambda x: x["doc_count"])
            df_sent.drop(columns=["bounced_emails"], inplace=True)

            df_sent["returned"] = 0
            df_combined = df_sent.merge(df_senderscore, on="sender_host_name", how="left")

            # Calculate additional metrics and update DataFrame for database insertion
            df_combined["inbox"] = df_combined["sent"] - df_combined["returned"]
            df_combined["inbox_rate"] = df_combined.apply(
                lambda row: min(max(100 * (row["inbox"] / row["sent"]), -100), 100) if row["sent"] != 0 else None,
                axis=1
            )
            df_combined["bounce_rate"] = df_combined.apply(
                lambda row: min(max(100 * (row["bounced"] / row["sent"]), -100), 100) if row["sent"] != 0 else None,
                axis=1
            )

            # Convert integer columns to nullable integer type, preserving NULLs
            integer_columns = ['inbox', 'returned', 'sender_score']
            for col in integer_columns:
                df_combined[col] = df_combined[col].apply(
                    lambda x: pd.NA if pd.isna(x) else int(x)
                ).astype('Int32')

            df_combined["spam_rate"] = None
            df_combined["not_delivered"] = None
            df_combined["spam"] = None
            df_combined["log_date"] = date.date()

            df_combined.drop(columns=["sent", "bounced"], inplace=True)
            df_combined.reset_index(drop=True, inplace=True)

            context.log.info(
                f"{date.date()}: fetched {len(df_combined)} records from Elasticsearch"
            )

            # Database operations: Save data and send notifications
            if not df_combined.empty:
                DwhOperations.save_to_dwh_copy_method(context, SCHEMA, TABLE_NAME, df=df_combined, destination_db=destination_db)
                context.log.info(
                    f"Data saved for the period {context.resources.globals['reload_date_start']} to {context.resources.globals['reload_date_end']}.\n"
                    f"Total records: {len(df_combined)}."
                )
            else:
                context.log.warning(
                    f"No records to save for {context.resources.globals['reload_date_start']} to {context.resources.globals['reload_date_end']}."
                )
                send_dwh_alert_slack_message(
                    f":warning: *Alert: {SCHEMA}.{TABLE_NAME} Data Issue*\n"
                    f"No records to save for the period {context.resources.globals['reload_date_start']} to {context.resources.globals['reload_date_end']}.\n"
                    f"Please check the data source. <!subteam^S02ETK2JYLF|dwh.analysts>"
                )

        except Exception as e:
            context.log.error(f"Error fetching data: {e}")
            send_dwh_alert_slack_message(
                f":error_alert: *{SCHEMA}.{TABLE_NAME}*\n"
                f"Error fetching data <!subteam^S02ETK2JYLF|dwh.analysts>"
            )
            raise e
    else:
        send_dwh_alert_slack_message(
            f":white_check_mark: *{SCHEMA}.{TABLE_NAME} Update*\n"
            f"> Period: *{context.resources.globals['reload_date_start']}* to *{context.resources.globals['reload_date_end']}*.\n"
            f"> Total records saved: *{len(df_combined)}*."
        )
        context.log.info("Slack notification sent.")


@op(required_resource_keys={"globals"})
def sender_score_delete_old_data_from_dwh(context):
    date_range = pd.date_range(
        start=context.resources.globals["reload_date_start"],
        end=context.resources.globals["reload_date_end"],
    )
    destination_db = context.resources.globals["destination_db"]

    for date in date_range:
        date = date.date()
        try:
            DwhOperations.delete_data_from_dwh_table(context=context,
                                                     schema=SCHEMA,
                                                     table_name=TABLE_NAME,
                                                     date_column=DATE_COLUMN,
                                                     date_start=date,
                                                     destination_db=destination_db)
        except Exception as e:
            context.log.error(f"Error deleting data from DWH: {e}")
            raise e


@job(
    resource_defs={
        "globals": make_values_resource(
            reload_date_start=Field(str, default_value=YESTERDAY_DATE),
            reload_date_end=Field(str, default_value=YESTERDAY_DATE),
            destination_db=Field(str, default_value='both')
        ),
        "io_manager": fs_io_manager.configured(
            {"base_dir": f"{get_io_manager_path()}"}
        ),
    },
    name=generate_job_name(TABLE_NAME),
    description=f"Job for '{SCHEMA}.{TABLE_NAME}': Fetches email data from Elasticsearch indexes "
                f"'{ES_INDEX_ALL_EMAILS}' and senderscore url '{SENDERSCORE_URL}', and stores it in the DWH. "
                f"This job also handles data deletion for the specified date range.",
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
)
def es_sender_score():
    delete = sender_score_delete_old_data_from_dwh()
    sender_score_fetch_and_save_data(delete)
