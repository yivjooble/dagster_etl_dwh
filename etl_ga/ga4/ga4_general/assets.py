# python modules
from datetime import datetime, timedelta
from copy import deepcopy
import pandas as pd

from dagster import asset, make_values_resource, Field

# custom modules
from .config import (
    country_domain_to_country_id,
    country_domain_to_property_id,
    request_params,
)

from ..logs_messages_tools.messages import send_dwh_alert_slack_message
from .etl_ga_functions import (
    get_session_custom_channel_group,
    get_report,
    get_request_body,
    report_to_dataframe,
)

from utility_hub import DwhOperations
from .config import analytics


YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime("%Y-%m-%d")
DAY_BEFORE_YESTERDAY = (datetime.now().date() - timedelta(2)).strftime("%Y-%m-%d")

globals_resource = {
    "globals": make_values_resource(
        reload_date_end=Field(str, default_value=YESTERDAY_DATE),
        reload_date_start=Field(str, default_value=DAY_BEFORE_YESTERDAY),
        destination_db=Field(str, default_value="both"),
    ),
}


def ga4_general_request_handler(context, date_start, date_end, destination_db):
    try:
        context.log.info("Connected to GA4 service")

        table_names = request_params.keys()

        for table_name in table_names:
            context.log.info(
                f"====== [{table_name}] started - parse date: {date_start}/-/{date_end}"
            )

            dataframes = {}
            rows_cnt_acc = 0

            for domain in country_domain_to_property_id:
                # Retrieve the property ID for the current domain
                property_id = country_domain_to_property_id.get(domain)
                # Get custom channel group for the property
                custom_channel_group = get_session_custom_channel_group(
                    property_id, domain
                )

                # Create a copy of request_params for each domain
                domain_request_params = deepcopy(request_params)

                if custom_channel_group:
                    for params in domain_request_params.values():
                        for dimension in params.get("dimensions", []):
                            if dimension.get("name") == "sessionDefaultChannelGrouping":
                                dimension.update(custom_channel_group)

                # get report for every domain
                request_body = get_request_body(
                    table_name=table_name,
                    requests_params=domain_request_params,
                    date_start=date_start,
                    date_end=date_end,
                )

                report = get_report(property_id, analytics, body=request_body)

                # json.dump(report, open(f'logs/{table_name}_{domain}_{parse_date}.json', 'w'), indent=4)

                rows_cnt = report.get("reports")[0].get("rowCount") or 0

                rows_cnt_acc += rows_cnt
                context.log.info(f"{domain} for {table_name}: {rows_cnt}")

                df = report_to_dataframe(report, table_name)
                df["country_domain"] = domain
                df["country_id"] = country_domain_to_country_id.get(domain)

                if table_name == "ga4_user_and_adsense_tests":
                    df["bounce_rate"] = float(df["bounce_rate"].iloc[0]) * 100

                dataframes[domain] = df
            # union dataframes for different domains
            result_df = pd.concat(dataframes.values())

            DwhOperations.save_to_dwh_pandas(
                context=context,
                df=result_df,
                schema="imp_api",
                table_name=table_name,
                destination_db=destination_db
            )

            countries_count = int(result_df["country_id"].nunique())
            send_dwh_alert_slack_message(
                f":add: *AGG: {table_name}*\n"
                f">*[{date_start}/-/{date_end}]:* {countries_count} *countries* & {result_df.shape[0]} *rows*"
            )
    except Exception as e:
        context.log.error(f"{e}\n\n")
        send_dwh_alert_slack_message("Error on GA4 integration", e)


@asset(
    group_name="ga4_general",
    key_prefix="ga4_general",
    resource_defs=globals_resource
)
def delete_history_data(context):
    """Delete history data from imp_api.ga4_general and imp_api.ga4_user_and_adsense_tests.
       Deletes data for every day from -2 day to -1 day.
    """
    destination_db = context.resources.globals["destination_db"]
    date_start = context.resources.globals["reload_date_start"]
    date_end = context.resources.globals["reload_date_end"]

    date_range = pd.date_range(
        pd.to_datetime(date_start),
        pd.to_datetime(date_end)
    )

    query1 = "delete from imp_api.ga4_general where action_date = %s;"
    query2 = "delete from imp_api.ga4_user_and_adsense_tests where action_date = %s;"

    for date in date_range:
        operation_date = date.strftime("%Y-%m-%d")

        context.log.info(
            f"Deleting data from imp_api.ga4_general for {operation_date}")
        DwhOperations.execute_on_dwh(
            context,
            query=query1,
            params=(operation_date,),
            destination_db=destination_db
        )

        context.log.info(
            f"Deleting data from imp_api.ga4_user_and_adsense_tests for {operation_date}")
        DwhOperations.execute_on_dwh(
            context,
            query=query2,
            params=(operation_date,),
            destination_db=destination_db
        )


@asset(
    group_name="ga4_general",
    key_prefix="ga4_general",
    resource_defs=globals_resource
)
def ga4_general_api_request_reload_date(context, delete_history_data):
    """Reload data for every day from reload_date_start to reload_date_end"""
    destination_db = context.resources.globals["destination_db"]
    date_start = context.resources.globals["reload_date_start"]
    date_end = context.resources.globals["reload_date_end"]

    date_range = pd.date_range(
        pd.to_datetime(date_start),
        pd.to_datetime(date_end)
    )

    for date in date_range:
        operation_date = date.strftime("%Y-%m-%d")
        ga4_general_request_handler(context, operation_date, operation_date, destination_db)
