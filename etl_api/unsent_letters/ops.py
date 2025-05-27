from datetime import datetime, timedelta
import pytz
import pandas as pd
from prometheus_api_client import PrometheusConnect

from etl_api.utils.utils import job_prefix
from etl_api.utils.io_manager_path import get_io_manager_path
from etl_api.utils.api_jobs_config import custom_retry_policy
from utility_hub import (
    DwhOperations,
    all_countries_list,
)
from utility_hub.core_tools import generate_job_name

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
    DynamicOut,
    DynamicOutput
)


SCHEMA = "email"
TABLE_NAME = "unsent_letters"
DATE_COLUMN = "date"
COUNTRY_COLUMN = "country_code"
YESTERDAY_DATE = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
JOB_PREFIX = job_prefix()


prom = PrometheusConnect(url="http://10.0.1.164:8428", disable_ssl=True)


@op(out=DynamicOut(), required_resource_keys={'globals'})
def unsent_letters_get_instance(context):

    tz = pytz.FixedOffset(180)  # timezone offset in minutes

    launch_countries = context.resources.globals["reload_countries"]
    date_range = pd.date_range(pd.to_datetime(context.resources.globals["reload_date_start"]).replace(tzinfo=tz),
                               pd.to_datetime(context.resources.globals["reload_date_end"]).replace(tzinfo=tz)).to_pydatetime()

    context.log.info('Getting instances...\n'
                     f'Selected  countries: {launch_countries}\n'
                     f'Date range: {context.resources.globals["reload_date_start"]} - {context.resources.globals["reload_date_end"]}'
                     )

    for country in launch_countries:
        yield DynamicOutput(
            value={'country': country.strip('_'),
                   'date_range': date_range
                   },
            mapping_key='country_' + country
        )


@op(required_resource_keys={"globals"}, retry_policy=custom_retry_policy)
def unsent_letters_fetch_data(context, instance_country: dict):

    country_code = instance_country["country"]

    for date in instance_country["date_range"]:

        try:
            operation_date = date.strftime('%Y-%m-%d')

            start_time = date
            end_time = start_time + timedelta(days=1) - timedelta(microseconds=1)

            query = f'increase(mail_sender_lt8_alerts_without_jobs{{job="mailsender", country=~"{country_code}"}}[5m])'

            aggregated_data = prom.get_metric_aggregation(
                query=query,
                operations=['sum'],
                start_time=start_time,
                end_time=end_time,
                step='600'
            )

            context.log.debug(f'aggregated_data for [{country_code.upper()}]: {aggregated_data}\n')

            if aggregated_data is not None:
                letter_cnt = aggregated_data['sum'].astype('int64')
            else:
                letter_cnt = 0

            data = {
                'country_code': [country_code],
                'date': [operation_date],
                'letter_type': [8],  # for now there is only one metric for unsent letters >> letter type = 8
                'letter_cnt': [letter_cnt]
            }

            df = pd.DataFrame(data)

            if not df.empty:

                # Delete old data only if data is returned
                DwhOperations.delete_data_from_dwh_table(
                    context=context,
                    schema=SCHEMA,
                    table_name=TABLE_NAME,
                    date_column=DATE_COLUMN,
                    country_column=COUNTRY_COLUMN,
                    date_start=operation_date,
                    country=country_code
                )

                DwhOperations.save_to_dwh_pandas(context=context, df=df, schema=SCHEMA, table_name=TABLE_NAME)

                context.log.info(f'|{country_code.upper()}|: Successfully saved df to dwh.')
            else:
                context.log.warning(
                    f"No records to save for {operation_date}."
                )
        except Exception as e:
            context.log.error(f"Error fetching data: {e}")
            raise e


@job(
    resource_defs={
        "globals": make_values_resource(reload_countries=Field(list, default_value=all_countries_list),
                                        reload_date_start=Field(str, default_value=YESTERDAY_DATE),
                                        reload_date_end=Field(str, default_value=YESTERDAY_DATE)),
        "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})
    },
    name=generate_job_name(TABLE_NAME),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "input_date": f"{YESTERDAY_DATE} - {YESTERDAY_DATE}",
        "destination_db": "dwh",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f"{SCHEMA}.{TABLE_NAME}",
)
def unsent_letters_job():
    instances = unsent_letters_get_instance()
    instances.map(unsent_letters_fetch_data)
