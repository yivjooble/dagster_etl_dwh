import requests
from typing import Optional

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)

# module import
from utility_hub import (
    repstat_job_config,
    retry_policy
)
from utility_hub.core_tools import get_creds_from_vault, generate_job_name
from utility_hub.connection_managers import DatabaseConnectionManager
from ..utils.io_manager_path import get_io_manager_path

# Define constants
TABLE_NAME = "currency_source"
SCHEMA = "dwh_test"


def fetch_api_data(context, source: str, endpoint: str, access_key: str, reload_date: Optional[str] = None) -> dict:
    """
    Fetch data from the API for a given source.

    :param source: The base currency source (e.g., 'usd', 'eur').
    :param endpoint: The API endpoint to use.
    :param access_key: Your API access key.
    :return: The JSON response parsed into a dictionary, or None if an error occurs.
    :reload_date: The date to load from API.
    """
    query_url = f"http://apilayer.net/api/{endpoint}?access_key={access_key}&source={source}"
    context.log.info(query_url)

    if reload_date:
        query_url = f"http://apilayer.net/api/historical?access_key={access_key}&source={source}&date={reload_date}"
        context.log.info(query_url)

    try:
        response = requests.get(query_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return None


def build_sql_query(source: str, reload_date: str) -> str:
    """
    Build the SQL upsert query for a given source currency.

    :param source: The source currency to determine the target column (e.g., to_usd, to_eur).
    :return: A formatted SQL query string.
    :reload_date: Date to load.
    """
    sql = f"""
        INSERT INTO {SCHEMA}.{TABLE_NAME} (date, currency, to_{source})
        VALUES (to_timestamp(%s)::date, %s, %s)
        ON CONFLICT (date, currency)
        DO UPDATE SET to_{source} = EXCLUDED.to_{source};
    """

    if reload_date != '':
        sql = f"""
            INSERT INTO {SCHEMA}.{TABLE_NAME} (date, currency, to_{source})
            VALUES (to_timestamp(%s)::date - interval '2 hours', %s, %s)
            ON CONFLICT (date, currency)
            DO UPDATE SET to_{source} = EXCLUDED.to_{source};
        """

    return sql


def process_quotes(context, data: dict, source: str, cursor, connection, reload_date: str):
    """
    Process the quotes data from the API response and perform upserts into the database.

    :param data: The API response data.
    :param source: The base currency used in the API request.
    :param cursor: The database cursor.
    :param connection: The active database connection.
    """
    if "quotes" not in data:
        return

    sql_query = build_sql_query(source, reload_date)
    timestamp = int(data.get("timestamp", 0))

    for full_currency_code, rate in data["quotes"].items():
        context.log.info("Processing currency: %s", full_currency_code)
        currency_code = full_currency_code[-3:]

        try:
            cursor.execute(sql_query, (timestamp, currency_code, rate))
            connection.commit()
        except Exception as e:
            context.log.error("Error processing currency '%s': %s", full_currency_code, e)
            connection.rollback()


@op(retry_policy=retry_policy, required_resource_keys={'globals'})
def currency_source_from_api_save_to_db(context):

    def _load_data_from_api(context, historical_date: Optional[str] = None) -> dict:
        try:
            endpoint = 'live'
            access_key = get_creds_from_vault("APILAYER")
            sources = ['usd', 'eur']
            result = {}

            if historical_date and historical_date != '':
                context.log.info(f'Date: {historical_date}')
                for source in sources:
                    api_data = fetch_api_data(context, source, endpoint, access_key, historical_date)
                    result[source] = api_data

                context.log.info("Data from API was loaded.")
                return result
            elif historical_date == '':
                context.log.info('No date provided, live data from API.')
                for source in sources:
                    api_data = fetch_api_data(context, source, endpoint, access_key)
                    result[source] = api_data

                context.log.info("Data from API was loaded.")
                return result

        except Exception as exc:
            context.log.error(f"{exc}")


    def _process(conn_params: dict, df: dict, reload_date: str):
        connection = DatabaseConnectionManager().get_psycopg2_connection(**conn_params)
        with connection as conn:
            with conn.cursor() as cur:
                for source, data in df.items():
                    if data is not None:
                        process_quotes(context, data, source, cur, conn, reload_date)
                cur.close()

    destination_db = context.resources.globals["destination_db"]
    reload_date = context.resources.globals["reload_date"]

    result_data = _load_data_from_api(context=context, historical_date=reload_date)

    if destination_db == "both":
        # dwh
        _process({}, result_data, reload_date)

        # cloudberry
        _process({
            "credential_key": "cloudberry",
            "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
            "database": 'an_dwh'
        }, result_data, reload_date)
        context.log.info(f"Done processing all currencies, saved to {destination_db}.")

    elif destination_db == "dwh":
        # dwh
        _process({}, result_data, reload_date)
        context.log.info(f"Done processing all currencies, saved to {destination_db}.")

    elif destination_db == "cloudberry":
        # cloudberry
        _process({
            "credential_key": "cloudberry",
            "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
            "database": 'an_dwh'
        }, result_data, reload_date)
        context.log.info(f"Done processing all currencies, saved to {destination_db}.")


@job(
    config=repstat_job_config,
    resource_defs={"globals": make_values_resource(reload_date=Field(str,
                                                                     default_value='',
                                                                     description='Live data from API if '' passed (default), historical data if some date is passed.'),
                                                   destination_db=Field(str,
                                                                        default_value='both',
                                                                        description='Postgres dwh OR cloudberry db.')),
                   "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
    name=generate_job_name(TABLE_NAME, '_from_api'),
    tags={"data_model": f"{SCHEMA}"},
    metadata={
        "destination_db": "dwh, cloudberry, both",
        "target_table": f"{SCHEMA}.{TABLE_NAME}",
    },
    description=f'{SCHEMA}.{TABLE_NAME}',
)
def currency_source_from_api_job():
    currency_source_from_api_save_to_db()
