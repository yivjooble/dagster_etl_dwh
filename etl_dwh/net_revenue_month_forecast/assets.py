import pandas as pd
from sqlalchemy import text
from prophet import Prophet
from dagster import asset, get_dagster_logger
import holidays

from utility_hub.core_tools import fetch_gitlab_data
from utility_hub import DwhOperations
from ..utils.dwh_connections import dwh_conn_sqlalchemy

TABLE_NAME = 'net_revenue_month_forecast_new'
SCHEMA = 'ono'
CLOUDBERRY_TABLE_NAME = 'net_revenue_month_forecast'
CLOUDBERRY_SCHEMA = 'aggregation'

GITLAB_SQL_Q, GITLAB_SQL_URL = fetch_gitlab_data(
    config_key="default",
    dir_name=SCHEMA,
    file_name=TABLE_NAME,
)


@asset(group_name='net_revenue_month_forecast_new_assets',
       key_prefix='net_revenue_month_forecast_new_assets',
       compute_kind='sql')
def net_revenue_load_data() -> pd.DataFrame:
    """
    Load data from the database.
    """
    logger = get_dagster_logger()
    logger.info("Loading data from the database.")
    query = GITLAB_SQL_Q
    df = pd.read_sql_query(text(query), con=dwh_conn_sqlalchemy())
    df = df.rename(columns={'date': 'ds', 'income': 'y'})
    return df


@asset(group_name='net_revenue_month_forecast_new_assets',
       key_prefix='net_revenue_month_forecast_new_assets')
def net_revenue_get_unique_channels(net_revenue_load_data):
    """
    Extract unique channels from the dataframe.
    """
    df = net_revenue_load_data
    return df['channel'].unique()


def create_holidays():
    """
    Create a DataFrame of holidays for Germany (DE).
    """
    de_holidays = holidays.Germany(years=[2023, 2024])
    holidays_df = pd.DataFrame(list(de_holidays.items()), columns=['ds', 'holiday'])
    holidays_df['ds'] = pd.to_datetime(holidays_df['ds'])
    return holidays_df


@asset(group_name='net_revenue_month_forecast_new_assets',
       key_prefix='net_revenue_month_forecast_new_assets')
def net_revenue_prepare_forecasts(net_revenue_get_unique_channels, net_revenue_load_data) -> pd.DataFrame:
    """
    Prepare forecasts for each channel and consolidate them into a single DataFrame.
    """
    logger = get_dagster_logger()
    all_forecasts = pd.DataFrame()
    df = net_revenue_load_data
    channels = net_revenue_get_unique_channels

    for channel in channels:
        logger.info(f"Processing channel: {channel}")
        channel_df = df[df['channel'] == channel].drop('channel', axis=1)
        holidays_df = create_holidays()

        m = Prophet(weekly_seasonality=True, holidays=holidays_df)
        m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        m.fit(channel_df)

        future = m.make_future_dataframe(periods=35)
        forecast = m.predict(future)
        forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(35)
        forecast['channel'] = channel

        all_forecasts = pd.concat([all_forecasts, forecast], ignore_index=True)

    all_forecasts = all_forecasts[['channel', 'ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    return all_forecasts


@asset(group_name='net_revenue_month_forecast_new_assets',
       key_prefix='net_revenue_month_forecast_new_assets')
def net_revenue_save_forecasts_to_db(context, net_revenue_prepare_forecasts):
    """
    Save the consolidated forecasts to db.
    """
    logger = get_dagster_logger()
    res_df = net_revenue_prepare_forecasts
    # add current datetime to the dataframe
    res_df['created_at'] = pd.Timestamp.now()

    # Postgres
    DwhOperations.save_to_dwh_pandas(
        context=context,
        df=res_df,
        schema=SCHEMA,
        table_name=TABLE_NAME,
        destination_db="dwh"
    )

    # Cloudberry
    DwhOperations.save_to_dwh_pandas(
        context=context,
        df=res_df,
        schema=CLOUDBERRY_SCHEMA,
        table_name=CLOUDBERRY_TABLE_NAME,
        destination_db="cloudberry"
    )

    logger.info(f"Saved forecasts to {SCHEMA}.{TABLE_NAME}")
