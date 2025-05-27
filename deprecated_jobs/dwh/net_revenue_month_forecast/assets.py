import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

from dagster import (
    asset
)

from prophet import Prophet
from sqlalchemy import text
from dotenv import load_dotenv
from ..utils.messages import send_dwh_alert_slack_message
from ..utils.dwh_connections import dwh_conn_sqlalchemy, dwh_conn_psycopg2

load_dotenv()

TABLE_NAME = 'net_revenue_month_forecast'
SCHEMA = 'aggregation'


# Define a custom growth rate function
def custom_growth_rate(ds):
    start_date = ds.min()
    end_date = ds.max()
    total_days = (end_date - start_date).days + 1

    # Define the growth pattern based on the specified requirements
    growth_pattern = np.concatenate((np.linspace(0, 1, num=3), np.linspace(1, 0.5, num=total_days-3)))
    return growth_pattern


@asset(group_name='net_revenue_month_forecast',
       key_prefix='net_revenue_month_forecast_assets')
def launch_revenue_model(context) -> pd.DataFrame:
    context.log.info(f"Loading started\n"
                f"Creating dataframe from sql-query")
    plt.style.use('fivethirtyeight')
    
    path_to_sql_file = os.path.join(os.path.dirname(__file__), os.path.join("sql", "net_revenue_forecast.sql"))
    with open(path_to_sql_file, 'r') as q:
        query = q.read()

    df = pd.read_sql_query(text(query), 
                           con=dwh_conn_sqlalchemy())
    context.log.info(f"Created dataframe from [ono.dv_revenue_by_placement_and_src]")

    # Sort the dataframe by date
    context.log.info(f'Sort the dataframe by date')
    df = df.sort_values('date')

    ## PART 1
    # Add the growth rate column to the dataframe
    context.log.info(f'Add the growth rate column to the dataframe')
    df['growth_rate'] = custom_growth_rate(df['date'])

    # Rename the columns as 'ds', 'y', and 'growth' for Prophet
    context.log.info("Rename the columns as 'ds', 'y', and 'growth' for Prophet")
    df.rename(columns={'date': 'ds', 'income': 'y', 'growth_rate': 'growth'}, inplace=True)

    # # Train the Prophet model
    context.log.info(f'Train the model')
    model = Prophet(growth='linear')
    model.add_seasonality(name='monthly', period=30, fourier_order=5, prior_scale=0.02)
    model.fit(df)

    # Make predictions for the next 35 days
    context.log.info("Make predictions for the next 35 days")
    future = model.make_future_dataframe(periods=35)
    future['growth'] = custom_growth_rate(future['ds'])
    predictions = model.predict(future)

    # Extract the predicted values and corresponding dates
    context.log.info("Extract the predicted values and corresponding dates")
    predicted_values = predictions[['ds', 'yhat']].tail(35)

    # Plot the predicted values
    plt.figure(figsize=(10, 6))
    plt.plot(df['ds'], df['y'], label='Actual')
    plt.plot(predicted_values['ds'], predicted_values['yhat'], label='Predicted')
    plt.xlabel('Date')
    plt.ylabel('Income')
    plt.title('Actual vs. Predicted Income')
    plt.legend()
    plt.xticks(rotation=45, ha='right')  # Set rotation and alignment of xticks
    plt.tight_layout()

    return predicted_values


@asset(group_name='net_revenue_month_forecast',
       key_prefix='net_revenue_month_forecast_assets',
       non_argument_deps={'launch_revenue_model'})
def delete_previous_data(context):
    try:
        context.log.info(f"Deleting previous data is started")
        with dwh_conn_psycopg2() as conn2:
            cur = conn2.cursor()
            cur.execute(f"delete from {SCHEMA}.{TABLE_NAME} where ds::date >= current_date;")
            context.log.info(f"Deleted (ds::date >= current_date) from {SCHEMA}.{TABLE_NAME}")
            conn2.commit()
            cur.close()
    except Exception as e:
        context.log.error(f"Error while deleting records: {e}")
        raise e
    

@asset(group_name='net_revenue_month_forecast',
       key_prefix='net_revenue_month_forecast_assets',
       non_argument_deps={'delete_previous_data'})
def save_to_dwh(context, launch_revenue_model: pd.DataFrame):
    # Save to dwh
    context.log.info(f"Start saving to dwh")
    res_df = launch_revenue_model
    res_df.to_sql(
                TABLE_NAME,
                con=dwh_conn_sqlalchemy(),
                schema=SCHEMA,
                index=False,
                chunksize=10000,
                if_exists='append'
                )

    context.log.info(f"Successfully inserted into: {TABLE_NAME}")
    send_dwh_alert_slack_message(f":done-1: *{TABLE_NAME}*")