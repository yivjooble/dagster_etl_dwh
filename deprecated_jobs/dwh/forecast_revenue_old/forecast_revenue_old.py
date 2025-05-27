# %%
import warnings
import itertools
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import psycopg2
import logging
import os

from joblib import Parallel, delayed
from urllib import parse, request
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()


logging.basicConfig(filename='logs/forecast_revenue.log',
                    filemode='a',
                    format='%(asctime)s, %(name)s - %(levelname)s >> %(message)s',
                    level=logging.DEBUG)



import datetime
SLACK_MESSAGE_BLOCK = ":python-agg: *_dwh-agg executing logs_* :arrow-down:"
CHANNEL_ID = os.environ.get('DWH_ALERTS_CHANNEL_ID')
CLIENT = WebClient(token=os.environ.get('DWH_ALERTS_TOKEN'))


def get_conversations_history(message_to_search: str):
    response = CLIENT.conversations_history(channel=CHANNEL_ID, limit=50)
    messages = response["messages"]
    latest_ts = None
    for message in messages:
        if f"{message_to_search}" in message["text"]:
            latest_ts = message["ts"]
            break
    return latest_ts


def send_message(message: str, latest_ts=None):
    if latest_ts is None:  
        CLIENT.chat_postMessage(channel=CHANNEL_ID, 
                                text=f"{SLACK_MESSAGE_BLOCK}")
        latest_ts =get_conversations_history(f"{SLACK_MESSAGE_BLOCK}")

        CLIENT.chat_postMessage(channel=CHANNEL_ID,
                                text=f"{message}",
                                thread_ts=latest_ts)
    else:
        CLIENT.chat_postMessage(channel=CHANNEL_ID,
                                text=f"{message}",
                                thread_ts=latest_ts)


def send_dwh_alert_slack_message(message: str):
    latest_ts = get_conversations_history(f"{SLACK_MESSAGE_BLOCK}")
    if latest_ts:
        ts_datetime = datetime.datetime.fromtimestamp(float(latest_ts)).strftime("%Y-%m-%d")
        if ts_datetime != datetime.datetime.today().strftime("%Y-%m-%d"):
            send_message(message)
        else:
            send_message(message, latest_ts)
    else:
        send_message(message)


def fit_model(y, param, param_seasonal):
    try:
        mod = sm.tsa.statespace.SARIMAX(y,
                                        order=param,
                                        seasonal_order=param_seasonal,
                                        enforce_stationarity=False,
                                        enforce_invertibility=False)
        results = mod.fit()
        return [param, param_seasonal, results.aic]
    except:
        return [param, param_seasonal, np.inf]

try:

    logging.info(f"loading started")

    plt.style.use('fivethirtyeight')
    
    engine = create_engine(f"postgresql+psycopg2://{os.environ.get('name')}:{os.environ.get('pass')}@10.0.1.61/postgres")
    logging.info(f"engine created")

    query = """Select cast(dv_revenue_by_placement_and_src.date as date) as date, 
            sum(revenue_usd) as revenue 
            from ono.dv_revenue_by_placement_and_src 
            where dv_revenue_by_placement_and_src.date between current_date - 290 and current_date - 1
            group by cast(dv_revenue_by_placement_and_src.date as date)"""
    df = pd.read_sql_query(text(query), con=engine)
    logging.info(f"created dataframe from [ono.dv_revenue_by_placement_and_src]")

    y = pd.Series(data=df['revenue'].values, index=df['date'])

    # Define the p, d and q parameters to take any value between 0 and 3
    p = d = q = range(0, 3)
    logging.info(f"Define the p, d and q parameters to take any value between 0 and 3")

    # Generate all different combinations of p, q and q triplets
    pdq = list(itertools.product(p, d, q))
    logging.info(f"Generate all different combinations of p, q and q triplets")

    # Generate all different combinations of seasonal p, q and q triplets
    seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]
    logging.info(f"Generate all different combinations of seasonal p, q and q triplets")
    # warnings.filterwarnings("ignore") # specify to ignore warning messages

    # Perform the grid search in parallel
    best_results = Parallel(n_jobs=4)(delayed(fit_model)(y, param, param_seasonal) for param in pdq for param_seasonal in seasonal_pdq)

    # Find the best result
    best_result = min(best_results, key=lambda x: x[2])
                
    # print('\nBest Result:', best_result)
    mod = sm.tsa.statespace.SARIMAX(y,
                                    order=(best_result[0][0], best_result[0][1], best_result[0][1]),
                                    seasonal_order=(best_result[1][0], best_result[1][1], best_result[1][2], best_result[1][3]),
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)

    results = mod.fit(maxiter=1000)  # Increase the maximum number of iterations

    # print(results.summary().tables[1])
    results.plot_diagnostics(figsize=(15, 12))
    # plt.show()
    # Get forecast 33 steps ahead in future
    pred_uc = results.get_forecast(steps=33)
    logging.info(f"Get forecast 33 steps ahead in future")

    # Get confidence intervals of forecasts
    pred_ci = pred_uc.conf_int()
    ax = y.plot(label='Observed', figsize=(15, 12))
    pred_uc.predicted_mean.plot(ax=ax, label='Forecast')
    ax.fill_between(pred_ci.index,
                    pred_ci.iloc[:, 0],
                    pred_ci.iloc[:, 1], color='k', alpha=.25)
    ax.set_xlabel('Date')
    ax.set_ylabel('Revenue')
    logging.info(f"Get confidence intervals of forecasts")

    # plt.legend()
    # plt.show()

    predict_dy_ci = pred_uc.conf_int(alpha=0.05) # defalut alpah=0.05 :returns a 95% confidence interval
    type(predict_dy_ci)


    # Save to dwh
    logging.info(f"Start saving to dwh")
    # 1
    pred_uc.predicted_mean

    # 2
    predict_dy_ci = pred_uc.conf_int(alpha=0.85) # defalut alpah=0.05 :returns a 95% confidence interval

    # result_df_1_row = pred_uc.predicted_mean.to_frame('revenue_forecast').rename_axis('date').reset_index()
    result_df_1_row = pred_uc.predicted_mean.to_frame('revenue_forecast')
    result_df_2_row = predict_dy_ci

    res_df_row = pd.merge(result_df_1_row, result_df_2_row, left_index=True, right_index=True)

    res_df = res_df_row.rename_axis('date').reset_index()


    # truncate table
    conn2 = psycopg2.connect(f"host=10.0.1.61 dbname=postgres user={os.environ.get('name')} password={os.environ.get('pass')}")
    logging.info(f"connected to dwh")

    cur = conn2.cursor()
    cur.execute("delete from aggregation.revenue_month_forecast where date::date >= current_date;")

    conn2.commit()
    conn2.close()
    logging.info(f"deleted (date::date >= current_date)  --> aggregation.revenue_month_forecast")

    table_name = 'revenue_month_forecast'

    # to dwh
    res_df.to_sql(
                    table_name,
                    con=engine,
                    schema='aggregation',
                    index=False,
                    chunksize=10000,
                    if_exists='append'
                )

    logging.info(f"successfully inserted into: {table_name}")
    send_dwh_alert_slack_message(f":done-1: *{table_name}*")

    type(predict_dy_ci)
    predict_dy_ci
except Exception as e:
    logging.warning(f"ERROR: {e}")
    send_dwh_alert_slack_message(f":error_alert: *{table_name}*")


# %%
pred_uc.predicted_mean

# %%



