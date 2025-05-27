from dagster import (
    load_assets_from_modules,
)

# DEFINE ASSETS
# etl_dwh
from .forecast_revenue import assets as forecast_revenue_assest
from .net_revenue_month_forecast import assets as net_revenue_month_forecast_assest


forecast_revenue_assets = load_assets_from_modules([forecast_revenue_assest])
net_revenue_month_forecast_assets = load_assets_from_modules([net_revenue_month_forecast_assest])
