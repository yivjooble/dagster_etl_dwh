from dagster import (
    load_assets_from_modules,
)

# DEFINE ASSETS
# imp_api
# from .ga4.ga4_dte import ops
# ga4_dte_assets = load_assets_from_modules([ops])

from .ga4.ga4_general import assets
ga4_general_assets = load_assets_from_modules([assets])

# from .ga4.ga4_landing_page_bounce import assets
# ga4_landing_page_bounce_assets = load_assets_from_modules([assets])