from dagster import (
    load_assets_from_modules,
)

# DEFINE ASSETS
# outreach
from .outreach.outreach_kpi_current import assets as kpi_current_assets_import
from .outreach.outreach_kpi_closed import assets as outreach_kpi_closed_assets_import
outreach_kpi_current_assets = load_assets_from_modules([kpi_current_assets_import])
outreach_kpi_closed_assets = load_assets_from_modules([outreach_kpi_closed_assets_import])