from dagster import (
    Definitions,
)

from models.aggregation.click_metric.jobs import (
    click_metric_yesterday_date_model
)
from models.aggregation.click_metric.schedules import (
    schedule_click_metric_yesterday_date_model
)


# define all: op, assets, jobs and schedules
defs = Definitions(
    schedules=[
        schedule_click_metric_yesterday_date_model
    ],
    jobs=[
        click_metric_yesterday_date_model
    ],
    sensors=[

    ],
    assets=[
    ],
)
