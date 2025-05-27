
from utility_hub.core_tools import define_schedule
from models.aggregation.click_metric.jobs import click_metric_yesterday_date_model


schedule_click_metric_yesterday_date_model = define_schedule(
    job=click_metric_yesterday_date_model,
    cron_schedule=['0 07 * * *', '30 11 * * *']
)
