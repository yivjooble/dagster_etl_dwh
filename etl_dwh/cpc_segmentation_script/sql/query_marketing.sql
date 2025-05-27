select
    country,
    sum(cost_usd) / sum(clicks) as marketing_CPC
from aggregation.v_paid_traffic_metrics_agg
where
    date between '{date_start}'::date and '{date_end}'::date
group by country
having sum(cost_usd) > 0 and sum(clicks) > 0