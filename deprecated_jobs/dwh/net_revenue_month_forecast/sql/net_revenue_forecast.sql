Select cast(dv_revenue_by_placement_and_src.date as date) as date,
       sum(revenue_usd) - sum(cost_usd)                   as income
from ono.dv_revenue_by_placement_and_src
where dv_revenue_by_placement_and_src.date between current_date - 290 and current_date - 1
group by cast(dv_revenue_by_placement_and_src.date as date);