select t.*, c.id as country_id
from aggregation.cpc_cluster_affiliate_price t
join dimension.countries c on c.alpha_2 = t.country
where date = '{target_date}'
