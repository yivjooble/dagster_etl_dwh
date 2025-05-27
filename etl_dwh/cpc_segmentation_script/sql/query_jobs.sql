select
    lower(c.alpha_2) as country,
    id_project,
    id_campaign,
    cast(date as date) as date,
    paid_job_count
from aggregation.jobs_stat_daily j
join dimension.countries c
    on c.id = j.id_country
where
    cast(date as date) between '{date_start}'::date and '{date_end}'::date
    and paid_job_count > 0