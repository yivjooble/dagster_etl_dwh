with base as (
    select
        partner,
        date::date,
        coalesce(sub_source, 'undefined')::text as sub_source,
        target_action,
        sum(click_cnt) as click_cnt,
        sum(conversion_cnt) as conversion_cnt,
        case
            when sum(click_cnt) = 0
            then 0
            else round(sum(conversion_cnt) / sum(click_cnt), 4) * 100
        end as conversion_rate,
        sum(benchmark_click_cnt) as benchmark_click_cnt,
        sum(benchmark_conversion_cnt) as benchmark_conversion_cnt
    from affiliate.get_conversion_report_v2()
    where
        date >= (NOW() - interval '1 day')::date
        and date::date < NOW()::date
        and lower(partner) like 'jobget%'
        and sub_source <> 'Total by date'
    group by
        date,
        coalesce(sub_source, 'undefined'),
        partner,
        target_action
), apply_target as (
    select *
    from base
    where target_action = 'Apply'
), click_apply_target as (
    select *
    from base
    where target_action = 'Click apply'
)
select coalesce(atarget.partner, catarget.partner)       as partner,
       coalesce(atarget.date, catarget.date)::varchar    as date,
       coalesce(atarget.sub_source, catarget.sub_source) as sub_source,

       coalesce(atarget.click_cnt, 0)                    as a_click_cnt,
       coalesce(atarget.conversion_cnt, 0)               as a_conversion_cnt,
       coalesce(atarget.conversion_rate, 0)              as a_conversion_rate,
       coalesce(atarget.benchmark_click_cnt, 0)          as a_benchmark_click_cnt,
       coalesce(atarget.benchmark_conversion_cnt, 0)     as a_benchmark_conversion_cnt,

       coalesce(catarget.click_cnt, 0)                   as ca_click_cnt,
       coalesce(catarget.conversion_cnt, 0)              as ca_conversion_cnt,
       coalesce(catarget.conversion_rate, 0)             as ca_conversion_rate,
       coalesce(catarget.benchmark_click_cnt, 0)         as ca_benchmark_click_cnt,
       coalesce(catarget.benchmark_conversion_cnt, 0)    as ca_benchmark_conversion_cnt
from apply_target atarget
full outer join click_apply_target catarget
on
    atarget.partner = catarget.partner
    and atarget.date = catarget.date
    and atarget.sub_source = catarget.sub_source