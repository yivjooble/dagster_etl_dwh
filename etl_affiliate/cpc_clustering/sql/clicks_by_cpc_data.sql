with
    t as (
        select
            co.alpha_2                                                              as country,
            acs.country_id,
            (total_value / coalesce(click_count, 1)::numeric)                       as cpc_usd,
            (click_count - coalesce(test_count, 0) - coalesce(duplicated_count, 0)) as click_count,
            total_value
        from
            aggregation.auction_click_statistic_analytics acs
            join dimension.countries co
                 on co.id = acs.country_id
            join dimension.info_project ip
                 on acs.country_id = ip.country
                     and acs.id_project = ip.id
                     and not ip.hide_in_search
        where
              acs.date::date between '{date_start}'::date and '{date_end}'::date
          and click_price > 0
    )
select
    country,
    country_id,
    round((total_value + coalesce(- (total_value - click_count::numeric * cpc_usd), 0::numeric)) /
    click_count, 4)                                                                           as click_price_usd,
    sum(click_count)                                                                          as click_count,
    sum(total_value + coalesce(- (total_value - click_count::numeric * cpc_usd), 0::numeric)) as revenue_usd
from
    t
where
    click_count > 0
group by
    country,
    country_id,
    round((total_value + coalesce(- (total_value - click_count::numeric * cpc_usd), 0::numeric)) / click_count, 4)
order by 1, 3
