with
    date as (
        Select
            make_date(cast(EXTRACT( year from  make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), cast(EXTRACT(month from make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), 1) as last_month_start,
            make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1   as last_month_end,
            make_date( cast(EXTRACT( year from  make_date(cast(EXTRACT( year from  make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), cast(EXTRACT(month from make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), 1)-1) as int), cast(EXTRACT(month from make_date(cast(EXTRACT( year from  make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), cast(EXTRACT(month from make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), 1)-1) as int), 1)   as previous_month_start,
            make_date(cast(EXTRACT( year from  make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), cast(EXTRACT(month from make_date( cast(EXTRACT( year from  current_date - 1) as int), cast(EXTRACT(month from current_date - 1) as int), 1) -1) as int), 1)-1 as previous_month_end

    ),
    revenue as (Select  --countries.alpha_2                 as country,
                     --   auction_click_statistic_analytics.id_user,
                        auction_click_statistic_analytics.site,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between (select last_month_start from date) and (select last_month_end from date)
                                    then total_value end) as last_month_revenue_usd,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between (select previous_month_start from date) and (select previous_month_end from date)
                                    then total_value end) as previous_month_revenue_usd,

                        sum(case
                                when date(auction_click_statistic_analytics.date) between (select last_month_start from date) and (select last_month_end from date)
                                    then click_count end) as last_month_click_cnt,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between (select previous_month_start from date) and (select previous_month_end from date)
                                    then click_count end) as previous_month_click_cnt
                 from aggregation.auction_click_statistic_analytics
                          join dimension.countries on auction_click_statistic_analytics.country_id = countries.id
                 where date(date) >= (select previous_month_start from date)
                 group by --countries.alpha_2,
                         -- auction_click_statistic_analytics.id_user,
                          auction_click_statistic_analytics.site
                 ),
     budget as (Select --v_budget_and_revenue.country,
                      -- v_budget_and_revenue.id_user,
                       v_budget_and_revenue.site,
                       v_budget_and_revenue.date,
                       sum(v_budget_and_revenue.revenue_usd) as revenue_usd,
                       sum(v_budget_and_revenue.potential_revenue_usd) as revenue_usd,
                       sum(v_budget_and_revenue.click_cnt) as click_cnt,
                       sum(case
                           when v_budget_and_revenue.campaign_cnt = v_budget_and_revenue.campaign_with_budget_cnt and
                                v_budget_and_revenue.campaign_budget_month_usd <
                                v_budget_and_revenue.user_budget_month_usd
                               then v_budget_and_revenue.campaign_budget_month_usd
                           when v_budget_and_revenue.campaign_budget_month_usd > 0 and
                                v_budget_and_revenue.campaign_budget_month_usd <
                                v_budget_and_revenue.user_budget_month_usd
                               then v_budget_and_revenue.campaign_budget_month_usd
                           when v_budget_and_revenue.is_unlim_cmp = 1 and v_budget_and_revenue.user_budget_month_usd >
                                                                          v_budget_and_revenue.campaign_budget_month_usd
                               then v_budget_and_revenue.user_budget_month_usd
                           when v_budget_and_revenue.user_budget_month_usd = 0
                               then v_budget_and_revenue.campaign_budget_month_usd
                           else v_budget_and_revenue.user_budget_month_usd end) as budget,
                       v_budget_and_revenue.sale_manager
                from aggregation.v_budget_and_revenue
                where date >= (select previous_month_start from date)
                group by v_budget_and_revenue.site,
                         v_budget_and_revenue.date,
                         v_budget_and_revenue.sale_manager
                ),
     periods as (Select --budget.country,
                        --budget.id_user,
                        budget.site,
                        last_month_revenue_usd,
                        previous_month_revenue_usd,

                        last_month_click_cnt,
                        previous_month_click_cnt,

                        budget.budget                      as last_week_budget,
                        budget_previous_month.budget       as previous_week_budget,

                        budget.sale_manager,
                        (select last_month_end from date)                 as last_date
                 from revenue
                          left join budget
                                    on --budget.country = revenue.country
                                        --and budget.id_user = revenue.id_user
                                         budget.site = revenue.site
                                        and budget.date = (select last_month_end from date)
                          left join budget budget_previous_month
                                    on --budget.country = budget_previous_month.country
                                        --and budget.id_user = budget_previous_month.id_user
                                         budget.site = budget_previous_month.site
                                        and budget_previous_month.date = (select previous_month_end from date)
                     )
Select null                                                                   as country,
       null                                                                   as id_user,
       periods.site                                                                     as site,
       round(periods.last_month_revenue_usd)::text                                       as last_month_revenue_usd,
       round((periods.last_month_revenue_usd - periods.previous_month_revenue_usd))::text as diff_with_previous_month_revenue_usd,
       periods.last_month_click_cnt::text                                                as last_month_click_cnt,
       periods.previous_month_click_cnt::text                                            as previous_month_click_cnt,
       round(periods.last_week_budget)::text                                            as last_month_budget,
       round(periods.previous_week_budget)::text                                        as previous_month_budget,
       round(periods.previous_month_revenue_usd)::text                                  as previous_month_revenue_usd,
       periods.sale_manager                                                             as sale_manager,
       periods.last_date::text                                                          as last_date
from periods
where periods.last_month_revenue_usd is not null
order by periods.last_month_revenue_usd desc
limit 15
;
