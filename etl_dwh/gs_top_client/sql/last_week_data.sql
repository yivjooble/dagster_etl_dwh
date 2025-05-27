with revenue as (Select countries.alpha_2                 as country,
                        auction_click_statistic_analytics.id_user,
                        auction_click_statistic_analytics.site,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between current_date - 7 and current_date - 1
                                    then total_value end) as last_week_revenue_usd,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between current_date - 14 and current_date - 8
                                    then total_value end) as previous_week_revenue_usd,

                        sum(case
                                when date(auction_click_statistic_analytics.date) between current_date - 7 and current_date - 1
                                    then click_count end) as last_week_click_cnt,
                        sum(case
                                when date(auction_click_statistic_analytics.date) between current_date - 14 and current_date - 8
                                    then click_count end) as previous_week_click_cnt
                 from aggregation.auction_click_statistic_analytics
                          join dimension.countries on auction_click_statistic_analytics.country_id = countries.id
                 where date(date) >= current_date - 14
                 group by countries.alpha_2,
                          auction_click_statistic_analytics.id_user,
                          auction_click_statistic_analytics.site),
     budget as (Select v_budget_and_revenue.country,
                       v_budget_and_revenue.id_user,
                       v_budget_and_revenue.site,
                       v_budget_and_revenue.date,
                       v_budget_and_revenue.revenue_usd,
                       v_budget_and_revenue.potential_revenue_usd,
                       v_budget_and_revenue.click_cnt,
                       case
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
                           else v_budget_and_revenue.user_budget_month_usd end as budget,
                       v_budget_and_revenue.sale_manager
                from aggregation.v_budget_and_revenue
                where date >= current_date - 31),
     periods as (Select budget.country,
                        budget.id_user,
                        budget.site,
                        last_week_revenue_usd,
                        previous_week_revenue_usd,

                        last_week_click_cnt,
                        previous_week_click_cnt,

                        budget.budget                     as last_week_budget,
                        budget_previous_week.budget       as previous_week_budget,

                        budget.revenue_usd                as current_month_revenue_usd,
                        case
                            when coalesce(budget.budget, 0) = 0 then budget.potential_revenue_usd
                            when budget.budget > budget.potential_revenue_usd then budget.potential_revenue_usd
                            else budget.budget end        as current_month_potential_revenue_usd,
                        budget_previous_month.revenue_usd as previous_month_revenue_usd,
                        budget.sale_manager,
                        current_date - 1                  as last_date
                 from revenue
                          left join budget
                                    on budget.country = revenue.country
                                        and budget.id_user = revenue.id_user
                                        and budget.site = revenue.site
                                        and budget.date = current_date - 1
                          left join budget budget_previous_week
                                    on budget.country = budget_previous_week.country
                                        and budget.id_user = budget_previous_week.id_user
                                        and budget.site = budget_previous_week.site
                                        and budget_previous_week.date = current_date - 8
                          left join budget budget_2_previous_week
                                    on budget.country = budget_2_previous_week.country
                                        and budget.id_user = budget_2_previous_week.id_user
                                        and budget.site = budget_2_previous_week.site
                                        and budget_2_previous_week.date = current_date - 14
                          left join budget budget_previous_month
                                    on budget.country = budget_previous_month.country
                                        and budget.id_user = budget_previous_month.id_user
                                        and budget.site = budget_previous_month.site
                                        and budget_previous_month.date =
                                            make_date(date_part('year'::text, CURRENT_DATE - 1)::integer,
                                                      date_part('month'::text, CURRENT_DATE - 1)::integer, 1) - 1)
Select periods.country::text                                                                  as country,
       periods.id_user::text                                                                  as id_user,
       periods.site::text                                                                     as site,
       round(periods.last_week_revenue_usd)::text                                       as last_week_revenue_usd,
       round((periods.last_week_revenue_usd - periods.previous_week_revenue_usd))::text as diff_with_previous_week_revenue_usd,
       periods.last_week_click_cnt::text                                                as last_week_click_cnt,
       periods.previous_week_click_cnt::text                                            as previous_week_click_cnt,
       round(periods.last_week_budget)::text                                            as last_week_budget,
       round(periods.previous_week_budget)::text                                        as previous_week_budget,
       round(periods.current_month_revenue_usd)::text                                   as current_month_revenue_usd,
       round(periods.current_month_potential_revenue_usd)::text                         as current_month_potential_revenue_usd,
       round(periods.previous_month_revenue_usd)::text                                  as previous_month_revenue_usd,
       periods.sale_manager::text                                                             as sale_manager,
       periods.last_date::text                                                          as last_date
from periods
where periods.last_week_revenue_usd is not null
order by periods.last_week_revenue_usd desc
limit 25
;
