WITH revenue AS (SELECT LOWER(c.alpha_2)                               AS country,
                        cd.id_project,
                        cd.id_campaign,
                        cd.revenue_usd / cd.certified_client_click_cnt AS click_price_usd,
                        cd.certified_client_click_cnt,
                        CASE
                            WHEN uts.is_paid THEN cd.certified_client_click_cnt
                            ELSE 0
                        END                                            AS paid_click_count,
                        cd.revenue_usd
                 FROM aggregation.click_data_agg cd
                          LEFT JOIN dimension.u_traffic_source uts
                                    ON cd.country_id = uts.country
                                        AND cd.id_current_traf_source = uts.id
                          LEFT JOIN dimension.countries c
                                    ON cd.country_id = c.id
                 WHERE cd.action_datediff BETWEEN
                     ('{date_start}' - DATE '1900-01-01') AND ('{date_end}' - DATE '1900-01-01')
                   AND cd.certified_client_click_cnt > 0
                   AND cd.revenue_usd > 0)
SELECT country,
       id_project,
       id_campaign,
       click_price_usd,
       SUM(certified_client_click_cnt) AS click_count,
       SUM(paid_click_count)           AS paid_click_count,
       SUM(revenue_usd)                AS revenue_usd
FROM revenue
GROUP BY country,
         id_project,
         id_campaign,
         click_price_usd;