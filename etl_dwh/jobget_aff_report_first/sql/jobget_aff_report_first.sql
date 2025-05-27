SELECT partner,
       date,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'total' ), 0)                                                 AS total_click_cnt,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'bot' ), 0)            AS bot_click_cnt,
       COALESCE(SUM(click_percent) FILTER ( WHERE click_type = 'bot' ), 0)        AS bot_click_percent,
       COALESCE(SUM(click_cost) FILTER ( WHERE click_type = 'bot' ), 0)           AS bot_click_cost,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'expired' ), 0)        AS expired_non_billable_click_cnt,
       COALESCE(SUM(click_percent) FILTER ( WHERE click_type = 'expired' ), 0)    AS expired_non_billable_click_percent,
       COALESCE(SUM(click_cost) FILTER ( WHERE click_type = 'expired' ), 0)       AS expired_non_billable_click_cost,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'duplicated' ), 0)     AS duplicated_click_cnt,
       COALESCE(SUM(click_percent) FILTER ( WHERE click_type = 'duplicated' ), 0) AS duplicated_click_percent,
       COALESCE(SUM(click_cost) FILTER ( WHERE click_type = 'duplicated' ), 0)    AS duplicated_click_cost,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'foreign' ), 0)        AS foreign_click_cnt,
       COALESCE(SUM(click_percent) FILTER ( WHERE click_type = 'foreign' ), 0)    AS foreign_click_percent,
       COALESCE(SUM(click_cost) FILTER ( WHERE click_type = 'foreign' ), 0)       AS foreign_click_cost,
       COALESCE(SUM(click_cnt) FILTER ( WHERE click_type = 'certified' ), 0)      AS certified_click_cnt,
       COALESCE(SUM(click_cost) FILTER ( WHERE click_type = 'certified' ), 0)     AS certified_revenue
FROM affiliate.v_click_cost_report_v2
WHERE date::date >= (NOW() - interval '1 day')::date
      AND date::date < NOW()::date
      AND LOWER(partner) LIKE 'jobget%'
GROUP BY partner, date;