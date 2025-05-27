select  country_code,
        country_id,
        query_request_date_diff,
        token_type,
        q_kw,
        q_region,
        free_cv_query_request_cnt,
        free_cv_query_with_away_cnt,
        free_cv_revenue,
        free_cv_away_cnt
from an.freecv_main_metrics_api
;