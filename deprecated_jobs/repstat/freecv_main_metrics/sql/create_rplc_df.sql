select  country_code,
        country_id,
        date_diff,
        is_bot,
        is_returned,
        device_type_id,
        is_local,
        session_create_page_type,
        id_traffic_source,
        id_current_traffic_source,
        q_kw,
        q_region,
        token_type,
        session_cnt,
        users_cnt,
        free_cv_revenue,
        free_cv_away_cnt
from an.freecv_main_metrics
;