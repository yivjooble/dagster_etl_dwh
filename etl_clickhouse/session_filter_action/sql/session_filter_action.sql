select id,
       id_search_prev,
       date_diff,
       date_time,
       action,
       filter_type,
       filter_value,
       id_search,
       filter_config
from public.session_filter_action
where date_diff = %(date)s
;