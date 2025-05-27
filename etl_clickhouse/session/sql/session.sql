select id,
       date_diff,
       start_date,
       cookie_label,
       anonymized_ip as ip,
       flags,
       user_agent_hash64,
       id_traf_source,
       ip_cc,
       first_visit_date_diff,
       id_current_traf_source,
       session_create_page_type
from public.session
where date_diff = %(date)s
;