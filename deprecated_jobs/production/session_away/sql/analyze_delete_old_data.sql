delete
from imp.session_away sa
where sa.date_diff < (current_date - '1900-01-01'::date) - 180
and sa.id_session not in (select id from imp.session s where s.country = sa.country and s.date_diff = sa.date_diff and s.id = sa.id_session and s.flags & 64 = 64);