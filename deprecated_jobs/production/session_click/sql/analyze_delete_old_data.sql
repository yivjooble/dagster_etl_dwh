delete
from imp.session_click sc
where sc.date_diff < (current_date - '1900-01-01'::date) - 30 and sc.country not in (1, 10, 11)
and sc.id_session not in (select id from imp.session s where s.country = sc.country and s.date_diff = sc.date_diff and s.id = sc.id_session and s.flags & 64 = 64);