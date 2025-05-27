delete
from imp.session
where date_diff < (current_date - '1900-01-01'::date) - 14 and country not in (1, 6, 10, 11) and flags & 64 <> 64;