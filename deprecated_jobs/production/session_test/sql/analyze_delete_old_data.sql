delete
from imp.session_test
where (date_diff < (current_date - '1900-01-01'::date) - 14 and country not in (1, 10)) or (date_diff < (current_date - '1900-01-01'::date) - 30 and country in (1, 10));