delete
from imp.session_away_message
where date_diff < (current_date - '1900-01-01'::date) - 180;