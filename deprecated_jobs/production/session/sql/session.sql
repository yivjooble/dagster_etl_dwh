SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;


select  
		id,
		date_diff,
		start_date,
		cookie_label,
		ip,
		flags,
		user_agent_hash64,
		id_traf_source,
		ip_cc,
		first_visit_date_diff,
		id_current_traf_source
from session with (nolock)
where date_diff between @dt_begin and @dt_end and flags & 1 = 0 and flags & 4 = 0