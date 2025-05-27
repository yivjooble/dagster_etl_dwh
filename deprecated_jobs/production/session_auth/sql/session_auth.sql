SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select 
		id,
		date_diff,
		[date],
		id_session,
		[source],
		flags,
		method,
		data
from session_auth
where date_diff between @dt_begin and @dt_end;
