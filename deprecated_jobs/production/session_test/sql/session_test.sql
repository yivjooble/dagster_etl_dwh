SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select  
		id_session,
		id_test,
		[group],
		date_diff,
		iteration
from session_test with (nolock)
where date_diff between @dt_begin and @dt_end