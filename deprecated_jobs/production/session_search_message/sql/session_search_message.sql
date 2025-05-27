SET NOCOUNT ON;

declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select  
		id_search,
		id_session,
		date_diff,
		id_message
from session_search_message with(nolock)
where date_diff between @dt_begin and @dt_end
