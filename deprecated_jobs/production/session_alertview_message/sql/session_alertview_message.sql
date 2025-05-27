SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select 
		id_alertview,
		id_message,
		date_diff,
		id_session
from session_alertview_message with(nolock)
where date_diff between @dt_begin and @dt_end