SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_int,
		@dt_end int = :to_sqlcode_date_int;

select 
	   id_account,
	   date_diff,
	   [date],
	   id_session
from session_account with (nolock)
where date_diff between @dt_begin and @dt_end
;