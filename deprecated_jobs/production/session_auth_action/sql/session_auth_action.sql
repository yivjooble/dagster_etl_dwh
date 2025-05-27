SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select  
		id,
		date_diff,
		[date],
		id_auth,
		[type],
		flags,
		[data],
		id_account,
		screen
from dbo.session_auth_action
where date_diff between @dt_begin and @dt_end;
