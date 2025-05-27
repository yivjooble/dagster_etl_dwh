SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select
		id,
		date_diff,
		id_session,
		id_search,
		id_alertview,
		uid_job,
		[date],
		position,
		flags,
		id_project,
		id_job,
		id_campaign,
		id_impression,
		click_price,
		id_currency,
		job_destination,
		id_recommend,
		id_impression_recommend
from session_click with (nolock)
where date_diff between @dt_begin and @dt_end