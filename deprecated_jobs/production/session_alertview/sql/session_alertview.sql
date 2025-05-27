SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;

select
		id,
		date_diff,
		id_session,
		[date],
		results_total,
		results_on_page,
		service_flags,
		elastic_query_time_ms,
		sub_id_alert,
		sub_date_from,
		sub_date_to,
		visible_results_count,
		showmore_pressed,
		elastic_search_time_ms,
		email_test_id,
		email_test_group,
		elastic_took_time_ms
from session_alertview with (nolock)
where date_diff between @dt_begin and @dt_end