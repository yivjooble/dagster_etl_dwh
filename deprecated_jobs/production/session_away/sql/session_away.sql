SET NOCOUNT ON;


declare @dt_begin int = :to_sqlcode_date_or_datediff_start,
		@dt_end int = :to_sqlcode_date_or_datediff_end;


select  
		id,
		date_diff,
		id_session,
		uid_job,
		id_project,
		cast(date as datetime) as date,
		flags,
		id_click,
		id_jdp,
		id_click_no_serp,
		base_score,
		score,
		base_rel_bonus,
		rel_bonus,
		id_campaign,
		click_price,
		id_currency,
		id_cdp,
		letter_type,
        id_job,
        serp_click_value,
        id_account
from session_away with(nolock)
where date_diff between @dt_begin and @dt_end