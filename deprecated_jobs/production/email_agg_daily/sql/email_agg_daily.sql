SET NOCOUNT ON;


declare
    @date_from int = :to_sqlcode_date_int,
    @date_to int = :to_sqlcode_date_int,
    @action_date datetime = :to_sqlcode_date_date;

declare
    @sent_lt1 int,
    @sent_lt8 int

begin
	select @sent_lt1 = count(1)
	from email_sent
	where date_diff >= @date_from and date_diff <= @date_to and letter_type = 1

	select @sent_lt8 = count(1)
	from email_sent
	where date_diff >= @date_from and date_diff <= @date_to and letter_type = 8


	create table #alertview (date_diff int, id bigint, sub_id_alert int, primary key (sub_id_alert, date_diff, id))
	insert into #alertview
	select date_diff, id, sub_id_alert from session_alertview where date_diff >= @date_from and date_diff <= @date_to


	create table #clicks (date_diff int, id bigint, id_alertview bigint, primary key (date_diff, id_alertview, id))
	insert into #clicks
	select date_diff,
			id,
			id_alertview
	from session_click
	where date_diff >= @date_from  and date_diff <= @date_to and id_alertview is not null


	create table #jdps (date_diff int, id bigint, id_click bigint, primary key (date_diff, id_click, id))
	insert into #jdps
	select date_diff,
			id,
			id_click
	from session_jdp
	where date_diff >= @date_from and date_diff <= @date_to and id_click is not null


	create table #jdp_aways (date_diff int, id bigint, id_jdp bigint, id_currency int, click_price money primary key (date_diff, id_jdp, id))
	insert into #jdp_aways
	select date_diff,
			id,
			id_jdp,
			id_currency,
			click_price
	from session_away
	where date_diff >= @date_from and date_diff <= @date_to and id_jdp is not null


	create table #serp_aways (date_diff int, id bigint, id_click bigint, id_currency int, click_price money primary key (date_diff, id_click, id))
	insert into #serp_aways
	select date_diff,
			id,
			id_click,
			id_currency,
			click_price
	from session_away
	where date_diff >= @date_from and date_diff <= @date_to and id_click is not null


	create table #jdps_lt8 (date_diff int, id bigint primary key (date_diff, id))
	insert into #jdps_lt8
		select date_diff,
				id
		from session_jdp
		where date_diff >= @date_from and date_diff <= @date_to and letter_type = 8


	create table #aways_lt8 (date_diff int, id bigint, id_currency int, click_price money primary key (date_diff, id))
	insert into #aways_lt8
	select date_diff,
			id,
			id_currency,
			click_price
	from session_away
	where date_diff >= @date_from and date_diff <= @date_to and letter_type = 8


	create table #jdp_aways_lt8 (date_diff int, id bigint, id_currency int, click_price money primary key (date_diff, id))
	insert into #jdp_aways_lt8
	select a.date_diff,
			a.id,
			id_currency,
			click_price
	from session_away a
	inner join #jdps_lt8 j on a.date_diff = j.date_diff and a.id_jdp = j.id
	where a.date_diff >= @date_from and a.date_diff <= @date_to

	declare @lt8_aways int,
			@lt8_revenue money,
			@lt8_jdps int,
			@lt8_jdp_aways int,
			@lt8_jdp_away_revenue money
	select @lt8_jdps = count(1)
	from #jdps_lt8

	select @lt8_jdp_aways = count(1),
			@lt8_jdp_away_revenue = isnull(sum(isnull(A.click_price * C.value_to_usd, 0)), 0)
	from #jdp_aways_lt8 A
	left join info_currency C on C.id = A.id_currency

	select @lt8_aways = count(1),
			@lt8_revenue = sum(isnull(A.click_price * C.value_to_usd, 0))
	from #aways_lt8 A
	left join info_currency C on C.id = A.id_currency

	select 
			cast(@action_date as datetime) as action_date,
			count(distinct SA.id) as alertview_cnt,
			count(distinct SC.id) as click_cnt,
			count(distinct JDP.id) as jdp_cnt,
			count(distinct jdp_away.id) as jdp_away_cnt,
			sum(isnull(jdp_away.click_price, 0) * jdp_away_currency.value_to_usd) as jdp_away_revenue,
			count(distinct serp_away.id) as serp_away_cnt,
			sum(isnull(serp_away.click_price, 0) * serp_away_currency.value_to_usd) as serp_away_revenue,
			@lt8_aways as lt8_away,
			@lt8_revenue as lt8_revenue,
			@lt8_jdps as lt8_jdp,
			@lt8_jdp_aways as lt8_jdp_away,
			@lt8_jdp_away_revenue as lt8_jdp_away_revenue,
			@sent_lt1 as lt1_sent,
			@sent_lt8 as lt8_sent
	from #alertview SA
	left join #clicks SC on SC.date_diff = SA.date_diff and SC.id_alertview = SA.id
	left join #jdps JDP on JDP.date_diff = SC.date_diff and JDP.id_click = SC.id
	left join #jdp_aways jdp_away on jdp_away.date_diff = jdp.date_diff and jdp_away.id_jdp = jdp.id
	left join info_currency jdp_away_currency on jdp_away_currency.id = jdp_away.id_currency
	left join #serp_aways serp_away on serp_away.date_diff = SC.date_diff and serp_away.id_click = SC.id
	left join info_currency serp_away_currency on serp_away_currency.id = serp_away.id_currency


	drop table #alertview
	drop table #clicks
	drop table #jdps
	drop table #jdp_aways
	drop table #serp_aways
	drop table #jdps_lt8
	drop table #aways_lt8
	drop table #jdp_aways_lt8


end