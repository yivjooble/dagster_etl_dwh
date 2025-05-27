SET NOCOUNT ON;
        

declare @date_diff int = :to_sqlcode_date_int,
		@from_email_click_flag int = 256,
		@from_external_click_flag int = 512,
		@date date = :to_sqlcode_date_date_start;



-- can leave only 1 and 5 if have some problem
select max(account_sessions.id_account) as id_account,
       account_sessions.id_session
into #all_session_account
from (select id_account,
             id_session
      from dbo.session_account with (nolock)
      where date_diff = @date_diff
      union
      select id_account,
             id_session
      from dbo.session_away with (nolock)
      where date_diff = @date_diff
      and id_account is not null
      union
      select id_account,
             id_session
      from dbo.session_jdp with (nolock)
      where date_diff = @date_diff
      and id_account is not null
      union
      select id_account,
             id_session
      from dbo.session_search with (nolock)
      where date_diff = @date_diff
      and id_account is not null
      union
      select ev.id_account,
             sv.id_session
      from dbo.email_alert ev with (nolock)
               join dbo.session_alertview sv with (nolock)
                    on ev.id = sv.sub_id_alert
      where sv.date_diff = @date_diff
      and ev.id_account is not null
      union
      select es.id_account,
             scm.id_session
      from dbo.session_click_message scm with (nolock)
               join dbo.email_sent es with (nolock)
                    on scm.id_message = es.id_message
      where scm.date_diff = @date_diff
      and id_account is not null
      union
      select min(es.id_account) as id_account,
             sam.id_session
      from dbo.session_alertview_message sam with (nolock)
               join dbo.email_sent es with (nolock)
                    on sam.id_message = es.id_message
      where sam.date_diff = @date_diff
      and id_account is not null
      group by sam.id_session
      union
      select es.id_account,
             ssm.id_session
      from dbo.session_search_message ssm with (nolock)
               join dbo.email_sent es with (nolock)
                    on ssm.id_message = es.id_message
      where ssm.date_diff = @date_diff
      and id_account is not null
      union
      select es.id_account,
             sjm.id_session
      from dbo.session_jdp_message sjm with (nolock)
               join dbo.email_sent es with (nolock)
                    on sjm.id_message = es.id_message
      where sjm.date_diff = @date_diff
      and id_account is not null
    ) account_sessions
         join dbo.session s with (nolock)
              on account_sessions.id_session = s.id
                  and s.date_diff = @date_diff
where s.flags & 1 <> 1
group by account_sessions.id_session

-- get away clicks --
select *
into #session_away_tmp
from dbo.session_away sa
where sa.date_diff = @date_diff

select sa.id_session,
       sum(case when sa.click_price > 0 then 1 else 0 end)              as away_clicks_premium,
       sum(case when coalesce(sa.click_price, 0) = 0 then 1 else 0 end) as away_clicks_free
into #session_away
from #session_away_tmp sa with (nolock)
         join dbo.session s with (nolock)
             on sa.id_session = s.id
where s.flags & 1 = 0
group by sa.id_session;







select *
into #session_tmp
from dbo.session s
where s.date_diff = @date_diff


select *
into #session_click_tmp
from dbo.session_click sc
where sc.date_diff = @date_diff


select *
into #session_click_no_serp_tmp
from dbo.session_click_no_serp scns
where scns.date_diff = @date_diff

-- get revenue for session --
select s1.id_session,
       sum(s1.click_price_usd)               as total_revenue,
       sum(case
               when s1.id_search is not null or s1.id_recommend is not null
                   then click_price_usd end) as serp_revenue,
       sum(case
               when s1.id_alertview is not null or s1.letter_type is not null
                   then click_price_usd end) as email_revenue
into #away_revenue
from (
         select s.id                                      id_session,
                sa.click_price * ic.value_to_usd          click_price_usd,
                isnull(sa.letter_type, sj.letter_type)    letter_type,
                isnull(sc.id_recommend, scj.id_recommend) id_recommend,
                isnull(sc.id_alertview, scj.id_alertview) id_alertview,
                isnull(sc.id_search, scj.id_search)       id_search,
                ext.id                                    id_external
         from #session_away_tmp sa (nolock)
                  inner join #session_tmp s (nolock) on sa.date_diff = s.date_diff
                            and sa.id_session = s.id
                  inner join dbo.info_currency ic (nolock) on ic.id = sa.id_currency
                  left join auction.campaign ac (nolock) on ac.id = sa.id_campaign
                  left join auction.site ast (nolock) on ac.id_site = ast.id
                  left join auction.[user] au (nolock) on au.id = ast.id_user
             -- serp -> away
                  left join dbo.session_click sc (nolock) on sc.date_diff = sa.date_diff
             and sc.id = sa.id_click
             -- serp -> jdp -> away
                  left join dbo.session_jdp sj (nolock) on sj.date_diff = sa.date_diff
             and sj.id = sa.id_jdp
                  left join dbo.session_click scj (nolock) on scj.date_diff = sj.date_diff
             and scj.id = sj.id_click
                  left join dbo.session_external ext (nolock) on ext.date_diff = sa.date_diff
             and ext.id_away = sa.id
         where sa.date_diff = @date_diff
           and isnull(s.flags, 0) & 1 = 0
           and (
                     sa.id_campaign = 0
                 or au.flags & 2 = 0
             )
           and isnull(sa.flags, 0) & 2 = 0
           and sa.flags & 512 = 0

         union all

         select s.id                                   id_session,
                sc.click_price * ic.value_to_usd       click_price_usd,
                isnull(sa.letter_type, sj.letter_type) letter_type,
                sc.id_recommend,
                sc.id_alertview,
                sc.id_search,
                ext.id                                 id_external
         from #session_click_tmp sc (nolock)
                  inner join #session_tmp s (nolock) on sc.date_diff = s.date_diff
             and sc.id_session = s.id
                  inner join dbo.info_currency ic (nolock) on ic.id = sc.id_currency
                  left join auction.campaign ac (nolock) on ac.id = sc.id_campaign
                  left join auction.site ast (nolock) on ac.id_site = ast.id
                  left join auction.[user] au (nolock) on au.id = ast.id_user
                  left join dbo.session_away sa (nolock) on sc.date_diff = sa.date_diff
             and sc.id = sa.id_click
                  left join dbo.session_jdp sj (nolock) on sj.date_diff = sa.date_diff
             and sj.id = sa.id_jdp
                  left join dbo.session_external ext (nolock) on ext.date_diff = sc.date_diff
             and (ext.id_away = sa.id or ext.id_jdp = sj.id)
         where sc.date_diff = @date_diff
           and isnull(s.flags, 0) & 1 = 0
           and (sc.id_campaign = 0 or au.flags & 2 = 2)
           and isnull(sc.flags, 0) & 16 = 0
           and sc.flags & 4096 = 0

         union all

         select s.id                               id_session,
                scns.click_price * ic.value_to_usd click_price_usd,
                scns.letter_type,
                scns.id_recommend,
                null                               id_alertview,
                null                               id_search,
                se.id                              id_external
         from #session_click_no_serp_tmp scns (nolock)
                  inner join dbo.session s (nolock) on scns.date_diff = s.date_diff and scns.id_session = s.id
                  inner join dbo.info_currency ic (nolock) on ic.id = scns.id_currency
                  inner join auction.campaign ac (nolock) on ac.id = scns.id_campaign
                  inner join auction.site ast (nolock) on ac.id_site = ast.id
                  inner join auction.[user] au (nolock) on au.id = ast.id_user
                  left join dbo.session_jdp sj (nolock)
                            on sj.id_click_no_serp = scns.id and sj.date_diff = scns.date_diff
                  left join dbo.session_away sa (nolock)
                            on sa.id_click_no_serp = scns.id and sa.date_diff = scns.date_diff
                  left join dbo.session_external se (nolock) on se.id_jdp = sj.id or se.id_away = sa.id
         where scns.date_diff = @date_diff
           and isnull(s.flags, 0) & 1 = 0
           and au.flags & 2 = 2
           and isnull(scns.flags, 0) & 16 = 0
           and scns.flags & 4096 = 0
     ) s1
group by id_session






-- calc and save result --
select
      all_session_account.id_account,
      @date_diff as date_diff,
      sum(session_away.away_clicks_free) as     away_clicks_free,
      sum(session_away.away_clicks_premium) as  away_clicks_premium,
      0 as dte_clicks_free,
      0 as dte_clicks_premium,
      sum(away_revenue.total_revenue) as total_revenue,
      sum(away_revenue.email_revenue) as email_revenue,
      sum(away_revenue.serp_revenue)  as serp_revenue
into #account_revenue_temp
from #all_session_account all_session_account
left join #session_away session_away
on all_session_account.id_session = session_away.id_session
left join #away_revenue away_revenue
on all_session_account.id_session = away_revenue.id_session
group by all_session_account.id_account
having sum(away_revenue.total_revenue) > 0 or
       sum(session_away.away_clicks_free + session_away.away_clicks_premium )  > 0




-- final table --
Select 
	   format(account_date, 'yyyy-MM-dd') as account_date, 
       format(revenue_date, 'yyyy-MM-dd') as revenue_date,
       source,
       device_type_id,
       max(away_cnt)            as away_cnt,
       max(account_revenue)     as account_revenue,
       max(new_account_cnt)     as new_account_cnt,
       max(revenue_account_cnt) as revenue_account_cnt,
       max(new_verifed_account_cnt) as new_verified_account_cnt,
       max(revenue_verifed_account_cnt) as revenue_verified_account_cnt,
       max(email_revenue)        as email_revenue,
       max(serp_revenue)         as serp_revenue,
       id_traf_src,
	   cast(@date as datetime) as load_date
from (
         SELECT cast(account.date_add as date)                        as account_date,
                cast(dateadd(day, account_revenue.date_diff, '1900-01-01') as date) as revenue_date,
                account.source,
                case
                when account.flags & 1 = 1 then 1 /*mobile*/
                when account.flags & 1 = 0 and account.source < 1000 then 0 /*desktop*/
                when account.source >= 1000 then 2 /*mobile app*/
                end              as device_type_id,
                sum(away_clicks_free + away_clicks_premium)           as away_cnt,
                sum(account_revenue.total_revenue)                    as account_revenue,
                0                                                     as new_account_cnt,
                count(distinct account_revenue.id_account)            as revenue_account_cnt,
                0                                                     as new_verifed_account_cnt,
                count(distinct case when account_contact.verify_date is not null then account_revenue.id_account end) as revenue_verifed_account_cnt,
                sum(account_revenue.email_revenue)                    as email_revenue,
                sum(account_revenue.serp_revenue)                     as serp_revenue,
                account_info.id_traf_src
         FROM #account_revenue_temp account_revenue
                  join dbo.account with (nolock)
                       on account_revenue.id_account = account.id
                  join dbo.account_contact with (nolock)
                        on account.id = account_contact.id_account
                  join dbo.account_info with (nolock)
                        on account.id = account_info.id_account
         group by cast(account.date_add as date),
                  dateadd(day, account_revenue.date_diff, '1900-01-01'),
                  account.source,
                  case
                when account.flags & 1 = 1 then 1 /*mobile*/
                when account.flags & 1 = 0 and account.source < 1000 then 0 /*desktop*/
                when account.source >= 1000 then 2 /*mobile app*/
                end,
                  account_info.id_traf_src
         union all
         Select cast(account.date_add as date) as account_date,
                null                           as revenue_date,
                account.source,
                case
                when account.flags & 1 = 1 then 1 /*mobile*/
                when account.flags & 1 = 0 and account.source < 1000 then 0 /*desktop*/
                when account.source >= 1000 then 2 /*mobile app*/
                end              as device_type_id,
                null                           as away_cnt,
                null                           as account_revenue,
                count(distinct account.id)              as new_account_cnt,
                null                           as revenue_account_cnt,
                count(distinct case when account_contact.verify_date is not null then account_contact.id_account end) as new_verifed_account_cnt,
                0 as revenue_verifed_account_cnt,
                null                            as email_revenue,
                null                            as serp_revenue,
                account_info.id_traf_src
         from dbo.account with (nolock)
         join dbo.account_contact with (nolock)
         on account.id = account_contact.id_account
         join dbo.account_info with (nolock)
         on account.id = account_info.id_account
         where cast(account.date_add as date) = @date
         group by cast(account.date_add as date), account.source,
             case
                when account.flags & 1 = 1 then 1 /*mobile*/
                when account.flags & 1 = 0 and account.source < 1000 then 0 /*desktop*/
                when account.source >= 1000 then 2 /*mobile app*/
                end,
              account_info.id_traf_src
     ) final
group by account_date,
         revenue_date,
         source,
         device_type_id,
         id_traf_src;



drop table #session_away_tmp;
drop table #all_session_account;
drop table #session_away;
drop table #away_revenue;
drop table #account_revenue_temp;
drop table #session_tmp;
drop table #session_click_tmp;
drop table #session_click_no_serp_tmp;