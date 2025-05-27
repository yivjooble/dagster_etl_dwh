SET NOCOUNT ON;



declare @start_datediff int = :to_sqlcode_date_int;

        
        create table #active_test
        (
            id_test int
        )
        insert into #active_test
        
        SELECT distinct email_account_test.id_test
        FROM dbo.email_account_test with (nolock)
                 join dbo.email_account_test_settings with (nolock)
                      on email_account_test.id_test = email_account_test_settings.id_test
        where (date_diff >= datediff(day, 0, getdate() - 7) and coalesce(cast(start_date as date), '2021-01-01') < '2021-08-10')
           or (coalesce(cast(start_date as date), '2021-01-01') > '2021-08-10' and
                (stop_date is null or stop_date >= @start_datediff))
        group by email_account_test.id_test
        
        
        create table #session_account
        (
            id_session    bigint,
            id_account int
        )
        
        insert into #session_account
        
                  Select  id_session,
                          max(id_account) as id_account
                  from dbo.session_account (nolock)
                  where date_diff =  @start_datediff
                  group by id_session
        
        create table #user_tests
        (
            id_account    int,
            account_tests char(50)
        )
        insert into #user_tests
        select id_account,
               replace(CONCAT(id_test, ':', id_group, ',', 
                              next1_id_test, ':', next1_id_group, ',', 
                              next2_id_test, ':', next2_id_group, ',',
                              next3_id_test, ':', next3_id_group, ',',
                              next4_id_test, ':', next4_id_group, ',',
                              next5_id_test, ':', next5_id_group, ',',
                              next6_id_test, ':', next6_id_group, ','
                             ), ':,', '') as account_tests
        from (
                 select id_account,
                        id_group,
                        EAT.id_test,
                        row_number() over (partition by id_account order by EAT.id_test desc ) as num,
                        LEAD(EAT.id_test, 1) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next1_id_test],
                        LEAD(id_group, 1) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next1_id_group],
                        LEAD(EAT.id_test, 2) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next2_id_test],
                        LEAD(id_group, 2) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next2_id_group],
                        LEAD(EAT.id_test, 3) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next3_id_test],
                        LEAD(id_group, 3) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next3_id_group],
                        LEAD(EAT.id_test, 4) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next4_id_test],
                        LEAD(id_group, 4) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next4_id_group],
                        LEAD(EAT.id_test, 5) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next5_id_test],
                        LEAD(id_group, 5) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next5_id_group],
                        LEAD(EAT.id_test, 6) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next6_id_test],
                        LEAD(id_group, 6) OVER (PARTITION BY [id_account]
                            ORDER BY EAT.[id_test] DESC)                                       AS [next6_id_group]
                 from dbo.email_account_test EAT with (nolock)
                          inner join
                      dbo.email_account_test_settings EATS with (nolock)
                      on
                          EAT.id_test = EATS.id_test
                 where EAT.id_test in
                       (
                           select id_test
                           from #active_test active_test
                       )
                    or is_active = 1
             ) a
        where num = 1
        
        create table #users
        (
            id_account     int,
            account_tests  char(50),
            account_age    char(50),
            account_active char(50)
        )
        insert into #users
        select ut.id_account,
               ut.account_tests,
               case
                   when (@start_datediff - datediff(day, '1900-01-01', cast(ACC.date_add as Date))) <= 7 then 'new'
                   when (@start_datediff - datediff(day, '1900-01-01', cast(ACC.date_add as Date))) between 8 and 29
                       then '8-29 days'
                   else '30+ days' end        as account_age,
               case
                   when (@start_datediff - AI.last_visit) <= 7 then 'active last 7days'
                   when (@start_datediff - AI.last_visit) between 8 and 29 then 'active last 8-29days'
                   else 'active 30+ days' end as account_active
        from #user_tests ut
                 inner join
             dbo.account ACC with (nolock)
             on
                 ACC.id = ut.id_account
                 inner join
             dbo.account_info AI with (nolock)
             on
                 AI.id_account = ut.id_account
        group by ut.id_account, ut.account_tests,
                 case
                     when (@start_datediff - datediff(day, '1900-01-01', cast(ACC.date_add as Date))) <= 7 then 'new'
                     when (@start_datediff - datediff(day, '1900-01-01', cast(ACC.date_add as Date))) between 8 and 29
                         then '8-29 days'
                     else '30+ days' end,
                 case
                     when (@start_datediff - AI.last_visit) <= 7 then 'active last 7days'
                     when (@start_datediff - AI.last_visit) between 8 and 29 then 'active last 8-29days'
                     else 'active 30+ days' end
        
        
        create table #accounts
        (
            date_diff      int,
            account_tests  char(50),
            account_age    char(50),
            account_active char(50),
            value          int
        )
        
        insert into #accounts
        select date_diff,
               account_tests,
               account_age,
               account_active,
               count(users.id_account) as value
        from #users users
                 inner join
             dbo.email_account_test EAT with (nolock)
             on users.id_account = EAT.id_account
        where date_diff = @start_datediff
        group by date_diff, account_tests,
                 account_age, account_active;
        
        
        -- Unsubscribed accounts
        create table #unsub
        (
            date_diff      int,
            account_tests  char(50),
            account_age    char(50),
            account_active char(50),
            value          int
        )
        insert into #unsub
        
        select datediff(day, 0, AI.unsub_date) as date_diff,
               users.account_tests,
               account_age,
               account_active,
               count(AI.id_account)            as value
        from dbo.account_info AI with (nolock)
                 inner join
             #users users
             on
                 users.id_account = AI.id_account
        where datediff(day, 0, AI.unsub_date) = @start_datediff
        group by datediff(day, 0, AI.unsub_date), account_tests,
                 account_age, account_active
        
        create table #sent
        (
            date_diff      int,
            account_tests  char(50),
            account_age    char(50),
            account_active char(50),
            letter_type    int,
            value          int
        )
        insert into #sent
        
        
        select ES.date_diff,
               users.account_tests,
               account_age,
               account_active,
               ES.letter_type,
               count(distinct ES.id_message) as value
        from dbo.email_sent ES with (nolock)
                 inner join
             #users users
             on
                 users.id_account = ES.id_account
        where ES.date_diff = @start_datediff
        group by ES.date_diff, account_tests, ES.letter_type,
                 account_age, account_active
        
        create table #opened
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            letter_open    int,
            account_age    char(50),
            account_active char(50)
        )
        insert into #opened
        
        select EO.email_date_diff as date_diff,
               users.account_tests,
               EO.letter_type,
               count(distinct EO.id_message) as letter_open,
               account_age,
               account_active
        from
            (
                select id_account, email_date_diff, letter_type, id_message
                from dbo.email_visit ev with(nolock)
                where
                    ev.date_diff = @start_datediff
        
                union
        
                select id_account, email_date_diff, letter_type, id_message
                from dbo.email_open eo with(nolock)
                where
                    eo.date_diff = @start_datediff
            ) EO
                 inner join
             #users users
             on
                 users.id_account = EO.id_account
        group by EO.email_date_diff, account_tests, EO.letter_type, account_age, account_active
        
        create table #visited
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            account_age    char(50),
            account_active char(50),
            letter_click   int
        )
        
        insert into #visited
        
        select EV.email_date_diff as date_diff,
               users.account_tests,
               EV.letter_type,
               case when EV.flags & 1 = 1 then 1 else 0 end as mobile,
               account_age,
               account_active,
               count(distinct EV.id_message)                as letter_click
        from dbo.email_visit EV with (nolock)
                 inner join
             #users users
             on
                 users.id_account = EV.id_account
        where EV.date_diff = @start_datediff
        group by EV.email_date_diff, account_tests, EV.letter_type,
                 case when EV.flags & 1 = 1 then 1 else 0 end,
                 account_age, account_active
        
        create table #alertview
        (
            date_diff      int,
            account_tests  char(50),
            mobile         int,
            account_age    char(50),
            account_active char(50),
            letter_type    int,
            alertview_cnt  int,
            click_cnt      int
        )
        
        insert into #alertview
        
        select SA.date_diff,
               users.account_tests,
               case when SA.service_flags & 1 = 1 then 1 else 0 end as mobile,
               account_age,
               account_active,
               email_sent.letter_type,
               count(distinct SA.id)                                as alertview_cnt,
               count(distinct SC.id)                                as click_cnt
        from dbo.session_alertview SA with (nolock)
                 join
             dbo.email_alert EA with (nolock)
             on SA.sub_id_alert = EA.id
                 join
             #users users
             on
                 users.id_account = EA.id_account
                 left join
             dbo.session_click SC with (nolock)
             on
                     SC.date_diff = SA.date_diff and
                     SC.id_alertview = SA.id
             join dbo.session with (nolock)
            on       SA.date_diff =   session.date_diff and
                     SA.id_session =  session.id
                left join dbo.session_alertview_message SAM with (nolock)
            on       SA.date_diff = SAM.date_diff
                     and SA.id = SAM.id_alertview
               left join dbo.email_sent with (nolock)
           on       SAM.id_message = email_sent.id_message
        
        
        where SA.date_diff = @start_datediff and coalesce(session.flags,0) & 1 = 0
        group by SA.date_diff, account_tests,
                 case when SA.service_flags & 1 = 1 then 1 else 0 end,
                 account_age, account_active, email_sent.letter_type
        
        
        create table #jdps
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            account_age    char(50),
            account_active char(50),
            jdp_cnt        int
        )
        
        insert into #jdps
        
        select SJ.date_diff,
               users.account_tests,
               coalesce(SJ.letter_type, email_sent.letter_type)    as letter_type,
               case when SJ.flags & 1 = 1 then 1 else 0 end as mobile,
               account_age,
               account_active,
               count(distinct SJ.id)                        as jdp_cnt
        from dbo.session_jdp SJ with (nolock)
                 join dbo.session_click SC with (nolock)
                      on SJ.date_diff = Sc.date_diff
                          and SJ.id_click = SC.id
                 inner join
             #users users
             on
                 users.id_account = SJ.id_account
                  join dbo.session with (nolock)
            on       SJ.date_diff =   session.date_diff and
                     SJ.id_session =  session.id
            left join dbo.session_alertview_message SAM with (nolock)
            on       SC.date_diff =  SAM.date_diff
                     and SC.id_alertview = SAM.id_alertview
            left join dbo.email_sent with (nolock)
            on       SAM.id_message = email_sent.id_message
        where SJ.date_diff = @start_datediff and coalesce(session.flags,0) & 1 = 0
          and (SC.id_alertview is not null or SJ.letter_type is not null)
        group by SJ.date_diff, account_tests, coalesce(SJ.letter_type, email_sent.letter_type),
                 case when SJ.flags & 1 = 1 then 1 else 0 end,
                 account_age, account_active
        
        
        
        
        --===================================================
        create table #aways
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            account_age    char(50),
            account_active char(50),
            jdp_aways      int,
            lt_aways       int,
            serp_aways     int,
        	conversions      int,
            conversion_aways  int,
            total_aways int,
            valid_mark int
        )
        
        insert into #aways
        
        select Away.date_diff,
               coalesce(users0.account_tests, users1.account_tests, users2.account_tests) as account_tests,
               coalesce(Away.letter_type, SJ.letter_type, email_sent.letter_type)             as letter_type,
               case when Away.flags & 1 = 1 then 1 else 0 end         as mobile,
               coalesce(users0.account_age, users1.account_age, users2.account_age) as account_age,
               coalesce(users0.account_active, users1.account_active, users2.account_active) as account_active,
               count(distinct case
                                  when Away.id_jdp is not null then Away.id end)               as jdp_aways,
               count(distinct case
                                  when coalesce(Away.letter_type, SJ.letter_type) is not null and coalesce(SC.id_alertview,SCJ.id_alertview) is null
                                      then Away.id end)               as lt_aways,
               count(distinct case
                                  when coalesce(SC.id_alertview,SCJ.id_alertview) is not null
                                      then Away.id end)               as serp_aways,
        	   count(distinct conv.id_session_away )               as conversions,
               count(distinct case when Away.date_diff >= conv_start.date_diff then Away.id end) as conversion_aways,
               count(distinct Away.id) as total_aways,
               1 as valid_mark
        from dbo.session_away Away with (nolock)
                 left join dbo.session_jdp SJ with (nolock)
                           on Away.date_diff = SJ.date_diff
                               and Away.id_jdp = SJ.id
                 left join dbo.session_click SC with (nolock)
                           on Away.date_diff = SC.date_diff
                               and Away.id_click = SC.id
                 left join dbo.session_click as SCJ with (nolock)
                           on SJ.date_diff = SCJ.date_diff
                               and SJ.id_click = SCJ.id
                 left join dbo.info_currency IC with (nolock)
                           on IC.id = Away.id_currency
        	     left join
        				 (select distinct date_diff, id_session_away
        				  from auction.conversion_away_connection with (nolock)
        				 ) conv
        				 on conv.date_diff = Away.date_diff
        					and conv.id_session_away = Away.id
                 left join (
        			select
        				sa.id_project,
        				min(con.date_diff) date_diff
        			from dbo.session_away sa with(nolock)
        			join auction.conversion_away_connection con with(nolock)
        				on con.date_diff = sa.date_diff
        				and con.id_session_away = sa.id
        			group by sa.id_project
        		 ) conv_start
        			 on Away.id_project = conv_start.id_project
                 left join dbo.session_alertview sal with(nolock)
                 on Away.date_diff = sal.date_diff and coalesce(sc.id_alertview, scj.id_alertview ) = sal.id
                 left join dbo.email_alert ea with(nolock)
                 on sal.sub_id_alert = ea.id
                 left join #session_account  session_account
                 on session_account.id_session = Away.id_session
        		 left join #users users0 on users0.id_account = Away.id_account
        		 left join #users users1 on users1.id_account = ea.id_account
        		 left join #users users2 on users2.id_account = session_account.id_account
        		 join dbo.session with (nolock)
        		  on Away.date_diff =   session.date_diff
        	      and Away.id_session =  session.id
                 left join dbo.session_alertview_message SAM with (nolock)
                   on     SC.date_diff =  SAM.date_diff
                     and  coalesce(SC.id_alertview, SCJ.id_alertview) = SAM.id_alertview
                  left join dbo.email_sent with (nolock)
                 on       SAM.id_message = email_sent.id_message
        where Away.date_diff = @start_datediff
        	  and coalesce(session.flags,0) & 1 = 0
        group by Away.date_diff,
        		coalesce(users0.account_tests, users1.account_tests, users2.account_tests),
                coalesce(users0.account_age, users1.account_age, users2.account_age),
        		coalesce(users0.account_active, users1.account_active, users2.account_active),
                 case
                     when Away.flags & 1 = 1 then 1
                     else 0 end,
                 coalesce(Away.letter_type, SJ.letter_type, email_sent.letter_type)
        --===================================================
        
        
        
        
        create table #revenue
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            account_age    char(50),
            account_active char(50),
            jdp_revenue      float,
            lt_revenue       float,
            serp_revenue     float,
        	conversion_revenue  float,
            total_revenue float,
            valid_mark int
        )
        
        insert into #revenue
        
        
        Select
               Away.date_diff,
               account_tests,
               letter_type,
               case when Away.flags & 16 = 16 then 1 else 0 end as mobile,
               account_age,
               account_active,
               sum(case
                       when Away.id_jdp is not null then click_price_usd else 0 end) as jdp_revenue,
               sum(case
                       when  Away.letter_type is not null and Away.id_alertview is null then click_price_usd
                       else 0 end)                                    as lt_revenue,
               sum(case
                       when Away.id_alertview is not null then click_price_usd else 0 end)    as serp_revenue,
        	   sum(case
        			   when Away.date_diff >= conv_start.date_diff then click_price_usd else 0 end)  as conversion_revenue,
               sum(click_price_usd) as total_revenue,
               1 as valid_mark
              from (
                       select s.date_diff,
                              s.id                                          id_session,
                              sa.click_price * coalesce(ic.value_to_usd, 0) click_price_usd,
                              coalesce(sa.letter_type, sj.letter_type,email_sent.letter_type )        letter_type,
                              coalesce(sc.id_alertview, scj.id_alertview)     id_alertview,
                              sa.id_jdp,
                              coalesce(sa.id_account, sj.id_account) as     id_account,
                              s.flags,
                              sa.id_project
                       from dbo.session_away sa (nolock)
                                inner join dbo.session s (nolock) on sa.date_diff = s.date_diff
                           and sa.id_session = s.id
                                inner join dbo.info_currency ic (nolock) on ic.id = sa.id_currency
                           -- serp -> away
                                left join dbo.session_click sc (nolock) on sc.date_diff = sa.date_diff
                           and sc.id = sa.id_click
                           -- serp -> jdp -> away
                                left join dbo.session_jdp sj (nolock) on sj.date_diff = sa.date_diff
                           and sj.id = sa.id_jdp
                                left join dbo.session_click scj (nolock) on scj.date_diff = sj.date_diff
                           and scj.id = sj.id_click
                           left join dbo.session_alertview_message SAM with (nolock)
                                on     SC.date_diff =  SAM.date_diff
                              and  coalesce(SC.id_alertview, SCJ.id_alertview) = SAM.id_alertview
                            left join dbo.email_sent with (nolock)
                                 on       SAM.id_message = email_sent.id_message
                       where sa.date_diff = @start_datediff
                         and isnull(s.flags, 0) & 1 = 0
                         and isnull(sa.flags, 0) & 2 = 0
                         and sa.flags & 512 = 0
                   ) Away
        left join #session_account session_account
        on Away.id_session = session_account.id_session
                  left join dbo.session_alertview sa (nolock)
                  on sa.date_diff = @start_datediff
                  and Away.id_alertview = sa.id
                  left join dbo.email_alert ea (nolock)
                  on sa.sub_id_alert = ea.id
        join #users users
        on users.id_account = coalesce(coalesce(Away.id_account,session_account.id_account ), ea.id_account)
         left join (
        			select
        				sa.id_project,
        				min(con.date_diff) date_diff
        			from dbo.session_away sa with(nolock)
        			join auction.conversion_away_connection con with(nolock)
        				on con.date_diff = sa.date_diff
        				and con.id_session_away = sa.id
        			group by sa.id_project
        		 ) conv_start
        			 on Away.id_project = conv_start.id_project
        group by
               Away.date_diff,
               account_tests,
               letter_type,
               case when Away.flags & 16 = 16 then 1 else 0 end,
               account_age,
               account_active
        
        create table #applies
        (
            date_diff      int,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            account_age    char(50),
            account_active char(50),
            applies_cnt    int
        )
        
        insert into #applies
        
        select SA.date_diff,
               account_tests,
               coalesce(SJ.letter_type, email_sent.letter_type)                                         as letter_type,
               case
                   when SJ.flags & 1 = 1 then 1
                   else 0 end                                         as mobile,
               account_age,
               account_active,
               count(distinct SA.id)                                  as applies_cnt
        from dbo.session_jdp SJ with (nolock)
                 inner join dbo.session_jdp_action SJA with (nolock)
                          on SJ.date_diff = SJA.date_diff
        		  and SJ.id = SJA.id_jdp
                 inner join dbo.session_apply SA
        	          on SA.date_diff = SJA.date_diff
        		  and SA.id_src_jdp_action = SJA.id
                 inner join #users users
                          on users.id_account = SA.id_account
                 join dbo.session with (nolock)
            on       SJ.date_diff =   session.date_diff and
                     SJ.id_session =  session.id
        
                                left join dbo.session_click scj (nolock) on scj.date_diff = sj.date_diff
                           and scj.id = sj.id_click
                           left join dbo.session_alertview_message SAM with (nolock)
                                on     scj.date_diff =  SAM.date_diff
                              and scj.id_alertview = SAM.id_alertview
                            left join dbo.email_sent with (nolock)
                                 on       SAM.id_message = email_sent.id_message
        where SA.date_diff = @start_datediff and coalesce(session.flags,0) & 1 = 0
        and ( SJ.letter_type is not null or scj.id_alertview is not null)
        group by SA.date_diff, account_tests,
                 account_age, account_active,
                 case
                     when SJ.flags & 1 = 1 then 1
                     else 0 end,
                 coalesce(SJ.letter_type, email_sent.letter_type)
        
        create table #Final
        (
            date           date,
            account_tests  char(50),
            letter_type    int,
            mobile         int,
            value          float,
            metric         char(50),
            account_age    char(50),
            account_active char(50),
            valid_mark     int
        )
        
        insert into #Final
        
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests                                       as account_test_group,
               null                                                as letter_type,
               null                                                as is_mobile,
               value,
               'Test Accounts'                                     as metric,
               account_age,
               account_active,
               1                                                   as is_valid
        from #accounts
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               null                                                as letter_type,
               null                                                as mobile,
               value,
               'Unsubscribed Accounts'                             as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #unsub
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               null                                                as mobile,
               value,
               'Letters Sent'                                      as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #sent
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               null                                                as mobile,
               letter_open,
               'Letters Opened'                                    as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #opened
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               --send_interval,
               mobile,
               letter_click,
               'Letters Clicked'                                   as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #visited
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type                                                as letter_type,
               mobile,
               alertview_cnt,
               'Alertviews'                                        as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #alertview
        union all
        
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type                                                as letter_type,
               mobile,
               click_cnt,
               'Clicks'                                            as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #alertview
        
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               jdp_cnt,
               'JDPs'                                              as metric,
               account_age,
               account_active,
               1                                                   as valid_mark
        from #jdps
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               jdp_aways,
               'JDP Aways'                                         as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               conversions,
               'Conversions'                                   as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               --send_interval,
               mobile,
               jdp_revenue,
               'JDP Revenue'                                       as metric,
               account_age,
               account_active,
               valid_mark
        from #revenue
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               --send_interval,
               mobile,
               serp_revenue,
               'SERP Revenue'                                       as metric,
               account_age,
               account_active,
               valid_mark
        from #revenue
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               --send_interval,
               mobile,
               conversion_revenue,
               'Conversion Revenue'                                       as metric,
               account_age,
               account_active,
               valid_mark
        from #revenue
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               serp_aways,
               'SERP Aways'                                        as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               total_aways,
               'Total Aways'                                  as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               conversion_aways,
               'Conversion Aways'                                 as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               lt_aways,
               'Letter Aways'                                      as metric,
               account_age,
               account_active,
               valid_mark
        from #aways
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               lt_revenue,
               'Letter Revenue'                                    as metric,
               account_age,
               account_active,
               valid_mark
        from #revenue
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               total_revenue,
               'Total Revenue'                                    as metric,
               account_age,
               account_active,
               valid_mark
        from #revenue
        union all
        SELECT cast(dateadd(day, date_diff, '1900-01-01') as Date) as date,
               account_tests,
               letter_type,
               mobile,
               applies_cnt,
               'Applies'                                          as metric,
               account_age,
               account_active,
               1                                                  as valid_mark
        from #applies
        
        
        Select cast(date as datetime) as action_date,
               account_tests as account_test_num,
               letter_type,
               mobile as is_mobile,
               value as metric_cnt,
               metric as metric_name,
               account_age,
               account_active,
               valid_mark as is_valid,
               @start_datediff as load_date_diff
        from #Final
        where value != 0 and account_tests is not null
        
        
        drop table #aways;
        drop table #accounts;
        drop table #active_test;
        drop table #applies;
        drop table #alertview;
        drop table #Final;
        drop table #revenue;
        drop table #user_tests;
        drop table #users;
        drop table #unsub;
        drop table #sent;
        drop table #opened;
        drop table #visited;
        drop table #session_account;
        drop table #jdps;

