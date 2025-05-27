SET NOCOUNT ON;

-- declare @period_previous int = 272;
/* To find last closed period_id use following script:
Select max(lp.id)
FROM dbo.link_period lp with (nolock)
where lp.closed = 1
 */

Select max(lp.id) as period_id
into #period_previous
FROM dbo.link_period lp with (nolock)
where lp.closed = 1
;

-- Filtering domain statuses from status log
Select *,
       dense_rank() over (order by a.id_account,a.id_domain, a.is_far_from_first) as rank
into #domain_log_raw
FROM (
             Select dostl.id,
                    dostl.dt_created,
                    dostl.id_account,
                    dostl.id_domain,
                    lost.id_domain      as lost_domain_id,
                    reject.id_domain    as rejected_domain_id,
                    dost.name           as old_status,
                    dost2.name          as new_status,
                    case
                        when lag(dostl.dt_created) over (partition by dostl.id_account,dostl.id_domain order by dostl.dt_created)
                            = lag(dostl.dt_created) over (order by dostl.dt_created) then 'true'
                        when lag(dostl.dt_created) over (partition by dostl.id_account,dostl.id_domain order by dostl.dt_created)
                            != lag(dostl.dt_created) over (order by dostl.dt_created) then 'false'
                        else 'true' end as is_far_from_first

             FROM dbo.domain_status_log dostl with (nolock)
                      left join dbo.domain_status dost with (nolock) on dostl.old_status = dost.id
                      left join dbo.domain_status dost2 with (nolock) on dostl.new_status = dost2.id
                      left join dbo.domain_status_log lost with (nolock) on dostl.id_domain = lost.id_domain and lost.new_status = 40
                      left join dbo.domain_status_log reject with (nolock) on dostl.id_domain = reject.id_domain and reject.new_status in (32, 35)
         ) a
;

Select *,
        FIRST_VALUE(f.old_status)
                    OVER (PARTITION BY f.id_domain, f.id_account, f.rank ORDER BY f.dt_created) as first_status_in_work,
        LAST_VALUE(f.new_status)
                    OVER (PARTITION BY f.id_domain, f.id_account, f.rank ORDER BY f.id_domain)  as last_status_in_work,
        min(f.dt_created)
                    OVER (PARTITION BY f.id_domain, f.id_account, f.rank)   as date_min,
        max(f.dt_created)
                    OVER (PARTITION BY f.id_domain, f.id_account, f.rank)   as date_max,
        min(f.rank)
                    OVER (PARTITION BY f.id_domain, f.id_account)           as min_rank,
        case when f.lost_domain_id is not null then 1 else 0 end            as domain_lost_status,
        case when f.rejected_domain_id is not null then 1 else 0 end        as domain_rejected_status
into #domain_log_agg
FROM #domain_log_raw f
;

Select ff.id_domain,
       ff.id_account,
       ff.domain_lost_status,
       ff.domain_rejected_status,
       ff.first_status_in_work,
       ff.last_status_in_work,
       case when DATEDIFF(day, ff.date_min, ff.date_max) = 0 then 1
            else DATEDIFF(day, ff.date_min, ff.date_max) end        as days_processing
into #domain_log_final
FROM #domain_log_agg ff
where ff.min_rank = ff.rank
Group by ff.id_domain,
         ff.id_account,
         ff.domain_lost_status,
         ff.domain_rejected_status,
         ff.first_status_in_work,
         ff.last_status_in_work,
         case when DATEDIFF(day, ff.date_min, ff.date_max) = 0 then 1
              else DATEDIFF(day, ff.date_min, ff.date_max) end
;

Select  log.id_domain,
        count(distinct log.id_account)  as account_work_cnt
into #domain_log_accounts
FROM #domain_log_final log
Group by log.id_domain
;

-- Aggregating all information

Select cast (getdate() as datetime)     as report_update_time,

       l.id             as link_id,
       l.id_period      as period_id,

       l.id_domain      as domain_id,
       do.domain        as domain,
       do.created_dt    as domain_date_created,
       dost.name        as domain_status,

       domain_log.days_processing,
       domain_log.first_status_in_work,
       domain_log.last_status_in_work,
       domain_log.domain_lost_status,
       domain_log.domain_rejected_status,
       domain_log2.account_work_cnt as num_account_work_w_domain,

       l.donor_url      as domain_donor_url,
       l.donor_domain   as domain_donor,
       l.acceptor_url,
       l.acceptor_domain,

       l.id_account     as account_id,
       acc.name         as account_name,
       acc.is_deleted   as account_is_deleted,
       acr.name         as account_role,
       pt.name          as account_pay_type,
       acc.id_region    as account_id_region,
       act.name         as team_name,
       pt2.name         as team_pay_type,
       acc2.name        as team_lead_name,

       l.id_country     as country_id,
       c.cc             as country_cc,
       c.coeff          as country_coeff,

       l.position       as link_position,
       lsty.name        as link_site_type,
       ls.name          as link_source,
       l.service_flags  as link_service_flags,
       lpt.name         as link_placing_type,
       l.thematic       as link_thematic,
       lt.name          as link_type,
       lst.name         as link_status,
       lpr.name         as link_problem,
       ldr.name         as link_delete_reason,
       l.approved       as approved,
       l.comment,
       l.seo_flags,

       l.created        as date_created,
       l.date_check     as link_date_check,
       l.date_problem   as link_date_problem,
       l.date_deleted   as link_date_deleted,


       l.dr,
       l.traffic,
       l.rd,
       l.ur,
       l.linked_domains,
       l.int_links,
       l.out_links,
       l.search_result,
       l.nofollow,

       l.score,
       l.score_penalty,
       l.score_ro_eur,
       l.score_penalty_ro_eur,
       dot.name                 as domain_type


FROM dbo.link l with (nolock)
join dbo.link_period lp with (nolock) on l.id_period = lp.id
left join dbo.account acc with (nolock) on l.id_account = acc.id
left join dbo.account_role acr with (nolock) on acc.id_role = acr.id
left join dbo.pay_type pt with (nolock) on acc.pay_type = pt.id
left join dbo.account_team act with (nolock) on acc.id_team = act.id
left join dbo.pay_type pt2 with (nolock) on act.pay_type = pt2.id
left join dbo.account acc2 with (nolock) on act.id_lead = acc2.id
left join dbo.country c with (nolock) on l.id_country = c.id
left join dbo.link_site_type lsty with (nolock) on l.id_site_type = lsty.id
left join dbo.link_source ls with (nolock) on l.id_source = ls.id
left join dbo.link_placing_type lpt with (nolock) on l.id_placing_type = lpt.id
left join dbo.link_type lt with (nolock) on l.type = lt.id
left join dbo.link_status lst with (nolock) on l.status = lst.id
left join dbo.link_problem lpr with (nolock) on l.problem = lpr.id
left join dbo.link_delete_reason ldr with (nolock) on l.delete_reason = ldr.id
left join dbo.domain do with (nolock) on l.id_domain = do.id
left join dbo.domain_status dost with (nolock) on do.id_status = dost.id
left join dbo.domain_type dot WITH (NOLOCK) ON dot.id = do.id_type
left join #domain_log_final domain_log on l.id_domain = domain_log.id_domain and l.id_account = domain_log.id_account
left join #domain_log_accounts domain_log2 on l.id_domain = domain_log2.id_domain

where l.id_period = (select period_id from #period_previous)
;