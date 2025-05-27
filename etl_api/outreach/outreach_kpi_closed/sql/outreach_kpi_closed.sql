/* Historical KPI results for closed periods only */

Select getdate() as report_update_time,
       lls.id_period        as period_id,
       lp.name              as period_name,
       case when lp.closed=1 then 'yes' else 'no' end        as period_closed,
       lp.year              as period_year,
       lls.id_team          as team_id,
       team.name            as team_name,
       lls.id_lead          as lead_id,
       acc.name             as lead_name,
       c.cc                 as lead_country_cc,
       pt.name              as pay_type,
       lls.bonus,
       lls.bonus_currency,
       lls.bonus_score,
       lls.bonus_link,
       lls.team_kpi,
       lls.person_kpi

FROM dbo.link_lead_salary lls with (nolock)
left join dbo.link_period lp with (nolock) on lls.id_period = lp.id
left join dbo.pay_type pt with (nolock) on lls.pay_type = pt.id
left join dbo.account acc with (nolock) on lls.id_lead = acc.id
left join dbo.account_team team with (nolock) on lls.id_team = team.id
left join dbo.country c with (nolock) on c.id = acc.id_region
;
