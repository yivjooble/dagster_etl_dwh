WITH log_delete AS (
    SELECT log2.edited_id_account,
           MAX(log2.dt_created) as account_delete_date
    FROM dbo.account_log log2 with (nolock)
    WHERE log2.action = 'delete'
    GROUP BY log2.edited_id_account
),

log_team AS (
    SELECT log_3.edited_id_account,
           log_3.team_change_date as last_team_change_date,
           log2.val_old,
           log2.val_new
    FROM (
        SELECT acc.edited_id_account,
               MAX(acc.dt_created) as team_change_date
        FROM dbo.account_log acc with (nolock)
        WHERE acc.action = 'id_team' and acc.val_old is not null
        GROUP BY acc.edited_id_account
    ) log_3
    LEFT JOIN dbo.account_log log2 with (nolock) 
    ON log_3.edited_id_account = log2.edited_id_account AND log_3.team_change_date = log2.dt_created and log2.action = 'id_team'
)

SELECT cast(getdate() as datetime) as report_update_time,
       acc.id as account_id,
       acc.name as account_name,
       acr.name as account_role,
       acc.is_deleted as account_is_deleted,
       acc.dt_created as account_create_date,
       CASE WHEN acc.is_deleted='true' THEN log_delete.account_delete_date END as account_delete_date_log,
       log_team.last_team_change_date as account_team_change_date_log,
       log_team.val_old as old_team_id,
       act3.name as old_team_name,
       log_team.val_new as new_team_id,
       act4.name as new_team_name,
       acc.id_region as account_region_id,
       c.cc as account_country,
       pt.name as account_pay_type,
       acc.id_team as team_id,
       act.name as team_name,
       pt2.name as team_pay_type,
       acc2.name as team_lead_name
FROM dbo.account acc with (nolock)
LEFT JOIN log_delete ON log_delete.edited_id_account = acc.id
LEFT JOIN log_team ON log_team.edited_id_account = acc.id
LEFT JOIN dbo.account_team act with (nolock) ON acc.id_team = act.id
LEFT JOIN dbo.account_team act3 with (nolock) ON log_team.val_old = act3.id
LEFT JOIN dbo.account_team act4 with (nolock) ON log_team.val_new = act4.id
LEFT JOIN dbo.account_role acr with (nolock) ON acc.id_role = acr.id
LEFT JOIN dbo.country c with (nolock) ON c.id = acc.id_region
LEFT JOIN dbo.pay_type pt with (nolock) ON acc.pay_type = pt.id
LEFT JOIN dbo.pay_type pt2 with (nolock) ON act.pay_type = pt2.id
LEFT JOIN dbo.account acc2 with (nolock) ON act.id_lead = acc2.id
WHERE acc.id_team IS NOT NULL;
