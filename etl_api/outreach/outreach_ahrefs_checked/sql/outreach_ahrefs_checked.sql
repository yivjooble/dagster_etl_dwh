SET NOCOUNT ON;

declare @date_start date = :to_sqlcode_date_or_datediff_start,
        @date_end date = :to_sqlcode_date_or_datediff_end;

Select
    distinct   ac.id            AS account_id,
    ac.name                     AS account_name,
    ac.is_deleted               AS account_is_deleted,
    ac.id_region                AS account_id_region,
    c.cc                        AS account_cc,
    acr.name                    AS account_role,
    act.name                    AS team_name,
    pt2.name                    AS team_pay_type,
    acc2.name                   AS team_lead_name

into #account_info
FROM dbo.account ac WITH(NOLOCK)
LEFT JOIN dbo.account_role acr with (nolock) on ac.id_role = acr.id
LEFT JOIN dbo.account_team act with (nolock) on ac.id_team = act.id
LEFT JOIN dbo.pay_type pt2 with (nolock) on act.pay_type = pt2.id
LEFT JOIN dbo.account acc2 with (nolock) on act.id_lead = acc2.id
LEFT JOIN dbo.country c with (nolock) on c.id = ac.id_region
;


SELECT
    CONVERT(date, bk.[dt_add])  AS date_add,
    ac.name                     AS account_name,
    info.account_is_deleted,
    info.account_id_region,
    info.account_cc,
    info.account_role,
    info.team_name,
    info.team_pay_type,
    info.team_lead_name,
    'bulk_check'                AS type,
    SUM([possible_spam_cnt] + [free_cnt] + [bad_donor_cnt]) AS total_checked
FROM dbo.bulk_check bk WITH(NOLOCK)
LEFT JOIN dbo.account ac WITH(NOLOCK) ON ac.id = bk.id_account
LEFT JOIN #account_info info ON ac.id = info.account_id
WHERE CONVERT(date, bk.[dt_add]) between @date_start and @date_end
GROUP BY    CONVERT(date, bk.[dt_add]),
            ac.name,
            info.account_is_deleted,
            info.account_id_region,
            info.account_cc,
            info.account_role,
            info.team_name,
            info.team_pay_type,
            info.team_lead_name
UNION ALL
SELECT
       ael.date         AS date_add,
       ac.name          AS account_name,
       info.account_is_deleted,
       info.account_id_region,
       info.account_cc,
       info.account_role,
       info.team_name,
       info.team_pay_type,
       info.team_lead_name,
       'plugin'         AS type,
       SUM(1)           AS total_checked
FROM  dbo.ahrefs_extension_log ael WITH(NOLOCK)
LEFT JOIN dbo.account ac WITH(NOLOCK)  on ac.id = ael.id_account
LEFT JOIN #account_info info ON ac.id = info.account_id
WHERE ael.date between @date_start and @date_end
GROUP BY  ael.date,
          ac.name,
          ael.id_account,
         info.account_is_deleted,
         info.account_id_region,
         info.account_cc,
         info.account_role,
         info.team_name,
         info.team_pay_type,
         info.team_lead_name
;

drop table #account_info;