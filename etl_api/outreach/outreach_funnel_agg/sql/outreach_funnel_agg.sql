SET NOCOUNT ON;

WITH filters AS (
        SELECT DISTINCT dostl.id_domain,
                CASE WHEN lost.id_domain IS NOT NULL THEN 1 ELSE 0 END        AS domain_lost_status,
                CASE WHEN reject.id_domain IS NOT NULL THEN 1 ELSE 0 END      AS domain_rejected_status,
                CASE WHEN easy_reject.id_domain IS NOT NULL THEN 1 ELSE 0 END AS domain_easy_reject,
                CASE WHEN hard_reject.id_domain IS NOT NULL THEN 1 ELSE 0 END AS domain_hard_reject
        FROM dbo.domain_status_log dostl WITH (NOLOCK)
        LEFT JOIN dbo.domain_status_log lost WITH (NOLOCK)
                        ON dostl.id_domain = lost.id_domain AND lost.new_status = 40
        LEFT JOIN dbo.domain_status_log reject WITH (NOLOCK)
                        ON dostl.id_domain = reject.id_domain AND reject.new_status IN (32, 35)
        LEFT JOIN dbo.domain_status_log easy_reject WITH (NOLOCK)
                        ON dostl.id_domain = easy_reject.id_domain AND easy_reject.new_status = 32
        LEFT JOIN dbo.domain_status_log hard_reject WITH (NOLOCK)
                        ON dostl.id_domain = hard_reject.id_domain AND hard_reject.new_status = 35
        ),

        links AS (
                SELECT  do.id      AS domain_id,
                        do.id_account,
                        do.last_dt AS last_domain_date,
                        l.id       AS link_id,
                        l.donor_url,
                        l.created  AS link_created_date
                FROM dbo.domain do WITH (NOLOCK)
                LEFT JOIN dbo.link l WITH (NOLOCK)
                                ON l.id_domain = do.id AND l.id_account = do.id_account AND l.created = do.last_dt AND l.date_deleted IS NULL
                WHERE do.id_status = 36 /* Closed Won */
                        AND do.last_dt > '2022-05-15'
                GROUP BY do.id, do.id_account, do.last_dt, l.id, l.donor_url, l.created
                )

SELECT          CAST(GETDATE() AS datetime)     AS report_update_time,
                dostl.id_domain                        AS domain_id,
                do.domain                              AS domain,
                c.cc                                   AS domain_cc,
                do.user_priority,
                ds.name                                AS last_domain_status,
                acc1.name                              AS last_account_name,
                do.flags                               AS domain_flags,
                do.email_flags                         AS domain_email_flags,
                dostl.id                               AS domain_log_id,
                COALESCE(dostl.dt_created, do.last_dt) AS date_created,
                dostl.id_account                       AS account_id,
                acc.name                               AS account_name,
                acc.is_deleted                         AS account_is_deleted,
                acr.name                               AS account_role,
                pt.name                                AS account_pay_type,
                acc.id_region                          AS account_id_region,
                act.name                               AS team_name,
                acc2.name                              AS team_lead_name,
                filters.domain_lost_status,
                filters.domain_rejected_status,
                filters.domain_easy_reject,
                filters.domain_hard_reject,
                dost.name                              AS old_status,
                COALESCE(dost2.name, ds.name)          AS new_status,
                skip.reason                            AS skip_reason,
                refusal.text                           AS refusal_text,
                links.link_id                          AS closed_won_link_id,
                links.donor_url                        AS closed_won_link_url,
                dot.name                               AS domain_type,
                dot.is_active                          AS domain_type_active,
                dot.priority                           AS domain_type_priority,
                dostl.id_refusal_contact               AS refusal_contact_id,
                ct.name                                AS refusal_contact_type,
                dc.is_unsubscribed                     AS refusal_contact_unsub,
                r.name                                 AS refusal_reason,
                dc.reject_service_flags                AS refusal_service_flags

         FROM dbo.domain_status_log dostl WITH (NOLOCK)
                  LEFT JOIN dbo.domain do with (nolock) ON do.id = dostl.id_domain
                  LEFT JOIN dbo.domain_type dot WITH (NOLOCK) ON dot.id = do.id_type
                  LEFT JOIN dbo.country c with (nolock) on do.id_country = c.id
                  LEFT JOIN dbo.domain_status ds WITH (NOLOCK) ON do.id_status = ds.id
                  LEFT JOIN dbo.domain_status dost WITH (NOLOCK) ON dostl.old_status = dost.id
                  LEFT JOIN dbo.domain_status dost2 WITH (NOLOCK) ON dostl.new_status = dost2.id
                  LEFT JOIN filters ON dostl.id_domain = filters.id_domain
                  LEFT JOIN dbo.account acc1 WITH (NOLOCK) ON do.id_account = acc1.id
                  LEFT JOIN dbo.account acc WITH (NOLOCK) ON dostl.id_account = acc.id
                  LEFT JOIN dbo.account_role acr WITH (NOLOCK) ON acc.id_role = acr.id
                  LEFT JOIN dbo.pay_type pt WITH (NOLOCK) ON acc.pay_type = pt.id
                  LEFT JOIN dbo.account_team act WITH (NOLOCK) ON acc.id_team = act.id
                  LEFT JOIN dbo.account acc2 WITH (NOLOCK) ON act.id_lead = acc2.id
                  LEFT JOIN dbo.domain_skip_reason skip WITH (NOLOCK) ON dostl.id_skip_reason = skip.id
                  LEFT JOIN dbo.domain_refusal_text refusal WITH (NOLOCK) ON dostl.id_refusal_text = refusal.id
                  LEFT JOIN links ON links.domain_id = dostl.id_domain AND links.id_account = do.id_account
                  LEFT JOIN dbo.domain_contact dc WITH (NOLOCK) on dc.id = dostl.id_refusal_contact
                  LEFT JOIN dbo.reject_reason r WITH (NOLOCK) on r.id = dc.id_reject_reason
                  LEFT JOIN dbo.contact_type ct WITH (NOLOCK) on dc.id_type = ct.id
WHERE cast(dostl.dt_created as date) >= '2022-05-01';
