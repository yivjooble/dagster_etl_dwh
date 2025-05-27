SELECT DISTINCT e.id,
                e.country_code,
                ec.company_name,
                COALESCE(j.step, 0)    AS step,
                COALESCE(ss.doc, 0)    AS documents,
                COALESCE(sss.email, 0) AS confirm_email,
                e.register_device,
                e.moderation_status,
                date(e.date_created)   AS date
FROM employer_account.employer e
         LEFT JOIN employer_account.employer_cdp ec ON e.id_cdp = ec.id
         LEFT JOIN employer_account.job_step j ON e.id = j.id_employer
         LEFT JOIN (SELECT statistics_employer_moderation.id_employer,
                           count(*)                                         AS doc,
                           max(statistics_employer_moderation.date_created) AS max
                    FROM employer_account.statistics_employer_moderation
                    WHERE statistics_employer_moderation.moderation_status = 0
                      AND statistics_employer_moderation.action_type = 3
                    GROUP BY statistics_employer_moderation.id_employer) ss ON e.id = ss.id_employer
         LEFT JOIN (SELECT statistics_employer_moderation.id_employer,
                           count(*)                                         AS email,
                           max(statistics_employer_moderation.date_created) AS max
                    FROM employer_account.statistics_employer_moderation
                    WHERE statistics_employer_moderation.moderation_status = 1
                      AND statistics_employer_moderation.action_type = 3
                    GROUP BY statistics_employer_moderation.id_employer) sss ON e.id = sss.id_employer
WHERE e.country_code = 'rs'
;