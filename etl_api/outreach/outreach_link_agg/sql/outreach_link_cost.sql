Select cast (getdate() as datetime)     as report_update_time,
       sc.id_link,
       link.created as link_created_date,
       sc.id_service_flags,
       flags.name   as name_service_flags,
       sc.cost,
       sc.currency,
       sc.cost_usd
FROM dbo.link_service_cost sc with (nolock)
left join dbo.link_service_flags flags with (nolock) on sc.id_service_flags=flags.id
left join dbo.link with (nolock) on link.id = sc.id_link;