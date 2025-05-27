SET NOCOUNT ON;

Select domain_tag.id                    as domain_tag_id,
       domain_tag.id_domain             as domain_id,
       cast(domain_tag.created as date) as date_created,
       domain_tag.id_tag                as tag_id,
       tag.name                         as tag_name
FROM dbo.domain_tag WITH (NOLOCK)
         LEFT JOIN dbo.tag WITH (NOLOCK) on domain_tag.id_tag = tag.id
where cast(domain_tag.created as date) between :to_sqlcode_date_or_datediff_start and :to_sqlcode_date_or_datediff_end
;