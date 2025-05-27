With clicks as (
    Select 'adwords' as source, ad.Campaign as campaign_name, sum(ad.Clicks) as clicks
    FROM PublicStatistic.dbo.adwords_2022 ad with (nolock)
    Where ad.Day >= '2021-01-01'
    Group by ad.Campaign
    union all
    Select 'facebook' as source, fb.campaign_name+'_facebook' as campaign_name, sum(fb.inline_link_clicks) as clicks
    FROM PublicStatistic.dbo.facebook_2018 fb with (nolock)
    Where fb.date_start >= '2021-01-01'
    Group by fb.campaign_name+'_facebook'
    union all
    Select 'bing' as source, bb.campaign_name+'_bing' as campaign_name, sum(bb.clicks) as clicks
    FROM PublicStatistic.dbo.bing bb with (nolock)
    Where bb.date >= '2021-01-01'
    Group by bb.campaign_name+'_bing'
)
Select clicks.source,
       clicks.campaign_name,
       clicks.clicks,
       Marketing.dbo.get_label_id_from_campaign(clicks.campaign_name) as labels_and_channels_id,
       labels_and_channels.name as traffic_source_name,
       labels_and_channels.type,
       labels_and_channels.label,
       labels_and_channels.source as labels_source,
       labels_and_channels.device
FROM clicks
left join Marketing.legent.labels_and_channels  on labels_and_channels.id = Marketing.dbo.get_label_id_from_campaign(clicks.campaign_name)
;