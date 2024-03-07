{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
CASE WHEN campaign_name !~* 'Traffic' AND campaign_name !~* 'Reactivation' THEN 'Campaign Type: Overall Excl. React & Traffic'
    WHEN campaign_name ~* 'Traffic' THEN 'Campaign Type: Traffic'
    WHEN campaign_name ~* 'Reactivation' THEN 'Campaign Type: Reactivation'
END AS campaign_type_custom,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
preview_links,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_ad') }}
LEFT JOIN (SELECT link as preview_links, ad_id FROM {{ source('gsheet_raw','meta_preview_links_automated') }}) USING(ad_id)
