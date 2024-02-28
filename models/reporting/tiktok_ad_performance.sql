{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name !~* 'Traffic' THEN 'Campaign Type: Overall Excl. Traffic'
    ELSE 'Campaign Type: Traffic'
END AS campaign_type_custom,
CASE WHEN campaign_name ~* 'Ulta' THEN 'Ulta'
    WHEN campaign_name !~* 'Ulta' THEN 'DTC'
END AS purchase_type,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
complete_payment_events as purchases,
complete_payment_value as revenue,
web_add_to_cart_events as atc
FROM {{ ref('tiktok_performance_by_ad') }}
