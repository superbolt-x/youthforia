{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'Branded' THEN 'Campaign Type: Search Branded'
    WHEN campaign_name !~* 'Branded' THEN 'Campaign Type: Search Unbranded'
    ELSE 'Campaign Type: Other'
END as campaign_type_default,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue

FROM {{ ref('googleads_performance_by_ad') }}
