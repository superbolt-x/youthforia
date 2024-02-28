{{ config (
    alias = target.database + '_blended_performance'
)}}

SELECT 'Facebook - Excl. React' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(link_clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','facebook_ad_performance') }}
WHERE campaign_name !~* 'reac'
GROUP BY 1,2,3

UNION ALL

SELECT 'Facebook - Reactivaion' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(link_clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','facebook_ad_performance') }}
WHERE campaign_name ~* 'reac'
GROUP BY 1,2,3

UNION ALL

SELECT 'Google - Excl. Branded' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','googleads_campaign_performance') }}
WHERE campaign_type_default != 'Campaign Type: Search Branded'
GROUP BY 1,2,3

UNION ALL

SELECT 'Google - Branded' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','googleads_campaign_performance') }}
WHERE campaign_type_default = 'Campaign Type: Search Branded'
GROUP BY 1,2,3

UNION ALL

SELECT 'TikTok - DTC' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','tiktok_ad_performance') }}
WHERE purchase_type = 'DTC'
GROUP BY 1,2,3

UNION ALL

SELECT 'TikTok - Ulta' as channel, date, date_granularity,
  COALESCE(SUM(spend),0) as spend,
  COALESCE(SUM(revenue),0) as revenue,
  COALESCE(SUM(purchases),0) as purchases,
  COALESCE(SUM(clicks),0) as clicks,
  COALESCE(SUM(impressions),0) as impressions
FROM {{ source('reporting','tiktok_ad_performance') }}
WHERE purchase_type = 'Ulta'
GROUP BY 1,2,3
