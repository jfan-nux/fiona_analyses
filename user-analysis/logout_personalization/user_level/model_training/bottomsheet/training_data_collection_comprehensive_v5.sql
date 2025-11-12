`-- Comprehensive Training Data Collection for App Download Bottom Sheet Experiment
-- Version 5 - Combines bottomsheet barebone with full CX360 features
-- Date Range: 2025-06-10 to 2025-06-17
-- Table: proddb.fionafan.logged_out_personalization_training_comprehensive_v5

SET observed_experiment_start_date = '2025-06-10'::DATE; 
SET long_term_start_date = $observed_experiment_start_date - INTERVAL '180 DAYS';
SET long_term_end_date = $observed_experiment_start_date - INTERVAL '1 DAYS';
SET short_term_start_date = $observed_experiment_start_date - INTERVAL '28 DAYS';
SET short_term_end_date = $observed_experiment_start_date - INTERVAL '1 DAYS';

/*
====================================================================================
Raw Web Session Data with Browser Detection
====================================================================================
*/

select platform, count(1) from proddb.fionafan.raw_web_sessions_data_comprehensive_v5 group by all;

CREATE OR REPLACE  TABLE proddb.fionafan.raw_web_sessions_data_comprehensive_v5 AS (

SELECT 
platform 
, dd_device_id 
, dd_session_id 
, consumer_id 
, timestamp AS session_first_timestamp 
, page 
, utm_campaign
, utm_source
, utm_medium
, referrer 
, CASE 
     WHEN referrer LIKE '%doordash.%' THEN 'Direct'
     WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND NULLIF(referrer, '') IS NULL THEN 'Direct'
     WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND (NULLIF(referrer, '') LIKE '%google.%' OR NULLIF(referrer, '') LIKE '%bing.%' OR NULLIF(referrer, '') LIKE '%search.yahoo.%') THEN 'Organic Search'
     WHEN NULLIF(utm_campaign, '') = 'gpa' THEN 'Organic Search'
     WHEN NULLIF(utm_medium, '') = 'Paid_Social' THEN 'Paid Social'
     WHEN NULLIF(utm_medium, '') = 'SEMb' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'SEMu' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'SEMc' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'PLA' THEN 'Paid Media'
     WHEN LOWER(NULLIF(utm_medium, '')) = 'email' THEN 'Email'
     WHEN LOWER(NULLIF(utm_medium, '')) LIKE '%enterprise%' OR LOWER(NULLIF(utm_source, '')) IN ('partner-link') THEN 'Partners'
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('affiliate') THEN 'Affiliate' 
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('web_display') THEN 'Paid Media'
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('video') THEN 'Paid Media'
     ELSE 'Other' END AS traffic_type 
, context_user_agent
, CASE
    -- In-App Social Media Browsers
    WHEN context_user_agent ILIKE '%FBAV%' OR context_user_agent ILIKE '%FB_IAB%' THEN 'Facebook In-App'
    WHEN context_user_agent ILIKE '%Instagram%' THEN 'Instagram In-App'
    WHEN context_user_agent ILIKE '%TikTok%' THEN 'TikTok In-App'
    WHEN context_user_agent ILIKE '%Twitter%' THEN 'Twitter In-App'
    WHEN context_user_agent ILIKE '%Snapchat%' THEN 'Snapchat In-App'
    WHEN context_user_agent ILIKE '%Pinterest%' THEN 'Pinterest In-App'
    WHEN context_user_agent ILIKE '%LinkedIn%' THEN 'LinkedIn In-App'
    -- Major Browsers
    WHEN context_user_agent ILIKE '%OPR%' OR context_user_agent ILIKE '%Opera%' THEN 'Opera'
    WHEN context_user_agent ILIKE '%Edg%' THEN 'Edge'
    WHEN context_user_agent ILIKE '%Chrome%' AND context_user_agent NOT ILIKE '%Edg%' AND context_user_agent NOT ILIKE '%OPR%' THEN 'Chrome'
    WHEN context_user_agent ILIKE '%CriOS%' THEN 'Chrome iOS'
    WHEN context_user_agent ILIKE '%FxiOS%' THEN 'Firefox iOS'
    WHEN context_user_agent ILIKE '%Firefox%' THEN 'Firefox'
    WHEN context_user_agent ILIKE '%Safari%' AND context_user_agent NOT ILIKE '%Chrome%' AND context_user_agent NOT ILIKE '%CriOS%' THEN 'Safari'
    WHEN context_user_agent ILIKE '%SamsungBrowser%' THEN 'Samsung Internet'
    WHEN context_user_agent ILIKE '%UCBrowser%' THEN 'UC Browser'
    WHEN context_user_agent ILIKE '%YaBrowser%' THEN 'Yandex'
    WHEN context_user_agent ILIKE '%Brave%' THEN 'Brave'
    WHEN context_user_agent ILIKE '%Vivaldi%' THEN 'Vivaldi'
    WHEN context_user_agent ILIKE '%DuckDuckGo%' THEN 'DuckDuckGo'
    WHEN context_user_agent ILIKE '%QQBrowser%' THEN 'QQ Browser'
    WHEN context_user_agent ILIKE '%Maxthon%' THEN 'Maxthon'
    WHEN context_user_agent ILIKE '%Sogou%' THEN 'Sogou'
    WHEN context_user_agent ILIKE '%Puffin%' THEN 'Puffin'
    WHEN context_user_agent ILIKE '%Naver%' THEN 'Naver Whale'
    ELSE 'Other'
  END AS browser_name
, CASE
    WHEN context_user_agent ILIKE '%Android%' THEN 'Android'
    WHEN context_user_agent ILIKE '%iPhone%' 
      OR context_user_agent ILIKE '%iPad%' 
      OR context_user_agent ILIKE '%iPod%' 
      OR context_user_agent ILIKE '%iOS%' THEN 'iOS'
    ELSE 'Other/Web'
  END AS inferred_os
FROM 
    (
    SELECT 
    platform 
    , dd_device_id 
    , dd_session_id 
    , consumer_id 
    , iguazu_timestamp AS timestamp
    , 'Store' AS page 
    , NULLIF(context_campaign_name, '') AS utm_campaign 
    , NULLIF(context_campaign_source, '') AS utm_source 
    , NULLIF(context_campaign_medium, '') AS utm_medium 
    , NULLIF(context_page_referrer, '') AS referrer 
    , context_user_agent 
    FROM iguazu.server_events_production.store_page_load_consumer 
    WHERE context_user_agent NOT ILIKE '%bot%'
        AND context_user_agent NOT ILIKE '%prerender%'
        AND context_user_agent NOT ILIKE '%read-aloud%'
        AND iguazu_timestamp::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
    UNION ALL 
    SELECT 
    platform 
    , dd_device_id 
    , dd_session_id 
    , consumer_id 
    , iguazu_timestamp AS timestamp 
    , 'Home' AS page 
    , NULLIF(context_campaign_name, '') AS utm_campaign 
    , NULLIF(context_campaign_source, '') AS utm_source 
    , NULLIF(context_campaign_medium, '') AS utm_medium 
    , NULLIF(context_page_referrer, '') AS referrer 
    , context_user_agent 
    FROM iguazu.consumer.home_page_view 
    WHERE context_user_agent NOT ILIKE '%bot%'
        AND context_user_agent NOT ILIKE '%prerender%'
        AND context_user_agent NOT ILIKE '%read-aloud%'
        AND iguazu_timestamp::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
    UNION ALL 
    SELECT 
    platform 
    , dd_device_id 
    , dd_session_id 
    , consumer_id 
    , iguazu_timestamp AS timestamp 
    , 'Explore' AS page 
    , NULLIF(context_campaign_name, '') AS utm_campaign 
    , NULLIF(context_campaign_source, '') AS utm_source 
    , NULLIF(context_campaign_medium, '') AS utm_medium 
    , NULLIF(context_page_referrer, '') AS referrer 
    , context_user_agent 
    FROM iguazu.server_events_production.store_content_page_load  
    WHERE context_user_agent NOT ILIKE '%bot%'
        AND context_user_agent NOT ILIKE '%prerender%'
        AND context_user_agent NOT ILIKE '%read-aloud%'
        AND iguazu_timestamp::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
    UNION ALL 
    SELECT 
    platform 
    , dd_device_id 
    , dd_session_id 
    , consumer_id 
    , iguazu_timestamp AS timestamp 
    , 'Business' AS page 
    , NULLIF(context_campaign_name, '') AS utm_campaign 
    , NULLIF(context_campaign_source, '') AS utm_source 
    , NULLIF(context_campaign_medium, '') AS utm_medium 
    , NULLIF(context_page_referrer, '') AS referrer 
    , context_user_agent 
    FROM iguazu.consumer.business_menu_page_load 
    WHERE context_user_agent NOT ILIKE '%bot%'
        AND context_user_agent NOT ILIKE '%prerender%'
        AND context_user_agent NOT ILIKE '%read-aloud%'
        AND iguazu_timestamp::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
    UNION ALL 
    SELECT 
    platform 
    , dd_device_id 
    , dd_session_id 
    , consumer_id 
    , iguazu_timestamp AS timestamp 
    , 'Product' AS page 
    , NULLIF(context_campaign_name, '') AS utm_campaign 
    , NULLIF(context_campaign_source, '') AS utm_source 
    , NULLIF(context_campaign_medium, '') AS utm_medium 
    , NULLIF(context_page_referrer, '') AS referrer 
    , context_user_agent 
    FROM iguazu.server_events_production.product_display_page_view 
    WHERE context_user_agent NOT ILIKE '%bot%'
        AND context_user_agent NOT ILIKE '%prerender%'
        AND context_user_agent NOT ILIKE '%read-aloud%'
        AND iguazu_timestamp::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
    )
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_session_id, dd_device_id ORDER BY timestamp ASC) = 1 
)
;


CREATE OR REPLACE TABLE proddb.fionafan.raw_mobile_sessions_data AS (


WITH base AS (
  SELECT
    platform,
    dd_device_id,
    /* fact_unique_visitors_full_pt does not contain a session id; keeping a placeholder for schema parity */
    CAST(NULL AS VARCHAR) AS dd_session_id,
    user_id AS consumer_id,
    first_timestamp AS session_first_timestamp,
    /* Map the first event to a simplified "page" label */
    CASE
      WHEN LOWER(first_event) LIKE '%store_page_load%' THEN 'Store'
      WHEN LOWER(first_event) LIKE '%home%' THEN 'Home'
      WHEN LOWER(first_event) LIKE '%store_content%' THEN 'Explore'
      WHEN LOWER(first_event) LIKE '%business_menu%' THEN 'Business'
      WHEN LOWER(first_event) LIKE '%product%' OR LOWER(first_event) LIKE '%item%' THEN 'Product'
      ELSE 'Other'
    END AS page,
    /* UTM fields and referrer are not present in fact_unique_visitors_full_pt; preserve columns as NULLs */
    CAST(NULL AS VARCHAR) AS utm_campaign,
    CAST(NULL AS VARCHAR) AS utm_source,
    CAST(NULL AS VARCHAR) AS utm_medium,
    CAST(NULL AS VARCHAR) AS referrer,
    /* Derive traffic_type primarily from CHANNEL / MEDIA_TYPE / SUBCHANNEL where available */
    CASE
      WHEN channel ILIKE '%Direct%' THEN 'Direct'
      WHEN channel ILIKE '%Organic_Search%' THEN 'Organic Search'
      WHEN channel ILIKE '%Paid_Social%' THEN 'Paid Social'
      WHEN LOWER(subchannel) IN ('web_display', 'video') THEN 'Paid Media'
      WHEN LOWER(media_type) IN ('semb', 'semu', 'semc', 'pla') THEN 'Paid Media'
      WHEN LOWER(subchannel) = 'email' THEN 'Email'
      WHEN LOWER(subchannel) LIKE '%enterprise%' OR LOWER(partner) = 'partner-link' THEN 'Partners'
      WHEN LOWER(subchannel) = 'affiliate' THEN 'Affiliate'
      ELSE COALESCE(channel, 'Other')
    END AS traffic_type,
    user_agent AS context_user_agent,
    /* Browser detection from user agent */
    CASE
      -- In-App Social Media Browsers
      WHEN user_agent ILIKE '%FBAV%' OR user_agent ILIKE '%FB_IAB%' THEN 'Facebook In-App'
      WHEN user_agent ILIKE '%Instagram%' THEN 'Instagram In-App'
      WHEN user_agent ILIKE '%TikTok%' THEN 'TikTok In-App'
      WHEN user_agent ILIKE '%Twitter%' THEN 'Twitter In-App'
      WHEN user_agent ILIKE '%Snapchat%' THEN 'Snapchat In-App'
      WHEN user_agent ILIKE '%Pinterest%' THEN 'Pinterest In-App'
      WHEN user_agent ILIKE '%LinkedIn%' THEN 'LinkedIn In-App'
      -- Major Browsers
      WHEN user_agent ILIKE '%OPR%' OR user_agent ILIKE '%Opera%' THEN 'Opera'
      WHEN user_agent ILIKE '%Edg%' THEN 'Edge'
      WHEN user_agent ILIKE '%Chrome%' AND user_agent NOT ILIKE '%Edg%' AND user_agent NOT ILIKE '%OPR%' THEN 'Chrome'
      WHEN user_agent ILIKE '%CriOS%' THEN 'Chrome iOS'
      WHEN user_agent ILIKE '%FxiOS%' THEN 'Firefox iOS'
      WHEN user_agent ILIKE '%Firefox%' THEN 'Firefox'
      WHEN user_agent ILIKE '%Safari%' AND user_agent NOT ILIKE '%Chrome%' AND user_agent NOT ILIKE '%CriOS%' THEN 'Safari'
      WHEN user_agent ILIKE '%SamsungBrowser%' THEN 'Samsung Internet'
      WHEN user_agent ILIKE '%UCBrowser%' THEN 'UC Browser'
      WHEN user_agent ILIKE '%YaBrowser%' THEN 'Yandex'
      WHEN user_agent ILIKE '%Brave%' THEN 'Brave'
      WHEN user_agent ILIKE '%Vivaldi%' THEN 'Vivaldi'
      WHEN user_agent ILIKE '%DuckDuckGo%' THEN 'DuckDuckGo'
      WHEN user_agent ILIKE '%QQBrowser%' THEN 'QQ Browser'
      WHEN user_agent ILIKE '%Maxthon%' THEN 'Maxthon'
      WHEN user_agent ILIKE '%Sogou%' THEN 'Sogou'
      WHEN user_agent ILIKE '%Puffin%' THEN 'Puffin'
      WHEN user_agent ILIKE '%Naver%' THEN 'Naver Whale'
      ELSE 'Other'
    END AS browser_name,
    event_date,
    -- Visitor type flags
    UNIQUE_VISITOR,
    UNIQUE_STORE_CONTENT_PAGE_VISITOR,
    UNIQUE_STORE_PAGE_VISITOR,
    UNIQUE_ORDER_CART_PAGE_VISITOR,
    UNIQUE_CHECKOUT_PAGE_VISITOR,
    UNIQUE_PURCHASER,
    UNIQUE_APP_INSTALLER,
    UNIQUE_CORE_VISITOR,
    HOME_PAGE_VISITOR,
    MOBILE_SPLASH_PAGE_VISITOR,
    MULTI_STORE_VISITOR
  FROM proddb.public.fact_unique_visitors_full_pt

  WHERE
    /* Exclude bots using explicit flag and a light UA filter similar to the original */
    COALESCE(is_bot, 0) = 0
    AND user_agent NOT ILIKE '%prerender%'
    AND user_agent NOT ILIKE '%read-aloud%'
    /* Default date range: last 7 days */
    AND event_date BETWEEN DATEADD(day, -180, DATE '2025-06-10') AND DATEADD(day, -1, DATE '2025-06-10')
)
SELECT *
FROM base
);

/*
====================================================================================
Deep Link and Singular Events for Device-User Linking
====================================================================================
*/

CREATE OR REPLACE TABLE proddb.fionafan.deep_link_singular_device_user_mapping AS

WITH 
-- Deep link events: extract all device IDs from a single scan
deep_link_all_ids AS (
  SELECT
    CAST(IGUAZU_USER_ID AS VARCHAR) AS user_id,
    IGUAZU_TIMESTAMP AS event_ts,
    CONTEXT_DEVICE_ID AS context_device_id,
    /* dd_device_id param is URL-encoded as dd_device_id%3D */
    NULLIF(SPLIT_PART(SPLIT_PART(DEEP_LINK_URL, 'dd_device_id%3D', 2), '%', 1), '') AS device_id_from_dd_param,
    /* web_consumer_id appears unencoded in some links */
    NULLIF(SPLIT_PART(SPLIT_PART(DEEP_LINK_URL, 'web_consumer_id=', 2), '&', 1), '') AS device_id_from_web_param
  FROM iguazu.server_events_production.m_deep_link
  WHERE IGUAZU_TIMESTAMP::DATE BETWEEN $long_term_start_date AND $long_term_end_date
),
-- Unpivot to get one row per device_id per event (avoiding duplicates with UNION, not UNION ALL)
deep_link_union AS (
  SELECT DISTINCT device_id, user_id, event_ts
  FROM (
    SELECT context_device_id AS device_id, user_id, event_ts
    FROM deep_link_all_ids
    WHERE context_device_id IS NOT NULL AND context_device_id <> ''
    UNION
    SELECT device_id_from_dd_param AS device_id, user_id, event_ts
    FROM deep_link_all_ids
    WHERE device_id_from_dd_param IS NOT NULL AND device_id_from_dd_param <> ''
    UNION
    SELECT device_id_from_web_param AS device_id, user_id, event_ts
    FROM deep_link_all_ids
    WHERE device_id_from_web_param IS NOT NULL AND device_id_from_web_param <> ''
  )
),
deep_link_final AS (
  SELECT device_id, user_id, CAST(NULL AS VARCHAR) AS consumer_id, event_ts
  FROM deep_link_union
),
-- 3) Singular mobile events: take DD_DEVICE_ID as device_id as-is; include consumer_id
singular_raw AS (
  SELECT
    DD_DEVICE_ID AS device_id,
    CAST(NULL AS VARCHAR) AS user_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    COALESCE(EVENT_TIMESTAMP, TO_TIMESTAMP_NTZ(EVENT_DATE)) AS event_ts
  FROM edw.growth.fact_singular_mobile_events
  WHERE EVENT_DATE BETWEEN $long_term_start_date AND $long_term_end_date
    AND DD_DEVICE_ID IS NOT NULL 
    AND DD_DEVICE_ID <> ''
),
combined AS (
  SELECT 'deep_link' AS source, device_id, consumer_id, user_id, event_ts 
  FROM deep_link_final
  UNION ALL
  SELECT 'singular' AS source, device_id, consumer_id, user_id, event_ts 
  FROM singular_raw
),
rolled AS (
  SELECT
    source,
    device_id,
    consumer_id,
    user_id,
    MIN(event_ts) AS first_seen_at,
    MAX(event_ts) AS last_seen_at,
    COUNT(*) AS event_count
  FROM combined
  WHERE device_id IS NOT NULL AND device_id <> ''
  GROUP BY 1,2,3,4
)
SELECT
  source,
  device_id,
  consumer_id,
  user_id,
  first_seen_at,
  last_seen_at,
  event_count
FROM rolled;

/*
====================================================================================
Fortified Web Visitor Data
====================================================================================
*/

select platform, count(1), count(distinct dd_device_id), count(distinct consumer_id) 
from proddb.fionafan.raw_web_sessions_data_comprehensive_v5 group by all;
select count(1) from (
    select
dd_device_id 
, platform 
, MAX(consumer_id) AS consumer_id 
FROM proddb.fionafan.raw_web_sessions_data_comprehensive_v5
GROUP BY 1,2 
);
CREATE OR REPLACE  TABLE proddb.fionafan.fortified_web_visitor_data_comprehensive_v5 AS (

WITH device_id AS (

select dd_device_id 
, platform 
, MAX(consumer_id) AS consumer_id_logged_in_status 
, max(case when inferred_os = 'Android' then 1 else 0 end) as is_android
, max(case when inferred_os = 'iOS' then 1 else 0 end) as is_ios
, max(case when inferred_os = 'Other/Web' then 1 else 0 end) as is_web
FROM proddb.fionafan.raw_web_sessions_data_comprehensive_v5
group by 1,2)
, joined AS (
SELECT 
a.dd_device_id 
, a.platform 
, a.consumer_id_logged_in_status
, dc.consumer_id AS consumer_id_checked_in_log_in_records 
FROM device_id a 
LEFT JOIN (
    SELECT 
    dd_device_id
    , user_id
    , ts 
    FROM proddb.public.fact_consumer_frontend_login_and_signup_events 
    WHERE ts::DATE BETWEEN $long_term_start_date - INTERVAL '181 DAYS' AND $long_term_end_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY ts DESC) = 1 
          ) b 
    ON a.dd_device_id::VARCHAR = b.dd_device_id::VARCHAR 
LEFT JOIN edw.consumer.dimension_consumers dc 
    ON dc.user_id::VARCHAR = b.user_id::VARCHAR 
)
SELECT 
dd_device_id 
, platform,
, is_android
, is_ios
, is_web
, CASE WHEN consumer_id_logged_in_status IS NOT NULL THEN consumer_id_logged_in_status ELSE consumer_id_checked_in_log_in_records END AS consumer_id 
, CASE WHEN consumer_id IS NOT NULL THEN True ELSE False END AS has_associated_consumer_id 
FROM joined 
)
;

select avg(has_associated_consumer_id::integer) from proddb.fionafan.fortified_web_visitor_data_comprehensive_v5;
select platform, count(distinct dd_device_id) from proddb.fionafan.fortified_web_visitor_data_comprehensive_v5 group by all;
/*
====================================================================================
Web Session Data - Recent 28 and 90 Days
====================================================================================
*/

/*
====================================================================================
Mobile Visitor Metrics - Separate Table (runs in parallel)
====================================================================================
*/
CREATE OR REPLACE TABLE proddb.fionafan.mobile_visitor_metrics_comprehensive_v5 AS
SELECT 
    dd_device_id,
    -- 28 day flags
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_STORE_CONTENT_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_store_content_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_STORE_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_store_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_ORDER_CART_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_order_cart_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_CHECKOUT_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_checkout_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_PURCHASER = 1 THEN True ELSE False END) AS device_id_unique_purchaser_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_APP_INSTALLER = 1 THEN True ELSE False END) AS device_id_unique_app_installer_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND UNIQUE_CORE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_core_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND HOME_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_home_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND MOBILE_SPLASH_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_mobile_splash_page_visitor_recent_28_days,
    MAX(CASE WHEN event_date BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND MULTI_STORE_VISITOR = 1 THEN True ELSE False END) AS device_id_multi_store_visitor_recent_28_days,
    -- 90 day flags
    MAX(CASE WHEN UNIQUE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_visitor_recent_90_days,
    MAX(CASE WHEN UNIQUE_STORE_CONTENT_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_store_content_page_visitor_recent_90_days,
    MAX(CASE WHEN UNIQUE_STORE_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_store_page_visitor_recent_90_days,
    MAX(CASE WHEN UNIQUE_ORDER_CART_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_order_cart_page_visitor_recent_90_days,
    MAX(CASE WHEN UNIQUE_CHECKOUT_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_checkout_page_visitor_recent_90_days,
    MAX(CASE WHEN UNIQUE_PURCHASER = 1 THEN True ELSE False END) AS device_id_unique_purchaser_recent_90_days,
    MAX(CASE WHEN UNIQUE_APP_INSTALLER = 1 THEN True ELSE False END) AS device_id_unique_app_installer_recent_90_days,
    MAX(CASE WHEN UNIQUE_CORE_VISITOR = 1 THEN True ELSE False END) AS device_id_unique_core_visitor_recent_90_days,
    MAX(CASE WHEN HOME_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_home_page_visitor_recent_90_days,
    MAX(CASE WHEN MOBILE_SPLASH_PAGE_VISITOR = 1 THEN True ELSE False END) AS device_id_mobile_splash_page_visitor_recent_90_days,
    MAX(CASE WHEN MULTI_STORE_VISITOR = 1 THEN True ELSE False END) AS device_id_multi_store_visitor_recent_90_days
FROM proddb.fionafan.raw_mobile_sessions_data
WHERE event_date BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date
GROUP BY dd_device_id
;

/*
====================================================================================
Table 1: Web Session & Login Metrics (Fast - lighter tables)
====================================================================================
*/
CREATE OR REPLACE TABLE proddb.fionafan.web_session_login_metrics_comprehensive_v5 AS

SELECT 
    a.dd_device_id,
    -- 28 day metrics
    MAX(CASE WHEN ws.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN True ELSE False END) AS device_id_is_active_recent_28_days,
    COUNT(DISTINCT CASE WHEN ws.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN ws.dd_session_id END) AS device_id_sessions_recent_28_days,
    MAX(CASE WHEN l.ts::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN True ELSE False END) AS device_id_has_logged_in_recent_28_days,
    -- 90 day metrics
    MAX(CASE WHEN ws.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_90_days,
    COUNT(DISTINCT ws.dd_session_id) AS device_id_sessions_recent_90_days,
    MAX(CASE WHEN l.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_90_days
FROM proddb.fionafan.fortified_web_visitor_data_comprehensive_v5 a
LEFT JOIN proddb.fionafan.raw_web_sessions_data_comprehensive_v5 ws 
    ON a.dd_device_id = ws.dd_device_id
    AND ws.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date
LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events l
    ON a.dd_device_id = l.dd_device_id
    AND l.ts::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date
GROUP BY a.dd_device_id
;

/*
====================================================================================
Table 2: Order & Address Metrics (Slower - heavier tables)
====================================================================================
*/
CREATE OR REPLACE TABLE proddb.fionafan.order_address_metrics_comprehensive_v5 AS
SELECT 
    a.dd_device_id,
    -- 28 day metrics
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN dd.delivery_id END) AS device_id_orders_recent_28_days,
    MAX(CASE WHEN addr.iguazu_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND addr.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_28_days,
    MAX(CASE WHEN addr.iguazu_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND addr.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_28_days,
    -- 90 day metrics
    COUNT(DISTINCT dd.delivery_id) AS device_id_orders_recent_90_days,
    MAX(CASE WHEN addr.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_90_days,
    MAX(CASE WHEN addr.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_90_days
FROM proddb.fionafan.fortified_web_visitor_data_comprehensive_v5 a
LEFT JOIN edw.finance.dimension_deliveries dd
    ON dd.is_caviar != 1
        AND dd.is_filtered_core = 1
        AND dd.dd_device_id = a.dd_device_id
        AND dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date
LEFT JOIN iguazu.server_events_production.debug_address_create addr
    ON addr.dd_device_id = a.dd_device_id
        AND addr.iguazu_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date
GROUP BY a.dd_device_id
;

/*
====================================================================================
Final Combined Table - Joins all pre-aggregated metrics
====================================================================================
*/
CREATE OR REPLACE  TABLE proddb.fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_comprehensive_v5 AS 
SELECT 
    a.dd_device_id,
    a.platform,
    a.consumer_id,
    a.has_associated_consumer_id,
    a.is_android,
    a.is_ios,
    a.is_web,
    -- Web session & login metrics
    MAX(COALESCE(ws.device_id_is_active_recent_28_days, False)) AS device_id_is_active_recent_28_days,
    MAX(COALESCE(ws.device_id_sessions_recent_28_days, 0)) AS device_id_sessions_recent_28_days,
    MAX(COALESCE(ws.device_id_has_logged_in_recent_28_days, False)) AS device_id_has_logged_in_recent_28_days,
    MAX(COALESCE(ws.device_id_is_active_recent_90_days, False)) AS device_id_is_active_recent_90_days,
    MAX(COALESCE(ws.device_id_sessions_recent_90_days, 0)) AS device_id_sessions_recent_90_days,
    MAX(COALESCE(ws.device_id_has_logged_in_recent_90_days, False)) AS device_id_has_logged_in_recent_90_days,
    -- Order & address metrics
    MAX(COALESCE(oa.device_id_orders_recent_28_days, 0)) AS device_id_orders_recent_28_days,
    MAX(COALESCE(oa.device_id_has_logged_out_address_entry_recent_28_days, False)) AS device_id_has_logged_out_address_entry_recent_28_days,
    MAX(COALESCE(oa.device_id_has_logged_in_address_entry_recent_28_days, False)) AS device_id_has_logged_in_address_entry_recent_28_days,
    MAX(COALESCE(oa.device_id_orders_recent_90_days, 0)) AS device_id_orders_recent_90_days,
    MAX(COALESCE(oa.device_id_has_logged_out_address_entry_recent_90_days, False)) AS device_id_has_logged_out_address_entry_recent_90_days,
    MAX(COALESCE(oa.device_id_has_logged_in_address_entry_recent_90_days, False)) AS device_id_has_logged_in_address_entry_recent_90_days,
    -- Mobile visitor flags
    MAX(COALESCE(m.device_id_unique_visitor_recent_28_days, False)) AS device_id_unique_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_store_content_page_visitor_recent_28_days, False)) AS device_id_unique_store_content_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_store_page_visitor_recent_28_days, False)) AS device_id_unique_store_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_order_cart_page_visitor_recent_28_days, False)) AS device_id_unique_order_cart_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_checkout_page_visitor_recent_28_days, False)) AS device_id_unique_checkout_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_purchaser_recent_28_days, False)) AS device_id_unique_purchaser_recent_28_days,
    MAX(COALESCE(m.device_id_unique_app_installer_recent_28_days, False)) AS device_id_unique_app_installer_recent_28_days,
    MAX(COALESCE(m.device_id_unique_core_visitor_recent_28_days, False)) AS device_id_unique_core_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_home_page_visitor_recent_28_days, False)) AS device_id_home_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_mobile_splash_page_visitor_recent_28_days, False)) AS device_id_mobile_splash_page_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_multi_store_visitor_recent_28_days, False)) AS device_id_multi_store_visitor_recent_28_days,
    MAX(COALESCE(m.device_id_unique_visitor_recent_90_days, False)) AS device_id_unique_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_unique_store_content_page_visitor_recent_90_days, False)) AS device_id_unique_store_content_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_unique_store_page_visitor_recent_90_days, False)) AS device_id_unique_store_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_unique_order_cart_page_visitor_recent_90_days, False)) AS device_id_unique_order_cart_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_unique_checkout_page_visitor_recent_90_days, False)) AS device_id_unique_checkout_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_unique_purchaser_recent_90_days, False)) AS device_id_unique_purchaser_recent_90_days,
    MAX(COALESCE(m.device_id_unique_app_installer_recent_90_days, False)) AS device_id_unique_app_installer_recent_90_days,
    MAX(COALESCE(m.device_id_unique_core_visitor_recent_90_days, False)) AS device_id_unique_core_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_home_page_visitor_recent_90_days, False)) AS device_id_home_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_mobile_splash_page_visitor_recent_90_days, False)) AS device_id_mobile_splash_page_visitor_recent_90_days,
    MAX(COALESCE(m.device_id_multi_store_visitor_recent_90_days, False)) AS device_id_multi_store_visitor_recent_90_days
FROM proddb.fionafan.fortified_web_visitor_data_comprehensive_v5 a
LEFT JOIN proddb.fionafan.web_session_login_metrics_comprehensive_v5 ws ON a.dd_device_id = ws.dd_device_id
LEFT JOIN proddb.fionafan.order_address_metrics_comprehensive_v5 oa ON a.dd_device_id = oa.dd_device_id
LEFT JOIN proddb.fionafan.mobile_visitor_metrics_comprehensive_v5 m ON a.dd_device_id = m.dd_device_id
GROUP BY a.dd_device_id, a.platform, a.consumer_id, a.has_associated_consumer_id, a.is_android, a.is_ios, a.is_web
;
select count(1) from proddb.fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_comprehensive_v5;

/*
====================================================================================
Web Session Data - Most Recent Session
====================================================================================
*/

CREATE OR REPLACE  TABLE proddb.fionafan.raw_unique_web_device_id_add_recent_session_web_info_comprehensive_v5 AS 

WITH most_recent_sessions AS (
SELECT 
* 
FROM proddb.fionafan.raw_web_sessions_data_comprehensive_v5
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY session_first_timestamp DESC) = 1 
)
SELECT 
a.* 
, b.dd_session_id AS device_id_session_id_most_recent_session
, b.traffic_type AS device_id_general_traffic_type_most_recent_session 
, b.browser_name AS device_id_browser_most_recent_session 
, DATEDIFF('DAYS', b.session_first_timestamp, $observed_experiment_start_date - INTERVAL '1 DAY') AS device_id_recency_most_recent_session
, MAX(CASE WHEN dd.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_placed_order_most_recent_session 
, MAX(CASE WHEN b.consumer_id IS NOT NULL THEN True ELSE False END) AS device_id_is_auto_logged_in_most_recent_session 
, MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_most_recent_session  
, MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_most_recent_session 
, MAX(CASE WHEN e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_most_recent_session  
-- Deep link and Singular events aggregations
, SUM(CASE WHEN dl.source = 'deep_link' THEN dl.event_count ELSE 0 END) AS device_id_deep_link_event_count_total
, SUM(CASE WHEN dl.source = 'singular' THEN dl.event_count ELSE 0 END) AS device_id_singular_event_count_total
, MIN(CASE WHEN dl.source = 'deep_link' THEN DATEDIFF('DAYS', dl.last_seen_at, $observed_experiment_start_date - INTERVAL '1 DAY') END) AS device_id_deep_link_recency_days
, MIN(CASE WHEN dl.source = 'singular' THEN DATEDIFF('DAYS', dl.last_seen_at, $observed_experiment_start_date - INTERVAL '1 DAY') END) AS device_id_singular_recency_days
FROM proddb.fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_comprehensive_v5 a 
LEFT JOIN most_recent_sessions b 
    ON a.dd_device_id = b.dd_device_id 
LEFT JOIN edw.finance.dimension_deliveries dd 
    ON dd.is_caviar != 1 
    AND dd.is_filtered_core = 1 
    AND dd.dd_device_id = a.dd_device_id 
    AND dd.created_at BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS' 
LEFT JOIN iguazu.server_events_production.debug_address_create c 
    ON c.is_guest = True 
    AND c.dd_device_id = a.dd_device_id 
    AND c.iguazu_timestamp::DATE BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS' 
    AND b.dd_device_id IS NOT NULL 
LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events e 
    ON e.dd_device_id = a.dd_device_id 
    AND e.ts::DATE BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS' 
    AND b.dd_device_id IS NOT NULL 
LEFT JOIN proddb.fionafan.deep_link_singular_device_user_mapping dl
    ON replace(lower(CASE WHEN dl.device_id LIKE 'dx_%' THEN dl.device_id ELSE 'dx_'||dl.device_id END), '-') = 
       replace(lower(CASE WHEN a.dd_device_id LIKE 'dx_%' THEN a.dd_device_id ELSE 'dx_'||a.dd_device_id END), '-')
GROUP BY all
;

/*
====================================================================================
Historical Features with Consumer-Level Metrics and CX360 Attributes
====================================================================================
*/

CREATE OR REPLACE  TABLE proddb.fionafan.logged_out_personalization_historical_web_device_id_comprehensive_v5 AS 

WITH recent_behavior AS (
SELECT 
a.* 
, $observed_experiment_start_date AS date_summarized 
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('android', 'ios') THEN dd.delivery_id ELSE NULL END) AS consumer_id_in_app_orders_recent_28_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('desktop', 'desktop-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_desktop_web_orders_recent_28_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_mobile_web_orders_recent_28_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN dd.delivery_id ELSE NULL END) AS consumer_id_total_orders_recent_28_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '89 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('android', 'ios') THEN dd.delivery_id ELSE NULL END) AS consumer_id_in_app_orders_recent_90_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '89 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('desktop', 'desktop-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_desktop_web_orders_recent_90_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '89 DAYS' AND $long_term_end_date AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_mobile_web_orders_recent_90_days
, COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '89 DAYS' AND $long_term_end_date THEN dd.delivery_id ELSE NULL END) AS consumer_id_total_orders_recent_90_days 
FROM proddb.fionafan.raw_unique_web_device_id_add_recent_session_web_info_comprehensive_v5 a 
LEFT JOIN edw.finance.dimension_deliveries dd 
    ON dd.is_caviar != 1 
    AND dd.is_filtered_core = 1 
    AND dd.creator_id::VARCHAR = a.consumer_id::VARCHAR 
    AND dd.created_at::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
GROUP BY all
)

SELECT 
a.*
, b.country AS cx360_country
, b.is_current_dashpass AS cx360_is_current_dashpass
, b.is_ever_dashpass AS cx360_is_ever_dashpass
, b.is_fraud AS cx360_is_fraud
, b.is_suma_cluster_account_most_active_recent AS cx360_is_suma_cluster_account_most_active_recent
, b.total_main_visitor_count_lifetime AS cx360_total_main_visitor_count_lifetime
, b.store_page_visitor_count_lifetime AS cx360_store_page_visitor_count_lifetime
, b.add_item_visitor_count_lifetime AS cx360_add_item_visitor_count_lifetime
, b.mon_thu_order_count_ratio_lifetime AS cx360_mon_thu_order_count_ratio_lifetime
, b.mon_thu_order_count_lifetime AS cx360_mon_thu_order_count_lifetime
, b.fri_order_count_ratio_lifetime AS cx360_fri_order_count_ratio_lifetime
, b.fri_order_count_lifetime AS cx360_fri_order_count_lifetime
, b.sat_order_count_ratio_lifetime AS cx360_sat_order_count_ratio_lifetime
, b.sat_order_count_lifetime AS cx360_sat_order_count_lifetime
, b.sun_order_count_ratio_lifetime AS cx360_sun_order_count_ratio_lifetime
, b.sun_order_count_lifetime AS cx360_sun_order_count_lifetime
, b.lunch_count_lifetime AS cx360_lunch_count_lifetime
, b.breakfast_count_lifetime AS cx360_breakfast_count_lifetime 
, b.dinner_count_lifetime AS cx360_dinner_count_lifetime 
, b.early_morning_count_lifetime AS cx360_early_morning_count_lifetime
, b.late_night_count_lifetime AS cx360_late_night_count_lifetime 
, b.lifestage AS cx360_lifestage 
, b.tenure_days AS cx360_lifetime_days 
, b.recency_order_frequency_category AS cx360_recency_order_frequency_category
, b.order_recency_category AS cx360_order_recency_category 
, b.freq_category AS cx360_freq_category 
, b.avg_order_interval_days AS cx360_avg_order_interval_days 
, b.order_count_lifetime AS cx360_order_count_lifetime
, b.avg_vp_lifetime AS cx360_avg_vp_lifetime 
, b.nv_orders_count_lifetime AS cx360_nv_orders_count_lifetime 
FROM recent_behavior a 
LEFT JOIN edw.growth.cx360_model_snapshot_dlcopy b 

    ON a.consumer_id = b.consumer_id 
    AND a.consumer_id IS NOT NULL 
    AND b.snapshot_date::DATE = DATE_TRUNC('WEEK', $long_term_end_date)::DATE - 1
;

/*
====================================================================================
Real-Time Experiment Data
====================================================================================
*/
CREATE OR REPLACE  TABLE proddb.fionafan.logged_out_personalization_real_time_web_device_id_comprehensive_v5 AS 
WITH orders AS (
SELECT 
dd.created_at AS event_ts 
, dd.dd_device_id 
, dd.creator_id AS consumer_id 
, dd.delivery_id 
, dd.submit_platform 
, dd.is_first_ordercart_dd 
FROM edw.finance.dimension_deliveries dd 
WHERE dd.is_caviar != 1 
    AND dd.is_filtered_core = 1 
    AND dd.created_at::DATE = $observed_experiment_start_date 
)

, web_to_app AS (
SELECT 
dd_device_id AS app_device_id
, CASE 
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(event_properties,'dd_device_id%3D',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(event_properties,'dd_device_id%3D',2),'%',1)
    ELSE NULLIF(SPLIT_PART(SPLIT_PART(event_properties,'web_consumer_id%3D',2),'%',1), '')
  END AS mweb_device_id 
, event_timestamp  
FROM edw.growth.fact_singular_mobile_events 
WHERE 
    (event_properties LIKE '%web_consumer_id%'
    OR event_properties LIKE '%dd_device_id%')
    AND event_properties LIKE '%mobile_web_to_app%'
    AND event_timestamp::DATE BETWEEN $observed_experiment_start_date - INTERVAL '28 DAYS' AND $observed_experiment_start_date 

UNION ALL 

SELECT
iguazu_other_properties:dd_device_id::VARCHAR AS app_device_id
, CASE 
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1)
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id%3D',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id%3D',2),'%',1)
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id=',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id=',2),'%',1)
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id=',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id=',2),'%',1)
  ELSE NULL 
  END AS mweb_device_id 
, iguazu_timestamp AS event_timestamp 
FROM iguazu.server_events_production.m_deep_link 
WHERE 
    (deep_link_url LIKE '%web_consumer_id%'
    OR deep_link_url LIKE '%dd_device_id%')
    AND iguazu_timestamp::DATE BETWEEN $observed_experiment_start_date - INTERVAL '28 DAYS' AND $observed_experiment_start_date
)

, consolidated AS (
/* In-App Orders Attributable to MWeb dd_device_id */
SELECT 
a.event_ts 
, a.consumer_id 
, a.delivery_id 
, wta.mweb_device_id AS device_id 
, 'MWeb->App' AS attribution 
, a.is_first_ordercart_dd 
FROM orders a 
JOIN web_to_app wta 
    ON wta.app_device_id::VARCHAR = a.dd_device_id::VARCHAR 
    AND wta.event_timestamp BETWEEN a.event_ts - INTERVAL '28 DAYS' AND a.event_ts
WHERE LOWER(a.submit_platform) IN ('ios', 'android')
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.event_ts, a.delivery_id ORDER BY wta.event_timestamp DESC) = 1

UNION ALL 

SELECT 
event_ts 
, consumer_id 
, delivery_id 
, dd_device_id AS device_id 
, 'Same-Device' AS attribution 
, is_first_ordercart_dd 
FROM orders
WHERE LOWER(submit_platform) NOT IN ('ios', 'android')
)

, exposures AS (
SELECT 
bucket_key AS dd_device_id 
, tag 
, NULLIF(custom_attributes:context:timezone::VARCHAR, '') AS timezone 
, NULLIF(custom_attributes:context:page:referrer::VARCHAR, '') AS referrer 
, NULLIF(custom_attributes:context:campaign:name::VARCHAR, '') AS utm_campaign 
, NULLIF(custom_attributes:context:campaign:source::VARCHAR, '') AS utm_source 
, NULLIF(custom_attributes:context:campaign:medium::VARCHAR, '') AS utm_medium 
, NULLIF(custom_attributes:isGuest::BOOLEAN, FALSE) AS is_guest 
, exposure_time AS first_exposure_time 
, CASE 
        WHEN timezone IS NOT NULL 
        AND timezone NOT ILIKE 'Etc/Unknown' 
        AND timezone NOT ILIKE 'Etc/%'
        AND timezone NOT LIKE '+%'
        AND timezone NOT LIKE '-%'
        AND timezone NOT IN ('GMT', 'UTC')
        AND timezone LIKE '%/%'  -- Valid timezones have format 'Region/City'
        AND timezone <> '' 
        AND LEN(timezone) > 3
        THEN CONVERT_TIMEZONE('UTC', timezone, exposure_time)
        ELSE CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', exposure_time)
      END AS first_exposure_time_local 
FROM proddb.public.fact_dedup_experiment_exposure 
WHERE experiment_name = 'app_download_bottomsheet_store_page_v2'
    AND experiment_version = 2 
    AND exposure_time::DATE >= '2025-06-10'::DATE
    AND exposure_time::DATE <= '2025-06-17'::DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY exposure_time ASC) = 1 
)

SELECT 
a.dd_device_id 
, CASE WHEN tag = 'treatment' THEN True ELSE False END AS device_id_show_app_download_bottom_sheet_real_time
, CASE 
     WHEN referrer LIKE '%doordash.%' THEN 'Direct'
     WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND NULLIF(referrer, '') IS NULL THEN 'Direct'
     WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND (NULLIF(referrer, '') LIKE '%google.%' OR NULLIF(referrer, '') LIKE '%bing.%' OR NULLIF(referrer, '') LIKE '%search.yahoo.%') OR NULLIF(referrer, '') LIKE '%duckduckgo.%' THEN 'Organic Search'
     WHEN NULLIF(utm_campaign, '') = 'gpa' THEN 'Organic Search'
     WHEN NULLIF(utm_medium, '') = 'Paid_Social' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'SEMb' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'SEMu' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'SEMc' THEN 'Paid Media'
     WHEN NULLIF(utm_medium, '') = 'PLA' THEN 'Paid Media'
     WHEN LOWER(NULLIF(utm_medium, '')) = 'email' THEN 'Email'
     WHEN LOWER(NULLIF(utm_medium, '')) LIKE '%enterprise%' OR LOWER(NULLIF(utm_source, '')) IN ('partner-link') THEN 'Partners'
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('affiliate') THEN 'Affiliate' 
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('web_display') THEN 'Paid Media'
     WHEN LOWER(NULLIF(utm_medium, '')) IN ('video') THEN 'Paid Media'
     WHEN utm_campaign is not null or lower(utm_medium) = 'paid' THEN 'Paid Media'
     when lower(referrer) like '%facebook.%' or lower(referrer) like '%tiktok.%' or lower(referrer) like '%instagram.%' THEN 'Paid Media'
     when utm_source ilike 'mx_share%' then 'Mx Share'
     when referrer is not null then 'Mx Share'
     ELSE 'Other' END AS device_id_general_traffic_type_real_time
, CASE WHEN device_id_general_traffic_type_real_time = 'Direct' THEN True ELSE False END AS device_id_channel_is_direct_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Organic Search' THEN True ELSE False END AS device_id_channel_is_organic_search_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Paid Media' THEN True ELSE False END AS device_id_channel_is_paid_media_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Email' THEN True ELSE False END AS device_id_channel_is_email_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Partners' THEN True ELSE False END AS device_id_channel_is_partners_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Affiliate' THEN True ELSE False END AS device_id_channel_is_affiliate_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Mx Share' THEN True ELSE False END AS device_id_channel_is_mx_share_real_time 
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 0 AND 3 THEN True ELSE False END AS device_id_first_exposure_time_is_0_to_3_real_time
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 4 AND 7 THEN True ELSE False END AS device_id_first_exposure_time_is_4_to_7_real_time
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 8 AND 11 THEN True ELSE False END AS device_id_first_exposure_time_is_8_to_11_real_time
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 12 AND 15 THEN True ELSE False END AS device_id_first_exposure_time_is_12_to_15_real_time
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 16 AND 19 THEN True ELSE False END AS device_id_first_exposure_time_is_16_to_19_real_time
, CASE WHEN HOUR(first_exposure_time_local) BETWEEN 20 AND 23 THEN True ELSE False END AS device_id_first_exposure_time_is_20_to_23_real_time
, first_exposure_time
, first_exposure_time_local 
, MAX(CASE WHEN dd.delivery_id IS NOT NULL THEN True ELSE False END) AS device_id_placed_an_order_real_time
FROM exposures a 
LEFT JOIN consolidated dd 
    ON dd.device_id = a.dd_device_id 
    AND dd.event_ts::DATE = a.first_exposure_time::DATE 
    AND dd.event_ts > a.first_exposure_time 
GROUP BY all
;

-- select tag, custom_attributes:context:page:referrer,custom_attributes:context:campaign:medium,count(1) cnt from proddb.public.fact_dedup_experiment_exposure 
-- WHERE experiment_name = 'app_download_bottomsheet_store_page_v2'
--     AND experiment_version = 2 
--     AND exposure_time::DATE >= '2025-06-10'::DATE
--     AND exposure_time::DATE <= '2025-06-17'::DATE group by all order by cnt desc limit 10000;

/*
====================================================================================
Training Data Ready for Modeling - WITH ALL CX360 FEATURES
====================================================================================
*/

CREATE OR REPLACE TABLE proddb.fionafan.logged_out_personalization_training_comprehensive_v5 AS 

SELECT 
a.* 
, b.has_associated_consumer_id 
, b.consumer_id 
, b.platform
, b.device_id_is_active_recent_28_days 
, b.device_id_sessions_recent_28_days 
, b.device_id_has_logged_in_recent_28_days
, b.device_id_is_active_recent_90_days
, b.device_id_sessions_recent_90_days
, b.device_id_has_logged_in_recent_90_days
, b.device_id_orders_recent_28_days
, b.device_id_has_logged_out_address_entry_recent_28_days
, b.device_id_has_logged_in_address_entry_recent_28_days
, b.device_id_orders_recent_90_days
, b.device_id_has_logged_out_address_entry_recent_90_days
, b.device_id_has_logged_in_address_entry_recent_90_days
, b.device_id_session_id_most_recent_session
, b.device_id_general_traffic_type_most_recent_session
, b.device_id_browser_most_recent_session
, b.device_id_recency_most_recent_session
, b.device_id_placed_order_most_recent_session
, b.device_id_is_auto_logged_in_most_recent_session
, b.device_id_has_logged_out_address_entry_most_recent_session
, b.device_id_has_logged_in_address_entry_most_recent_session
, b.device_id_has_logged_in_recent_most_recent_session
, b.date_summarized
, b.consumer_id_in_app_orders_recent_28_days
, b.consumer_id_desktop_web_orders_recent_28_days
, b.consumer_id_mobile_web_orders_recent_28_days
, b.consumer_id_total_orders_recent_28_days
, b.consumer_id_in_app_orders_recent_90_days
, b.consumer_id_desktop_web_orders_recent_90_days
, b.consumer_id_mobile_web_orders_recent_90_days
, b.consumer_id_total_orders_recent_90_days
-- CX360 Features
, b.cx360_country
, b.cx360_is_current_dashpass
, b.cx360_is_ever_dashpass
, b.cx360_is_fraud
, b.cx360_is_suma_cluster_account_most_active_recent
, b.cx360_total_main_visitor_count_lifetime
, b.cx360_store_page_visitor_count_lifetime
, b.cx360_add_item_visitor_count_lifetime
, b.cx360_mon_thu_order_count_ratio_lifetime
, b.cx360_mon_thu_order_count_lifetime
, b.cx360_fri_order_count_ratio_lifetime
, b.cx360_fri_order_count_lifetime
, b.cx360_sat_order_count_ratio_lifetime
, b.cx360_sat_order_count_lifetime
, b.cx360_sun_order_count_ratio_lifetime
, b.cx360_sun_order_count_lifetime
, b.cx360_lunch_count_lifetime
, b.cx360_breakfast_count_lifetime
, b.cx360_dinner_count_lifetime
, b.cx360_early_morning_count_lifetime
, b.cx360_late_night_count_lifetime
, b.cx360_lifestage
, b.cx360_lifetime_days
, b.cx360_recency_order_frequency_category
, b.cx360_order_recency_category
, b.cx360_freq_category
, b.cx360_avg_order_interval_days
, b.cx360_order_count_lifetime
, b.cx360_avg_vp_lifetime
, b.cx360_nv_orders_count_lifetime
-- Device OS flags
, b.is_android
, b.is_ios
, b.is_web
-- Visitor type flags - 28 days
, b.device_id_unique_visitor_recent_28_days
, b.device_id_unique_store_content_page_visitor_recent_28_days
, b.device_id_unique_store_page_visitor_recent_28_days
, b.device_id_unique_order_cart_page_visitor_recent_28_days
, b.device_id_unique_checkout_page_visitor_recent_28_days
, b.device_id_unique_purchaser_recent_28_days
, b.device_id_unique_app_installer_recent_28_days
, b.device_id_unique_core_visitor_recent_28_days
, b.device_id_home_page_visitor_recent_28_days
, b.device_id_mobile_splash_page_visitor_recent_28_days
, b.device_id_multi_store_visitor_recent_28_days
-- Visitor type flags - 90 days
, b.device_id_unique_visitor_recent_90_days
, b.device_id_unique_store_content_page_visitor_recent_90_days
, b.device_id_unique_store_page_visitor_recent_90_days
, b.device_id_unique_order_cart_page_visitor_recent_90_days
, b.device_id_unique_checkout_page_visitor_recent_90_days
, b.device_id_unique_purchaser_recent_90_days
, b.device_id_unique_app_installer_recent_90_days
, b.device_id_unique_core_visitor_recent_90_days
, b.device_id_home_page_visitor_recent_90_days
, b.device_id_mobile_splash_page_visitor_recent_90_days
, b.device_id_multi_store_visitor_recent_90_days
-- Deep link and Singular events
, b.device_id_deep_link_event_count_total
, b.device_id_singular_event_count_total
, b.device_id_deep_link_recency_days
, b.device_id_singular_recency_days
FROM proddb.fionafan.logged_out_personalization_real_time_web_device_id_comprehensive_v5 a 
LEFT JOIN proddb.fionafan.logged_out_personalization_historical_web_device_id_comprehensive_v5 b 
    ON a.dd_device_id = b.dd_device_id 
;

GRANT SELECT ON proddb.fionafan.logged_out_personalization_training_comprehensive_v5 TO ROLE read_only_users; 

select count(1) from proddb.fionafan.logged_out_personalization_training_comprehensive_v5;
select count(1) from proddb.fionafan.logged_out_personalization_real_time_web_device_id_comprehensive_v5;

select count(1)
FROM proddb.public.fact_dedup_experiment_exposure 
WHERE experiment_name = 'app_download_bottomsheet_store_page_v2'
    AND experiment_version = 2 
    AND exposure_time::DATE >= '2025-06-10'::DATE
    AND exposure_time::DATE <= '2025-06-17'::DATE;
    -- AND exposure_time::DATE = '2025-06-10'::DATE;