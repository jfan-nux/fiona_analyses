-- verions 2 
--2025-06-10
--2025-06-17
SET observed_experiment_start_date = '2025-06-10'::DATE; 
SET long_term_start_date = $observed_experiment_start_date - INTERVAL '180 DAYS';
SET long_term_end_date = $observed_experiment_start_date - INTERVAL '1 DAYS';

/*
====================================================================================
Raw Web Session Data
====================================================================================
*/
create or replace table fionafan.raw_web_sessions_data_v2 as (
    select * from proddb.fionafan.raw_web_sessions_data_v2
);
CREATE OR REPLACE  TABLE fionafan.raw_web_sessions_data_v2 AS (
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

/*
====================================================================================
Fortified Web Visitor Data
====================================================================================
*/

CREATE OR REPLACE TABLE fionafan.fortified_web_visitor_data_v2 AS (
WITH device_id AS (
SELECT 
dd_device_id 
, platform 
, MAX(consumer_id) AS consumer_id_logged_in_status 
FROM fionafan.raw_web_sessions_data_v2
GROUP BY 1,2 
)
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
, platform 
, CASE WHEN consumer_id_logged_in_status IS NOT NULL THEN consumer_id_logged_in_status ELSE consumer_id_checked_in_log_in_records END AS consumer_id 
, CASE WHEN consumer_id IS NOT NULL THEN True ELSE False END AS has_associated_consumer_id 
FROM joined 
)

;
/*
====================================================================================
Web Session Data
====================================================================================
*/

/* Section 1: Getting Last 28 and 90 Days. */
CREATE OR REPLACE TABLE fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_v2 AS 
SELECT 
a.* 
, MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND b.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_28_days 
, COUNT(DISTINCT CASE WHEN b.dd_device_id IS NULL THEN NULL ELSE IFNULL(b.dd_session_id, 'Null') END) AS device_id_sessions_recent_28_days 
, COUNT(DISTINCT CASE WHEN b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date THEN dd.delivery_id ELSE NULL END) AS device_id_orders_recent_28_days 
, MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_28_days 
, MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_28_days 
, MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '27 DAYS' AND $long_term_end_date AND e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_28_days 
, MAX(CASE WHEN b.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_90_days 
, COUNT(DISTINCT CASE WHEN b.dd_device_id IS NULL THEN NULL ELSE IFNULL(b.dd_session_id, 'Null') END) AS device_id_sessions_recent_90_days 
, COUNT(DISTINCT dd.delivery_id) AS device_id_orders_recent_90_days 
, MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_90_days 
, MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_90_days 
, MAX(CASE WHEN e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_90_days 
FROM fionafan.fortified_web_visitor_data_v2 a 
LEFT JOIN fionafan.raw_web_sessions_data_v2 b 
    ON a.dd_device_id = b.dd_device_id 
    AND b.session_first_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date 
LEFT JOIN edw.finance.dimension_deliveries dd 
    ON dd.is_caviar != 1 
    AND dd.is_filtered_core = 1 
    AND dd.dd_device_id = a.dd_device_id 
    AND dd.created_at::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date 
    AND b.dd_device_id IS NOT NULL 
LEFT JOIN iguazu.server_events_production.debug_address_create c 
    ON c.is_guest = True 
    AND c.dd_device_id = a.dd_device_id 
    AND c.iguazu_timestamp::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date 
    AND b.dd_device_id IS NOT NULL 
LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events e 
    ON e.dd_device_id = a.dd_device_id 
    AND e.ts::DATE BETWEEN $long_term_end_date - INTERVAL '90 DAYS' AND $long_term_end_date 
GROUP BY 1,2,3,4

; 

/* Section 2: Getting Most Recent Session. */
CREATE OR REPLACE TABLE fionafan.raw_unique_web_device_id_add_recent_session_web_info_v2 AS 
WITH most_recent_sessions AS (
SELECT 
* 
FROM fionafan.raw_web_sessions_data_v2
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
FROM fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_v2 a 
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

;

/*
====================================================================================
Historical on Web Device 
====================================================================================
*/

CREATE OR REPLACE TABLE fionafan.logged_out_personalization_historical_web_device_id_v2 AS 
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
FROM fionafan.raw_unique_web_device_id_add_recent_session_web_info_v2 a 
LEFT JOIN edw.finance.dimension_deliveries dd 
    ON dd.is_caviar != 1 
    AND dd.is_filtered_core = 1 
    AND dd.creator_id::VARCHAR = a.consumer_id::VARCHAR 
    AND dd.created_at::DATE BETWEEN $long_term_start_date AND $long_term_end_date 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
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
    AND b.snapshot_date::DATE = DATE_TRUNC('WEEK', $long_term_end_date)::DATE - 1 /* Getting the Cx360 attributes from the beginnning of the week. */
;

/*
====================================================================================
Real-Time Web Data
====================================================================================
*/

CREATE OR REPLACE TABLE fionafan.logged_out_personalization_real_time_web_device_id_v2 AS 
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
    AND event_properties LIKE '%mobile_web_to_app%' /* The configuration to track MWeb to App. */
    AND event_timestamp::DATE BETWEEN $observed_experiment_start_date - INTERVAL '28 DAYS' AND $observed_experiment_start_date 

UNION ALL 

SELECT
iguazu_other_properties:dd_device_id::VARCHAR AS app_device_id
, CASE 
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1)
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id%3D',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id%3D',2),'%',1)
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id=',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id=',2),'%',1) /* In case we look at Adjust historical data. */
    WHEN NULLIF(SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id=',2),'%',1), '') IS NOT NULL THEN SPLIT_PART(SPLIT_PART(deep_link_url,'web_consumer_id=',2),'%',1) /* In case we look at Adjust historical data. */
  ELSE NULL 
  END AS mweb_device_id 
, iguazu_timestamp AS event_timestamp 
FROM iguazu.server_events_production.m_deep_link 
WHERE 
    (deep_link_url LIKE '%web_consumer_id%'
    OR deep_link_url LIKE '%dd_device_id%')
    AND iguazu_timestamp::DATE BETWEEN $observed_experiment_start_date - INTERVAL '28 DAYS' AND $observed_experiment_start_date /* Update this to ETLXpress Macro. */
)

, consolidated AS (
/* In-App Orders Attribute-able to Mweb dd_device_id */
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
, exposure_time AS first_exposure_time 
, CASE WHEN timezone IS NOT NULL THEN CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', exposure_time) ELSE NULL END AS first_exposure_time_local 
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
     WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND (NULLIF(referrer, '') LIKE '%google.%' OR NULLIF(referrer, '') LIKE '%bing.%' OR NULLIF(referrer, '') LIKE '%search.yahoo.%') THEN 'Organic Search'
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
     ELSE 'Other' END AS device_id_general_traffic_type_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Direct' THEN True ELSE False END AS device_id_channel_is_direct_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Organic Search' THEN True ELSE False END AS device_id_channel_is_organic_search_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Paid Media' THEN True ELSE False END AS device_id_channel_is_paid_media_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Email' THEN True ELSE False END AS device_id_channel_is_email_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Partners' THEN True ELSE False END AS device_id_channel_is_partners_real_time 
, CASE WHEN device_id_general_traffic_type_real_time = 'Affiliate' THEN True ELSE False END AS device_id_channel_is_affiliate_real_time 
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
WHERE a.first_exposure_time::DATE = $observed_experiment_start_date 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 

;

/*
====================================================================================
Training Data Ready for Modeling
====================================================================================
*/

CREATE TABLE fionafan.logged_out_personalization_training_v2 AS 
SELECT 
a.* 
, b.has_associated_consumer_id 
, b.consumer_id 
, b.device_id_is_active_recent_28_days 
, b.device_id_sessions_recent_28_days 
, b.device_id_orders_recent_28_days
, b.device_id_has_logged_out_address_entry_recent_28_days
, b.device_id_has_logged_in_recent_28_days
, b.device_id_session_id_most_recent_session
, b.device_id_general_traffic_type_most_recent_session
, b.device_id_recency_most_recent_session
, b.device_id_placed_order_most_recent_session
, b.device_id_is_auto_logged_in_most_recent_session
, b.date_summarized
, b.consumer_id_in_app_orders_recent_28_days
, b.consumer_id_desktop_web_orders_recent_28_days
, b.consumer_id_mobile_web_orders_recent_28_days
, b.consumer_id_total_orders_recent_28_days
, b.consumer_id_in_app_orders_recent_90_days
, b.consumer_id_desktop_web_orders_recent_90_days
, b.consumer_id_mobile_web_orders_recent_90_days
, b.consumer_id_total_orders_recent_90_days
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
FROM fionafan.logged_out_personalization_real_time_web_device_id_v2 a 
LEFT JOIN fionafan.logged_out_personalization_historical_web_device_id_v2 b 
    ON a.dd_device_id = b.dd_device_id 

;

GRANT SELECT ON fionafan.logged_out_personalization_training_v2 TO ROLE read_only_users; 
