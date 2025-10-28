-- CTE-driven, no exposures, runtime dates

create or replace table proddb.fionafan.logged_out_personalization_inference_sql as (
WITH vars AS (
  SELECT
    (CURRENT_DATE()::DATE - INTERVAL '1 DAY') AS observed_experiment_start_date,
    (CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '180 DAYS') AS long_term_start_date,
    (CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '1 DAYS') AS long_term_end_date
),

/* Raw Web Session Data (same unions as before, uses vars) */
raw_web_sessions_data_v2 AS (
  SELECT *
  FROM (
    SELECT
      platform,
      dd_device_id,
      dd_session_id,
      consumer_id,
      iguazu_timestamp AS session_first_timestamp,
      'Store' AS page,
      NULLIF(context_campaign_name, '') AS utm_campaign,
      NULLIF(context_campaign_source, '') AS utm_source,
      NULLIF(context_campaign_medium, '') AS utm_medium,
      NULLIF(context_page_referrer, '') AS referrer,
      context_user_agent
    FROM iguazu.server_events_production.store_page_load_consumer
    WHERE context_user_agent NOT ILIKE '%bot%'
      AND context_user_agent NOT ILIKE '%prerender%'
      AND context_user_agent NOT ILIKE '%read-aloud%'
      AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

    UNION ALL

    SELECT
      platform,
      dd_device_id,
      dd_session_id,
      consumer_id,
      iguazu_timestamp AS session_first_timestamp,
      'Home' AS page,
      NULLIF(context_campaign_name, '') AS utm_campaign,
      NULLIF(context_campaign_source, '') AS utm_source,
      NULLIF(context_campaign_medium, '') AS utm_medium,
      NULLIF(context_page_referrer, '') AS referrer,
      context_user_agent
    FROM iguazu.consumer.home_page_view
    WHERE context_user_agent NOT ILIKE '%bot%'
      AND context_user_agent NOT ILIKE '%prerender%'
      AND context_user_agent NOT ILIKE '%read-aloud%'
      AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

    UNION ALL

    SELECT
      platform,
      dd_device_id,
      dd_session_id,
      consumer_id,
      iguazu_timestamp AS session_first_timestamp,
      'Explore' AS page,
      NULLIF(context_campaign_name, '') AS utm_campaign,
      NULLIF(context_campaign_source, '') AS utm_source,
      NULLIF(context_campaign_medium, '') AS utm_medium,
      NULLIF(context_page_referrer, '') AS referrer,
      context_user_agent
    FROM iguazu.server_events_production.store_content_page_load
    WHERE context_user_agent NOT ILIKE '%bot%'
      AND context_user_agent NOT ILIKE '%prerender%'
      AND context_user_agent NOT ILIKE '%read-aloud%'
      AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

    UNION ALL

    SELECT
      platform,
      dd_device_id,
      dd_session_id,
      consumer_id,
      iguazu_timestamp AS session_first_timestamp,
      'Business' AS page,
      NULLIF(context_campaign_name, '') AS utm_campaign,
      NULLIF(context_campaign_source, '') AS utm_source,
      NULLIF(context_campaign_medium, '') AS utm_medium,
      NULLIF(context_page_referrer, '') AS referrer,
      context_user_agent
    FROM iguazu.consumer.business_menu_page_load
    WHERE context_user_agent NOT ILIKE '%bot%'
      AND context_user_agent NOT ILIKE '%prerender%'
      AND context_user_agent NOT ILIKE '%read-aloud%'
      AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

    UNION ALL

    SELECT
      platform,
      dd_device_id,
      dd_session_id,
      consumer_id,
      iguazu_timestamp AS session_first_timestamp,
      'Product' AS page,
      NULLIF(context_campaign_name, '') AS utm_campaign,
      NULLIF(context_campaign_source, '') AS utm_source,
      NULLIF(context_campaign_medium, '') AS utm_medium,
      NULLIF(context_page_referrer, '') AS referrer,
      context_user_agent
    FROM iguazu.server_events_production.product_display_page_view
    WHERE context_user_agent NOT ILIKE '%bot%'
      AND context_user_agent NOT ILIKE '%prerender%'
      AND context_user_agent NOT ILIKE '%read-aloud%'
      AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_session_id, dd_device_id ORDER BY session_first_timestamp ASC) = 1
),

/* Add traffic_type and browser_name only where based on session-level fields (these do NOT come from exposures) */
raw_web_sessions_data_v2_enriched AS (
  SELECT
    r.*,
    CASE
      WHEN referrer LIKE '%doordash.%' THEN 'Direct'
      WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL AND NULLIF(referrer, '') IS NULL THEN 'Direct'
      WHEN NULLIF(utm_medium, '') IS NULL AND NULLIF(utm_source, '') IS NULL AND NULLIF(utm_campaign, '') IS NULL
           AND (NULLIF(referrer, '') LIKE '%google.%' OR NULLIF(referrer, '') LIKE '%bing.%' OR NULLIF(referrer, '') LIKE '%search.yahoo.%') THEN 'Organic Search'
      WHEN NULLIF(utm_campaign, '') = 'gpa' THEN 'Organic Search'
      WHEN NULLIF(utm_medium, '') = 'Paid_Social' THEN 'Paid Social'
      WHEN NULLIF(utm_medium, '') IN ('SEMb','SEMu','SEMc','PLA','web_display','video') THEN 'Paid Media'
      WHEN LOWER(NULLIF(utm_medium, '')) = 'email' THEN 'Email'
      WHEN LOWER(NULLIF(utm_medium, '')) LIKE '%enterprise%' OR LOWER(NULLIF(utm_source, '')) IN ('partner-link') THEN 'Partners'
      WHEN LOWER(NULLIF(utm_medium, '')) IN ('affiliate') THEN 'Affiliate'
      ELSE 'Other'
    END AS traffic_type,
    CASE
      WHEN context_user_agent ILIKE '%FBAV%' OR context_user_agent ILIKE '%FB_IAB%' THEN 'Facebook In-App'
      WHEN context_user_agent ILIKE '%Instagram%' THEN 'Instagram In-App'
      WHEN context_user_agent ILIKE '%TikTok%' THEN 'TikTok In-App'
      WHEN context_user_agent ILIKE '%Twitter%' THEN 'Twitter In-App'
      WHEN context_user_agent ILIKE '%Snapchat%' THEN 'Snapchat In-App'
      WHEN context_user_agent ILIKE '%Pinterest%' THEN 'Pinterest In-App'
      WHEN context_user_agent ILIKE '%LinkedIn%' THEN 'LinkedIn In-App'
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
  FROM raw_web_sessions_data_v2 r
),

/* Fortified visitor mapping (device -> consumer) */
fortified_web_visitor_data_v2 AS (
  WITH device_id AS (
    SELECT dd_device_id, platform, MAX(consumer_id) AS consumer_id_logged_in_status
    FROM raw_web_sessions_data_v2_enriched
    GROUP BY 1,2
  )
  SELECT
    a.dd_device_id,
    a.platform,
    CASE WHEN a.consumer_id_logged_in_status IS NOT NULL THEN a.consumer_id_logged_in_status
         ELSE dc.consumer_id END AS consumer_id,
    CASE WHEN (CASE WHEN a.consumer_id_logged_in_status IS NOT NULL THEN a.consumer_id_logged_in_status
         ELSE dc.consumer_id END) IS NOT NULL THEN True ELSE False END AS has_associated_consumer_id
  FROM device_id a
  LEFT JOIN (
    SELECT dd_device_id, user_id, ts
    FROM proddb.public.fact_consumer_frontend_login_and_signup_events
    WHERE ts::DATE BETWEEN (SELECT long_term_start_date FROM vars) - INTERVAL '181 DAYS' AND (SELECT long_term_end_date FROM vars)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY ts DESC) = 1
  ) b ON a.dd_device_id::VARCHAR = b.dd_device_id::VARCHAR
  LEFT JOIN edw.consumer.dimension_consumers dc ON dc.user_id::VARCHAR = b.user_id::VARCHAR
),

/* Section 1: 28 & 90 day aggregates (uses session_first_timestamp & deliveries) */
raw_unique_web_device_id_add_recent_28_days_web_info_v2 AS (
  SELECT
    a.*,
    MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND b.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_28_days,
    COUNT(DISTINCT CASE WHEN b.dd_device_id IS NULL THEN NULL ELSE IFNULL(b.dd_session_id,'Null') END) AS device_id_sessions_recent_28_days,
    COUNT(DISTINCT CASE WHEN b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) THEN dd.delivery_id ELSE NULL END) AS device_id_orders_recent_28_days,
    MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_28_days,
    MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_28_days,
    MAX(CASE WHEN b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_28_days,
    MAX(CASE WHEN b.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_90_days,
    COUNT(DISTINCT CASE WHEN b.dd_device_id IS NULL THEN NULL ELSE IFNULL(b.dd_session_id,'Null') END) AS device_id_sessions_recent_90_days,
    COUNT(DISTINCT dd.delivery_id) AS device_id_orders_recent_90_days,
    MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_90_days,
    MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_recent_90_days,
    MAX(CASE WHEN e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_90_days
  FROM fortified_web_visitor_data_v2 a
  LEFT JOIN raw_web_sessions_data_v2_enriched b
    ON a.dd_device_id = b.dd_device_id
    AND b.session_first_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT long_term_end_date FROM vars)
  LEFT JOIN edw.finance.dimension_deliveries dd
    ON dd.is_caviar != 1
    AND dd.is_filtered_core = 1
    AND dd.dd_device_id = a.dd_device_id
    AND dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT long_term_end_date FROM vars)
    AND b.dd_device_id IS NOT NULL
  LEFT JOIN iguazu.server_events_production.debug_address_create c
    ON c.dd_device_id = a.dd_device_id
    AND c.iguazu_timestamp::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT long_term_end_date FROM vars)
    -- NOTE: original used c.is_guest in selective aggregates; here we keep c available and filter in aggregates above
    AND b.dd_device_id IS NOT NULL
  LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events e
    ON e.dd_device_id = a.dd_device_id
    AND e.ts::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT long_term_end_date FROM vars)
  GROUP BY 1,2,3,4
),

/* Section 2: Most recent session info (session-level traffic_type & browser_name, recency, placed-order within 24h) */
raw_unique_web_device_id_add_recent_session_web_info_v2 AS (
  WITH most_recent_sessions AS (
    SELECT *
    FROM raw_web_sessions_data_v2_enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY session_first_timestamp DESC) = 1
  )
  SELECT
    a.*,
    b.dd_session_id AS device_id_session_id_most_recent_session,
    b.traffic_type AS device_id_general_traffic_type_most_recent_session,
    b.browser_name AS device_id_browser_most_recent_session,
    DATEDIFF('DAYS', b.session_first_timestamp, (SELECT observed_experiment_start_date FROM vars) - INTERVAL '1 DAY') AS device_id_recency_most_recent_session,
    MAX(CASE WHEN dd.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_placed_order_most_recent_session,
    MAX(CASE WHEN b.consumer_id IS NOT NULL THEN True ELSE False END) AS device_id_is_auto_logged_in_most_recent_session,
    MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = True THEN True ELSE False END) AS device_id_has_logged_out_address_entry_most_recent_session,
    MAX(CASE WHEN c.dd_device_id IS NOT NULL AND c.is_guest = False THEN True ELSE False END) AS device_id_has_logged_in_address_entry_most_recent_session,
    MAX(CASE WHEN e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_most_recent_session
  FROM raw_unique_web_device_id_add_recent_28_days_web_info_v2 a
  LEFT JOIN most_recent_sessions b
    ON a.dd_device_id = b.dd_device_id
  LEFT JOIN edw.finance.dimension_deliveries dd
    ON dd.is_caviar != 1
    AND dd.is_filtered_core = 1
    AND dd.dd_device_id = a.dd_device_id
    AND dd.created_at BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS'
  LEFT JOIN iguazu.server_events_production.debug_address_create c
    ON c.dd_device_id = a.dd_device_id
    AND c.iguazu_timestamp::DATE BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS'
    AND b.dd_device_id IS NOT NULL
  LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events e
    ON e.dd_device_id = a.dd_device_id
    AND e.ts::DATE BETWEEN b.session_first_timestamp AND b.session_first_timestamp + INTERVAL '24 HOURS'
    AND b.dd_device_id IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

/* Historical aggregates (consumer-level orders counts) */
recent_behavior AS (
  SELECT
    a.*,
    (SELECT observed_experiment_start_date FROM vars) AS date_summarized,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('android','ios') THEN dd.delivery_id ELSE NULL END) AS consumer_id_in_app_orders_recent_28_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('desktop','desktop-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_desktop_web_orders_recent_28_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile','mobile-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_mobile_web_orders_recent_28_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '27 DAYS' AND (SELECT long_term_end_date FROM vars) THEN dd.delivery_id ELSE NULL END) AS consumer_id_total_orders_recent_28_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '89 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('android','ios') THEN dd.delivery_id ELSE NULL END) AS consumer_id_in_app_orders_recent_90_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '89 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('desktop','desktop-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_desktop_web_orders_recent_90_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '89 DAYS' AND (SELECT long_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile','mobile-web') THEN dd.delivery_id ELSE NULL END) AS consumer_id_mobile_web_orders_recent_90_days,
    COUNT(DISTINCT CASE WHEN dd.created_at::DATE BETWEEN (SELECT long_term_end_date FROM vars) - INTERVAL '89 DAYS' AND (SELECT long_term_end_date FROM vars) THEN dd.delivery_id ELSE NULL END) AS consumer_id_total_orders_recent_90_days
  FROM raw_unique_web_device_id_add_recent_session_web_info_v2 a
  LEFT JOIN edw.finance.dimension_deliveries dd
    ON dd.is_caviar != 1
    AND dd.is_filtered_core = 1
    AND dd.creator_id::VARCHAR = a.consumer_id::VARCHAR
    AND dd.created_at::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
),

/* Join with cx360 snapshot to add consumer-level attributes (same as original) */
logged_out_personalization_historical_web_device_id_v2 AS (
  SELECT
    rb.*,
    c.country AS cx360_country,
    c.is_current_dashpass AS cx360_is_current_dashpass,
    c.is_ever_dashpass AS cx360_is_ever_dashpass,
    c.is_fraud AS cx360_is_fraud,
    c.is_suma_cluster_account_most_active_recent AS cx360_is_suma_cluster_account_most_active_recent,
    c.total_main_visitor_count_lifetime AS cx360_total_main_visitor_count_lifetime,
    c.store_page_visitor_count_lifetime AS cx360_store_page_visitor_count_lifetime,
    c.add_item_visitor_count_lifetime AS cx360_add_item_visitor_count_lifetime,
    c.mon_thu_order_count_ratio_lifetime AS cx360_mon_thu_order_count_ratio_lifetime,
    c.mon_thu_order_count_lifetime AS cx360_mon_thu_order_count_lifetime,
    c.fri_order_count_ratio_lifetime AS cx360_fri_order_count_ratio_lifetime,
    c.fri_order_count_lifetime AS cx360_fri_order_count_lifetime,
    c.sat_order_count_ratio_lifetime AS cx360_sat_order_count_ratio_lifetime,
    c.sat_order_count_lifetime AS cx360_sat_order_count_lifetime,
    c.sun_order_count_ratio_lifetime AS cx360_sun_order_count_ratio_lifetime,
    c.sun_order_count_lifetime AS cx360_sun_order_count_lifetime,
    c.lunch_count_lifetime AS cx360_lunch_count_lifetime,
    c.breakfast_count_lifetime AS cx360_breakfast_count_lifetime,
    c.dinner_count_lifetime AS cx360_dinner_count_lifetime,
    c.early_morning_count_lifetime AS cx360_early_morning_count_lifetime,
    c.late_night_count_lifetime AS cx360_late_night_count_lifetime,
    c.lifestage AS cx360_lifestage,
    c.tenure_days AS cx360_lifetime_days,
    c.recency_order_frequency_category AS cx360_recency_order_frequency_category,
    c.order_recency_category AS cx360_order_recency_category,
    c.freq_category AS cx360_freq_category,
    c.avg_order_interval_days AS cx360_avg_order_interval_days,
    c.order_count_lifetime AS cx360_order_count_lifetime,
    c.avg_vp_lifetime AS cx360_avg_vp_lifetime,
    c.nv_orders_count_lifetime AS cx360_nv_orders_count_lifetime
  FROM recent_behavior rb
  LEFT JOIN edw.growth.cx360_model_snapshot_dlcopy c
    ON rb.consumer_id = c.consumer_id
    AND rb.consumer_id IS NOT NULL
    AND c.snapshot_date::DATE = DATE_TRUNC('WEEK', (SELECT long_term_end_date FROM vars))::DATE - 1
)

/* Final training-ready selection — join real-time (a) to historical (b).
   Note: the real-time side no longer contains exposures-derived fields, so those fields are not present here.
*/
SELECT
  b.dd_device_id,
  -- historical features
  b.has_associated_consumer_id,
  b.consumer_id,
  b.device_id_is_active_recent_28_days,
  b.device_id_sessions_recent_28_days,
  b.device_id_orders_recent_28_days,
  b.device_id_has_logged_out_address_entry_recent_28_days,
  b.device_id_has_logged_in_recent_28_days,
  b.device_id_session_id_most_recent_session,
  b.device_id_general_traffic_type_most_recent_session,
  b.device_id_recency_most_recent_session,
  b.device_id_placed_order_most_recent_session,
  b.device_id_is_auto_logged_in_most_recent_session,
  b.date_summarized,
  b.consumer_id_in_app_orders_recent_28_days,
  b.consumer_id_desktop_web_orders_recent_28_days,
  b.consumer_id_mobile_web_orders_recent_28_days,
  b.consumer_id_total_orders_recent_28_days,
  b.consumer_id_in_app_orders_recent_90_days,
  b.consumer_id_desktop_web_orders_recent_90_days,
  b.consumer_id_mobile_web_orders_recent_90_days,
  b.consumer_id_total_orders_recent_90_days,
  b.cx360_country,
  b.cx360_is_current_dashpass,
  b.cx360_is_ever_dashpass,
  b.cx360_is_fraud,
  b.cx360_is_suma_cluster_account_most_active_recent,
  b.cx360_total_main_visitor_count_lifetime,
  b.cx360_store_page_visitor_count_lifetime,
  b.cx360_add_item_visitor_count_lifetime,
  b.cx360_mon_thu_order_count_ratio_lifetime,
  b.cx360_mon_thu_order_count_lifetime,
  b.cx360_fri_order_count_ratio_lifetime,
  b.cx360_fri_order_count_lifetime,
  b.cx360_sat_order_count_ratio_lifetime,
  b.cx360_sat_order_count_lifetime,
  b.cx360_sun_order_count_ratio_lifetime,
  b.cx360_sun_order_count_lifetime,
  b.cx360_lunch_count_lifetime,
  b.cx360_breakfast_count_lifetime,
  b.cx360_dinner_count_lifetime,
  b.cx360_early_morning_count_lifetime,
  b.cx360_late_night_count_lifetime,
  b.cx360_lifestage,
  b.cx360_lifetime_days,
  b.cx360_recency_order_frequency_category,
  b.cx360_order_recency_category,
  b.cx360_freq_category,
  b.cx360_avg_order_interval_days,
  b.cx360_order_count_lifetime,
  b.cx360_avg_vp_lifetime,
  b.cx360_nv_orders_count_lifetime
FROM logged_out_personalization_historical_web_device_id_v2 b
);

select count (1) from proddb.fionafan.raw_web_sessions_data_v2;