-- Track all sessions within 28 days after onboarding
-- Base: Users who started onboarding (new_user type)
-- Output: All dd_session_id from store_content_page_load within 28 days post-onboarding
-- Grain: consumer_id, dd_device_id, dd_device_id_filtered, dd_session_id

CREATE OR REPLACE TABLE proddb.fionafan.sessions_28d_post_onboarding AS

WITH onboarding_cohort AS (
  SELECT DISTINCT
    cast(iguazu_timestamp as date) AS onboarding_day,
    iguazu_timestamp as exposure_time,
    consumer_id,
    DD_DEVICE_ID,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID else 'dx_'||DD_DEVICE_ID end), '-') as dd_device_id_filtered,
    dd_platform,
    lower(onboarding_type) as onboarding_type,
    promo_title,
    'start_page' as onboarding_page
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-07-01' AND '2025-07-31'
),

sessions_28d_post_onboarding AS (
  SELECT 
    o.onboarding_day,
    o.exposure_time,
    o.consumer_id,
    o.DD_DEVICE_ID,
    o.dd_device_id_filtered,
    o.dd_platform,
    o.onboarding_type,
    o.promo_title,
    s.dd_session_id,
    min(s.iguazu_timestamp) as session_timestamp,
    min(cast(s.iguazu_timestamp as date)) as session_date,
    datediff('day', o.exposure_time, min(s.iguazu_timestamp)) as days_since_onboarding
  FROM onboarding_cohort o
  INNER JOIN iguazu.server_events_production.m_store_content_page_load s
    ON o.dd_device_id_filtered = replace(lower(CASE WHEN s.DD_DEVICE_ID like 'dx_%' then s.DD_DEVICE_ID else 'dx_'||s.DD_DEVICE_ID end), '-')
    AND s.iguazu_timestamp >= o.exposure_time
    AND s.iguazu_timestamp <= dateadd('day', 28, o.exposure_time)
  GROUP BY all
)

SELECT DISTINCT
  onboarding_day,
  exposure_time,
  consumer_id,
  DD_DEVICE_ID,
  dd_device_id_filtered,
  dd_platform,
  onboarding_type,
  promo_title,
  dd_session_id,
  session_date,
  session_timestamp,
  days_since_onboarding
FROM sessions_28d_post_onboarding
ORDER BY onboarding_day, consumer_id, session_timestamp;

-- Validation query
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(DISTINCT dd_session_id) as unique_sessions
FROM proddb.fionafan.sessions_28d_post_onboarding;




-- Consumer-level aggregation of sessions with key session markers
-- Base table: proddb.fionafan.sessions_28d_post_onboarding
-- Grain: consumer_id
-- Features: onboarding info, session counts, first/second/third session IDs, latest session IDs by day milestone

CREATE OR REPLACE TABLE proddb.fionafan.sessions_28d_enriched AS

WITH session_sequences AS (
  SELECT 
    *,
    -- Nth session since exposure at consumer+device level
    ROW_NUMBER() OVER (
      PARTITION BY consumer_id, dd_device_id_filtered 
      ORDER BY session_timestamp
    ) as nth_session_device,
    -- Nth session since exposure at consumer level (across all devices)
    ROW_NUMBER() OVER (
      PARTITION BY consumer_id
      ORDER BY session_timestamp
    ) as nth_session_consumer
  FROM proddb.fionafan.sessions_28d_post_onboarding
),

consumer_onboarding AS (
  -- Get earliest onboarding info per consumer
  SELECT 
    consumer_id,
    MIN(onboarding_day) as onboarding_day,
    MIN(exposure_time) as exposure_time,
    MIN(dd_platform) as dd_platform,
    MIN(onboarding_type) as onboarding_type,
    MIN(promo_title) as promo_title
  FROM proddb.fionafan.sessions_28d_post_onboarding
  GROUP BY consumer_id
),

latest_sessions AS (
  SELECT
    consumer_id,
    MAX(CASE WHEN rn_week_1 = 1 THEN dd_session_id END) as latest_session_day_0_7,
    MAX(CASE WHEN rn_week_2 = 1 THEN dd_session_id END) as latest_session_day_8_14,
    MAX(CASE WHEN rn_week_3 = 1 THEN dd_session_id END) as latest_session_day_15_21,
    MAX(CASE WHEN rn_week_4 = 1 THEN dd_session_id END) as latest_session_day_22_28
  FROM (
    SELECT
      consumer_id,
      dd_session_id,
      days_since_onboarding,
      session_timestamp,
      CASE 
        WHEN days_since_onboarding BETWEEN 0 AND 7 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_onboarding BETWEEN 0 AND 7 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_1,
      CASE 
        WHEN days_since_onboarding BETWEEN 8 AND 14 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_onboarding BETWEEN 8 AND 14 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_2,
      CASE 
        WHEN days_since_onboarding BETWEEN 15 AND 21 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_onboarding BETWEEN 15 AND 21 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_3,
      CASE 
        WHEN days_since_onboarding BETWEEN 22 AND 28 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_onboarding BETWEEN 22 AND 28 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_4
    FROM session_sequences
  ) ranked
  GROUP BY consumer_id
),

consumer_sessions AS (
  SELECT
    consumer_id,
    -- Total session counts
    COUNT(DISTINCT dd_session_id) as total_sessions,
    COUNT(DISTINCT dd_device_id_filtered) as total_devices,
    
    -- Sample session IDs (first, second, third overall)
    MAX(CASE WHEN nth_session_consumer = 1 THEN dd_session_id END) as first_session_id,
    MAX(CASE WHEN nth_session_consumer = 2 THEN dd_session_id END) as second_session_id,
    MAX(CASE WHEN nth_session_consumer = 3 THEN dd_session_id END) as third_session_id,
    
    -- Session counts by week buckets
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 0 AND 7 THEN dd_session_id END) as sessions_day_0_7,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 8 AND 14 THEN dd_session_id END) as sessions_day_8_14,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 15 AND 21 THEN dd_session_id END) as sessions_day_15_21,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 22 AND 28 THEN dd_session_id END) as sessions_day_22_28
    
  FROM session_sequences
  GROUP BY consumer_id
)

SELECT 
  o.consumer_id,
  o.onboarding_day,
  o.exposure_time,
  o.dd_platform,
  o.onboarding_type,
  o.promo_title,
  s.total_sessions,
  s.total_devices,
  s.first_session_id,
  s.second_session_id,
  s.third_session_id,
  l.latest_session_day_0_7,
  l.latest_session_day_8_14,
  l.latest_session_day_15_21,
  l.latest_session_day_22_28,
  s.sessions_day_0_7,
  s.sessions_day_8_14,
  s.sessions_day_15_21,
  s.sessions_day_22_28
FROM consumer_onboarding o
LEFT JOIN consumer_sessions s
  ON o.consumer_id = s.consumer_id
LEFT JOIN latest_sessions l
  ON o.consumer_id = l.consumer_id
ORDER BY o.onboarding_day, o.consumer_id;

-- Validation query
SELECT 
  COUNT(*) as total_consumers,
  AVG(total_sessions) as avg_sessions_per_consumer,
  AVG(total_devices) as avg_devices_per_consumer,
  COUNT(CASE WHEN first_session_id IS NOT NULL THEN 1 END) as consumers_with_first_session,
  COUNT(CASE WHEN second_session_id IS NOT NULL THEN 1 END) as consumers_with_second_session,
  COUNT(CASE WHEN third_session_id IS NOT NULL THEN 1 END) as consumers_with_third_session,
  AVG(sessions_day_0_7) as avg_sessions_week_1,
  AVG(sessions_day_8_14) as avg_sessions_week_2,
  AVG(sessions_day_15_21) as avg_sessions_week_3,
  AVG(sessions_day_22_28) as avg_sessions_week_4
FROM proddb.fionafan.sessions_28d_enriched;
