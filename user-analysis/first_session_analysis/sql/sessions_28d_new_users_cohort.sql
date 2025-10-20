-- Track all sessions within 28 days for new users (sampled 1.5M consumers)
-- Base: New Cx consumers who placed first order in July 2025 (from growth accounting)
-- Output: All dd_session_id from store_content_page_load within 28 days post-first order
-- Grain: consumer_id, dd_device_id, dd_device_id_filtered, dd_session_id

-- Step 1: Create sampled new user cohort (first order in July 2025)
CREATE OR REPLACE TABLE proddb.fionafan.new_user_july_cohort AS

WITH new_user_cohort AS (
  SELECT
    c.CONSUMER_ID,
    c.LIFESTAGE,
    c.first_order_date,
    c.SCD_START_DATE,
    c.SCD_END_DATE
  FROM edw.growth.consumer_growth_accounting_scd3 c
  JOIN proddb.public.dimension_consumer dc
    ON c.CONSUMER_ID = dc.ID
  WHERE 1=1
    AND c.LIFESTAGE ILIKE 'New Cx'
    AND c.first_order_date >= '2025-07-01'
    AND c.first_order_date < '2025-08-01'
    AND dc.DEFAULT_COUNTRY = 'United States'
)

SELECT 
  CONSUMER_ID,
  LIFESTAGE,
  first_order_date,
  SCD_START_DATE,
  SCD_END_DATE,
  first_order_date as exposure_time  -- Using first_order_date as the exposure time
FROM new_user_cohort
ORDER BY RANDOM()
LIMIT 1500000;

-- Validation of cohort
SELECT 
  COUNT(*) as total_consumers,
  MIN(first_order_date) as min_first_order,
  MAX(first_order_date) as max_first_order
FROM proddb.fionafan.new_user_july_cohort;


-- Step 2: Create base sessions table (28 days post first order)
CREATE OR REPLACE TABLE proddb.fionafan.new_user_sessions_28d AS

WITH cohort AS (
  SELECT DISTINCT
    CONSUMER_ID as consumer_id,
    CAST(first_order_date AS date) AS onboarding_day,
    first_order_date as exposure_time,
    LIFESTAGE as lifestage
  FROM proddb.fionafan.new_user_july_cohort
),

sessions_28d AS (
  SELECT 
    c.onboarding_day,
    c.exposure_time,
    c.consumer_id,
    s.DD_DEVICE_ID,
    replace(lower(CASE WHEN s.DD_DEVICE_ID like 'dx_%' then s.DD_DEVICE_ID else 'dx_'||s.DD_DEVICE_ID end), '-') as dd_device_id_filtered,
    s.dd_platform,
    c.lifestage,
    s.dd_session_id,
    min(s.iguazu_timestamp) as session_timestamp,
    min(cast(s.iguazu_timestamp as date)) as session_date,
    datediff('day', c.exposure_time, min(s.iguazu_timestamp)) as days_since_onboarding
  FROM cohort c
  INNER JOIN iguazu.server_events_production.m_store_content_page_load s
    ON c.consumer_id::varchar = s.consumer_id::varchar
    AND s.iguazu_timestamp >= c.exposure_time
    AND s.iguazu_timestamp <= dateadd('day', 28, c.exposure_time)
  GROUP BY all
)

SELECT DISTINCT
  onboarding_day,
  exposure_time,
  consumer_id,
  DD_DEVICE_ID,
  dd_device_id_filtered,
  dd_platform,
  lifestage,
  dd_session_id,
  session_date,
  session_timestamp,
  days_since_onboarding
FROM sessions_28d
ORDER BY onboarding_day, consumer_id, session_timestamp;

-- Validation query
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(DISTINCT dd_session_id) as unique_sessions
FROM proddb.fionafan.new_user_sessions_28d;


-- Step 3: Create consumer-level enriched table with session metrics
CREATE OR REPLACE TABLE proddb.fionafan.new_user_sessions_enriched AS

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
  FROM proddb.fionafan.new_user_sessions_28d
),

consumer_onboarding AS (
  -- Get earliest onboarding info per consumer
  SELECT 
    consumer_id,
    MIN(onboarding_day) as onboarding_day,
    MIN(exposure_time) as exposure_time,
    MIN(dd_platform) as dd_platform,
    MIN(lifestage) as lifestage
  FROM proddb.fionafan.new_user_sessions_28d
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
  o.lifestage,
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
FROM proddb.fionafan.new_user_sessions_enriched;

