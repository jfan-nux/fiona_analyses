-- Track all sessions within 28 days for active users (sampled 1.5M consumers)
-- Base: Active users from growth accounting in July 2025
-- Output: All dd_session_id from store_content_page_load within 28 days post-first order
-- Grain: consumer_id, dd_device_id, dd_device_id_filtered, dd_session_id

-- Step 1: Create sampled active user cohort (first order in July 2025)
CREATE OR REPLACE TABLE proddb.fionafan.active_user_july_cohort AS
WITH active_user_cohort AS (
  SELECT
    c.CONSUMER_ID,
    c.LIFESTAGE,
    c.LAST_ORDER_DATE,
    c.SCD_START_DATE,
    c.SCD_END_DATE
  FROM edw.growth.consumer_growth_accounting_scd3 c
  JOIN proddb.public.dimension_consumer dc
    ON c.CONSUMER_ID = dc.ID
  WHERE 1=1
    AND c.LIFESTAGE ILIKE 'Active'
    AND c.LAST_ORDER_DATE >= '2025-07-01'
    AND c.LAST_ORDER_DATE < '2025-08-01'
    AND dc.DEFAULT_COUNTRY = 'United States'
)

SELECT 
  CONSUMER_ID,
  LIFESTAGE,
  LAST_ORDER_DATE,
  SCD_START_DATE,
  SCD_END_DATE,
  LAST_ORDER_DATE as exposure_time  -- Using LAST_ORDER_DATE as the exposure time
FROM active_user_cohort
ORDER BY RANDOM()
LIMIT 1500000;

-- Validation of cohort
SELECT 
  COUNT(*) as total_consumers,
  MIN(LAST_ORDER_DATE) as min_first_order,
  MAX(LAST_ORDER_DATE) as max_first_order
FROM proddb.fionafan.active_user_july_cohort;


-- Step 2: Create base sessions table (28 days post first order)
CREATE OR REPLACE TABLE proddb.fionafan.active_user_sessions_28d AS

WITH cohort AS (
  SELECT DISTINCT
    CONSUMER_ID as consumer_id,
    CAST(LAST_ORDER_DATE AS date) AS onboarding_day,
    LAST_ORDER_DATE as exposure_time,
    LIFESTAGE as lifestage
  FROM proddb.fionafan.active_user_july_cohort
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
FROM proddb.fionafan.active_user_sessions_28d;


-- Step 3: Create consumer-level enriched table with session metrics
CREATE OR REPLACE TABLE proddb.fionafan.active_user_sessions_enriched AS

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
  FROM proddb.fionafan.active_user_sessions_28d
),

consumer_onboarding AS (
  -- Get earliest onboarding info per consumer
  SELECT 
    consumer_id,
    MIN(onboarding_day) as onboarding_day,
    MIN(exposure_time) as exposure_time,
    MIN(dd_platform) as dd_platform,
    MIN(lifestage) as lifestage
  FROM proddb.fionafan.active_user_sessions_28d
  GROUP BY consumer_id
),

latest_sessions AS (
  SELECT
    consumer_id,
    MAX(CASE WHEN rn_week_1 = 1 THEN dd_session_id END) as latest_session_day_0_7,
    MAX(CASE WHEN rn_week_1 = 1 THEN session_timestamp END) as latest_session_day_0_7_ts,
    MAX(CASE WHEN rn_week_2 = 1 THEN dd_session_id END) as latest_session_day_8_14,
    MAX(CASE WHEN rn_week_2 = 1 THEN session_timestamp END) as latest_session_day_8_14_ts,
    MAX(CASE WHEN rn_week_3 = 1 THEN dd_session_id END) as latest_session_day_15_21,
    MAX(CASE WHEN rn_week_3 = 1 THEN session_timestamp END) as latest_session_day_15_21_ts,
    MAX(CASE WHEN rn_week_4 = 1 THEN dd_session_id END) as latest_session_day_22_28,
    MAX(CASE WHEN rn_week_4 = 1 THEN session_timestamp END) as latest_session_day_22_28_ts
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
    MAX(CASE WHEN nth_session_consumer = 1 THEN session_timestamp END) as first_session_ts,
    MAX(CASE WHEN nth_session_consumer = 2 THEN dd_session_id END) as second_session_id,
    MAX(CASE WHEN nth_session_consumer = 2 THEN session_timestamp END) as second_session_ts,
    MAX(CASE WHEN nth_session_consumer = 3 THEN dd_session_id END) as third_session_id,
    MAX(CASE WHEN nth_session_consumer = 3 THEN session_timestamp END) as third_session_ts,
    
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
  s.first_session_ts,
  s.second_session_id,
  s.second_session_ts,
  s.third_session_id,
  s.third_session_ts,
  l.latest_session_day_0_7,
  l.latest_session_day_0_7_ts,
  l.latest_session_day_8_14,
  l.latest_session_day_8_14_ts,
  l.latest_session_day_15_21,
  l.latest_session_day_15_21_ts,
  l.latest_session_day_22_28,
  l.latest_session_day_22_28_ts,
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

-- Join session data to events_all table
-- Unpivot session IDs and timestamps, then join to events
CREATE OR REPLACE TABLE proddb.fionafan.active_user_sessions_with_events AS


WITH unpivoted_sessions AS (
  -- Stack all session types using UNION ALL
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    first_session_id as session_id,
    first_session_ts as session_ts,
    'first_session' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE first_session_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    second_session_id as session_id,
    second_session_ts as session_ts,
    'second_session' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE second_session_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    third_session_id as session_id,
    third_session_ts as session_ts,
    'third_session' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE third_session_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    latest_session_day_0_7 as session_id,
    latest_session_day_0_7_ts as session_ts,
    'latest_week_1' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE latest_session_day_0_7 IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    latest_session_day_8_14 as session_id,
    latest_session_day_8_14_ts as session_ts,
    'latest_week_2' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE latest_session_day_8_14 IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    latest_session_day_15_21 as session_id,
    latest_session_day_15_21_ts as session_ts,
    'latest_week_3' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE latest_session_day_15_21 IS NOT NULL
  
  UNION ALL
  
  SELECT 
    consumer_id,
    onboarding_day,
    exposure_time,
    dd_platform,
    lifestage,
    latest_session_day_22_28 as session_id,
    latest_session_day_22_28_ts as session_ts,
    'latest_week_4' as session_type
  FROM proddb.fionafan.active_user_sessions_enriched
  WHERE latest_session_day_22_28 IS NOT NULL
),

session_event_dates AS (
  SELECT
    *,
    -- Convert UTC session timestamp to PST date for matching
    DATE(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', session_ts)) as session_date_pst
  FROM unpivoted_sessions
)

SELECT 
  e.*,
  s.session_type,
  s.session_ts as session_ts_utc,
  s.session_date_pst,
  s.onboarding_day,
  s.exposure_time,
  s.lifestage
FROM proddb.tyleranderson.events_all e
INNER JOIN session_event_dates s
  ON e.user_id::varchar = s.consumer_id::varchar
  AND e.event_date = s.session_date_pst
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.user_id, e.event_date, e.session_num, e.timestamp, e.event_rank
  ORDER BY 
    CASE s.session_type
      WHEN 'first_session' THEN 1
      WHEN 'second_session' THEN 2
      WHEN 'third_session' THEN 3
      WHEN 'latest_week_1' THEN 4
      WHEN 'latest_week_2' THEN 5
      WHEN 'latest_week_3' THEN 6
      WHEN 'latest_week_4' THEN 7
    END
) = 1
ORDER BY e.user_id, e.timestamp, e.event_rank;

select * from proddb.fionafan.new_user_sessions_with_events where user_id = '109515099' order by event_date;

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
FROM proddb.fionafan.active_user_sessions_enriched;





WITH total_cohort AS (
  -- Get total number of consumers in the cohort (baseline)
  SELECT COUNT(DISTINCT consumer_id) as total_consumers
  FROM proddb.fionafan.active_user_sessions_28d
),

daily_active_consumers AS (
  SELECT
    days_since_onboarding,
    COUNT(DISTINCT consumer_id) as active_consumers,
    COUNT(DISTINCT dd_session_id) as total_sessions
  FROM proddb.fionafan.active_user_sessions_28d
  WHERE days_since_onboarding BETWEEN 0 AND 28
  GROUP BY days_since_onboarding
),

-- Add baseline day -1 (total cohort)
all_days AS (
  SELECT 
    -1 as days_since_onboarding,
    c.total_consumers as active_consumers,
    0 as total_sessions
  FROM total_cohort c
  
  UNION ALL
  
  SELECT 
    days_since_onboarding,
    active_consumers,
    total_sessions
  FROM daily_active_consumers
)

SELECT
  d.days_since_onboarding,
  d.active_consumers,
  d.total_sessions,
  CASE WHEN d.total_sessions > 0 
    THEN ROUND(d.total_sessions::FLOAT / NULLIF(d.active_consumers, 0), 2)
    ELSE NULL 
  END as avg_sessions_per_consumer,
  
  -- Drop-off from previous day
  LAG(d.active_consumers) OVER (ORDER BY d.days_since_onboarding) as prev_day_consumers,
  d.active_consumers - LAG(d.active_consumers) OVER (ORDER BY d.days_since_onboarding) as dropoff_from_prev_day,
  ROUND(
    100.0 * (d.active_consumers - LAG(d.active_consumers) OVER (ORDER BY d.days_since_onboarding))::FLOAT 
    / NULLIF(LAG(d.active_consumers) OVER (ORDER BY d.days_since_onboarding), 0),
    2
  ) as pct_change_from_prev_day,
  
  -- Retention from day -1 (baseline)
  c.total_consumers as day_minus_1_baseline,
  d.active_consumers - c.total_consumers as dropoff_from_baseline,
  ROUND(100.0 * d.active_consumers::FLOAT / NULLIF(c.total_consumers, 0), 2) as retention_rate_from_baseline,
  ROUND(100.0 * (c.total_consumers - d.active_consumers)::FLOAT / NULLIF(c.total_consumers, 0), 2) as dropoff_rate_from_baseline

FROM all_days d
CROSS JOIN total_cohort c
ORDER BY d.days_since_onboarding;


WITH total_cohort AS (
  -- Get total number of consumers in the cohort (baseline)
  SELECT COUNT(DISTINCT consumer_id) as total_consumers
  FROM proddb.fionafan.active_user_sessions_28d
),

weekly_active_consumers AS (
  SELECT
    CASE 
      WHEN days_since_onboarding BETWEEN 0 AND 7 THEN 'Week 1 (Days 0-7)'
      WHEN days_since_onboarding BETWEEN 8 AND 14 THEN 'Week 2 (Days 8-14)'
      WHEN days_since_onboarding BETWEEN 15 AND 21 THEN 'Week 3 (Days 15-21)'
      WHEN days_since_onboarding BETWEEN 22 AND 28 THEN 'Week 4 (Days 22-28)'
    END as week_label,
    CASE 
      WHEN days_since_onboarding BETWEEN 0 AND 7 THEN 1
      WHEN days_since_onboarding BETWEEN 8 AND 14 THEN 2
      WHEN days_since_onboarding BETWEEN 15 AND 21 THEN 3
      WHEN days_since_onboarding BETWEEN 22 AND 28 THEN 4
    END as week_number,
    COUNT(DISTINCT consumer_id) as active_consumers,
    COUNT(DISTINCT dd_session_id) as total_sessions
  FROM proddb.fionafan.active_user_sessions_28d
  WHERE days_since_onboarding BETWEEN 0 AND 28
  GROUP BY 1, 2
),

-- Add baseline week 0 (total cohort)
all_weeks AS (
  SELECT 
    0 as week_number,
    'Week 0 (Baseline)' as week_label,
    c.total_consumers as active_consumers,
    0 as total_sessions
  FROM total_cohort c
  
  UNION ALL
  
  SELECT 
    week_number,
    week_label,
    active_consumers,
    total_sessions
  FROM weekly_active_consumers
)

SELECT
  w.week_number,
  w.week_label,
  w.active_consumers,
  w.total_sessions,
  CASE WHEN w.total_sessions > 0 
    THEN ROUND(w.total_sessions::FLOAT / NULLIF(w.active_consumers, 0), 2)
    ELSE NULL 
  END as avg_sessions_per_consumer,
  
  -- Drop-off from previous week
  LAG(w.active_consumers) OVER (ORDER BY w.week_number) as prev_week_consumers,
  w.active_consumers - LAG(w.active_consumers) OVER (ORDER BY w.week_number) as dropoff_from_prev_week,
  ROUND(
    100.0 * (w.active_consumers - LAG(w.active_consumers) OVER (ORDER BY w.week_number))::FLOAT 
    / NULLIF(LAG(w.active_consumers) OVER (ORDER BY w.week_number), 0),
    2
  ) as pct_change_from_prev_week,
  
  -- Retention from week 0 (baseline)
  c.total_consumers as week_0_baseline,
  w.active_consumers - c.total_consumers as dropoff_from_baseline,
  ROUND(100.0 * w.active_consumers::FLOAT / NULLIF(c.total_consumers, 0), 2) as retention_rate_from_baseline,
  ROUND(100.0 * (c.total_consumers - w.active_consumers)::FLOAT / NULLIF(c.total_consumers, 0), 2) as dropoff_rate_from_baseline

FROM all_weeks w
CROSS JOIN total_cohort c
ORDER BY w.week_number;