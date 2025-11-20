-- Consolidated analysis for all three cohorts: Active Users, New Users, and Post Onboarding
-- Each table includes a 'cohort_type' column to identify which cohort the consumer belongs to
-- Output: Unified tables tracking sessions within 28 days for all cohort types

-- Step 1: Create combined cohort base table
CREATE OR REPLACE TABLE proddb.fionafan.all_user_july_cohort AS

-- Active Users Cohort
WITH active_users AS (
  SELECT 
    c.consumer_id,
    c.lifestage,
    min(c.exposure_time) as exposure_time,
    min(c.exposure_time) as onboarding_day,
    'active' as cohort_type
  FROM proddb.fionafan.active_user_july_cohort c
  group by all
),

-- New Users Cohort
new_users AS (
  SELECT
    CONSUMER_ID as consumer_id,
    LIFESTAGE as lifestage,
    min(first_order_date) as exposure_time,
    min(first_order_date) as onboarding_day,
    'new' as cohort_type
  FROM proddb.fionafan.new_user_july_cohort
  group by all
),

-- Post Onboarding Cohort
post_onboarding AS (
  SELECT DISTINCT
    consumer_id,
    NULL as lifestage,
    min(exposure_time) as  exposure_time,
    cast(min(exposure_time) as date) as onboarding_day,
    'post_onboarding' as cohort_type
  FROM (
    SELECT DISTINCT
      consumer_id,
      iguazu_timestamp as exposure_time
    FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
    WHERE iguazu_timestamp BETWEEN '2025-07-01' AND '2025-07-31'
  )
  group by all
)

SELECT * FROM active_users
UNION ALL
SELECT * FROM new_users
UNION ALL
SELECT * FROM post_onboarding;


-- Validation of combined cohort
SELECT 
  cohort_type,
  COUNT(*) as total_consumers,
  MIN(onboarding_day) as min_exposure,
  MAX(onboarding_day) as max_exposure
FROM proddb.fionafan.all_user_july_cohort
GROUP BY cohort_type
ORDER BY cohort_type;

-- select consumer_id, count(1) cnt from proddb.fionafan.all_user_july_cohort group by all having cnt>1 order by cnt desc limit 10;
select * from proddb.fionafan.all_user_july_cohort WHERE  consumer_id = '1960477287' limit 10;


select *  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice where iguazu_timestamp BETWEEN '2025-07-01' AND '2025-07-31' and consumer_id = '1960477287' limit 200;
-- Step 2: Create combined sessions table (28 days post exposure)
-- Concatenate the three existing sessions_28d tables from each cohort
CREATE OR REPLACE TABLE proddb.fionafan.all_user_sessions_28d AS

-- Active Users Sessions
SELECT 
  'active' as cohort_type,
  onboarding_day,
  exposure_time,
  consumer_id,
  DD_DEVICE_ID,
  dd_device_id_filtered,
  dd_platform,
  lifestage,
  dd_session_id,
  session_timestamp,
  session_date,
  days_since_onboarding
FROM proddb.fionafan.active_user_sessions_28d

UNION ALL

-- New Users Sessions
SELECT 
  'new' as cohort_type,
  onboarding_day,
  exposure_time,
  consumer_id,
  DD_DEVICE_ID,
  dd_device_id_filtered,
  dd_platform,
  lifestage,
  dd_session_id,
  session_timestamp,
  session_date,
  days_since_onboarding
FROM proddb.fionafan.new_user_sessions_28d

UNION ALL

-- Post Onboarding Sessions
SELECT 
  'post_onboarding' as cohort_type,
  onboarding_day,
  exposure_time,
  consumer_id,
  DD_DEVICE_ID,
  dd_device_id_filtered,
  dd_platform,
  NULL as lifestage,
  dd_session_id,
  session_timestamp,
  session_date,
  days_since_onboarding
FROM proddb.fionafan.sessions_28d_post_onboarding

ORDER BY cohort_type, consumer_id, session_timestamp;


-- Validation of sessions
SELECT 
  cohort_type,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(DISTINCT dd_session_id) as total_sessions,
  ROUND(COUNT(DISTINCT dd_session_id)::FLOAT / COUNT(DISTINCT consumer_id), 2) as avg_sessions_per_consumer
FROM proddb.fionafan.all_user_sessions_28d
GROUP BY cohort_type
ORDER BY cohort_type;


-- Step 3: Create enriched sessions table with session metadata
-- Concatenate the three existing sessions_enriched tables from each cohort
CREATE OR REPLACE TABLE proddb.fionafan.all_user_sessions_enriched AS

-- Active Users Enriched
SELECT 
  'active' as cohort_type,
  consumer_id,
  onboarding_day,
  exposure_time,
  dd_platform,
  lifestage,
  total_sessions,
  total_devices,
  first_session_id,
  first_session_ts,
  second_session_id,
  second_session_ts,
  third_session_id,
  third_session_ts,
  latest_session_day_0_7,
  latest_session_day_0_7_ts,
  latest_session_day_8_14,
  latest_session_day_8_14_ts,
  latest_session_day_15_21,
  latest_session_day_15_21_ts,
  latest_session_day_22_28,
  latest_session_day_22_28_ts,
  sessions_day_0_7,
  sessions_day_8_14,
  sessions_day_15_21,
  sessions_day_22_28
FROM proddb.fionafan.active_user_sessions_enriched

UNION ALL

-- New Users Enriched
SELECT 
  'new' as cohort_type,
  consumer_id,
  onboarding_day,
  exposure_time,
  dd_platform,
  lifestage,
  total_sessions,
  total_devices,
  first_session_id,
  first_session_ts,
  second_session_id,
  second_session_ts,
  third_session_id,
  third_session_ts,
  latest_session_day_0_7,
  latest_session_day_0_7_ts,
  latest_session_day_8_14,
  latest_session_day_8_14_ts,
  latest_session_day_15_21,
  latest_session_day_15_21_ts,
  latest_session_day_22_28,
  latest_session_day_22_28_ts,
  sessions_day_0_7,
  sessions_day_8_14,
  sessions_day_15_21,
  sessions_day_22_28
FROM proddb.fionafan.new_user_sessions_enriched

UNION ALL

-- Post Onboarding Enriched
SELECT 
  'post_onboarding' as cohort_type,
  consumer_id,
  onboarding_day,
  exposure_time,
  dd_platform,
  NULL as lifestage,
  total_sessions,
  total_devices,
  first_session_id,
  first_session_ts,
  second_session_id,
  second_session_ts,
  third_session_id,
  third_session_ts,
  latest_session_day_0_7,
  latest_session_day_0_7_ts,
  latest_session_day_8_14,
  latest_session_day_8_14_ts,
  latest_session_day_15_21,
  latest_session_day_15_21_ts,
  latest_session_day_22_28,
  latest_session_day_22_28_ts,
  sessions_day_0_7,
  sessions_day_8_14,
  sessions_day_15_21,
  sessions_day_22_28
FROM proddb.fionafan.sessions_28d_enriched

ORDER BY cohort_type, onboarding_day, consumer_id;


-- Validation of enriched sessions
SELECT 
  cohort_type,
  COUNT(*) as total_consumers,
  AVG(total_sessions) as avg_sessions_per_consumer,
  AVG(total_devices) as avg_devices_per_consumer,
  COUNT(CASE WHEN first_session_id IS NOT NULL THEN 1 END) as consumers_with_first_session,
  COUNT(CASE WHEN second_session_id IS NOT NULL THEN 1 END) as consumers_with_second_session,
  COUNT(CASE WHEN third_session_id IS NOT NULL THEN 1 END) as consumers_with_third_session,
  AVG(sessions_day_0_7) as avg_sessions_week_1,
  AVG(sessions_day_8_14) as avg_sessions_week_2,
  AVG(sessions_day_15_21) as avg_sessions_week_3,
  AVG(sessions_day_22_28) as avg_sessions_week_4,
  count(case when session_day_0_7>0 then 1 end) as consumers_with_session_day_0_7,
  count(case when session_day_8_14>0 then 1 end) as consumers_with_session_day_8_14,
  count(case when session_day_15_21>0 then 1 end) as consumers_with_session_day_15_21,
  count(case when session_day_22_28>0 then 1 end) as consumers_with_session_day_22_28
FROM proddb.fionafan.all_user_sessions_enriched
GROUP BY cohort_type
ORDER BY cohort_type;




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
-- Step 4: Create sessions with full event data
-- Concatenate the three existing sessions_with_events tables from each cohort
CREATE OR REPLACE TABLE proddb.fionafan.all_user_sessions_with_events AS


-- Active Users Sessions with Events
SELECT 
  'active' as cohort_type,
  event_date,
  session_num,
  platform,
  dd_device_id,
  user_id,
  timestamp,
  event_type,
  event,
  discovery_surface,
  discovery_feature,
  detail,
  store_id,
  store_name,
  context_timezone,
  context_os_version,
  event_rank,
  discovery_surface_click_attr,
  discovery_feature_click_attr,
  discovery_surface_imp_attr,
  discovery_feature_imp_attr,
  pre_purchase_flag,
  l91d_orders,
  last_order_date,
  session_type,
  session_ts_utc,
  session_date_pst,
  onboarding_day,
  exposure_time,
  lifestage,
  NULL as onboarding_type,
  NULL as promo_title
FROM proddb.fionafan.active_user_sessions_with_events

UNION ALL

-- New Users Sessions with Events
SELECT 
  'new' as cohort_type,
  event_date,
  session_num,
  platform,
  dd_device_id,
  user_id,
  timestamp,
  event_type,
  event,
  discovery_surface,
  discovery_feature,
  detail,
  store_id,
  store_name,
  context_timezone,
  context_os_version,
  event_rank,
  discovery_surface_click_attr,
  discovery_feature_click_attr,
  discovery_surface_imp_attr,
  discovery_feature_imp_attr,
  pre_purchase_flag,
  l91d_orders,
  last_order_date,
  session_type,
  session_ts_utc,
  session_date_pst,
  onboarding_day,
  exposure_time,
  lifestage,
  NULL as onboarding_type,
  NULL as promo_title
FROM proddb.fionafan.new_user_sessions_with_events

UNION ALL

-- Post Onboarding Sessions with Events
SELECT 
  'post_onboarding' as cohort_type,
  event_date,
  session_num,
  platform,
  dd_device_id,
  user_id,
  timestamp,
  event_type,
  event,
  discovery_surface,
  discovery_feature,
  detail,
  store_id,
  store_name,
  context_timezone,
  context_os_version,
  event_rank,
  discovery_surface_click_attr,
  discovery_feature_click_attr,
  discovery_surface_imp_attr,
  discovery_feature_imp_attr,
  pre_purchase_flag,
  l91d_orders,
  last_order_date,
  session_type,
  session_ts_utc,
  session_date_pst,
  onboarding_day,
  exposure_time,
  NULL as lifestage,
  onboarding_type,
  promo_title
FROM proddb.fionafan.post_onboarding_sessions_with_events

ORDER BY cohort_type, user_id, timestamp, event_rank;


-- Validation of sessions with events
SELECT 
  cohort_type,
  COUNT(DISTINCT user_id) as unique_consumers,
  COUNT(*) as total_events,
  ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT user_id), 2) as avg_events_per_consumer
FROM proddb.fionafan.all_user_sessions_with_events
GROUP BY cohort_type
ORDER BY cohort_type;


-- Daily drop-off analysis - ALL COHORTS
-- Shows how many consumers had sessions on each day (0-28) since exposure
-- Includes day -1 baseline showing total cohort size

WITH total_cohort AS (
  SELECT 
    cohort_type,
    COUNT(DISTINCT consumer_id) as total_consumers
  FROM proddb.fionafan.all_user_sessions_28d
  where DAYOFWEEK(onboarding_day) = 3
  GROUP BY cohort_type
),

daily_active_consumers AS (
  SELECT
    cohort_type,
    days_since_onboarding,
    COUNT(DISTINCT consumer_id) as active_consumers,
    COUNT(DISTINCT dd_session_id) as total_sessions
  FROM proddb.fionafan.all_user_sessions_28d
  WHERE days_since_onboarding BETWEEN 0 AND 28
  and DAYOFWEEK(onboarding_day) = 3
  GROUP BY cohort_type, days_since_onboarding
),

all_days AS (
  SELECT 
    cohort_type,
    -1 as days_since_onboarding,
    total_consumers as active_consumers,
    0 as total_sessions
  FROM total_cohort
  
  UNION ALL
  
  SELECT 
    cohort_type,
    days_since_onboarding,
    active_consumers,
    total_sessions
  FROM daily_active_consumers
)

SELECT
  d.cohort_type,
  d.days_since_onboarding,
  d.active_consumers,
  d.total_sessions,
  CASE WHEN d.total_sessions > 0 
    THEN ROUND(d.total_sessions::FLOAT / NULLIF(d.active_consumers, 0), 2)
    ELSE NULL 
  END as avg_sessions_per_consumer,
  
  -- Drop-off from previous day
  LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as prev_day_consumers,
  d.active_consumers - LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as dropoff_from_prev_day,
  ROUND(
    100.0 * (d.active_consumers - LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding))::FLOAT 
    / NULLIF(LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding), 0),
    2
  ) as pct_change_from_prev_day,
  
  -- Retention from day -1 (baseline)
  c.total_consumers as day_minus_1_baseline,
  d.active_consumers - c.total_consumers as dropoff_from_baseline,
  ROUND(100.0 * d.active_consumers::FLOAT / NULLIF(c.total_consumers, 0), 2) as retention_rate_from_baseline,
  ROUND(100.0 * (c.total_consumers - d.active_consumers)::FLOAT / NULLIF(c.total_consumers, 0), 2) as dropoff_rate_from_baseline

FROM all_days d
INNER JOIN total_cohort c
  ON d.cohort_type = c.cohort_type
ORDER BY d.cohort_type, d.days_since_onboarding;


-- Weekly drop-off analysis - ALL COHORTS
-- Shows how many consumers had sessions in each week since exposure
-- Includes week 0 baseline showing total cohort size

WITH total_cohort AS (
  SELECT 
    cohort_type,
    COUNT(DISTINCT consumer_id) as total_consumers
  FROM proddb.fionafan.all_user_sessions_28d
  GROUP BY cohort_type
),

weekly_active_consumers AS (
  SELECT
    cohort_type,
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
  FROM proddb.fionafan.all_user_sessions_28d
  WHERE days_since_onboarding BETWEEN 0 AND 28
  GROUP BY cohort_type, 2, 3
),

all_weeks AS (
  SELECT 
    cohort_type,
    0 as week_number,
    'Week 0 (Baseline)' as week_label,
    total_consumers as active_consumers,
    0 as total_sessions
  FROM total_cohort
  
  UNION ALL
  
  SELECT 
    cohort_type,
    week_number,
    week_label,
    active_consumers,
    total_sessions
  FROM weekly_active_consumers
)

SELECT
  w.cohort_type,
  w.week_number,
  w.week_label,
  w.active_consumers,
  w.total_sessions,
  CASE WHEN w.total_sessions > 0 
    THEN ROUND(w.total_sessions::FLOAT / NULLIF(w.active_consumers, 0), 2)
    ELSE NULL 
  END as avg_sessions_per_consumer,
  
  -- Drop-off from previous week
  LAG(w.active_consumers) OVER (PARTITION BY w.cohort_type ORDER BY w.week_number) as prev_week_consumers,
  w.active_consumers - LAG(w.active_consumers) OVER (PARTITION BY w.cohort_type ORDER BY w.week_number) as dropoff_from_prev_week,
  ROUND(
    100.0 * (w.active_consumers - LAG(w.active_consumers) OVER (PARTITION BY w.cohort_type ORDER BY w.week_number))::FLOAT 
    / NULLIF(LAG(w.active_consumers) OVER (PARTITION BY w.cohort_type ORDER BY w.week_number), 0),
    2
  ) as pct_change_from_prev_week,
  
  -- Retention from week 0 (baseline)
  c.total_consumers as week_0_baseline,
  w.active_consumers - c.total_consumers as dropoff_from_baseline,
  ROUND(100.0 * w.active_consumers::FLOAT / NULLIF(c.total_consumers, 0), 2) as retention_rate_from_baseline,
  ROUND(100.0 * (c.total_consumers - w.active_consumers)::FLOAT / NULLIF(c.total_consumers, 0), 2) as dropoff_rate_from_baseline

FROM all_weeks w
INNER JOIN total_cohort c
  ON w.cohort_type = c.cohort_type
ORDER BY w.cohort_type, w.week_number;


-- Day of week analysis to investigate weekly bumps
-- Check if exposure dates are concentrated on certain days of the week

SELECT 
  cohort_type,
  DAYOFWEEK(onboarding_day) as day_of_week_num,
  DAYNAME(onboarding_day) as day_name,
  COUNT(DISTINCT consumer_id) as consumers,
  ROUND(100.0 * COUNT(DISTINCT consumer_id) / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort
FROM proddb.fionafan.all_user_sessions_28d
GROUP BY cohort_type, day_of_week_num, day_name
ORDER BY cohort_type, day_of_week_num;


