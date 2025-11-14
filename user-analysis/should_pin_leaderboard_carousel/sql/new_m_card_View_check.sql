
create or replace table proddb.fionafan.should_pin_leaderboard_new_exp_data as (

WITH params AS (
  SELECT '2025-10-03'::date AS START_DATE, CURRENT_DATE AS END_DATE
),

onboarding_users AS (
  SELECT DISTINCT
      REPLACE(LOWER(CASE WHEN DD_DEVICE_ID LIKE 'dx_%' THEN DD_DEVICE_ID ELSE 'dx_'||DD_DEVICE_ID END), '-') AS dd_device_id_filtered,
      iguazu_timestamp AS join_time,
      CAST(iguazu_timestamp AS DATE) AS onboard_day,
      consumer_id
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  CROSS JOIN params p
  WHERE iguazu_timestamp BETWEEN p.START_DATE AND p.END_DATE
),

/* ~1% sample of exposed consumers (by consumer_id) to meet 15s limit */
experiment_data AS (
  SELECT
      ee.tag,
      ee.result,
      ee.bucket_key AS consumer_id_str,
      TRY_TO_NUMBER(ee.bucket_key) AS consumer_id_num,
      ee.segment,
      MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)::date) AS exposure_day_pst,
      MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)) AS exposure_time_pst
      , ou.onboard_day
      , ou.join_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  left JOIN onboarding_users ou
    ON ee.bucket_key = ou.consumer_id
   AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)::date = ou.onboard_day
  CROSS JOIN params p
  WHERE ee.experiment_name = 'should_pin_leaderboard_carousel'
    AND ee.segment = 'iOS'
    AND ee.experiment_version::INT = 6
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time) BETWEEN p.START_DATE AND p.END_DATE
    AND TRY_TO_NUMBER(ee.bucket_key) IS NOT NULL
  GROUP BY all
)
select * from experiment_data
);

/* First session after onboarding (within 24h). Pruned by event_date. */
create or replace table proddb.fionafan.should_pin_leaderboard_new_first_session_pick as (


with first_session_pick AS (
  SELECT
      f.consumer_id,
      ed.result,
      ed.tag,
      ed.segment,
      ed.onboard_day,
      ed.join_time,
      f.dd_session_id,
      f.event_time AS first_session_start_time,
      ROW_NUMBER() OVER (PARTITION BY f.consumer_id ORDER BY f.event_time ASC) AS rn
  FROM edw.consumer.fact_consumer_carousel_impressions f
  JOIN proddb.fionafan.should_pin_leaderboard_new_exp_data ed
    ON f.consumer_id = ed.consumer_id_num
  WHERE f.platform ILIKE '%ios%'
    AND f.dd_session_id IS NOT NULL
    AND f.event_time >= ed.join_time
    AND f.event_time <  ed.join_time + INTERVAL '5 days'
  QUALIFY rn = 1
),

/* Only "Customer Favorites" rows within that first session (<=2h) and same day for partition pruning. */
first_session_cf_impressions AS (
  SELECT
      f.consumer_id,
      p.result,
      p.tag,
      p.segment,
      p.dd_session_id,
      p.first_session_start_time,
      f.event_time,
      f.event_date,
      f.container_name,
      f.card_position,
      f.vertical_position
  FROM edw.consumer.fact_consumer_carousel_impressions f
  JOIN first_session_pick p
    ON f.consumer_id = p.consumer_id
   AND f.dd_session_id = p.dd_session_id
  WHERE f.event_time >= p.first_session_start_time
--     AND f.event_time <  p.first_session_start_time + INTERVAL '2 hours'   -- tighter session bound
    AND f.event_date = CAST(p.first_session_start_time AS DATE)
    -- AND f.container_name ILIKE '%customer%favorite%'   -- adjust to canonical name/id if known
)
select * from first_session_cf_impressions
);
select tag, count(1), sum(case when container_name ilike '%customer%favorite%' then 1 else 0 end) as cf_impressions_count 
from proddb.fionafan.should_pin_leaderboard_new_first_session_pick group by all;
create or replace table proddb.fionafan.should_pin_leaderboard_new_first_session_cf_impressions as (

/* Session-level metrics (1 row/user): CF impression count and average positions */
with session_level AS (
  SELECT
      p.consumer_id,
      p.result,
      p.tag,
      p.segment,
      p.first_session_start_time,
      COALESCE(COUNT(*), 0) AS cf_impressions_in_session,
      AVG(vertical_position) AS avg_cf_vertical_position,
      AVG(card_position)     AS avg_cf_card_position,
      CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS has_any_cf
  FROM first_session_cf_impressions p
  GROUP BY 1,2,3,4,5
),

/* Orders only for sampled exposed consumers; pruned by ACTIVE_DATE_UTC and ORDER_CREATED_TIME */
orders_in_windows AS (
  SELECT
      s.consumer_id,
      s.first_session_start_time,
      MAX(CASE WHEN d.order_created_time >= s.first_session_start_time
                AND d.order_created_time <  s.first_session_start_time + INTERVAL '1 hour' THEN 1 ELSE 0 END) AS conv_1h,
      MAX(CASE WHEN d.order_created_time >= s.first_session_start_time
                AND d.order_created_time <  s.first_session_start_time + INTERVAL '4 hours' THEN 1 ELSE 0 END) AS conv_4h,
      MAX(CASE WHEN d.order_created_time >= s.first_session_start_time
                AND d.order_created_time <  s.first_session_start_time + INTERVAL '1 day' THEN 1 ELSE 0 END) AS conv_1d,
      MAX(CASE WHEN d.order_created_time >= s.first_session_start_time
                AND d.order_created_time <  s.first_session_start_time + INTERVAL '7 days' THEN 1 ELSE 0 END) AS conv_7d,
      MAX(CASE WHEN d.order_created_time >= s.first_session_start_time
                AND d.order_created_time <  s.first_session_start_time + INTERVAL '30 days' THEN 1 ELSE 0 END) AS conv_30d
  FROM session_level s
  LEFT JOIN edw.growth.fact_consumer_deliveries d
    ON d.consumer_id = s.consumer_id
   AND d.order_created_time >= s.first_session_start_time
   AND d.order_created_time <  s.first_session_start_time + INTERVAL '30 days'
   AND d.active_date_utc BETWEEN CAST(s.first_session_start_time AS DATE)
                             AND CAST(s.first_session_start_time + INTERVAL '30 days' AS DATE)
  GROUP BY 1,2
)
select s.*, o.conv_1h, o.conv_4h, o.conv_1d, o.conv_7d, o.conv_30d
FROM session_level s
LEFT JOIN orders_in_windows o
  ON s.consumer_id = o.consumer_id
 AND s.first_session_start_time = o.first_session_start_time
);
SELECT
  s.result,                               -- treatment vs control
  COUNT(*) AS users_with_first_session,
  AVG(s.cf_impressions_in_session) AS avg_cf_impressions_in_first_session,
  AVG(CASE WHEN s.has_any_cf = 1 THEN s.avg_cf_vertical_position END) AS avg_vertical_position_when_cf_seen,
  AVG(CASE WHEN s.has_any_cf = 1 THEN s.avg_cf_card_position END)     AS avg_card_position_when_cf_seen,
  AVG(o.conv_1h)::float  AS conv_rate_1h,
  AVG(o.conv_4h)::float  AS conv_rate_4h,
  AVG(o.conv_1d)::float  AS conv_rate_1d,
  AVG(o.conv_7d)::float  AS conv_rate_7d,
  AVG(o.conv_30d)::float AS conv_rate_30d
FROM session_level s
LEFT JOIN orders_in_windows o
  ON s.consumer_id = o.consumer_id
 AND s.first_session_start_time = o.first_session_start_time
GROUP BY 1
ORDER BY 1;
