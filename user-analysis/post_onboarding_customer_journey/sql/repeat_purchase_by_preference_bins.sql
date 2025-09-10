-- Repeat purchase analysis by expressed preference count bins
-- Bins: 1-2 vs 3-7 entities selected on onboarding preference page
-- Cohort: Treatment users exposed to cx_mobile_onboarding_preferences (iOS, v1)
-- Window: Orders created between 2025-08-04 and 2025-09-30; orders must occur on/after exposure

WITH exposure AS (
  SELECT 
    ee.tag,
    LOWER(ee.segment) AS segments,
    REPLACE(LOWER(CASE WHEN ee.bucket_key LIKE 'dx_%' THEN ee.bucket_key ELSE 'dx_'||ee.bucket_key END), '-') AS dd_device_id_filtered,
    ee.custom_attributes:userId AS consumer_id,
    MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)) AS exposure_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = 'cx_mobile_onboarding_preferences'
    AND ee.experiment_version::INT = 1
    AND ee.segment IN ('iOS')
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time) BETWEEN '2025-08-04' AND '2025-09-30'
  GROUP BY 1,2,3,4
),

entity_counts AS (
  SELECT consumer_id, entity_cnt
  FROM proddb.fionafan.preference_entity_cnt_distribution
  WHERE page = 'onboarding_preference_page' AND entity_cnt IS NOT NULL
),

orders AS (
  SELECT DISTINCT 
    REPLACE(LOWER(CASE WHEN a.dd_device_id LIKE 'dx_%' THEN a.dd_device_id ELSE 'dx_'||a.dd_device_id END), '-') AS dd_device_id_filtered,
    CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.timestamp) AS order_ts,
    dd.delivery_id
  FROM segment_events_raw.consumer_production.order_cart_submit_received a
  JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.created_at) BETWEEN '2025-08-04' AND '2025-09-30'
  WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.timestamp) BETWEEN '2025-08-04' AND '2025-09-30'
),

cohort AS (
  SELECT 
    e.dd_device_id_filtered,
    e.consumer_id,
    e.exposure_time,
    CASE 
      WHEN ec.entity_cnt BETWEEN 1 AND 2 THEN 'bin_1_2'
      WHEN ec.entity_cnt BETWEEN 3 AND 7 THEN 'bin_3_7'
      ELSE 'other'
    END AS preference_bin
  FROM exposure e
  JOIN entity_counts ec
    ON e.consumer_id = ec.consumer_id
  WHERE e.tag <> 'control'
),

orders_after_exposure AS (
  SELECT 
    c.dd_device_id_filtered,
    c.preference_bin,
    COUNT(DISTINCT o.delivery_id) AS orders_after_exp
  FROM cohort c
  LEFT JOIN orders o
    ON c.dd_device_id_filtered = o.dd_device_id_filtered
    AND o.order_ts >= c.exposure_time
  GROUP BY 1,2
),

bin_metrics AS (
  SELECT 
    preference_bin,
    COUNT(*) AS cohort_users,
    COUNT(CASE WHEN orders_after_exp >= 1 THEN 1 END) AS buyers,
    COUNT(CASE WHEN orders_after_exp >= 2 THEN 1 END) AS repeat_buyers,
    ROUND(COUNT(CASE WHEN orders_after_exp >= 2 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN orders_after_exp >= 1 THEN 1 END), 0), 2) AS repeat_purchase_rate_pct
  FROM orders_after_exposure
--   WHERE preference_bin IN ('bin_1_2','bin_3_7')
  GROUP BY preference_bin
)

SELECT 
  preference_bin,
  cohort_users,
  buyers,
  repeat_buyers,
  repeat_purchase_rate_pct
FROM bin_metrics
ORDER BY CASE preference_bin WHEN 'bin_1_2' THEN 1 WHEN 'bin_3_7' THEN 2 ELSE 3 END;


