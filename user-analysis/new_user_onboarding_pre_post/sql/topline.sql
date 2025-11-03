--------------------- experiment exposure (consumer-level, filtered to consumers in start_page_view)
create or replace table proddb.fionafan.npws_prepost_topline_exposure as (
WITH start_page_view AS (
  -- the filter universe: consumers who viewed the onboarding start promo page in the window
  SELECT DISTINCT
    consumer_id::varchar AS consumer_id,
    CAST(iguazu_timestamp AS DATE) AS day
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-10-15' AND '2025-10-27'
),

exposure AS (
  SELECT
    ee.tag,
    ee.result,
    ee.bucket_key::varchar AS consumer_id,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date) AS day,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)) AS exposure_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  INNER JOIN start_page_view spv
    ON ee.bucket_key::varchar = spv.consumer_id::varchar
  WHERE experiment_name = 'enable_post_onboarding_in_consumer_targeting'
    AND experiment_version::INT = 1
    AND tag <> 'overridden'
    AND convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)
        BETWEEN '2025-10-15' AND '2025-10-27'
  GROUP BY 1,2,3
),

orders AS (
  SELECT DISTINCT
    dd.creator_id::varchar AS consumer_id,
    convert_timezone('UTC','America/Los_Angeles', a.timestamp)::date AS day,
    dd.delivery_ID,
    dd.is_first_ordercart_DD,
    dd.is_filtered_core,
    dd.variable_profit * 0.01 AS variable_profit,
    dd.gov * 0.01 AS gov
  FROM segment_events_raw.consumer_production.order_cart_submit_received a
  JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles', dd.created_at)
        BETWEEN '2025-10-15' AND '2025-10-27'
  INNER JOIN start_page_view spv
    ON dd.creator_id::varchar = spv.consumer_id::varchar
  WHERE convert_timezone('UTC','America/Los_Angeles', a.timestamp)
        BETWEEN '2025-10-15' AND '2025-10-27'
),

-- per-consumer metrics after exposure (only consumers in start_page_view)
checkout_per_consumer AS (
  SELECT
    e.tag,
    e.consumer_id,
    MIN(e.day) AS first_exposure_day,
    MAX(CASE WHEN o.delivery_ID IS NOT NULL THEN 1 ELSE 0 END) AS had_order,
    MAX(CASE WHEN o.is_first_ordercart_DD = 1 AND o.is_filtered_core = 1 THEN 1 ELSE 0 END) AS had_new_cx,
    COALESCE(SUM(CASE WHEN o.is_filtered_core = 1 THEN o.variable_profit ELSE 0 END),0) AS sum_variable_profit,
    COALESCE(SUM(CASE WHEN o.is_filtered_core = 1 THEN o.gov ELSE 0 END),0) AS sum_gov,
    COUNT(DISTINCT CASE WHEN o.is_filtered_core = 1 THEN o.delivery_ID END) AS n_orders_for_consumer
  FROM exposure e
  LEFT JOIN orders o
    ON e.consumer_id::varchar = o.consumer_id::varchar
    AND e.day <= o.day
  WHERE e.tag NOT IN ('internal_test','reserved')
  GROUP BY 1,2
),

-- aggregate to tag-level
checkout AS (
  SELECT
    c.tag,
    COUNT(DISTINCT c.consumer_id) AS exposure_onboard,
    SUM(c.n_orders_for_consumer) AS orders,
    SUM(c.had_new_cx) AS new_cx,
    (SUM(c.n_orders_for_consumer)::FLOAT) / NULLIF(COUNT(DISTINCT c.consumer_id),0) AS order_rate,
    (SUM(c.had_new_cx)::FLOAT) / NULLIF(COUNT(DISTINCT c.consumer_id),0) AS new_cx_rate,
    SUM(c.sum_variable_profit) AS variable_profit,
    (SUM(c.sum_variable_profit)::FLOAT) / NULLIF(COUNT(DISTINCT c.consumer_id),0) AS VP_per_consumer,
    SUM(c.sum_gov) AS gov,
    (SUM(c.sum_gov)::FLOAT) / NULLIF(COUNT(DISTINCT c.consumer_id),0) AS gov_per_consumer,
    STDDEV_SAMP(c.sum_variable_profit) AS std_variable_profit,
    STDDEV_SAMP(c.sum_gov) AS std_gov,
    SUM(c.n_orders_for_consumer) AS n_orders_for_stats
  FROM checkout_per_consumer c
  GROUP BY 1
  ORDER BY 1
),

-- MAU (orders in past 28 days) restricted to start_page_view consumers
MAU AS (
  SELECT
    e.tag,
    COUNT(DISTINCT o.consumer_id) AS MAU,
    (COUNT(DISTINCT o.consumer_id)::FLOAT) / NULLIF(COUNT(DISTINCT e.consumer_id),0) AS MAU_rate
  FROM exposure e
  LEFT JOIN orders o
    ON e.consumer_id::varchar = o.consumer_id::varchar
    AND o.day BETWEEN DATEADD('day', -28, current_date) AND DATEADD('day', -1, current_date)
  GROUP BY 1
  ORDER BY 1
),

res AS (
  SELECT
    ch.*,
    COALESCE(m.MAU,0) AS MAU,
    COALESCE(m.MAU_rate,0) AS MAU_rate
  FROM checkout ch
  LEFT JOIN MAU m ON ch.tag = m.tag
)

-- final comparison to control group
SELECT
  r1.tag,
  r1.exposure_onboard AS exposure,
  r1.orders,
  r1.order_rate,
  (r1.order_rate / NULLIF(r2.order_rate,0) - 1) AS Lift_order_rate,
  r1.new_cx,
  r1.new_cx_rate,
  (r1.new_cx_rate / NULLIF(r2.new_cx_rate,0) - 1) AS Lift_new_cx_rate,
  r1.variable_profit,
  (r1.variable_profit / NULLIF(r2.variable_profit,0) - 1) AS Lift_VP,
  r1.VP_per_consumer,
  (r1.VP_per_consumer / NULLIF(r2.VP_per_consumer,0) - 1) AS Lift_VP_per_consumer,
  r1.gov,
  (r1.gov / NULLIF(r2.gov,0) - 1) AS Lift_gov,
  r1.gov_per_consumer,
  (r1.gov_per_consumer / NULLIF(r2.gov_per_consumer,0) - 1) AS Lift_gov_per_consumer,
  r1.MAU,
  r1.MAU_rate,
  (r1.MAU_rate / NULLIF(r2.MAU_rate,0) - 1) AS Lift_MAU_rate,
  -- optional diagnostic columns
  r1.std_variable_profit,
  r1.std_gov,
  r2.order_rate      AS control_order_rate,
  r2.new_cx_rate     AS control_new_cx_rate,
  r2.variable_profit AS control_variable_profit,
  r2.VP_per_consumer AS control_VP_per_consumer,
  r2.gov_per_consumer AS control_gov_per_consumer,
  r2.MAU_rate        AS control_MAU_rate,
  r2.std_variable_profit AS control_std_variable_profit,
  r2.std_gov            AS control_std_gov,
  r2.n_orders_for_stats  AS control_n_orders,
  r2.exposure_onboard    AS control_exposure,
  r2.orders              AS control_orders,
  r2.new_cx              AS control_new_cx,
  r2.MAU                 AS control_MAU
FROM res r1
LEFT JOIN res r2
  ON r2.tag = 'control'
WHERE r1.tag IS NOT NULL
ORDER BY 1 DESC)
;