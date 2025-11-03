---------------------
-- universe: consumers who viewed the onboarding start promo page
create or replace table proddb.fionafan.npws_prepost_funnel as (
WITH start_page_view AS (
  SELECT DISTINCT
    consumer_id::varchar AS consumer_id,
    CAST(iguazu_timestamp AS DATE) AS day
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-10-15' AND '2025-10-27'
),

-- exposures limited to the start_page_view universe (inner join)
exposure AS (
  SELECT
    ee.tag,
    ee.result,
    ee.bucket_key AS consumer_id,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date) AS day,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)) AS exposure_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  INNER JOIN start_page_view spv
    ON ee.bucket_key::varchar = spv.consumer_id::varchar
  WHERE ee.experiment_name = 'enable_post_onboarding_in_consumer_targeting'
    -- AND ee.experiment_version::INT = 1
    AND convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)
        BETWEEN '2025-10-15' AND '2025-10-27'
  GROUP BY 1,2,3
),

-- each page CTE is filtered to the same start_page_view universe (join + require spv.consumer_id IS NOT NULL)
explore_page AS (
  SELECT DISTINCT
    ep.iguazu_user_id::varchar AS consumer_id,
    convert_timezone('UTC','America/Los_Angeles', ep.iguazu_timestamp)::date AS day
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD ep
  INNER JOIN start_page_view spv
    ON ep.iguazu_user_id::varchar = spv.consumer_id::varchar
  WHERE convert_timezone('UTC','America/Los_Angeles', ep.iguazu_timestamp) BETWEEN '2025-10-15' AND '2025-10-27'
),

store_page AS (
  SELECT DISTINCT
    sp.user_id::varchar AS consumer_id,
    convert_timezone('UTC','America/Los_Angeles', sp.timestamp)::date AS day
  FROM segment_events_RAW.consumer_production.m_store_page_load sp
  INNER JOIN start_page_view spv
    ON sp.user_id::varchar = spv.consumer_id::varchar
  WHERE convert_timezone('UTC','America/Los_Angeles', sp.timestamp) BETWEEN '2025-10-15' AND '2025-10-27'
),

cart_page AS (
  SELECT DISTINCT
    cp.consumer_id::varchar AS consumer_id,
    convert_timezone('UTC','America/Los_Angeles', cp.iguazu_timestamp)::date AS day
  FROM iguazu.consumer.m_order_cart_page_load cp
  INNER JOIN start_page_view spv
    ON cp.consumer_id::varchar = spv.consumer_id::varchar
  WHERE convert_timezone('UTC','America/Los_Angeles', cp.iguazu_timestamp) BETWEEN '2025-10-15' AND '2025-10-27'
),

checkout_page AS (
  SELECT DISTINCT
    cpi.user_id::varchar AS consumer_id,
    convert_timezone('UTC','America/Los_Angeles', cpi.timestamp)::date AS day
  FROM segment_events_RAW.consumer_production.m_checkout_page_load cpi
  INNER JOIN start_page_view spv
    ON cpi.user_id::varchar = spv.consumer_id::varchar
  WHERE convert_timezone('UTC','America/Los_Angeles', cpi.timestamp) BETWEEN '2025-10-15' AND '2025-10-27'
),

-- Build per-(tag, consumer_id, exposure_day) flags: did the consumer view explore/store/cart/checkout after exposure?
explore AS (
  SELECT
    e.tag,
    e.consumer_id,
    e.day AS exposure_day,
    MAX(CASE WHEN ep.consumer_id IS NOT NULL AND e.day <= ep.day THEN 1 ELSE 0 END) AS explore_view,
    MAX(CASE WHEN sp.consumer_id IS NOT NULL AND e.day <= sp.day THEN 1 ELSE 0 END) AS store_view,
    MAX(CASE WHEN cp.consumer_id IS NOT NULL AND e.day <= cp.day THEN 1 ELSE 0 END) AS cart_view,
    MAX(CASE WHEN cpi.consumer_id IS NOT NULL AND e.day <= cpi.day THEN 1 ELSE 0 END) AS checkout_view
  FROM exposure e
  LEFT JOIN explore_page ep
    ON e.consumer_id::varchar = ep.consumer_id::varchar
    AND e.day <= ep.day
  LEFT JOIN store_page sp
    ON e.consumer_id::varchar = sp.consumer_id::varchar
    AND e.day <= sp.day
  LEFT JOIN cart_page cp
    ON e.consumer_id::varchar = cp.consumer_id::varchar
    AND e.day <= cp.day
  LEFT JOIN checkout_page cpi
    ON e.consumer_id::varchar = cpi.consumer_id::varchar
    AND e.day <= cpi.day
  WHERE e.tag NOT IN ('internal_test','reserved')
  GROUP BY 1,2,3
),

-- roll up to tag-level with safe rates
explore_res AS (
  SELECT
    tag,
    COUNT(DISTINCT consumer_id) AS exposure_onboard,
    SUM(explore_view) AS explore_view,
    (SUM(explore_view)::FLOAT) / NULLIF(COUNT(DISTINCT consumer_id),0) AS explore_rate,
    SUM(store_view) AS store_view,
    (SUM(store_view)::FLOAT) / NULLIF(SUM(explore_view),0) AS store_rate,
    SUM(cart_view) AS cart_view,
    (SUM(cart_view)::FLOAT) / NULLIF(SUM(store_view),0) AS cart_rate,
    SUM(checkout_view) AS checkout_view,
    (SUM(checkout_view)::FLOAT) / NULLIF(SUM(cart_view),0) AS checkout_rate
  FROM explore
  GROUP BY 1
  ORDER BY 1
),

res AS (
  SELECT * FROM explore_res
)

SELECT
  r1.tag,
  r1.exposure_onboard,
  r1.explore_view,
  r1.explore_rate,
  (r1.explore_rate / NULLIF(r2.explore_rate,0) - 1) AS Lift_explore_rate,
  r1.store_view,
  r1.store_rate,
  (r1.store_rate / NULLIF(r2.store_rate,0) - 1) AS Lift_store_rate,
  r1.cart_view,
  r1.cart_rate,
  (r1.cart_rate / NULLIF(r2.cart_rate,0) - 1) AS Lift_cart_rate,
  r1.checkout_view,
  r1.checkout_rate,
  (r1.checkout_rate / NULLIF(r2.checkout_rate,0) - 1) AS Lift_checkout_rate
FROM res r1
LEFT JOIN res r2
  ON r2.tag = 'control'
ORDER BY 1 DESC)
;

select * from proddb.fionafan.npws_prepost_funnel ;