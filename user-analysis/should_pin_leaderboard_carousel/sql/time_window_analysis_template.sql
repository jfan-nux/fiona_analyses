-- Should Pin Leaderboard Carousel Experiment Analysis Template
-- Time window analysis comparing different post-onboarding order windows
-- Template parameters: {time_value}, {time_unit}

WITH onboarding_users AS
(SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , iguazu_timestamp as join_time
      , cast(iguazu_timestamp as date) AS onboard_day
      , consumer_id
from iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN '2025-08-25'::date-60 AND CURRENT_DATE
)

, exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key AS consumer_id
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
               , MIN(ou.join_time) AS onboard_time
FROM proddb.public.fact_dedup_experiment_exposure ee
INNER JOIN onboarding_users ou 
    ON ee.bucket_key = ou.consumer_id
    AND convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date = ou.onboard_day
WHERE experiment_name = 'should_pin_leaderboard_carousel'
AND experiment_version::INT = 2
AND segment = 'iOS'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-25' AND CURRENT_DATE
GROUP BY 1,2,3
)

, orders AS
(SELECT DISTINCT dd.creator_id AS consumer_id
        , convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as day
        , convert_timezone('UTC','America/Los_Angeles',a.timestamp) as order_timestamp
        , dd.delivery_ID
        , dd.is_first_ordercart_DD
        , dd.is_filtered_core
        , dd.variable_profit * 0.01 AS variable_profit
        , dd.gov * 0.01 AS gov
FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-08-25' AND CURRENT_DATE
WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-08-25' AND CURRENT_DATE
)

, checkout_window AS
(SELECT  e.tag
        , '{time_value}{time_unit}' AS time_window
        , COUNT(distinct e.consumer_id) as exposure_onboard
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) orders
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) new_Cx
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.consumer_id) order_rate
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.consumer_id) new_cx_rate
        , SUM(variable_profit) AS variable_profit
        , SUM(variable_profit) / COUNT(DISTINCT e.consumer_id) AS VP_per_device
        , SUM(gov) AS gov
        , SUM(gov) / COUNT(DISTINCT e.consumer_id) AS gov_per_device
FROM exposure e
LEFT JOIN orders o
    ON e.consumer_id = o.consumer_id 
    AND o.order_timestamp >= e.onboard_time
    AND o.order_timestamp <= DATEADD('{time_unit}', {time_value}, e.onboard_time)
WHERE TAG NOT IN ('internal_test','reserved')
GROUP BY 1
ORDER BY 1
)

,  MAU_window AS (
SELECT  e.tag
        , COUNT(DISTINCT o.consumer_id) as MAU
        , COUNT(DISTINCT o.consumer_id) / COUNT(DISTINCT e.consumer_id) as MAU_rate
FROM exposure e
LEFT JOIN orders o
    ON e.consumer_id = o.consumer_id 
    AND o.day BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date)
GROUP BY 1
ORDER BY 1
)

, res_window AS
(SELECT c.*
        , m.MAU 
        , m.mau_rate
FROM checkout_window c
JOIN MAU_window m 
  on c.tag = m.tag
ORDER BY 1
)

SELECT r1.tag 
        , r1.time_window
        , r1.exposure_onboard
        , r1.orders
        , r1.order_rate
        , r1.order_rate / NULLIF(r2.order_rate,0) - 1 AS Lift_order_rate
        , r1.new_cx
        , r1.new_cx_rate
        , r1.new_cx_rate / NULLIF(r2.new_cx_rate,0) - 1 AS Lift_new_cx_rate
        , r1.variable_profit
        , r1.variable_profit / nullif(r2.variable_profit,0) - 1 AS Lift_VP
        , r1.VP_per_device
        , r1.VP_per_device / nullif(r2.VP_per_device,0) -1 AS Lift_VP_per_device   
        , r1.gov
        , r1.gov / r2.gov - 1 AS Lift_gov
        , r1.gov_per_device
        , r1.gov_per_device / r2.gov_per_device -1 AS Lift_gov_per_device
        , r1.mau 
        , r1.mau_rate
        , r1.mau_rate / nullif(r2.mau_rate,0) - 1 AS Lift_mau_rate
FROM res_window r1
LEFT JOIN res_window r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
ORDER BY 1 desc;
