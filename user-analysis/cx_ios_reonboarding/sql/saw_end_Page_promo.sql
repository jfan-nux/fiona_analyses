SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-09-08';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

--------------------- experiment exposure
WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = $exp_name
AND experiment_version::INT = $version
AND segment = $segment
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY 1,2,3,4
)

, saw_end_page_promo AS (
SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
      , promo_title
      , CASE WHEN POSITION('%', promo_title) > 0 THEN 'YES' ELSE 'NO' END AS saw_promo
FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
AND onboarding_type = 'resurrected_user'
)

, orders AS
(SELECT DISTINCT a.DD_DEVICE_ID
        , replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                    else 'dx_'||a.DD_device_id end), '-') AS dd_device_ID_filtered
        , convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as day
        , dd.delivery_ID
        , dd.is_first_ordercart_DD
        , dd.is_filtered_core
        , dd.variable_profit * 0.01 AS variable_profit
        , dd.gov * 0.01 AS gov
FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN $start_date AND $end_date
WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN $start_date AND $end_date

)

, checkout AS
(SELECT  e.tag
        , CASE WHEN sep.dd_device_ID_filtered IS NOT NULL THEN 'YES' ELSE 'NO' END AS saw_endpage
        , COALESCE(sep.saw_promo, 'NO') AS saw_promo
        , COUNT(distinct e.dd_device_ID_filtered) as exposure_count
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) orders
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) new_Cx
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_ID_filtered) order_rate
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_ID_filtered) new_cx_rate
        , SUM(variable_profit) AS variable_profit
        , SUM(variable_profit) / COUNT(DISTINCT e.dd_device_ID_filtered) AS VP_per_device
        , SUM(gov) AS gov
        , SUM(gov) / COUNT(DISTINCT e.dd_device_ID_filtered) AS gov_per_device
FROM exposure e
LEFT JOIN saw_end_page_promo sep
    ON e.dd_device_ID_filtered = sep.dd_device_ID_filtered 
    AND e.day <= sep.day
LEFT JOIN orders o
    ON e.dd_device_ID_filtered = o.dd_device_ID_filtered 
    AND e.day <= o.day
WHERE TAG NOT IN ('internal_test','reserved')
GROUP BY 1,2,3
ORDER BY 1,2,3)

,  MAU AS (
SELECT  e.tag
        , CASE WHEN sep.dd_device_ID_filtered IS NOT NULL THEN 'YES' ELSE 'NO' END AS saw_endpage
        , COALESCE(sep.saw_promo, 'NO') AS saw_promo
        , COUNT(DISTINCT o.dd_device_ID_filtered) as MAU
        , COUNT(DISTINCT o.dd_device_ID_filtered) / COUNT(DISTINCT e.dd_device_ID_filtered) as MAU_rate
FROM exposure e
LEFT JOIN saw_end_page_promo sep
    ON e.dd_device_ID_filtered = sep.dd_device_ID_filtered 
    AND e.day <= sep.day
LEFT JOIN orders o
    ON e.dd_device_ID_filtered = o.dd_device_ID_filtered 
    --AND e.day <= o.day
    AND o.day BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) -- past 28 days orders
-- WHERE e.day <= DATEADD('day',-28,$end_date) --- exposed at least 28 days ago
GROUP BY 1,2,3
ORDER BY 1,2,3
)

, res AS
(SELECT c.*
        , m.MAU 
        , m.mau_rate
FROM checkout c
JOIN MAU m 
  ON c.tag = m.tag 
  AND c.saw_endpage = m.saw_endpage
  AND c.saw_promo = m.saw_promo
ORDER BY 1,2,3
)

SELECT r1.tag 
        , r1.saw_endpage
        , r1.saw_promo
        , r1.exposure_count
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
FROM res r1
LEFT JOIN res r2
    ON r1.tag = r2.tag
    AND r1.saw_endpage = 'YES'
    AND r2.saw_endpage = 'YES'
    AND r1.saw_promo = 'YES'
    AND r2.saw_promo = 'NO'
ORDER BY 1,2,3 desc