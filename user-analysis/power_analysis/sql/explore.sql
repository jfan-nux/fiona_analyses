--- start of funnel device_id_level
SELECT  DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN '2025-08-18' AND '2025-09-30';


-- device_id_level topline queries
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
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-08-18' AND '2025-09-30'
WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-08-18' AND '2025-09-30'

)

, checkout AS
(SELECT  e.tag
        , e.segments
        , COUNT(distinct e.dd_device_ID_filtered) as exposure_onboard
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) orders
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) new_Cx
        , COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_ID_filtered) order_rate
        , COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_ID_filtered) new_cx_rate
        , SUM(variable_profit) AS variable_profit
        , SUM(variable_profit) / COUNT(DISTINCT e.dd_device_ID_filtered) AS VP_per_device
        , SUM(gov) AS gov
        , SUM(gov) / COUNT(DISTINCT e.dd_device_ID_filtered) AS gov_per_device
        
        -- Statistical variables for p-value calculation
        -- For continuous variables: need std dev and sample size
        , STDDEV_SAMP(variable_profit) AS std_variable_profit
        , STDDEV_SAMP(gov) AS std_gov
        , COUNT(CASE WHEN is_filtered_core = 1 THEN o.delivery_ID END) AS n_orders_for_stats  -- sample size for continuous vars
        
        -- Rate variables already have numerator/denominator:
        -- order_rate: orders/exposure_onboard
        -- new_cx_rate: new_cx/exposure_onboard
        
FROM exposure e
LEFT JOIN orders o
    ON e.dd_device_ID_filtered = o.dd_device_ID_filtered 
    AND e.day <= o.day
WHERE TAG NOT IN ('internal_test','reserved')
GROUP BY 1, 2
ORDER BY 1, 2)

,  MAU AS (
SELECT  e.tag
        , e.segments
        , COUNT(DISTINCT o.dd_device_ID_filtered) as MAU
        , COUNT(DISTINCT o.dd_device_ID_filtered) / COUNT(DISTINCT e.dd_device_ID_filtered) as MAU_rate
FROM exposure e
LEFT JOIN orders o
    ON e.dd_device_ID_filtered = o.dd_device_ID_filtered 
    --AND e.day <= o.day
    AND o.day BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) -- past 28 days orders
-- WHERE e.day <= DATEADD('day',-28,'2025-09-30') --- exposed at least 28 days ago
GROUP BY 1, 2
ORDER BY 1, 2
)

, res AS
(SELECT c.*
        , m.MAU 
        , m.mau_rate
FROM checkout c
JOIN MAU m 
  on c.tag = m.tag AND c.segments = m.segments
ORDER BY 1, 2
)

SELECT r1.tag 
        , r1.segments
        , r1.exposure_onboard AS exposure
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
        
        -- Statistical variables for p-value calculations
        -- Treatment group statistics (r1)
        , r1.std_variable_profit AS std_variable_profit
        , r1.std_gov AS std_gov
        -- Control group statistics (r2) for rate variables
        , r2.order_rate AS control_order_rate
        , r2.new_cx_rate AS control_new_cx_rate
        , r2.variable_profit AS control_VP  -- For Lift_VP calculation
        , r2.VP_per_device AS control_VP_per_device
        , r2.gov_per_device AS control_gov_per_device
        , r2.mau_rate AS control_mau_rate
        , r2.variable_profit AS control_variable_profit
        , r2.gov AS control_gov
        , r2.std_variable_profit AS control_std_variable_profit
        , r2.std_gov AS control_std_gov
        , r2.n_orders_for_stats AS control_n_orders
        , r2.exposure_onboard AS control_exposure
        , r2.orders AS control_orders
        , r2.new_cx AS control_new_cx
        , r2.mau AS control_mau
        
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r2.tag = 'control'
    AND r1.segments = r2.segments
ORDER BY 1, 2 desc