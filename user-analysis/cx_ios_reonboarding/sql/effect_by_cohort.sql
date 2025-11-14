
SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-09-08';
SET end_date = '2025-11-03';
SET version = 1;
SET segment = 'Users';
select count(1) from proddb.public.cx_resurrection_cohorts;
--------------------- experiment exposure with user ordering type
WITH exposure AS (
    SELECT 
        e.tag,
        e.result,
        e.bucket_key,
        e.dd_device_id_filtered,
        e.consumer_id,
        e.day,
        e.exposure_time
        ,COALESCE(umb.resurrection_cohort, 'unmatched') AS resurrection_cohorts
    FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures e
    -- inner join dimension_consumer dc on e.consumer_id::varchar = dc.user_id::varchar
    inner JOIN proddb.public.cx_resurrection_cohorts umb
        ON e.consumer_id::varchar = umb.consumer_id::varchar
    WHERE e.day BETWEEN $start_date AND $end_date
)

, orders AS (
    SELECT DISTINCT 
        a.DD_DEVICE_ID,
        replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                    else 'dx_'||a.DD_device_id end), '-') AS dd_device_id_filtered,
        convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as day,
        dd.delivery_ID,
        dd.is_first_ordercart_DD,
        dd.is_filtered_core,
        dd.variable_profit * 0.01 AS variable_profit,
        dd.gov * 0.01 AS gov
    FROM segment_events_raw.consumer_production.order_cart_submit_received a
        JOIN dimension_deliveries dd
        ON a.order_cart_id = dd.order_cart_id
        AND dd.is_filtered_core = 1
        AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN $start_date AND $end_date
    WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN $start_date AND $end_date
)

, checkout AS (
    SELECT  
        e.tag,
        e.resurrection_cohorts,
        COUNT(distinct e.dd_device_id_filtered) as exposure_onboard,
        COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) orders,
        COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) new_Cx,
        COUNT(DISTINCT CASE WHEN is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_id_filtered) order_rate,
        COUNT(DISTINCT CASE WHEN is_first_ordercart_DD = 1 AND is_filtered_core = 1 THEN o.delivery_ID ELSE NULL END) /  COUNT(DISTINCT e.dd_device_id_filtered) new_cx_rate,
        SUM(variable_profit) AS variable_profit,
        SUM(variable_profit) / COUNT(DISTINCT e.dd_device_id_filtered) AS VP_per_device,
        SUM(gov) AS gov,
        SUM(gov) / COUNT(DISTINCT e.dd_device_id_filtered) AS gov_per_device
    FROM exposure e
    LEFT JOIN orders o
        ON e.dd_device_id_filtered = o.dd_device_id_filtered 
        AND e.day <= o.day
    WHERE TAG NOT IN ('internal_test','reserved')
    GROUP BY ALL
    ORDER BY 1,2
)

, MAU AS (
    SELECT  
        e.tag,
        e.resurrection_cohorts,
        COUNT(DISTINCT o.dd_device_id_filtered) as MAU,
        COUNT(DISTINCT o.dd_device_id_filtered) / COUNT(DISTINCT e.dd_device_id_filtered) as MAU_rate
    FROM exposure e
    LEFT JOIN orders o
        ON e.dd_device_id_filtered = o.dd_device_id_filtered 
        AND o.day BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) -- past 28 days orders
    GROUP BY ALL
    ORDER BY 1,2
)

, res AS (
    SELECT 
        c.*,
        m.MAU,
        m.mau_rate
    FROM checkout c
    JOIN MAU m 
        ON c.tag = m.tag 
        AND c.resurrection_cohorts = m.resurrection_cohorts
    ORDER BY 1,2
)

SELECT 
    r1.tag,
    r1.resurrection_cohorts,
    r1.exposure_onboard,
    r1.orders,
    r1.order_rate,
    r1.order_rate / NULLIF(r2.order_rate,0) - 1 AS Lift_order_rate,
    r1.new_cx,
    r1.new_cx_rate,
    r1.new_cx_rate / NULLIF(r2.new_cx_rate,0) - 1 AS Lift_new_cx_rate,
    r1.variable_profit,
    r1.variable_profit / nullif(r2.variable_profit,0) - 1 AS Lift_VP,
    r1.VP_per_device,
    r1.VP_per_device / nullif(r2.VP_per_device,0) -1 AS Lift_VP_per_device,
    r1.gov,
    r1.gov / r2.gov - 1 AS Lift_gov,
    r1.gov_per_device,
    r1.gov_per_device / r2.gov_per_device -1 AS Lift_gov_per_device,
    r1.mau,
    r1.mau_rate,
    r1.mau_rate / nullif(r2.mau_rate,0) - 1 AS Lift_mau_rate
FROM res r1
LEFT JOIN res r2
    ON r1.tag != r2.tag
    AND r1.resurrection_cohorts = r2.resurrection_cohorts
    AND r2.tag = 'control'
ORDER BY 2,1;

