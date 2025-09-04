-- Two experiment analysis: consumer_id level
SET start_date = '2025-08-25';
SET end_date = CURRENT_DATE;

-- Experiment 1: should_pin_leaderboard_carousel (consumer_id level)
SET exp1_name = 'should_pin_leaderboard_carousel';
SET exp1_version = 2;

-- Experiment 2: cx_mobile_onboarding_preferences (device_id level, iOS)
SET exp2_name = 'cx_mobile_onboarding_preferences';
SET exp2_version = 1;

-- Get exposure for experiment 1 (consumer_id level)
-- Create table with results
CREATE OR REPLACE TABLE proddb.fionafan.two_experiment_analysis AS (


WITH exp1_exposure AS (
    SELECT 
        ee.bucket_key AS consumer_id,
        ee.tag AS exp1_tag,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS exp1_exposure_date,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS exp1_exposure_time
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE lower(experiment_name) = lower($exp1_name)
        AND experiment_version::INT = $exp1_version
        AND segment = 'iOS'
        AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
        AND ee.tag NOT IN ('internal_test','reserved')
    GROUP BY 1, 2
)

-- Get exposure for experiment 2 (device_id level, convert to consumer_id)
,exp2_exposure AS (
    SELECT 
        PARSE_JSON(ee.custom_attributes):userId::STRING AS consumer_id,
        ee.tag AS exp2_tag,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS exp2_exposure_date,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS exp2_exposure_time
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE lower(experiment_name) = lower($exp2_name)
        AND experiment_version::INT = $exp2_version
        AND segment = 'iOS'
        AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
        AND ee.tag NOT IN ('internal_test','reserved')
        -- AND PARSE_JSON(ee.custom_attributes):userId IS NOT NULL
    GROUP BY 1, 2
),

-- Combine both experiments at consumer_id level
combined_exposure AS (
    SELECT 
        COALESCE(e1.consumer_id, e2.consumer_id) AS consumer_id,
        e1.exp1_tag,
        e1.exp1_exposure_date,
        e2.exp2_tag,
        e2.exp2_exposure_date,
        LEAST(
            COALESCE(e1.exp1_exposure_date, '2099-01-01'::date),
            COALESCE(e2.exp2_exposure_date, '2099-01-01'::date)
        ) AS earliest_exposure_date
    FROM exp1_exposure e1
    FULL OUTER JOIN exp2_exposure e2 ON e1.consumer_id = e2.consumer_id
    WHERE COALESCE(e1.consumer_id, e2.consumer_id) IS NOT NULL
)

-- Funnel Analysis: Explore Page Views
,explore_page AS (
    SELECT DISTINCT 
        iguazu_user_id as consumer_id,
        convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS activity_date
    FROM IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD
    WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN $start_date AND $end_date
        AND iguazu_user_id IS NOT NULL
)

-- Funnel Analysis: Store Page Views  
, store_page AS (
    SELECT DISTINCT
        user_id as consumer_id,
        convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS activity_date
    FROM segment_events_RAW.consumer_production.m_store_page_load
    WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
        AND user_id IS NOT NULL
)

-- Funnel Analysis: Cart Page Views
, cart_page AS (
    SELECT DISTINCT
        iguazu_user_id as consumer_id,
        convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS activity_date
    FROM iguazu.consumer.m_order_cart_page_load
    WHERE convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN $start_date AND $end_date
        AND iguazu_user_id IS NOT NULL
)

-- Funnel Analysis: Checkout Page Views
, checkout_page AS (
    SELECT DISTINCT
        user_id as consumer_id,
        convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS activity_date
    FROM segment_events_RAW.consumer_production.m_checkout_page_load
    WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
        AND user_id IS NOT NULL
)

-- Funnel Metrics Aggregated by Consumer
, funnel_analysis AS (
    SELECT 
        e.consumer_id,
        MAX(CASE WHEN ep.consumer_id IS NOT NULL AND ep.activity_date >= e.earliest_exposure_date THEN 1 ELSE 0 END) AS explored_after_exposure,
        MAX(CASE WHEN sp.consumer_id IS NOT NULL AND sp.activity_date >= e.earliest_exposure_date THEN 1 ELSE 0 END) AS viewed_store_after_exposure,
        MAX(CASE WHEN cp.consumer_id IS NOT NULL AND cp.activity_date >= e.earliest_exposure_date THEN 1 ELSE 0 END) AS viewed_cart_after_exposure,
        MAX(CASE WHEN c.consumer_id IS NOT NULL AND c.activity_date >= e.earliest_exposure_date THEN 1 ELSE 0 END) AS viewed_checkout_after_exposure
    FROM combined_exposure e
    LEFT JOIN explore_page ep ON e.consumer_id = ep.consumer_id
    LEFT JOIN store_page sp ON e.consumer_id = sp.consumer_id  
    LEFT JOIN cart_page cp ON e.consumer_id = cp.consumer_id
    LEFT JOIN checkout_page c ON e.consumer_id = c.consumer_id
    GROUP BY e.consumer_id
)

-- Get orders aggregated at consumer_id and date level for proper exposure filtering
, orders AS (
    SELECT 
        dd.creator_id as consumer_id,
        convert_timezone('UTC','America/Los_Angeles',dd.created_at)::date as order_date,
        COUNT(DISTINCT dd.delivery_ID) AS daily_orders,
        COUNT(DISTINCT CASE WHEN dd.is_first_ordercart_DD = 1 THEN dd.delivery_ID END) AS daily_new_customer_orders,
        SUM(dd.variable_profit * 0.01) AS daily_variable_profit,
        SUM(dd.gov * 0.01) AS daily_gov
    FROM dimension_deliveries dd
    WHERE dd.is_filtered_core = 1
        AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN $start_date AND $end_date
        AND dd.creator_id IS NOT NULL
    GROUP BY dd.creator_id, convert_timezone('UTC','America/Los_Angeles',dd.created_at)::date
)

-- Calculate metrics by experiment combinations with proper exposure date filtering
, final_analysis AS (
    SELECT 
        e.consumer_id,
        COALESCE(e.exp1_tag, 'not_in_exp1') AS exp1_status,
        COALESCE(e.exp2_tag, 'not_in_exp2') AS exp2_status,
        e.earliest_exposure_date,
        
        -- Order metrics (only count orders after exposure)
        SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_orders ELSE 0 END) AS orders_count,
        SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_new_customer_orders ELSE 0 END) AS new_customer_orders,
        
        -- Revenue metrics (only count revenue from orders after exposure)  
        SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_variable_profit ELSE 0 END) AS total_variable_profit,
        SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_gov ELSE 0 END) AS total_gov,
        
        -- MAU metric (orders in last 28 days regardless of exposure timing)
        SUM(CASE WHEN o.order_date BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) THEN o.daily_orders ELSE 0 END) AS mau_orders,
        
        -- Funnel metrics (post-exposure activity)
        MAX(COALESCE(f.explored_after_exposure, 0)) AS explored_after_exposure,
        MAX(COALESCE(f.viewed_store_after_exposure, 0)) AS viewed_store_after_exposure,
        MAX(COALESCE(f.viewed_cart_after_exposure, 0)) AS viewed_cart_after_exposure,
        MAX(COALESCE(f.viewed_checkout_after_exposure, 0)) AS viewed_checkout_after_exposure,
        
        -- Binary indicators
        CASE WHEN SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_orders ELSE 0 END) > 0 THEN 1 ELSE 0 END AS ordered,
        CASE WHEN SUM(CASE WHEN o.order_date >= e.earliest_exposure_date THEN o.daily_new_customer_orders ELSE 0 END) > 0 THEN 1 ELSE 0 END AS new_customer,
        CASE WHEN SUM(CASE WHEN o.order_date BETWEEN DATEADD('day',-28,current_date) AND DATEADD('day',-1,current_date) THEN o.daily_orders ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_mau
        
    FROM combined_exposure e
    LEFT JOIN orders o ON e.consumer_id = o.consumer_id
    LEFT JOIN funnel_analysis f ON e.consumer_id = f.consumer_id
    GROUP BY all
)


SELECT 
    consumer_id,
    exp1_status,
    exp2_status,
    earliest_exposure_date,
    orders_count,
    new_customer_orders,
    total_variable_profit,
    total_gov,
    mau_orders,
    ordered,
    new_customer,
    is_mau,
    -- Funnel metrics
    explored_after_exposure,
    viewed_store_after_exposure,
    viewed_cart_after_exposure,
    viewed_checkout_after_exposure,
    current_timestamp() as created_at
FROM final_analysis
);
select count(1) cnt, 
       sum(ordered) ordered, 
       sum(new_customer) new_customer, 
       sum(is_mau) is_mau,
       -- Funnel metrics
       sum(explored_after_exposure) explored_after_exposure,
       sum(viewed_store_after_exposure) viewed_store_after_exposure,
       sum(viewed_cart_after_exposure) viewed_cart_after_exposure,
       sum(viewed_checkout_after_exposure) viewed_checkout_after_exposure,
       -- Funnel conversion rates
       sum(explored_after_exposure)::float / count(1) as explore_rate,
       sum(viewed_store_after_exposure)::float / nullif(sum(explored_after_exposure), 0) as store_conversion_rate,
       sum(viewed_cart_after_exposure)::float / nullif(sum(viewed_store_after_exposure), 0) as cart_conversion_rate,
       sum(viewed_checkout_after_exposure)::float / nullif(sum(viewed_cart_after_exposure), 0) as checkout_conversion_rate
from proddb.fionafan.two_experiment_analysis
where 1=1 
and exp1_status = 'treatment'
-- and exp2_status = 'cx_mobile_onboarding_preferences'
-- and is_mau = 1
;

select count(1) cnt from proddb.fionafan.two_experiment_analysis where exp2_status in (
    'treatment', 'control'
)
group by all having cnt>1;

select * from proddb.fionafan.two_experiment_analysis where consumer_id = '1965315491';

