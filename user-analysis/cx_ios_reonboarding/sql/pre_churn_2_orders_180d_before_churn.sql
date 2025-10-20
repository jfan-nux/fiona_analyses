-- Step 2: Get the most recent SCD record and pull orders from 180 days before last_order_date
-- This gives us the pre-churn order history at delivery level

-- Get the current/most recent SCD record for each user
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_current_scd AS (
SELECT 
    tag,
    result,
    dd_device_id_filtered,
    exposure_day,
    exposure_time,
    consumer_id,
    scd_consumer_id,
    scd_start_date,
    scd_end_date,
    signup_date,
    first_order_date,
    last_order_date,
    lifestage,
    lifestage_bucket,
    experience,
    business_vertical_line,
    country_id,
    region_name,
    submarket_id
FROM proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records
where exposure_day between scd_start_date and scd_end_date 
);

select count(1), count(distinct consumer_id), count(distinct dd_device_id_filtered) from proddb.fionafan.cx_ios_reonboarding_current_scd;

-- Get all orders from 180 days before last_order_date (delivery level)
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_180d AS (
SELECT 
    scd.tag,
    scd.result,
    scd.consumer_id,
    scd.scd_consumer_id,
    scd.dd_device_id_filtered,
    scd.exposure_day,
    scd.last_order_date,
    scd.lifestage,
    scd.lifestage_bucket,
    
    -- Order information
    dd.delivery_id,
    dd.order_cart_id,
    dd.store_id,
    dd.store_name,
    CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date AS order_date,
    CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time) AS order_timestamp,
    
    -- Calculate days before last order
    DATEDIFF('day', CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date, scd.last_order_date) AS days_before_last_order,
    
    -- Order metrics
    dd.gov / 100.0 AS order_value,
    dd.variable_profit / 100.0 AS variable_profit,
    dd.subtotal / 100.0 AS subtotal,
    dd.delivery_fee / 100.0 AS delivery_fee,
    dd.tip / 100.0 AS tip,
    dd.store_refund / 100.0 AS store_refund,
    
    -- Order characteristics
    dd.is_filtered_core,
    dd.is_first_ordercart,
    dd.business_name AS vertical,
    dd.submarket_id AS order_submarket_id,
    dd.submarket_name AS order_submarket_name,
    
    -- Duration and ratings
    dd.r2c_duration,
    dd.d2c_duration,
    dd.delivery_rating,
    
    
FROM proddb.fionafan.cx_ios_reonboarding_current_scd scd
INNER JOIN dimension_deliveries dd
    ON scd.dd_device_id_filtered = REPLACE(LOWER(CASE WHEN dd.dd_device_id LIKE 'dx_%' 
                                                       THEN dd.dd_device_id
                                                       ELSE 'dx_'||dd.dd_device_id END), '-')
WHERE 
    -- Get orders from 180 days before last_order_date
    CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date 
        BETWEEN DATEADD('day', -180, scd.last_order_date) 
        AND scd.last_order_date
    -- Only filtered core orders
    AND dd.is_filtered_core = TRUE
);
-- Summary statistics
SELECT 
    'Pre-Churn Orders (180d)' AS analysis_period,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT scd_consumer_id) AS unique_consumers,
    COUNT(DISTINCT delivery_id) AS total_orders,
    COUNT(DISTINCT store_id) AS unique_stores,
    COUNT(*) / COUNT(DISTINCT consumer_id) AS avg_orders_per_user,
    AVG(order_value) AS avg_order_value,
    MEDIAN(order_value) AS median_order_value
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_180d;

-- Distribution of orders by time before churn
SELECT 
    CASE 
        WHEN days_before_last_order BETWEEN 0 AND 30 THEN '0-30 days before'
        WHEN days_before_last_order BETWEEN 31 AND 60 THEN '31-60 days before'
        WHEN days_before_last_order BETWEEN 61 AND 90 THEN '61-90 days before'
        WHEN days_before_last_order BETWEEN 91 AND 120 THEN '91-120 days before'
        WHEN days_before_last_order BETWEEN 121 AND 150 THEN '121-150 days before'
        WHEN days_before_last_order BETWEEN 151 AND 180 THEN '151-180 days before'
        ELSE 'Other'
    END AS time_bucket,
    COUNT(DISTINCT delivery_id) AS order_count,
    COUNT(DISTINCT delivery_id) * 100.0 / SUM(COUNT(DISTINCT delivery_id)) OVER() AS order_count_pct,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER() AS unique_users_pct,
    AVG(order_value) AS avg_order_value
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_180d
GROUP BY ALL
ORDER BY 
    CASE 
        WHEN time_bucket = '0-30 days before' THEN 1
        WHEN time_bucket = '31-60 days before' THEN 2
        WHEN time_bucket = '61-90 days before' THEN 3
        WHEN time_bucket = '91-120 days before' THEN 4
        WHEN time_bucket = '121-150 days before' THEN 5
        WHEN time_bucket = '151-180 days before' THEN 6
        ELSE 7
    END;



