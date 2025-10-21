
-- Get all orders since 2025-01-01 before exposure (delivery level)
-- Joins directly to experiment exposures based on filtered device_id
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd AS (

SELECT 
    exp.tag,
    exp.result,
    exp.consumer_id,
    exp.bucket_key,
    REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') as dd_device_id_filtered,
    exp.exposure_time,
    date(exp.exposure_time) as exposure_day,
    
    -- Order information
    dd.delivery_id,
    dd.order_cart_id,
    dd.store_id,
    dd.store_name,
    CONVERT_TIMEZONE('UTC',TIMEZONE, dd.actual_order_place_time)::date AS order_date,
    CONVERT_TIMEZONE('UTC',TIMEZONE, dd.actual_order_place_time) AS order_timestamp,
    
    -- Calculate days before exposure
    DATEDIFF('day', CONVERT_TIMEZONE('UTC',TIMEZONE, dd.actual_order_place_time)::date, date(exp.exposure_time)) AS days_before_exposure,
    
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
    dd.delivery_rating
    
FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp
INNER JOIN dimension_deliveries dd

    ON REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-')
       = REPLACE(LOWER(CASE WHEN dd.dd_device_id LIKE 'dx_%' THEN dd.dd_device_id ELSE 'dx_'||dd.dd_device_id END), '-')
WHERE 
    -- Get orders since 2025-01-01 up to the day before exposure
    CONVERT_TIMEZONE('UTC',TIMEZONE, dd.actual_order_place_time)::date >= '2025-01-01'
    AND CONVERT_TIMEZONE('UTC',TIMEZONE, dd.actual_order_place_time)::date < date(exp.exposure_time)
    -- Only filtered core orders
    AND dd.is_filtered_core = TRUE
);
-- Summary statistics
SELECT 
    'Pre-Exposure Orders (2025-01-01+)' AS analysis_period,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT dd_device_id_filtered) AS unique_devices,
    COUNT(DISTINCT delivery_id) AS total_orders,
    COUNT(DISTINCT store_id) AS unique_stores,
    COUNT(*) / COUNT(DISTINCT consumer_id) AS avg_orders_per_user,
    AVG(order_value) AS avg_order_value,
    MEDIAN(order_value) AS median_order_value
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd;

-- Distribution of orders by time before exposure
SELECT 
    CASE 
        WHEN days_before_exposure BETWEEN 0 AND 7 THEN '0-7 days before'
        WHEN days_before_exposure BETWEEN 8 AND 14 THEN '8-14 days before'
        WHEN days_before_exposure BETWEEN 15 AND 30 THEN '15-30 days before'
        WHEN days_before_exposure BETWEEN 31 AND 60 THEN '31-60 days before'
        WHEN days_before_exposure BETWEEN 61 AND 90 THEN '61-90 days before'
        WHEN days_before_exposure BETWEEN 91 AND 120 THEN '91-120 days before'
        WHEN days_before_exposure BETWEEN 121 AND 180 THEN '121-180 days before'
        WHEN days_before_exposure > 180 THEN '180+ days before'
        ELSE 'Other'
    END AS time_bucket,
    COUNT(DISTINCT delivery_id) AS order_count,
    COUNT(DISTINCT delivery_id) * 100.0 / SUM(COUNT(DISTINCT delivery_id)) OVER() AS order_count_pct,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER() AS unique_users_pct,
    AVG(order_value) AS avg_order_value
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd
GROUP BY ALL
ORDER BY 
    CASE 
        WHEN time_bucket = '0-7 days before' THEN 1
        WHEN time_bucket = '8-14 days before' THEN 2
        WHEN time_bucket = '15-30 days before' THEN 3
        WHEN time_bucket = '31-60 days before' THEN 4
        WHEN time_bucket = '61-90 days before' THEN 5
        WHEN time_bucket = '91-120 days before' THEN 6
        WHEN time_bucket = '121-180 days before' THEN 7
        WHEN time_bucket = '180+ days before' THEN 8
        ELSE 9
    END;



