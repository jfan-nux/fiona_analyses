/*
Purpose: Add actual order data from dimension_deliveries
Creates: proddb.fionafan.guest_vs_onboarding_august_orders
*/

CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_orders AS
SELECT 
    c.dd_device_id_filtered,
    c.consumer_id,
    c.cohort_date,
    c.cohort_type,
    c.platform,
    
    -- Count orders at different periods
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 1 THEN d.delivery_id END) as d1_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 7 THEN d.delivery_id END) as d7_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 14 THEN d.delivery_id END) as d14_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 28 THEN d.delivery_id END) as d28_orders,
    
    -- First time orders at different periods
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 1 AND d.is_first_ordercart_dd = 1 THEN d.delivery_id END) as d1_first_time_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 7 AND d.is_first_ordercart_dd = 1 THEN d.delivery_id END) as d7_first_time_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 14 AND d.is_first_ordercart_dd = 1 THEN d.delivery_id END) as d14_first_time_orders,
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 28 AND d.is_first_ordercart_dd = 1 THEN d.delivery_id END) as d28_first_time_orders,
    
    -- MAU (consumer_id if user placed any order, NULL otherwise)
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 1 THEN c.consumer_id END) as d1_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 7 THEN c.consumer_id END) as d7_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 14 THEN c.consumer_id END) as d14_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 28 THEN c.consumer_id END) as d28_mau,
    
    -- New User MAU (consumer_id if user placed first order, NULL otherwise)
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 1 AND d.is_first_ordercart_dd = 1 THEN c.consumer_id END) as d1_new_user_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 7 AND d.is_first_ordercart_dd = 1 THEN c.consumer_id END) as d7_new_user_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 14 AND d.is_first_ordercart_dd = 1 THEN c.consumer_id END) as d14_new_user_mau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 28 AND d.is_first_ordercart_dd = 1 THEN c.consumer_id END) as d28_new_user_mau,
    
    -- GMV at different periods
    SUM(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 1 THEN d.subtotal ELSE 0 END) as d1_gmv,
    SUM(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 7 THEN d.subtotal ELSE 0 END) as d7_gmv,
    SUM(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 14 THEN d.subtotal ELSE 0 END) as d14_gmv,
    SUM(CASE WHEN DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date) <= 28 THEN d.subtotal ELSE 0 END) as d28_gmv,
    
    -- First order timing
    MIN(DATEDIFF(day, c.cohort_date, d.actual_delivery_time::date)) as days_to_first_order

FROM proddb.fionafan.guest_vs_onboarding_august_cohorts c
LEFT JOIN proddb.public.dimension_deliveries d
    ON c.dd_device_id_filtered = replace(lower(CASE WHEN d.dd_device_id like 'dx_%' then d.dd_device_id
                                                    else 'dx_'||d.dd_device_id end), '-')
    AND d.actual_delivery_time::date >= c.cohort_date
    AND d.actual_delivery_time::date <= DATEADD(day, 28, c.cohort_date)
    AND d.is_filtered_core = TRUE
    and d.CANCELLED_AT is null
    AND d.actual_delivery_time IS NOT NULL
GROUP BY 
    c.dd_device_id_filtered,
    c.consumer_id,
    c.cohort_date,
    c.cohort_type,
    c.platform;

-- Verify
SELECT 
    cohort_type,
    platform,
    COUNT(*) as user_count,
    SUM(CASE WHEN d1_orders > 0 THEN 1 ELSE 0 END) as users_with_d1_orders,
    SUM(CASE WHEN d28_orders > 0 THEN 1 ELSE 0 END) as users_with_d28_orders,
    SUM(d28_first_time_orders) as total_d28_first_time_orders,
    COUNT(DISTINCT d28_mau) as d28_mau_count,
    COUNT(DISTINCT d28_new_user_mau) as d28_new_user_mau_count
FROM proddb.fionafan.guest_vs_onboarding_august_orders
GROUP BY ALL
ORDER BY cohort_type, platform;

