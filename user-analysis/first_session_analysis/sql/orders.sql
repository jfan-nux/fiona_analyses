-- 28-Day Order Analysis for July Cohorts
-- Delivery-level table
CREATE OR REPLACE TABLE proddb.fionafan.july_cohort_deliveries_28d AS
SELECT 
    c.consumer_id,
    c.lifestage,
    c.exposure_time,
    c.onboarding_day,
    c.cohort_type,
    d.delivery_id,
    d.created_at AS delivery_created_at,
    DATEDIFF(day, c.onboarding_day, DATE(d.created_at)) AS days_since_onboarding
FROM proddb.fionafan.all_user_july_cohort c
inner JOIN proddb.public.dimension_deliveries d
    ON c.consumer_id = d.creator_id
    AND d.created_at >= c.onboarding_day
    AND d.created_at < DATEADD(day, 28, c.onboarding_day)
    AND d.is_filtered_core = 1
WHERE DATEDIFF(day, c.onboarding_day, DATE(d.created_at)) BETWEEN 0 AND 28;

-- Consumer-level aggregated table by cohort and days since onboarding
CREATE OR REPLACE TABLE proddb.fionafan.july_cohort_orders_by_day_28d AS
SELECT 
    cohort_type,
    days_since_onboarding,
    COUNT(DISTINCT consumer_id) AS num_consumers_ordering,
    COUNT(DISTINCT delivery_id) AS num_orders,
    COUNT(DISTINCT delivery_id) * 1.0 / NULLIF(COUNT(DISTINCT consumer_id), 0) AS orders_per_consumer
FROM proddb.fionafan.july_cohort_deliveries_28d
GROUP BY cohort_type, days_since_onboarding
ORDER BY cohort_type, days_since_onboarding;

-- Summary by cohort (all 28 days)
SELECT 
    c.cohort_type,
    COUNT(DISTINCT c.consumer_id) AS total_consumers_in_cohort,
    COUNT(DISTINCT d.consumer_id) AS consumers_with_orders,
    COUNT(DISTINCT d.delivery_id) AS total_orders_28d,
    COUNT(DISTINCT d.consumer_id) * 1.0 / COUNT(DISTINCT c.consumer_id) AS pct_consumers_ordering,
    COUNT(DISTINCT d.delivery_id) * 1.0 / COUNT(DISTINCT c.consumer_id) AS orders_per_consumer_overall
FROM proddb.fionafan.all_user_july_cohort c
LEFT JOIN proddb.fionafan.july_cohort_deliveries_28d d
    ON c.consumer_id = d.consumer_id
GROUP BY c.cohort_type
ORDER BY c.cohort_type;

-- View the daily aggregated data
SELECT * FROM proddb.fionafan.july_cohort_orders_by_day_28d
ORDER BY cohort_type, days_since_onboarding;

-- Day-over-day changes
SELECT 
    cohort_type,
    days_since_onboarding,
    num_consumers_ordering,
    LAG(num_consumers_ordering) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS prev_day_consumers,
    num_consumers_ordering - LAG(num_consumers_ordering) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS consumer_change,
    num_orders,
    LAG(num_orders) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS prev_day_orders,
    num_orders - LAG(num_orders) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS order_change,
    orders_per_consumer,
    LAG(orders_per_consumer) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS prev_day_orders_per_consumer,
    orders_per_consumer - LAG(orders_per_consumer) OVER (PARTITION BY cohort_type ORDER BY days_since_onboarding) AS orders_per_consumer_change
FROM proddb.fionafan.july_cohort_orders_by_day_28d
ORDER BY cohort_type, days_since_onboarding;
