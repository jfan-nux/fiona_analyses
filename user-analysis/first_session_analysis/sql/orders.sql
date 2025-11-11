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


-- ============================================================================
-- First and Second Order Analysis by Day Part and Weekday
-- ============================================================================

-- Step 1: Identify first and second orders for each consumer
CREATE OR REPLACE TABLE proddb.fionafan.july_cohort_first_second_orders AS
WITH ranked_orders AS (
    SELECT 
        consumer_id,
        cohort_type,
        delivery_id,
        delivery_created_at,
        days_since_onboarding,
        ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY delivery_created_at) AS order_number
    FROM proddb.fionafan.july_cohort_deliveries_28d
)
SELECT 
    consumer_id,
    cohort_type,
    delivery_id,
    delivery_created_at,
    days_since_onboarding,
    order_number,
    -- Day part classification
    CASE 
        WHEN HOUR(delivery_created_at) BETWEEN 6 AND 10 THEN 'Breakfast (6-10am)'
        WHEN HOUR(delivery_created_at) BETWEEN 11 AND 13 THEN 'Lunch (11am-1pm)'
        WHEN HOUR(delivery_created_at) BETWEEN 14 AND 17 THEN 'Happy Hour (2-5pm)'
        WHEN HOUR(delivery_created_at) BETWEEN 18 AND 21 THEN 'Dinner (6-9pm)'
        ELSE 'Midnight (10pm-5am)'
    END AS day_part,
    -- Weekday
    DAYNAME(delivery_created_at) AS weekday,
    CASE 
        WHEN DAYOFWEEK(delivery_created_at) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend
FROM ranked_orders
WHERE order_number IN (1, 2);

-- Step 2: Summary by cohort and order number
SELECT 
    cohort_type,
    order_number,
    COUNT(DISTINCT consumer_id) AS num_consumers,
    COUNT(DISTINCT delivery_id) AS num_orders
FROM proddb.fionafan.july_cohort_first_second_orders
GROUP BY cohort_type, order_number
ORDER BY cohort_type, order_number;

-- Step 3: Day Part Analysis - First Order
SELECT 
    cohort_type,
    day_part,
    COUNT(DISTINCT consumer_id) AS num_consumers_first_order,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type) AS pct_of_cohort
FROM proddb.fionafan.july_cohort_first_second_orders
WHERE order_number = 1
GROUP BY cohort_type, day_part
ORDER BY cohort_type, 
    CASE day_part
        WHEN 'Breakfast (6-10am)' THEN 1
        WHEN 'Lunch (11am-1pm)' THEN 2
        WHEN 'Happy Hour (2-5pm)' THEN 3
        WHEN 'Dinner (6-9pm)' THEN 4
        WHEN 'Midnight (10pm-5am)' THEN 5
    END;

-- Step 4: Day Part Analysis - Second Order
SELECT 
    cohort_type,
    day_part,
    COUNT(DISTINCT consumer_id) AS num_consumers_second_order,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type) AS pct_of_cohort
FROM proddb.fionafan.july_cohort_first_second_orders
WHERE order_number = 2
GROUP BY cohort_type, day_part
ORDER BY cohort_type, 
    CASE day_part
        WHEN 'Breakfast (6-10am)' THEN 1
        WHEN 'Lunch (11am-1pm)' THEN 2
        WHEN 'Happy Hour (2-5pm)' THEN 3
        WHEN 'Dinner (6-9pm)' THEN 4
        WHEN 'Midnight (10pm-5am)' THEN 5
    END;

-- Step 5: Weekday Analysis - First Order
SELECT 
    cohort_type,
    weekday,
    is_weekend,
    COUNT(DISTINCT consumer_id) AS num_consumers_first_order,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type) AS pct_of_cohort
FROM proddb.fionafan.july_cohort_first_second_orders
WHERE order_number = 1
GROUP BY cohort_type, weekday, is_weekend
ORDER BY cohort_type, 
    pct_of_cohort desc;

-- Step 6: Weekday Analysis - Second Order
SELECT 
    cohort_type,
    weekday,
    is_weekend,
    COUNT(DISTINCT consumer_id) AS num_consumers_second_order,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type) AS pct_of_cohort
FROM proddb.fionafan.july_cohort_first_second_orders
WHERE order_number = 2
GROUP BY cohort_type, weekday, is_weekend
ORDER BY cohort_type, 
    CASE weekday
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- Step 7: Second Order Conversion Rate by First Order Day Part
WITH first_orders AS (
    SELECT consumer_id, cohort_type, day_part
    FROM proddb.fionafan.july_cohort_first_second_orders
    WHERE order_number = 1
),
second_orders AS (
    SELECT consumer_id
    FROM proddb.fionafan.july_cohort_first_second_orders
    WHERE order_number = 2
)
SELECT 
    f.cohort_type,
    f.day_part AS first_order_day_part,
    COUNT(DISTINCT f.consumer_id) AS total_consumers_first_order,
    COUNT(DISTINCT s.consumer_id) AS consumers_with_second_order,
    COUNT(DISTINCT s.consumer_id) * 100.0 / NULLIF(COUNT(DISTINCT f.consumer_id), 0) AS second_order_conversion_rate
FROM first_orders f
LEFT JOIN second_orders s ON f.consumer_id = s.consumer_id
GROUP BY f.cohort_type, f.day_part
ORDER BY f.cohort_type, 
    CASE f.day_part
        WHEN 'Breakfast (6-10am)' THEN 1
        WHEN 'Lunch (11am-1pm)' THEN 2
        WHEN 'Happy Hour (2-5pm)' THEN 3
        WHEN 'Dinner (6-9pm)' THEN 4
        WHEN 'Midnight (10pm-5am)' THEN 5
    END;

-- Step 8: Second Order Conversion Rate by First Order Weekday
WITH first_orders AS (
    SELECT consumer_id, cohort_type, weekday, is_weekend
    FROM proddb.fionafan.july_cohort_first_second_orders


    WHERE order_number = 1
),
second_orders AS (
    SELECT consumer_id
    FROM proddb.fionafan.july_cohort_first_second_orders

    WHERE order_number = 2
)
SELECT 
    f.cohort_type,
    f.weekday AS first_order_weekday,
    f.is_weekend,
    COUNT(DISTINCT f.consumer_id) AS total_consumers_first_order,
    COUNT(DISTINCT s.consumer_id) AS consumers_with_second_order,
    COUNT(DISTINCT s.consumer_id) * 100.0 / NULLIF(COUNT(DISTINCT f.consumer_id), 0) AS second_order_conversion_rate
FROM first_orders f
LEFT JOIN second_orders s ON f.consumer_id = s.consumer_id
GROUP BY f.cohort_type, f.weekday, f.is_weekend
ORDER BY f.cohort_type, 
    CASE f.weekday
        WHEN 'Mon' THEN 1
        WHEN 'Tue' THEN 2
        WHEN 'Wed' THEN 3
        WHEN 'Thu' THEN 4
        WHEN 'Fri' THEN 5
        WHEN 'Sat' THEN 6
        WHEN 'Sun' THEN 7
    END;

-- Step 9: Combined Day Part and Weekend Analysis for Second Order Conversion
WITH first_orders AS (
    SELECT consumer_id, cohort_type, day_part, is_weekend
    FROM proddb.fionafan.july_cohort_first_second_orders
    WHERE order_number = 1
),
second_orders AS (
    SELECT consumer_id
    FROM proddb.fionafan.july_cohort_first_second_orders
    WHERE order_number = 2
)
SELECT 
    f.cohort_type,
    f.day_part AS first_order_day_part,
    f.is_weekend AS first_order_is_weekend,
    COUNT(DISTINCT f.consumer_id) AS total_consumers_first_order,
    COUNT(DISTINCT s.consumer_id) AS consumers_with_second_order,
    COUNT(DISTINCT s.consumer_id) * 100.0 / NULLIF(COUNT(DISTINCT f.consumer_id), 0) AS second_order_conversion_rate
FROM first_orders f
LEFT JOIN second_orders s ON f.consumer_id = s.consumer_id
GROUP BY f.cohort_type, f.day_part, f.is_weekend
ORDER BY f.cohort_type, f.is_weekend,
    CASE f.day_part
        WHEN 'Breakfast (6-10am)' THEN 1
        WHEN 'Lunch (11am-1pm)' THEN 2
        WHEN 'Happy Hour (2-5pm)' THEN 3
        WHEN 'Dinner (6-9pm)' THEN 4
        WHEN 'Midnight (10pm-5am)' THEN 5
    END;
