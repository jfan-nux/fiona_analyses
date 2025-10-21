-- Step 4: Pre-Exposure Order Frequency & Engagement Patterns
-- Analyzes ordering cadence, streaks, and inter-order gaps before exposure (YTD since 2025-01-01)

-- Calculate order-to-order gaps and ordering patterns
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency AS (
WITH order_sequence AS (
    SELECT 
        consumer_id,
        tag,
        result,
        exposure_day,
        delivery_id,
        order_date,
        order_value,
        store_id,
        store_name,
        days_before_exposure,
        
        -- Calculate order sequence number
        ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY order_date) AS order_sequence_num,
        
        -- Get previous order date
        LAG(order_date) OVER (PARTITION BY consumer_id ORDER BY order_date) AS prev_order_date,
        
        -- Get next order date
        LEAD(order_date) OVER (PARTITION BY consumer_id ORDER BY order_date) AS next_order_date,
        
        -- Calculate days since previous order
        DATEDIFF('day', LAG(order_date) OVER (PARTITION BY consumer_id ORDER BY order_date), order_date) AS days_since_prev_order,
        
        -- Calculate days to next order
        DATEDIFF('day', order_date, LEAD(order_date) OVER (PARTITION BY consumer_id ORDER BY order_date)) AS days_to_next_order,
        
        -- Week number before exposure
        FLOOR(days_before_exposure / 7) AS week_before_exposure,
        
        -- Time bucket
        CASE 
            WHEN days_before_exposure BETWEEN 0 AND 7 THEN '0-7 days before'
            WHEN days_before_exposure BETWEEN 8 AND 14 THEN '8-14 days before'
            WHEN days_before_exposure BETWEEN 15 AND 30 THEN '15-30 days before'
            WHEN days_before_exposure BETWEEN 31 AND 60 THEN '31-60 days before'
            WHEN days_before_exposure BETWEEN 61 AND 90 THEN '61-90 days before'
            WHEN days_before_exposure BETWEEN 91 AND 120 THEN '91-120 days before'
            WHEN days_before_exposure BETWEEN 121 AND 180 THEN '121-180 days before'
            WHEN days_before_exposure > 180 THEN '180+ days before'
        END AS time_bucket
        
    FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd
),

-- Calculate weekly streaks (weeks with at least 1 order)
weekly_activity AS (
    SELECT 
        consumer_id,
        week_before_exposure,
        COUNT(DISTINCT delivery_id) AS orders_in_week,
        MIN(order_date) AS week_start_date
    FROM order_sequence
    GROUP BY 1, 2
),

week_gaps AS (
    SELECT 
        consumer_id,
        week_before_exposure,
        orders_in_week,
        
        -- Identify streak breaks (weeks with gaps)
        week_before_exposure - LAG(week_before_exposure, 1, week_before_exposure + 1) OVER (PARTITION BY consumer_id ORDER BY week_before_exposure DESC) AS week_gap
    FROM weekly_activity
),

streak_analysis AS (
    SELECT 
        consumer_id,
        week_before_exposure,
        orders_in_week,
        week_gap,
        
        -- Create streak group ID based on pre-calculated week_gap
        SUM(CASE WHEN week_gap = 1 THEN 0 ELSE 1 END) OVER (PARTITION BY consumer_id ORDER BY week_before_exposure DESC) AS streak_group
    FROM week_gaps
),

-- Calculate longest streak per user
user_streaks AS (
    SELECT 
        consumer_id,
        streak_group,
        COUNT(*) AS consecutive_weeks,
        SUM(orders_in_week) AS orders_in_streak,
        MIN(week_before_exposure) AS streak_end_week,
        MAX(week_before_exposure) AS streak_start_week
    FROM streak_analysis
    GROUP BY 1, 2
)

SELECT 
    o.consumer_id,
    o.tag,
    o.result,
    o.exposure_day,
    
    -- Total ordering stats
    COUNT(DISTINCT o.delivery_id) AS total_orders_ytd,
    COUNT(DISTINCT o.order_date) AS distinct_order_days,
    COUNT(DISTINCT o.store_id) AS unique_stores,
    SUM(o.order_value) AS total_spend_ytd,
    
    -- Calculate actual YTD period length per user
    DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) AS ytd_period_days,
    DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 7.0 AS ytd_period_weeks,
    
    -- Frequency metrics (based on actual period length)
    COUNT(DISTINCT o.delivery_id) / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 7.0, 0) AS avg_orders_per_week,
    COUNT(DISTINCT o.delivery_id) / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 30.0, 0) AS avg_orders_per_month,
    DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / NULLIF(COUNT(DISTINCT o.delivery_id), 0) AS avg_days_between_orders,
    
    -- Inter-order gap statistics
    AVG(o.days_since_prev_order) AS avg_days_between_orders_actual,
    MEDIAN(o.days_since_prev_order) AS median_days_between_orders,
    MIN(o.days_since_prev_order) AS min_gap_between_orders,
    MAX(o.days_since_prev_order) AS max_gap_between_orders,
    STDDEV(o.days_since_prev_order) AS stddev_gap_between_orders,
    
    -- Streak metrics
    MAX(us.consecutive_weeks) AS longest_streak_weeks,
    MAX(us.orders_in_streak) AS orders_in_longest_streak,
    COUNT(DISTINCT us.streak_group) AS number_of_streaks,
    
    -- Recency patterns - orders by time before exposure
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 0 AND 7 THEN o.delivery_id END) AS orders_0_7d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 8 AND 14 THEN o.delivery_id END) AS orders_8_14d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 15 AND 30 THEN o.delivery_id END) AS orders_15_30d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 31 AND 60 THEN o.delivery_id END) AS orders_31_60d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 61 AND 90 THEN o.delivery_id END) AS orders_61_90d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure BETWEEN 91 AND 180 THEN o.delivery_id END) AS orders_91_180d,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure > 180 THEN o.delivery_id END) AS orders_180plus_d,
    
    -- Weekly activity count (how many weeks had at least 1 order)
    COUNT(DISTINCT o.week_before_exposure) AS active_weeks,
    COUNT(DISTINCT o.week_before_exposure) * 100.0 / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 7.0, 0) AS pct_weeks_active,
    
    -- Ordering pattern classification (using dynamic period calculation)
    CASE 
        WHEN COUNT(DISTINCT o.delivery_id) / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 7.0, 0) >= 2 THEN 'High Frequency (2+ orders/week)'
        WHEN COUNT(DISTINCT o.delivery_id) / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 7.0, 0) >= 1 THEN 'Medium Frequency (1-2 orders/week)'
        WHEN COUNT(DISTINCT o.delivery_id) / NULLIF(DATEDIFF('day', MIN(o.order_date), MAX(o.exposure_day)) / 30.0, 0) >= 1 THEN 'Low Frequency (1+ order/month)'
        ELSE 'Very Low Frequency (<1 order/month)'
    END AS frequency_bucket,
    
    -- Regularity classification (based on stddev of gaps)
    CASE 
        WHEN STDDEV(o.days_since_prev_order) <= 7 THEN 'Very Regular (consistent weekly)'
        WHEN STDDEV(o.days_since_prev_order) <= 14 THEN 'Regular (consistent bi-weekly)'
        WHEN STDDEV(o.days_since_prev_order) <= 21 THEN 'Somewhat Regular'
        ELSE 'Sporadic'
    END AS regularity_pattern,
    
    -- Activity trend (comparing first half vs last half of period)
    COUNT(DISTINCT CASE WHEN o.days_before_exposure > 90 THEN o.delivery_id END) AS orders_early_period,
    COUNT(DISTINCT CASE WHEN o.days_before_exposure <= 90 THEN o.delivery_id END) AS orders_late_period,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN o.days_before_exposure <= 90 THEN o.delivery_id END) > 
             COUNT(DISTINCT CASE WHEN o.days_before_exposure > 90 THEN o.delivery_id END) THEN 'Increasing'
        WHEN COUNT(DISTINCT CASE WHEN o.days_before_exposure <= 90 THEN o.delivery_id END) < 
             COUNT(DISTINCT CASE WHEN o.days_before_exposure > 90 THEN o.delivery_id END) THEN 'Declining'
        ELSE 'Stable'
    END AS activity_trend

FROM order_sequence o
LEFT JOIN user_streaks us
    ON o.consumer_id = us.consumer_id
GROUP BY 1, 2, 3, 4
);

-- Summary: Distribution by frequency bucket
SELECT 
    frequency_bucket,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    SUM(total_orders_ytd) AS total_deliveries,
    SUM(total_orders_ytd) * 100.0 / SUM(SUM(total_orders_ytd)) OVER () AS pct_total_deliveries,
    SUM(total_spend_ytd) AS total_gov,
    SUM(total_spend_ytd) * 100.0 / SUM(SUM(total_spend_ytd)) OVER () AS pct_total_gov,
    AVG(avg_orders_per_week) AS avg_orders_per_week,
    AVG(avg_days_between_orders) AS avg_days_between_orders,
    AVG(longest_streak_weeks) AS avg_longest_streak_weeks
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1
ORDER BY 
    CASE frequency_bucket
        WHEN 'High Frequency (2+ orders/week)' THEN 1
        WHEN 'Medium Frequency (1-2 orders/week)' THEN 2
        WHEN 'Low Frequency (1+ order/month)' THEN 3
        ELSE 4
    END;

-- Summary: Distribution by regularity pattern
SELECT 
    regularity_pattern,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    AVG(avg_orders_per_week) AS avg_orders_per_week,
    AVG(median_days_between_orders) AS median_days_between_orders,
    AVG(stddev_gap_between_orders) AS avg_stddev_gaps
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1
ORDER BY 
    CASE regularity_pattern
        WHEN 'Very Regular (consistent weekly)' THEN 1
        WHEN 'Regular (consistent bi-weekly)' THEN 2
        WHEN 'Somewhat Regular' THEN 3
        ELSE 4
    END;

-- Summary: Activity trend before exposure
SELECT 
    activity_trend,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    AVG(orders_early_period) AS avg_orders_early_period,
    AVG(orders_late_period) AS avg_orders_late_period,
    AVG(orders_0_7d) AS avg_orders_0_7d,
    AVG(orders_15_30d) AS avg_orders_15_30d
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1
ORDER BY user_count DESC;

-- Cross-tab: Frequency vs Regularity
SELECT 
    frequency_bucket,
    regularity_pattern,
    COUNT(DISTINCT consumer_id) AS user_count,
    AVG(longest_streak_weeks) AS avg_longest_streak,
    AVG(total_orders_ytd) AS avg_total_orders
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1, 2
ORDER BY 1, 2;

-- Streak distribution
SELECT 
    CASE 
        WHEN longest_streak_weeks >= 8 THEN '8+ weeks'
        WHEN longest_streak_weeks >= 4 THEN '4-7 weeks'
        WHEN longest_streak_weeks >= 2 THEN '2-3 weeks'
        ELSE '0-1 week'
    END AS streak_length,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    AVG(total_orders_ytd) AS avg_orders,
    AVG(avg_orders_per_week) AS avg_orders_per_week
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1
ORDER BY 
    CASE 
        WHEN streak_length = '8+ weeks' THEN 1
        WHEN streak_length = '4-7 weeks' THEN 2
        WHEN streak_length = '2-3 weeks' THEN 3
        ELSE 4
    END;

