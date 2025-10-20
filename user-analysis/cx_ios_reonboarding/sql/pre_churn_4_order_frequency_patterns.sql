-- Step 4: Pre-Churn Order Frequency & Engagement Patterns
-- Analyzes ordering cadence, streaks, and inter-order gaps before churn

-- Calculate order-to-order gaps and ordering patterns
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency AS (
WITH order_sequence AS (
    SELECT 
        consumer_id,
        tag,
        result,
        exposure_day,
        last_order_date,
        delivery_id,
        order_date,
        order_value,
        store_id,
        store_name,
        
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
        
        -- Week number in the 180d period
        FLOOR(days_before_last_order / 7) AS week_before_churn,
        
        -- Month bucket
        CASE 
            WHEN days_before_last_order BETWEEN 0 AND 30 THEN 'Month 1 (0-30d)'
            WHEN days_before_last_order BETWEEN 31 AND 60 THEN 'Month 2 (31-60d)'
            WHEN days_before_last_order BETWEEN 61 AND 90 THEN 'Month 3 (61-90d)'
            WHEN days_before_last_order BETWEEN 91 AND 120 THEN 'Month 4 (91-120d)'
            WHEN days_before_last_order BETWEEN 121 AND 150 THEN 'Month 5 (121-150d)'
            WHEN days_before_last_order BETWEEN 151 AND 180 THEN 'Month 6 (151-180d)'
        END AS month_bucket,
        
        days_before_last_order
        
    FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_180d
),

-- Calculate weekly streaks (weeks with at least 1 order)
weekly_activity AS (
    SELECT 
        consumer_id,
        week_before_churn,
        COUNT(DISTINCT delivery_id) AS orders_in_week,
        MIN(order_date) AS week_start_date
    FROM order_sequence
    GROUP BY 1, 2
),

streak_analysis AS (
    SELECT 
        consumer_id,
        week_before_churn,
        orders_in_week,
        
        -- Identify streak breaks (weeks with gaps)
        week_before_churn - LAG(week_before_churn, 1, week_before_churn + 1) OVER (PARTITION BY consumer_id ORDER BY week_before_churn DESC) AS week_gap,
        
        -- Create streak group ID
        SUM(CASE WHEN week_before_churn - LAG(week_before_churn, 1, week_before_churn + 1) OVER (PARTITION BY consumer_id ORDER BY week_before_churn DESC) = 1 
                 THEN 0 ELSE 1 END) OVER (PARTITION BY consumer_id ORDER BY week_before_churn DESC) AS streak_group
    FROM weekly_activity
),

-- Calculate longest streak per user
user_streaks AS (
    SELECT 
        consumer_id,
        streak_group,
        COUNT(*) AS consecutive_weeks,
        SUM(orders_in_week) AS orders_in_streak,
        MIN(week_before_churn) AS streak_end_week,
        MAX(week_before_churn) AS streak_start_week
    FROM streak_analysis
    GROUP BY 1, 2
)

SELECT 
    o.consumer_id,
    o.tag,
    o.result,
    o.exposure_day,
    o.last_order_date,
    
    -- Total ordering stats
    COUNT(DISTINCT o.delivery_id) AS total_orders_180d,
    COUNT(DISTINCT o.order_date) AS distinct_order_days,
    COUNT(DISTINCT o.store_id) AS unique_stores,
    SUM(o.order_value) AS total_spend_180d,
    
    -- Frequency metrics
    COUNT(DISTINCT o.delivery_id) / 26.0 AS avg_orders_per_week,  -- 180d = ~26 weeks
    COUNT(DISTINCT o.delivery_id) / 6.0 AS avg_orders_per_month,  -- 180d = 6 months
    180.0 / NULLIF(COUNT(DISTINCT o.delivery_id), 0) AS avg_days_between_orders,
    
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
    
    -- Recency patterns - orders in last 30d vs earlier periods
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 0 AND 30 THEN o.delivery_id END) AS orders_last_30d,
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 31 AND 60 THEN o.delivery_id END) AS orders_31_60d,
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 61 AND 90 THEN o.delivery_id END) AS orders_61_90d,
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 91 AND 180 THEN o.delivery_id END) AS orders_91_180d,
    
    -- Weekly activity count (how many weeks had at least 1 order)
    COUNT(DISTINCT o.week_before_churn) AS active_weeks,
    COUNT(DISTINCT o.week_before_churn) * 100.0 / 26.0 AS pct_weeks_active,
    
    -- Ordering pattern classification
    CASE 
        WHEN COUNT(DISTINCT o.delivery_id) / 26.0 >= 2 THEN 'High Frequency (2+ orders/week)'
        WHEN COUNT(DISTINCT o.delivery_id) / 26.0 >= 1 THEN 'Medium Frequency (1-2 orders/week)'
        WHEN COUNT(DISTINCT o.delivery_id) / 6.0 >= 1 THEN 'Low Frequency (1+ order/month)'
        ELSE 'Very Low Frequency (<1 order/month)'
    END AS frequency_bucket,
    
    -- Regularity classification (based on stddev of gaps)
    CASE 
        WHEN STDDEV(o.days_since_prev_order) <= 7 THEN 'Very Regular (consistent weekly)'
        WHEN STDDEV(o.days_since_prev_order) <= 14 THEN 'Regular (consistent bi-weekly)'
        WHEN STDDEV(o.days_since_prev_order) <= 21 THEN 'Somewhat Regular'
        ELSE 'Sporadic'
    END AS regularity_pattern,
    
    -- Activity trend (comparing first 90d vs last 90d)
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 91 AND 180 THEN o.delivery_id END) AS orders_first_90d,
    COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 0 AND 90 THEN o.delivery_id END) AS orders_last_90d,
    
    CASE 
        WHEN COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 0 AND 90 THEN o.delivery_id END) > 
             COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 91 AND 180 THEN o.delivery_id END) THEN 'Increasing'
        WHEN COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 0 AND 90 THEN o.delivery_id END) < 
             COUNT(DISTINCT CASE WHEN o.days_before_last_order BETWEEN 91 AND 180 THEN o.delivery_id END) THEN 'Declining'
        ELSE 'Stable'
    END AS activity_trend

FROM order_sequence o
LEFT JOIN user_streaks us
    ON o.consumer_id = us.consumer_id
GROUP BY 1, 2, 3, 4, 5
);

-- Summary: Distribution by frequency bucket
SELECT 
    frequency_bucket,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    SUM(total_orders_180d) AS total_deliveries,
    SUM(total_orders_180d) * 100.0 / SUM(SUM(total_orders_180d)) OVER () AS pct_total_deliveries,
    SUM(total_spend_180d) AS total_gov,
    SUM(total_spend_180d) * 100.0 / SUM(SUM(total_spend_180d)) OVER () AS pct_total_gov,
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

-- Summary: Activity trend before churn
SELECT 
    activity_trend,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    AVG(orders_first_90d) AS avg_orders_first_90d,
    AVG(orders_last_90d) AS avg_orders_last_90d,
    AVG(orders_last_30d) AS avg_orders_last_30d
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency
GROUP BY 1
ORDER BY user_count DESC;

-- Cross-tab: Frequency vs Regularity
SELECT 
    frequency_bucket,
    regularity_pattern,
    COUNT(DISTINCT consumer_id) AS user_count,
    AVG(longest_streak_weeks) AS avg_longest_streak,
    AVG(total_orders_180d) AS avg_total_orders
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
    AVG(total_orders_180d) AS avg_orders,
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

