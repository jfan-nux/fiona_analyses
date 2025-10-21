-- Step 3: Pre-Exposure Merchant Affinity Analysis
-- Analyzes which merchants users ordered from before exposure (YTD since 2025-01-01)
-- Identifies "favorite" merchants and ordering patterns

-- Calculate merchant-level metrics for each user
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_churn_merchant_stats AS (
WITH user_merchant_orders AS (
    SELECT 
        consumer_id,
        tag,
        result,
        exposure_day,
        store_id,
        store_name,
        vertical,
        COUNT(DISTINCT delivery_id) AS orders_from_merchant,
        COUNT(DISTINCT order_date) AS days_ordered_from_merchant,
        SUM(order_value) AS total_spend_at_merchant,
        AVG(order_value) AS avg_order_value_at_merchant,
        MIN(order_date) AS first_order_date_at_merchant,
        MAX(order_date) AS last_order_date_at_merchant,
        AVG(delivery_rating) AS avg_delivery_rating,
        AVG(r2c_duration) AS avg_r2c_duration,
        AVG(d2c_duration) AS avg_d2c_duration
    FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd
    GROUP BY ALL
),

user_totals AS (
    SELECT 
        consumer_id,
        COUNT(DISTINCT delivery_id) AS total_orders,
        COUNT(DISTINCT store_id) AS total_unique_merchants,
        SUM(order_value) AS total_spend,
        COUNT(DISTINCT vertical) AS unique_verticals
    FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd
    GROUP BY 1
)

SELECT 
    u.consumer_id,
    u.tag,
    u.result,
    u.exposure_day,
    u.store_id,
    u.store_name,
    u.vertical,
    u.orders_from_merchant,
    u.days_ordered_from_merchant,
    u.total_spend_at_merchant,
    u.avg_order_value_at_merchant,
    u.first_order_date_at_merchant,
    u.last_order_date_at_merchant,
    u.avg_delivery_rating,
    u.avg_r2c_duration,
    u.avg_d2c_duration,
    
    -- User-level totals
    t.total_orders,
    t.total_unique_merchants,
    t.total_spend,
    t.unique_verticals,
    
    -- Calculate order share
    u.orders_from_merchant * 100.0 / NULLIF(t.total_orders, 0) AS pct_orders_from_merchant,
    u.total_spend_at_merchant * 100.0 / NULLIF(t.total_spend, 0) AS pct_spend_at_merchant,
    
    -- Calculate recency (days between last order at merchant and exposure)
    DATEDIFF('day', u.last_order_date_at_merchant, u.exposure_day) AS days_since_last_merchant_order,
    
    -- Rank merchants by order count for each user
    ROW_NUMBER() OVER (PARTITION BY u.consumer_id ORDER BY u.orders_from_merchant DESC, u.total_spend_at_merchant DESC) AS merchant_rank_by_orders
    
FROM user_merchant_orders u
INNER JOIN user_totals t
    ON u.consumer_id = t.consumer_id
);

-- Identify "favorite" merchants for each user
-- Definition: 3+ orders OR 40%+ order share
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_favorite_merchants AS (
SELECT 
    consumer_id,
    tag,
    result,
    exposure_day,
    store_id,
    store_name,
    vertical,
    orders_from_merchant,
    pct_orders_from_merchant,
    total_spend_at_merchant,
    pct_spend_at_merchant,
    last_order_date_at_merchant,
    days_since_last_merchant_order,
    merchant_rank_by_orders,
    avg_delivery_rating,
    
    -- Classify favorite type
    CASE 
        WHEN orders_from_merchant >= 3 AND pct_orders_from_merchant >= 40 THEN 'High Frequency + High Share'
        WHEN orders_from_merchant >= 3 THEN 'High Frequency'
        WHEN pct_orders_from_merchant >= 40 THEN 'High Share'
        ELSE 'Other'
    END AS favorite_type
    
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_merchant_stats
-- WHERE orders_from_merchant >= 3 
--    OR pct_orders_from_merchant >= 40
--    OR merchant_rank_by_orders = 1  -- Always include top merchant
);

-- User-level merchant behavior summary
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior AS (
SELECT 
    m.consumer_id,
    m.tag,
    m.result,
    m.exposure_day,
    
    -- Overall ordering stats (YTD pre-exposure)
    MAX(m.total_orders) AS total_orders_ytd,
    MAX(m.total_unique_merchants) AS unique_merchants_ytd,
    MAX(m.total_spend) AS total_spend_ytd,
    MAX(m.unique_verticals) AS unique_verticals_ytd,
    
    -- Favorite merchants
    COUNT(DISTINCT CASE WHEN f.consumer_id IS NOT NULL THEN m.store_id END) AS num_favorite_merchants,
    MAX(CASE WHEN m.merchant_rank_by_orders = 1 THEN m.store_id END) AS top_merchant_id,
    MAX(CASE WHEN m.merchant_rank_by_orders = 1 THEN m.store_name END) AS top_merchant_name,
    MAX(CASE WHEN m.merchant_rank_by_orders = 1 THEN m.vertical END) AS top_merchant_vertical,
    MAX(CASE WHEN m.merchant_rank_by_orders = 1 THEN m.orders_from_merchant END) AS top_merchant_orders,
    MAX(CASE WHEN m.merchant_rank_by_orders = 1 THEN m.pct_orders_from_merchant END) AS top_merchant_pct_orders,
    
    -- Concentration metrics
    MAX(m.pct_orders_from_merchant) AS max_merchant_order_concentration,
    AVG(m.pct_orders_from_merchant) AS avg_merchant_order_share,
    
    -- Diversity score (inverse of concentration)
    CASE 
        WHEN MAX(m.total_unique_merchants) >= 10 THEN 'High Diversity (10+ merchants)'
        WHEN MAX(m.total_unique_merchants) >= 5 THEN 'Medium Diversity (5-9 merchants)'
        WHEN MAX(m.total_unique_merchants) >= 2 THEN 'Low Diversity (2-4 merchants)'
        ELSE 'Single Merchant'
    END AS merchant_diversity,
    
    -- User type classification
    CASE 
        WHEN MAX(m.pct_orders_from_merchant) >= 60 THEN 'Loyalist (60%+ from one merchant)'
        WHEN MAX(m.total_unique_merchants) >= 10 THEN 'Explorer (10+ merchants)'
        WHEN MAX(m.unique_verticals) = 1 THEN 'Category-Focused (single vertical)'
        WHEN MAX(m.total_unique_merchants) <= 3 THEN 'Few Favorites (2-3 merchants)'
        ELSE 'Balanced'
    END AS user_ordering_type
    
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_merchant_stats m
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_favorite_merchants f
    ON m.consumer_id = f.consumer_id
    AND m.store_id = f.store_id
GROUP BY 1, 2, 3, 4
);

-- Summary: Distribution of user types
SELECT 
    user_ordering_type,
    COUNT(DISTINCT consumer_id) AS user_count,
    SUM(total_orders_ytd) AS total_deliveries,
    SUM(total_spend_ytd) AS total_gov,
    COUNT(DISTINCT consumer_id) * 100.0 / SUM(COUNT(DISTINCT consumer_id)) OVER () AS pct_users,
    SUM(total_orders_ytd) * 100.0 / SUM(SUM(total_orders_ytd)) OVER () AS pct_total_deliveries,
    SUM(total_spend_ytd) * 100.0 / SUM(SUM(total_spend_ytd)) OVER () AS pct_total_gov,
    AVG(total_orders_ytd) AS avg_orders,
    AVG(unique_merchants_ytd) AS avg_unique_merchants,
    AVG(num_favorite_merchants) AS avg_favorite_merchants

FROM proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior
GROUP BY 1
ORDER BY user_count DESC;

select consumer_id, count(1) cnt from proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior group by all having cnt>1 limit 10;

-- Summary: Favorite merchant statistics
SELECT 
    'Users with Favorite Merchants' AS metric,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / (SELECT COUNT(DISTINCT consumer_id) FROM proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior) AS pct_users
FROM proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior
WHERE num_favorite_merchants > 0

UNION ALL

SELECT 
    'Users with Multiple Favorites' AS metric,
    COUNT(DISTINCT consumer_id) AS user_count,
    COUNT(DISTINCT consumer_id) * 100.0 / (SELECT COUNT(DISTINCT consumer_id) FROM proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior) AS pct_users
FROM proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior
WHERE num_favorite_merchants > 1;

-- Top 20 most common favorite merchants across all users
SELECT 
    store_id,
    store_name,
    vertical,
    COUNT(DISTINCT consumer_id) AS num_users_favoriting,
    AVG(orders_from_merchant) AS avg_orders_per_user,
    AVG(pct_orders_from_merchant) AS avg_pct_orders,
    AVG(total_spend_at_merchant) AS avg_spend_per_user
FROM proddb.fionafan.cx_ios_reonboarding_favorite_merchants
GROUP BY 1, 2, 3
ORDER BY num_users_favoriting DESC
LIMIT 20;

-- Vertical distribution in pre-exposure period (YTD)
SELECT 
    vertical,
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT delivery_id) AS total_orders,
    SUM(order_value) AS total_gmv,
    AVG(order_value) AS avg_order_value
FROM proddb.fionafan.cx_ios_reonboarding_pre_churn_orders_ytd
GROUP BY 1
ORDER BY total_orders DESC;

