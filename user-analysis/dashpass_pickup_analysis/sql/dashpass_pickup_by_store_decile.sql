-- DashPass vs Pickup Analysis by Store Volume Deciles
-- Analysis: Breakdown DashPass pickup uplift by store order volume deciles
-- Store deciles based on 30 days prior to 2025-07-25 (i.e., 2025-06-25 to 2025-07-24)
-- Main analysis period: 2025-07-01 to 2025-08-16

WITH store_volume_baseline AS (
    -- Calculate store order volume for the 30 days prior to 2025-07-25
    SELECT 
        store_id,
        COUNT(*) as baseline_order_count,
        COUNT(DISTINCT creator_id) as baseline_unique_customers
    FROM proddb.public.dimension_deliveries
    WHERE 1=1
        AND is_filtered_core = 1
        AND country_id = 1
        AND active_date BETWEEN '2025-06-25' AND '2025-07-24'  -- 30 days prior to 2025-07-25
        AND store_id IS NOT NULL
    GROUP BY store_id
),

store_deciles AS (
    -- Create store volume deciles based on baseline order count
    SELECT 
        store_id,
        baseline_order_count,
        baseline_unique_customers,
        NTILE(10) OVER (ORDER BY baseline_order_count) as volume_decile,
        
        -- Add volume tier labels for clarity
        CASE 
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 1 THEN 'D1_Lowest_Volume'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 2 THEN 'D2_Low_Volume'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 3 THEN 'D3_Below_Average'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 4 THEN 'D4_Low_Medium'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 5 THEN 'D5_Medium'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 6 THEN 'D6_Medium_High'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 7 THEN 'D7_Above_Average'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 8 THEN 'D8_High_Volume'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 9 THEN 'D9_Very_High_Volume'
            WHEN NTILE(10) OVER (ORDER BY baseline_order_count) = 10 THEN 'D10_Highest_Volume'
        END as volume_tier_label
    FROM store_volume_baseline
),

main_analysis_data AS (
    -- Main analysis period data with store decile information
    SELECT 
        dd.delivery_id,
        dd.creator_id,
        dd.store_id,
        dd.active_date,
        dd.is_subscribed_consumer,
        dd.is_consumer_pickup,
        sd.volume_decile,
        sd.volume_tier_label,
        sd.baseline_order_count
        
    FROM proddb.public.dimension_deliveries dd
    INNER JOIN store_deciles sd ON dd.store_id = sd.store_id
    WHERE 1=1
        AND dd.is_filtered_core = 1
        AND dd.country_id = 1
        AND dd.active_date BETWEEN '2025-07-01' AND '2025-08-16'
        AND dd.creator_id IS NOT NULL
),

decile_summary AS (
    -- Summary by store volume decile and DashPass status
    SELECT 
        volume_decile,
        volume_tier_label,
        
        -- DashPass breakdown
        CASE WHEN is_subscribed_consumer = true THEN 'DashPass' ELSE 'Non-DashPass' END as customer_type,
        
        -- Basic metrics
        COUNT(*) as total_orders,
        COUNT(DISTINCT creator_id) as unique_customers,
        COUNT(DISTINCT store_id) as unique_stores,
        
        -- Pickup metrics
        SUM(CASE WHEN is_consumer_pickup = true THEN 1 ELSE 0 END) as pickup_orders,
        ROUND((SUM(CASE WHEN is_consumer_pickup = true THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) as pct_orders_pickup,
        
        -- Store volume stats for context
        AVG(baseline_order_count) as avg_store_baseline_volume,
        MIN(baseline_order_count) as min_store_baseline_volume,
        MAX(baseline_order_count) as max_store_baseline_volume
        
    FROM main_analysis_data
    GROUP BY volume_decile, volume_tier_label, (CASE WHEN is_subscribed_consumer = true THEN 'DashPass' ELSE 'Non-DashPass' END)
)

SELECT 
    volume_decile,
    volume_tier_label,
    customer_type,
    total_orders,
    unique_customers,
    unique_stores,
    pickup_orders,
    pct_orders_pickup,
    avg_store_baseline_volume,
    min_store_baseline_volume,
    max_store_baseline_volume
FROM decile_summary
ORDER BY volume_decile, customer_type;

-- Additional analysis: Calculate the DashPass pickup "uplift" by decile
WITH decile_comparison AS (
    SELECT 
        volume_decile,
        volume_tier_label,
        
        -- DashPass metrics
        SUM(CASE WHEN is_subscribed_consumer = true THEN 1 ELSE 0 END) as dashpass_orders,
        SUM(CASE WHEN is_subscribed_consumer = true AND is_consumer_pickup = true THEN 1 ELSE 0 END) as dashpass_pickup_orders,
        
        -- Non-DashPass metrics  
        SUM(CASE WHEN is_subscribed_consumer = false THEN 1 ELSE 0 END) as non_dashpass_orders,
        SUM(CASE WHEN is_subscribed_consumer = false AND is_consumer_pickup = true THEN 1 ELSE 0 END) as non_dashpass_pickup_orders,
        
        -- Store context
        COUNT(DISTINCT store_id) as stores_in_decile,
        AVG(baseline_order_count) as avg_baseline_volume
        
    FROM main_analysis_data
    GROUP BY volume_decile, volume_tier_label
)

SELECT 
    volume_decile,
    volume_tier_label,
    stores_in_decile,
    ROUND(avg_baseline_volume, 0) as avg_baseline_volume,
    
    -- Pickup rates
    ROUND((dashpass_pickup_orders * 100.0) / NULLIF(dashpass_orders, 0), 2) as dashpass_pickup_rate,
    ROUND((non_dashpass_pickup_orders * 100.0) / NULLIF(non_dashpass_orders, 0), 2) as non_dashpass_pickup_rate,
    
    -- Pickup rate difference (negative means DashPass has lower pickup rate)
    ROUND(
        (dashpass_pickup_orders * 100.0) / NULLIF(dashpass_orders, 0) - 
        (non_dashpass_pickup_orders * 100.0) / NULLIF(non_dashpass_orders, 0), 
        2
    ) as pickup_rate_difference,
    
    -- Relative pickup rate ratio
    ROUND(
        (non_dashpass_pickup_orders * 100.0) / NULLIF(non_dashpass_orders, 0) / 
        NULLIF((dashpass_pickup_orders * 100.0) / NULLIF(dashpass_orders, 0), 0), 
        2
    ) as pickup_rate_ratio
    
FROM decile_comparison
ORDER BY volume_decile;
