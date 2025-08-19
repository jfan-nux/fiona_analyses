-- DashPass vs Pickup Analysis
-- Question: Do DashPass Cx have lower % pickup Cx?
-- Analysis compares pickup behavior between DashPass and non-DashPass customers

-- Set analysis period (adjust as needed)
SET start_date = '2025-07-01';
SET end_date = '2025-08-16';

WITH pickup_analysis AS (
    SELECT 
        -- DashPass status
        CASE 
            WHEN is_subscribed_consumer = 1 THEN 'DashPass'
            ELSE 'Non-DashPass' 
        END as customer_type,
        
        -- Pickup behavior
        CASE 
            WHEN is_consumer_pickup = 1 THEN 'Pickup'
            ELSE 'Delivery' 
        END as order_type,
        
        -- Customer and order counts
        creator_id as customer_id,
        delivery_id,
        active_date
        
    FROM proddb.public.dimension_deliveries
    WHERE 1=1
        AND is_filtered_core = 1  -- Standard filter for valid orders
        AND country_id = 1        -- US only
        AND active_date BETWEEN $start_date AND $end_date
        AND creator_id IS NOT NULL
),

-- Aggregate by customer to get customer-level pickup behavior
customer_behavior AS (
    SELECT 
        customer_id,
        customer_type,
        
        -- Customer-level metrics
        COUNT(DISTINCT delivery_id) as total_orders,
        COUNT(DISTINCT CASE WHEN order_type = 'Pickup' THEN delivery_id END) as pickup_orders,
        
        -- Customer pickup behavior classification
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN order_type = 'Pickup' THEN delivery_id END) > 0 
            THEN 'Has_Pickup_Orders'
            ELSE 'Delivery_Only' 
        END as pickup_behavior
        
    FROM pickup_analysis
    GROUP BY customer_id, customer_type
),

-- Summary statistics
summary_stats AS (
    SELECT 
        customer_type,
        
        -- Customer counts
        COUNT(DISTINCT customer_id) as total_customers,
        COUNT(DISTINCT CASE WHEN pickup_behavior = 'Has_Pickup_Orders' THEN customer_id END) as pickup_customers,
        
        -- Pickup customer percentage
        ROUND(
            (COUNT(DISTINCT CASE WHEN pickup_behavior = 'Has_Pickup_Orders' THEN customer_id END) * 100.0) / 
            NULLIF(COUNT(DISTINCT customer_id), 0), 
            2
        ) as pct_customers_with_pickup,
        
        -- Order-level statistics
        SUM(total_orders) as total_orders,
        SUM(pickup_orders) as total_pickup_orders,
        ROUND(
            (SUM(pickup_orders) * 100.0) / NULLIF(SUM(total_orders), 0), 
            2
        ) as pct_orders_pickup
        
    FROM customer_behavior
    GROUP BY customer_type
)

SELECT 
    customer_type,
    total_customers,
    pickup_customers,
    pct_customers_with_pickup,
    total_orders,
    total_pickup_orders,
    pct_orders_pickup
FROM summary_stats
ORDER BY customer_type;

-- Additional analysis: Statistical significance test preparation
-- This provides data for further statistical analysis if needed
WITH detailed_comparison AS (
    SELECT 
        'DashPass' as segment,
        COUNT(DISTINCT CASE WHEN pickup_behavior = 'Has_Pickup_Orders' THEN customer_id END) as pickup_customers,
        COUNT(DISTINCT customer_id) as total_customers
    FROM customer_behavior 
    WHERE customer_type = 'DashPass'
    
    UNION ALL
    
    SELECT 
        'Non-DashPass' as segment,
        COUNT(DISTINCT CASE WHEN pickup_behavior = 'Has_Pickup_Orders' THEN customer_id END) as pickup_customers,
        COUNT(DISTINCT customer_id) as total_customers
    FROM customer_behavior 
    WHERE customer_type = 'Non-DashPass'
)

SELECT 
    segment,
    pickup_customers,
    total_customers,
    ROUND((pickup_customers * 100.0) / total_customers, 2) as pickup_rate_pct,
    total_customers - pickup_customers as delivery_only_customers
FROM detailed_comparison
ORDER BY segment;
