-- Onboarding funnel metrics time series analysis
-- Change date: 9/25/2025
-- Analysis window: 13 days before and after (9/12/2025 - 10/7/2025)
-- Pre: 9/12 - 9/24 (13 days), Post: 9/25 - 10/7 (13 days)

WITH onboarding_top_of_funnel AS (
    -- Top of funnel: all users who viewed onboarding promo page
    SELECT DISTINCT
        DATE(iguazu_partition_date) AS event_date,
        replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered,
        consumer_id,
        dd_platform AS platform
    FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
    WHERE iguazu_partition_date >= '2025-09-12'
        AND iguazu_partition_date <= '2025-10-07'
),

funnel_data AS (
    -- Left join funnel progression data
    SELECT 
        o.event_date,
        o.dd_device_id_filtered,
        o.consumer_id,
        o.platform,
        COALESCE(f.unique_store_content_page_visitor, 0) AS unique_store_content_page_visitor,
        COALESCE(f.unique_store_page_visitor, 0) AS unique_store_page_visitor,
        COALESCE(f.unique_order_cart_page_visitor, 0) AS unique_order_cart_page_visitor,
        COALESCE(f.unique_checkout_page_visitor, 0) AS unique_checkout_page_visitor,
        COALESCE(f.unique_purchaser, 0) AS unique_purchaser,
        CASE 
            WHEN o.event_date < '2025-09-25' THEN 'pre'
            ELSE 'post'
        END AS period
    FROM onboarding_top_of_funnel o
    LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
        ON o.dd_device_id_filtered = replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-')
        AND o.event_date = f.event_date
),

daily_metrics AS (
    SELECT
        event_date,
        period,
        platform,
        -- Top of funnel
        COUNT(DISTINCT dd_device_id_filtered) AS unique_onboarding_visitors,
        
        -- Funnel stages
        COUNT(DISTINCT CASE WHEN unique_store_page_visitor = 1 THEN dd_device_id_filtered END) AS store_page_visitors,
        COUNT(DISTINCT CASE WHEN unique_store_content_page_visitor = 1 THEN dd_device_id_filtered END) AS store_content_visitors,
        COUNT(DISTINCT CASE WHEN unique_order_cart_page_visitor = 1 THEN dd_device_id_filtered END) AS cart_visitors,
        COUNT(DISTINCT CASE WHEN unique_checkout_page_visitor = 1 THEN dd_device_id_filtered END) AS checkout_visitors,
        COUNT(DISTINCT CASE WHEN unique_purchaser = 1 THEN dd_device_id_filtered END) AS purchasers,
        
        -- Conversion rates
        DIV0(COUNT(DISTINCT CASE WHEN unique_store_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
             COUNT(DISTINCT dd_device_id_filtered)) AS store_page_cvr,
        DIV0(COUNT(DISTINCT CASE WHEN unique_store_content_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
             COUNT(DISTINCT dd_device_id_filtered)) AS store_content_cvr,
        DIV0(COUNT(DISTINCT CASE WHEN unique_order_cart_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
             COUNT(DISTINCT dd_device_id_filtered)) AS cart_cvr,
        DIV0(COUNT(DISTINCT CASE WHEN unique_checkout_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
             COUNT(DISTINCT dd_device_id_filtered)) AS checkout_cvr,
        DIV0(COUNT(DISTINCT CASE WHEN unique_purchaser = 1 THEN dd_device_id_filtered END) * 100.0,
             COUNT(DISTINCT dd_device_id_filtered)) AS purchase_cvr
    FROM funnel_data
    GROUP BY event_date, period, platform
)

SELECT 
    event_date,
    period,
    platform,
    unique_onboarding_visitors,
    store_page_visitors,
    store_content_visitors,
    cart_visitors,
    checkout_visitors,
    purchasers,
    ROUND(store_page_cvr, 2) AS store_page_cvr_pct,
    ROUND(store_content_cvr, 2) AS store_content_cvr_pct,
    ROUND(cart_cvr, 2) AS cart_cvr_pct,
    ROUND(checkout_cvr, 2) AS checkout_cvr_pct,
    ROUND(purchase_cvr, 2) AS purchase_cvr_pct
FROM daily_metrics
ORDER BY event_date, platform

