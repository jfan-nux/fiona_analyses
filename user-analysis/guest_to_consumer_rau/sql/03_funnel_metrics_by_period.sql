/*
Purpose: Calculate funnel metrics at D1, D7, D14, D28
Creates: proddb.fionafan.guest_vs_onboarding_august_funnel_metrics
*/

CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_funnel_metrics AS
SELECT 
    dd_device_id_filtered,
    cohort_date,
    cohort_type,
    platform,
    
    -- D1 metrics (within 1 day)
    MAX(CASE WHEN days_since_cohort <= 1 AND had_app_visit = 1 THEN 1 ELSE 0 END) as d1_app_visit,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_store_content_visit = 1 THEN 1 ELSE 0 END) as d1_store_content_visit,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_store_page_visit = 1 THEN 1 ELSE 0 END) as d1_store_page_visit,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_add_to_cart = 1 THEN 1 ELSE 0 END) as d1_add_to_cart,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_cart_page_visit = 1 THEN 1 ELSE 0 END) as d1_cart_page_visit,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_checkout_page_visit = 1 THEN 1 ELSE 0 END) as d1_checkout_page_visit,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_place_order_action = 1 THEN 1 ELSE 0 END) as d1_place_order_action,
    MAX(CASE WHEN days_since_cohort <= 1 AND had_purchase = 1 THEN 1 ELSE 0 END) as d1_purchase,
    
    -- D7 metrics (within 7 days)
    MAX(CASE WHEN days_since_cohort <= 7 AND had_app_visit = 1 THEN 1 ELSE 0 END) as d7_app_visit,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_store_content_visit = 1 THEN 1 ELSE 0 END) as d7_store_content_visit,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_store_page_visit = 1 THEN 1 ELSE 0 END) as d7_store_page_visit,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_add_to_cart = 1 THEN 1 ELSE 0 END) as d7_add_to_cart,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_cart_page_visit = 1 THEN 1 ELSE 0 END) as d7_cart_page_visit,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_checkout_page_visit = 1 THEN 1 ELSE 0 END) as d7_checkout_page_visit,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_place_order_action = 1 THEN 1 ELSE 0 END) as d7_place_order_action,
    MAX(CASE WHEN days_since_cohort <= 7 AND had_purchase = 1 THEN 1 ELSE 0 END) as d7_purchase,
    
    -- D14 metrics (within 14 days)
    MAX(CASE WHEN days_since_cohort <= 14 AND had_app_visit = 1 THEN 1 ELSE 0 END) as d14_app_visit,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_store_content_visit = 1 THEN 1 ELSE 0 END) as d14_store_content_visit,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_store_page_visit = 1 THEN 1 ELSE 0 END) as d14_store_page_visit,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_add_to_cart = 1 THEN 1 ELSE 0 END) as d14_add_to_cart,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_cart_page_visit = 1 THEN 1 ELSE 0 END) as d14_cart_page_visit,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_checkout_page_visit = 1 THEN 1 ELSE 0 END) as d14_checkout_page_visit,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_place_order_action = 1 THEN 1 ELSE 0 END) as d14_place_order_action,
    MAX(CASE WHEN days_since_cohort <= 14 AND had_purchase = 1 THEN 1 ELSE 0 END) as d14_purchase,
    
    -- D28 metrics (within 28 days)
    MAX(CASE WHEN days_since_cohort <= 28 AND had_app_visit = 1 THEN 1 ELSE 0 END) as d28_app_visit,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_store_content_visit = 1 THEN 1 ELSE 0 END) as d28_store_content_visit,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_store_page_visit = 1 THEN 1 ELSE 0 END) as d28_store_page_visit,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_add_to_cart = 1 THEN 1 ELSE 0 END) as d28_add_to_cart,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_cart_page_visit = 1 THEN 1 ELSE 0 END) as d28_cart_page_visit,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_checkout_page_visit = 1 THEN 1 ELSE 0 END) as d28_checkout_page_visit,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_place_order_action = 1 THEN 1 ELSE 0 END) as d28_place_order_action,
    MAX(CASE WHEN days_since_cohort <= 28 AND had_purchase = 1 THEN 1 ELSE 0 END) as d28_purchase

FROM proddb.fionafan.guest_vs_onboarding_august_funnel_events
GROUP BY 
    dd_device_id_filtered,
    cohort_date,
    cohort_type,
    platform;

-- Verify
SELECT 
    cohort_type,
    platform,
    COUNT(*) as user_count
FROM proddb.fionafan.guest_vs_onboarding_august_funnel_metrics
GROUP BY ALL
ORDER BY cohort_type, platform;

