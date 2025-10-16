/*
Quick Reference Queries for Guest vs Onboarding Analysis with RAU Breakdown
Final table structure includes: analysis_period (D1/D7/D14/D28), cohort_type, platform, rau_segment
*/

-- 1. Final Summary Results (with RAU breakdown by time period)
SELECT * 
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
ORDER BY analysis_period, cohort_type, platform, rau_segment;

-- 2. Cohort Sizes
SELECT 
    platform,
    cohort_type,
    COUNT(DISTINCT dd_device_id_filtered) as user_count,
    COUNT(DISTINCT consumer_id) as unique_consumers
FROM proddb.fionafan.guest_vs_onboarding_august_cohorts
GROUP BY ALL
ORDER BY platform, cohort_type;

-- 3. RAU Exposure Summary by Time Period
SELECT 
    analysis_period,
    cohort_type,
    platform,
    rau_segment,
    total_users,
    rau_exposure_rate,
    avg_days_to_first_rau,
    avg_milestones_seen
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
ORDER BY analysis_period, cohort_type, platform, rau_segment;

-- 4. D1 Funnel Comparison by RAU (iOS only) 
SELECT 
    cohort_type,
    rau_segment,
    total_users,
    d1_app_visit_rate,
    d1_store_content_rate,
    d1_store_page_rate,
    d1_add_to_cart_rate,
    d1_cart_page_rate,
    d1_checkout_rate,
    d1_purchase_rate,
    d1_order_conversion_rate
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D1' AND LOWER(platform) = 'ios'
ORDER BY cohort_type, rau_segment;

-- 5. D28 Funnel Comparison by RAU (iOS only)
SELECT 
    cohort_type,
    rau_segment,
    total_users,
    d28_store_content_rate,
    d28_store_page_rate,
    d28_add_to_cart_rate,
    d28_checkout_rate,
    d28_purchase_rate,
    d28_order_conversion_rate,
    avg_d28_orders_per_user,
    avg_d28_gmv_per_user
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D28' AND LOWER(platform) = 'ios'
ORDER BY cohort_type, rau_segment;

-- 6. D28 Analysis - Order Timing by RAU
SELECT 
    cohort_type,
    platform,
    rau_segment,
    total_users,
    avg_days_to_first_order,
    median_days_to_first_order,
    avg_days_to_first_rau
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D28'
ORDER BY cohort_type, platform, rau_segment;

-- 7. D28 Analysis - GMV and Order Value by RAU
SELECT 
    cohort_type,
    platform,
    rau_segment,
    total_users,
    avg_d28_orders_per_user,
    avg_d28_first_time_orders_per_user,
    d28_mau_rate,
    d28_new_user_mau_rate,
    avg_d28_gmv_per_user,
    avg_order_value_d28
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D28'
ORDER BY cohort_type, platform, rau_segment;

-- 8. All Time Periods - First Time Orders and MAU (D28 analysis)
SELECT 
    cohort_type,
    platform,
    rau_segment,
    total_users,
    d1_first_time_order_rate,
    d1_new_user_mau_rate,
    d1_mau_rate,
    d7_first_time_order_rate,
    d7_new_user_mau_rate,
    d7_mau_rate,
    d14_first_time_order_rate,
    d14_new_user_mau_rate,
    d14_mau_rate,
    d28_first_time_order_rate,
    d28_new_user_mau_rate,
    d28_mau_rate
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D28'
ORDER BY cohort_type, platform, rau_segment;

-- 9. Guest-to-Consumer: RAU Impact at D28 (iOS only)
SELECT 
    rau_segment,
    total_users,
    rau_exposure_rate,
    d1_order_conversion_rate,
    d7_order_conversion_rate,
    d14_order_conversion_rate,
    d28_order_conversion_rate,
    avg_d28_orders_per_user,
    d28_mau_rate,
    avg_d28_gmv_per_user
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE analysis_period = 'D28' 
    AND cohort_type = 'guest_to_consumer' 
    AND LOWER(platform) = 'ios'
ORDER BY rau_segment;

-- 10. Compare RAU Impact Across All Time Periods (iOS Guest-to-Consumer)
SELECT 
    analysis_period,
    rau_segment,
    total_users,
    d1_order_conversion_rate,
    d7_order_conversion_rate,
    d14_order_conversion_rate,
    d28_order_conversion_rate
FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis
WHERE cohort_type = 'guest_to_consumer' AND LOWER(platform) = 'ios'
ORDER BY analysis_period, rau_segment;

-- 11. User-Level Sample Data
SELECT *
FROM proddb.fionafan.guest_vs_onboarding_august_combined
WHERE LOWER(platform) = 'ios'
LIMIT 100;
