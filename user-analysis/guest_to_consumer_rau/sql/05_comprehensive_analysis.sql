/*
Purpose: Final comprehensive analysis combining funnel and order data
Creates: proddb.fionafan.guest_vs_onboarding_august_final_analysis
*/

-- Create user-level combined table
CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_combined AS

SELECT 
    c.dd_device_id_filtered, c.consumer_id,
    c.cohort_date,
    c.cohort_type,
    c.platform,
    
    -- Funnel metrics
    COALESCE(f.d1_app_visit, 0) as d1_app_visit,
    COALESCE(f.d1_store_content_visit, 0) as d1_store_content_visit,
    COALESCE(f.d1_store_page_visit, 0) as d1_store_page_visit,
    COALESCE(f.d1_add_to_cart, 0) as d1_add_to_cart,
    COALESCE(f.d1_cart_page_visit, 0) as d1_cart_page_visit,
    COALESCE(f.d1_checkout_page_visit, 0) as d1_checkout_page_visit,
    COALESCE(f.d1_place_order_action, 0) as d1_place_order_action,
    COALESCE(f.d1_purchase, 0) as d1_purchase,
    
    COALESCE(f.d7_app_visit, 0) as d7_app_visit,
    COALESCE(f.d7_store_content_visit, 0) as d7_store_content_visit,
    COALESCE(f.d7_store_page_visit, 0) as d7_store_page_visit,
    COALESCE(f.d7_add_to_cart, 0) as d7_add_to_cart,
    COALESCE(f.d7_cart_page_visit, 0) as d7_cart_page_visit,
    COALESCE(f.d7_checkout_page_visit, 0) as d7_checkout_page_visit,
    COALESCE(f.d7_place_order_action, 0) as d7_place_order_action,
    COALESCE(f.d7_purchase, 0) as d7_purchase,
    
    COALESCE(f.d14_app_visit, 0) as d14_app_visit,
    COALESCE(f.d14_store_content_visit, 0) as d14_store_content_visit,
    COALESCE(f.d14_store_page_visit, 0) as d14_store_page_visit,
    COALESCE(f.d14_add_to_cart, 0) as d14_add_to_cart,
    COALESCE(f.d14_cart_page_visit, 0) as d14_cart_page_visit,
    COALESCE(f.d14_checkout_page_visit, 0) as d14_checkout_page_visit,
    COALESCE(f.d14_place_order_action, 0) as d14_place_order_action,
    COALESCE(f.d14_purchase, 0) as d14_purchase,
    
    COALESCE(f.d28_app_visit, 0) as d28_app_visit,
    COALESCE(f.d28_store_content_visit, 0) as d28_store_content_visit,
    COALESCE(f.d28_store_page_visit, 0) as d28_store_page_visit,
    COALESCE(f.d28_add_to_cart, 0) as d28_add_to_cart,
    COALESCE(f.d28_cart_page_visit, 0) as d28_cart_page_visit,
    COALESCE(f.d28_checkout_page_visit, 0) as d28_checkout_page_visit,
    COALESCE(f.d28_place_order_action, 0) as d28_place_order_action,
    COALESCE(f.d28_purchase, 0) as d28_purchase,
    
    -- Order metrics
    COALESCE(o.d1_orders, 0) as d1_orders,
    COALESCE(o.d7_orders, 0) as d7_orders,
    COALESCE(o.d14_orders, 0) as d14_orders,
    COALESCE(o.d28_orders, 0) as d28_orders,
    
    -- First time order metrics
    COALESCE(o.d1_first_time_orders, 0) as d1_first_time_orders,
    COALESCE(o.d7_first_time_orders, 0) as d7_first_time_orders,
    COALESCE(o.d14_first_time_orders, 0) as d14_first_time_orders,
    COALESCE(o.d28_first_time_orders, 0) as d28_first_time_orders,
    
    -- MAU counts (keep as is, don't coalesce)
    o.d1_mau,
    o.d7_mau,
    o.d14_mau,
    o.d28_mau,
    
    -- New user MAU counts (keep as is, don't coalesce)
    o.d1_new_user_mau,
    o.d7_new_user_mau,
    o.d14_new_user_mau,
    o.d28_new_user_mau,
    
    COALESCE(o.d1_gmv, 0) as d1_gmv,
    COALESCE(o.d7_gmv, 0) as d7_gmv,
    COALESCE(o.d14_gmv, 0) as d14_gmv,
    COALESCE(o.d28_gmv, 0) as d28_gmv,
    
    o.days_to_first_order,
    
    -- RAU exposure flags
    COALESCE(r.d1_saw_rau, 0) as d1_saw_rau,
    COALESCE(r.d7_saw_rau, 0) as d7_saw_rau,
    COALESCE(r.d14_saw_rau, 0) as d14_saw_rau,
    COALESCE(r.d28_saw_rau, 0) as d28_saw_rau,
    r.days_to_first_rau,
    r.unique_milestones_seen_d28

FROM proddb.fionafan.guest_vs_onboarding_august_cohorts c
LEFT JOIN proddb.fionafan.guest_vs_onboarding_august_funnel_metrics f
    ON c.dd_device_id_filtered = f.dd_device_id_filtered 
LEFT JOIN proddb.fionafan.guest_vs_onboarding_august_orders o
    ON c.dd_device_id_filtered = o.dd_device_id_filtered
LEFT JOIN proddb.fionafan.guest_vs_onboarding_august_rau r
    ON c.dd_device_id_filtered = r.dd_device_id_filtered;

-- Create aggregated analysis with separate time periods
CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_final_analysis AS

-- D1 Analysis (grouped by D1 RAU exposure)
SELECT 
    'D1' as analysis_period,
    cohort_type,
    platform,
    CASE WHEN d1_saw_rau = 1 THEN 'Saw RAU' ELSE 'No RAU' END as rau_segment,
    COUNT(DISTINCT dd_device_id_filtered) as total_users,
    
    -- RAU exposure metrics
    AVG(d1_saw_rau) as rau_exposure_rate,
    AVG(CASE WHEN days_to_first_rau IS NOT NULL AND days_to_first_rau <= 1 THEN days_to_first_rau END) as avg_days_to_first_rau,
    AVG(CASE WHEN unique_milestones_seen_d28 IS NOT NULL THEN unique_milestones_seen_d28 END) as avg_milestones_seen,
    
    -- D1 Funnel Rates
    AVG(d1_app_visit) as d1_app_visit_rate,
    AVG(d1_store_content_visit) as d1_store_content_rate,
    AVG(d1_store_page_visit) as d1_store_page_rate,
    AVG(d1_add_to_cart) as d1_add_to_cart_rate,
    AVG(d1_cart_page_visit) as d1_cart_page_rate,
    AVG(d1_checkout_page_visit) as d1_checkout_rate,
    AVG(d1_place_order_action) as d1_place_order_rate,
    AVG(d1_purchase) as d1_purchase_rate,
    
    -- D7 Funnel Rates
    AVG(d7_app_visit) as d7_app_visit_rate,
    AVG(d7_store_content_visit) as d7_store_content_rate,
    AVG(d7_store_page_visit) as d7_store_page_rate,
    AVG(d7_add_to_cart) as d7_add_to_cart_rate,
    AVG(d7_cart_page_visit) as d7_cart_page_rate,
    AVG(d7_checkout_page_visit) as d7_checkout_rate,
    AVG(d7_place_order_action) as d7_place_order_rate,
    AVG(d7_purchase) as d7_purchase_rate,
    
    -- D14 Funnel Rates
    AVG(d14_app_visit) as d14_app_visit_rate,
    AVG(d14_store_content_visit) as d14_store_content_rate,
    AVG(d14_store_page_visit) as d14_store_page_rate,
    AVG(d14_add_to_cart) as d14_add_to_cart_rate,
    AVG(d14_cart_page_visit) as d14_cart_page_rate,
    AVG(d14_checkout_page_visit) as d14_checkout_rate,
    AVG(d14_place_order_action) as d14_place_order_rate,
    AVG(d14_purchase) as d14_purchase_rate,
    
    -- D28 Funnel Rates
    AVG(d28_app_visit) as d28_app_visit_rate,
    AVG(d28_store_content_visit) as d28_store_content_rate,
    AVG(d28_store_page_visit) as d28_store_page_rate,
    AVG(d28_add_to_cart) as d28_add_to_cart_rate,
    AVG(d28_cart_page_visit) as d28_cart_page_rate,
    AVG(d28_checkout_page_visit) as d28_checkout_rate,
    AVG(d28_place_order_action) as d28_place_order_rate,
    AVG(d28_purchase) as d28_purchase_rate,
    
    -- Order Metrics (per user averages and conversion rates)
    AVG(d1_orders) as avg_d1_orders_per_user,
    AVG(d7_orders) as avg_d7_orders_per_user,
    AVG(d14_orders) as avg_d14_orders_per_user,
    AVG(d28_orders) as avg_d28_orders_per_user,
    
    AVG(CASE WHEN d1_orders > 0 THEN 1 ELSE 0 END) as d1_order_conversion_rate,
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d7_order_conversion_rate,
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d14_order_conversion_rate,
    AVG(CASE WHEN d28_orders > 0 THEN 1 ELSE 0 END) as d28_order_conversion_rate,
    
    -- First Time Order Metrics (per user averages and conversion rates)
    AVG(d1_first_time_orders) as avg_d1_first_time_orders_per_user,
    AVG(d7_first_time_orders) as avg_d7_first_time_orders_per_user,
    AVG(d14_first_time_orders) as avg_d14_first_time_orders_per_user,
    AVG(d28_first_time_orders) as avg_d28_first_time_orders_per_user,
    
    AVG(CASE WHEN d1_first_time_orders > 0 THEN 1 ELSE 0 END) as d1_first_time_order_rate,
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d7_first_time_order_rate,
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d14_first_time_order_rate,
    AVG(CASE WHEN d28_first_time_orders > 0 THEN 1 ELSE 0 END) as d28_first_time_order_rate,
    
    -- MAU Metrics (count distinct consumer IDs who placed orders, relative to cohort)
    COUNT(DISTINCT d1_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_mau_rate,
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_mau_rate,
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_mau_rate,
    COUNT(DISTINCT d28_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_mau_rate,
    
    -- New User MAU Metrics (count distinct consumer IDs who placed first order, relative to cohort)
    COUNT(DISTINCT d1_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_new_user_mau_rate,
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_new_user_mau_rate,
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_new_user_mau_rate,
    COUNT(DISTINCT d28_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_new_user_mau_rate,
    
    -- GMV Metrics (per user averages)
    AVG(d1_gmv) as avg_d1_gmv_per_user,
    AVG(d7_gmv) as avg_d7_gmv_per_user,
    AVG(d14_gmv) as avg_d14_gmv_per_user,
    AVG(d28_gmv) as avg_d28_gmv_per_user,
    AVG(CASE WHEN d28_orders > 0 THEN d28_gmv / d28_orders END) as avg_order_value_d28,
    
    -- First Order Timing
    AVG(CASE WHEN days_to_first_order IS NOT NULL THEN days_to_first_order END) as avg_days_to_first_order,
    MEDIAN(CASE WHEN days_to_first_order IS NOT NULL THEN days_to_first_order END) as median_days_to_first_order

FROM proddb.fionafan.guest_vs_onboarding_august_combined
GROUP BY cohort_type, platform, rau_segment

UNION ALL

-- D7 Analysis (grouped by D7 RAU exposure)
SELECT 
    'D7' as analysis_period,
    cohort_type,
    platform,
    CASE WHEN d7_saw_rau = 1 THEN 'Saw RAU' ELSE 'No RAU' END as rau_segment,
    COUNT(DISTINCT dd_device_id_filtered) as total_users,
    
    AVG(d7_saw_rau) as rau_exposure_rate,
    AVG(CASE WHEN days_to_first_rau IS NOT NULL AND days_to_first_rau <= 7 THEN days_to_first_rau END) as avg_days_to_first_rau,
    AVG(CASE WHEN unique_milestones_seen_d28 IS NOT NULL THEN unique_milestones_seen_d28 END) as avg_milestones_seen,
    
    AVG(d7_app_visit) as d1_app_visit_rate,
    AVG(d7_store_content_visit) as d1_store_content_rate,
    AVG(d7_store_page_visit) as d1_store_page_rate,
    AVG(d7_add_to_cart) as d1_add_to_cart_rate,
    AVG(d7_cart_page_visit) as d1_cart_page_rate,
    AVG(d7_checkout_page_visit) as d1_checkout_rate,
    AVG(d7_place_order_action) as d1_place_order_rate,
    AVG(d7_purchase) as d1_purchase_rate,
    
    AVG(d7_app_visit) as d7_app_visit_rate,
    AVG(d7_store_content_visit) as d7_store_content_rate,
    AVG(d7_store_page_visit) as d7_store_page_rate,
    AVG(d7_add_to_cart) as d7_add_to_cart_rate,
    AVG(d7_cart_page_visit) as d7_cart_page_rate,
    AVG(d7_checkout_page_visit) as d7_checkout_rate,
    AVG(d7_place_order_action) as d7_place_order_rate,
    AVG(d7_purchase) as d7_purchase_rate,
    
    AVG(d7_app_visit) as d14_app_visit_rate,
    AVG(d7_store_content_visit) as d14_store_content_rate,
    AVG(d7_store_page_visit) as d14_store_page_rate,
    AVG(d7_add_to_cart) as d14_add_to_cart_rate,
    AVG(d7_cart_page_visit) as d14_cart_page_rate,
    AVG(d7_checkout_page_visit) as d14_checkout_rate,
    AVG(d7_place_order_action) as d14_place_order_rate,
    AVG(d7_purchase) as d14_purchase_rate,
    
    AVG(d7_app_visit) as d28_app_visit_rate,
    AVG(d7_store_content_visit) as d28_store_content_rate,
    AVG(d7_store_page_visit) as d28_store_page_rate,
    AVG(d7_add_to_cart) as d28_add_to_cart_rate,
    AVG(d7_cart_page_visit) as d28_cart_page_rate,
    AVG(d7_checkout_page_visit) as d28_checkout_rate,
    AVG(d7_place_order_action) as d28_place_order_rate,
    AVG(d7_purchase) as d28_purchase_rate,
    
    AVG(d7_orders) as avg_d1_orders_per_user,
    AVG(d7_orders) as avg_d7_orders_per_user,
    AVG(d7_orders) as avg_d14_orders_per_user,
    AVG(d7_orders) as avg_d28_orders_per_user,
    
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d1_order_conversion_rate,
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d7_order_conversion_rate,
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d14_order_conversion_rate,
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d28_order_conversion_rate,
    
    AVG(d7_first_time_orders) as avg_d1_first_time_orders_per_user,
    AVG(d7_first_time_orders) as avg_d7_first_time_orders_per_user,
    AVG(d7_first_time_orders) as avg_d14_first_time_orders_per_user,
    AVG(d7_first_time_orders) as avg_d28_first_time_orders_per_user,
    
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d1_first_time_order_rate,
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d7_first_time_order_rate,
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d14_first_time_order_rate,
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d28_first_time_order_rate,
    
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_mau_rate,
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_mau_rate,
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_mau_rate,
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_mau_rate,
    
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_new_user_mau_rate,
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_new_user_mau_rate,
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_new_user_mau_rate,
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_new_user_mau_rate,
    
    AVG(d7_gmv) as avg_d1_gmv_per_user,
    AVG(d7_gmv) as avg_d7_gmv_per_user,
    AVG(d7_gmv) as avg_d14_gmv_per_user,
    AVG(d7_gmv) as avg_d28_gmv_per_user,
    AVG(CASE WHEN d7_orders > 0 THEN d7_gmv / d7_orders END) as avg_order_value_d28,
    
    AVG(CASE WHEN days_to_first_order IS NOT NULL AND days_to_first_order <= 7 THEN days_to_first_order END) as avg_days_to_first_order,
    MEDIAN(CASE WHEN days_to_first_order IS NOT NULL AND days_to_first_order <= 7 THEN days_to_first_order END) as median_days_to_first_order

FROM proddb.fionafan.guest_vs_onboarding_august_combined
GROUP BY cohort_type, platform, rau_segment

UNION ALL

-- D14 Analysis (grouped by D14 RAU exposure)
SELECT 
    'D14' as analysis_period,
    cohort_type,
    platform,
    CASE WHEN d14_saw_rau = 1 THEN 'Saw RAU' ELSE 'No RAU' END as rau_segment,
    COUNT(DISTINCT dd_device_id_filtered) as total_users,
    
    AVG(d14_saw_rau) as rau_exposure_rate,
    AVG(CASE WHEN days_to_first_rau IS NOT NULL AND days_to_first_rau <= 14 THEN days_to_first_rau END) as avg_days_to_first_rau,
    AVG(CASE WHEN unique_milestones_seen_d28 IS NOT NULL THEN unique_milestones_seen_d28 END) as avg_milestones_seen,
    
    AVG(d14_app_visit) as d1_app_visit_rate,
    AVG(d14_store_content_visit) as d1_store_content_rate,
    AVG(d14_store_page_visit) as d1_store_page_rate,
    AVG(d14_add_to_cart) as d1_add_to_cart_rate,
    AVG(d14_cart_page_visit) as d1_cart_page_rate,
    AVG(d14_checkout_page_visit) as d1_checkout_rate,
    AVG(d14_place_order_action) as d1_place_order_rate,
    AVG(d14_purchase) as d1_purchase_rate,
    
    AVG(d14_app_visit) as d7_app_visit_rate,
    AVG(d14_store_content_visit) as d7_store_content_rate,
    AVG(d14_store_page_visit) as d7_store_page_rate,
    AVG(d14_add_to_cart) as d7_add_to_cart_rate,
    AVG(d14_cart_page_visit) as d7_cart_page_rate,
    AVG(d14_checkout_page_visit) as d7_checkout_rate,
    AVG(d14_place_order_action) as d7_place_order_rate,
    AVG(d14_purchase) as d7_purchase_rate,
    
    AVG(d14_app_visit) as d14_app_visit_rate,
    AVG(d14_store_content_visit) as d14_store_content_rate,
    AVG(d14_store_page_visit) as d14_store_page_rate,
    AVG(d14_add_to_cart) as d14_add_to_cart_rate,
    AVG(d14_cart_page_visit) as d14_cart_page_rate,
    AVG(d14_checkout_page_visit) as d14_checkout_rate,
    AVG(d14_place_order_action) as d14_place_order_rate,
    AVG(d14_purchase) as d14_purchase_rate,
    
    AVG(d14_app_visit) as d28_app_visit_rate,
    AVG(d14_store_content_visit) as d28_store_content_rate,
    AVG(d14_store_page_visit) as d28_store_page_rate,
    AVG(d14_add_to_cart) as d28_add_to_cart_rate,
    AVG(d14_cart_page_visit) as d28_cart_page_rate,
    AVG(d14_checkout_page_visit) as d28_checkout_rate,
    AVG(d14_place_order_action) as d28_place_order_rate,
    AVG(d14_purchase) as d28_purchase_rate,
    
    AVG(d14_orders) as avg_d1_orders_per_user,
    AVG(d14_orders) as avg_d7_orders_per_user,
    AVG(d14_orders) as avg_d14_orders_per_user,
    AVG(d14_orders) as avg_d28_orders_per_user,
    
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d1_order_conversion_rate,
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d7_order_conversion_rate,
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d14_order_conversion_rate,
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d28_order_conversion_rate,
    
    AVG(d14_first_time_orders) as avg_d1_first_time_orders_per_user,
    AVG(d14_first_time_orders) as avg_d7_first_time_orders_per_user,
    AVG(d14_first_time_orders) as avg_d14_first_time_orders_per_user,
    AVG(d14_first_time_orders) as avg_d28_first_time_orders_per_user,
    
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d1_first_time_order_rate,
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d7_first_time_order_rate,
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d14_first_time_order_rate,
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d28_first_time_order_rate,
    
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_mau_rate,
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_mau_rate,
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_mau_rate,
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_mau_rate,
    
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_new_user_mau_rate,
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_new_user_mau_rate,
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_new_user_mau_rate,
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_new_user_mau_rate,
    
    AVG(d14_gmv) as avg_d1_gmv_per_user,
    AVG(d14_gmv) as avg_d7_gmv_per_user,
    AVG(d14_gmv) as avg_d14_gmv_per_user,
    AVG(d14_gmv) as avg_d28_gmv_per_user,
    AVG(CASE WHEN d14_orders > 0 THEN d14_gmv / d14_orders END) as avg_order_value_d28,
    
    AVG(CASE WHEN days_to_first_order IS NOT NULL AND days_to_first_order <= 14 THEN days_to_first_order END) as avg_days_to_first_order,
    MEDIAN(CASE WHEN days_to_first_order IS NOT NULL AND days_to_first_order <= 14 THEN days_to_first_order END) as median_days_to_first_order

FROM proddb.fionafan.guest_vs_onboarding_august_combined
GROUP BY cohort_type, platform, rau_segment

UNION ALL

-- D28 Analysis (grouped by D28 RAU exposure)
SELECT 
    'D28' as analysis_period,
    cohort_type,
    platform,
    CASE WHEN d28_saw_rau = 1 THEN 'Saw RAU' ELSE 'No RAU' END as rau_segment,
    COUNT(DISTINCT dd_device_id_filtered) as total_users,
    
    AVG(d28_saw_rau) as rau_exposure_rate,
    AVG(CASE WHEN days_to_first_rau IS NOT NULL THEN days_to_first_rau END) as avg_days_to_first_rau,
    AVG(CASE WHEN unique_milestones_seen_d28 IS NOT NULL THEN unique_milestones_seen_d28 END) as avg_milestones_seen,
    
    AVG(d28_app_visit) as d1_app_visit_rate,
    AVG(d28_store_content_visit) as d1_store_content_rate,
    AVG(d28_store_page_visit) as d1_store_page_rate,
    AVG(d28_add_to_cart) as d1_add_to_cart_rate,
    AVG(d28_cart_page_visit) as d1_cart_page_rate,
    AVG(d28_checkout_page_visit) as d1_checkout_rate,
    AVG(d28_place_order_action) as d1_place_order_rate,
    AVG(d28_purchase) as d1_purchase_rate,
    
    AVG(d28_app_visit) as d7_app_visit_rate,
    AVG(d28_store_content_visit) as d7_store_content_rate,
    AVG(d28_store_page_visit) as d7_store_page_rate,
    AVG(d28_add_to_cart) as d7_add_to_cart_rate,
    AVG(d28_cart_page_visit) as d7_cart_page_rate,
    AVG(d28_checkout_page_visit) as d7_checkout_rate,
    AVG(d28_place_order_action) as d7_place_order_rate,
    AVG(d28_purchase) as d7_purchase_rate,
    
    AVG(d28_app_visit) as d14_app_visit_rate,
    AVG(d28_store_content_visit) as d14_store_content_rate,
    AVG(d28_store_page_visit) as d14_store_page_rate,
    AVG(d28_add_to_cart) as d14_add_to_cart_rate,
    AVG(d28_cart_page_visit) as d14_cart_page_rate,
    AVG(d28_checkout_page_visit) as d14_checkout_rate,
    AVG(d28_place_order_action) as d14_place_order_rate,
    AVG(d28_purchase) as d14_purchase_rate,
    
    AVG(d28_app_visit) as d28_app_visit_rate,
    AVG(d28_store_content_visit) as d28_store_content_rate,
    AVG(d28_store_page_visit) as d28_store_page_rate,
    AVG(d28_add_to_cart) as d28_add_to_cart_rate,
    AVG(d28_cart_page_visit) as d28_cart_page_rate,
    AVG(d28_checkout_page_visit) as d28_checkout_rate,
    AVG(d28_place_order_action) as d28_place_order_rate,
    AVG(d28_purchase) as d28_purchase_rate,
    
    AVG(d1_orders) as avg_d1_orders_per_user,
    AVG(d7_orders) as avg_d7_orders_per_user,
    AVG(d14_orders) as avg_d14_orders_per_user,
    AVG(d28_orders) as avg_d28_orders_per_user,
    
    AVG(CASE WHEN d1_orders > 0 THEN 1 ELSE 0 END) as d1_order_conversion_rate,
    AVG(CASE WHEN d7_orders > 0 THEN 1 ELSE 0 END) as d7_order_conversion_rate,
    AVG(CASE WHEN d14_orders > 0 THEN 1 ELSE 0 END) as d14_order_conversion_rate,
    AVG(CASE WHEN d28_orders > 0 THEN 1 ELSE 0 END) as d28_order_conversion_rate,
    
    AVG(d1_first_time_orders) as avg_d1_first_time_orders_per_user,
    AVG(d7_first_time_orders) as avg_d7_first_time_orders_per_user,
    AVG(d14_first_time_orders) as avg_d14_first_time_orders_per_user,
    AVG(d28_first_time_orders) as avg_d28_first_time_orders_per_user,
    
    AVG(CASE WHEN d1_first_time_orders > 0 THEN 1 ELSE 0 END) as d1_first_time_order_rate,
    AVG(CASE WHEN d7_first_time_orders > 0 THEN 1 ELSE 0 END) as d7_first_time_order_rate,
    AVG(CASE WHEN d14_first_time_orders > 0 THEN 1 ELSE 0 END) as d14_first_time_order_rate,
    AVG(CASE WHEN d28_first_time_orders > 0 THEN 1 ELSE 0 END) as d28_first_time_order_rate,
    
    COUNT(DISTINCT d1_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_mau_rate,
    COUNT(DISTINCT d7_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_mau_rate,
    COUNT(DISTINCT d14_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_mau_rate,
    COUNT(DISTINCT d28_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_mau_rate,
    
    COUNT(DISTINCT d1_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d1_new_user_mau_rate,
    COUNT(DISTINCT d7_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d7_new_user_mau_rate,
    COUNT(DISTINCT d14_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d14_new_user_mau_rate,
    COUNT(DISTINCT d28_new_user_mau)::FLOAT / COUNT(DISTINCT consumer_id) as d28_new_user_mau_rate,
    
    AVG(d1_gmv) as avg_d1_gmv_per_user,
    AVG(d7_gmv) as avg_d7_gmv_per_user,
    AVG(d14_gmv) as avg_d14_gmv_per_user,
    AVG(d28_gmv) as avg_d28_gmv_per_user,
    AVG(CASE WHEN d28_orders > 0 THEN d28_gmv / d28_orders END) as avg_order_value_d28,
    
    AVG(CASE WHEN days_to_first_order IS NOT NULL THEN days_to_first_order END) as avg_days_to_first_order,
    MEDIAN(CASE WHEN days_to_first_order IS NOT NULL THEN days_to_first_order END) as median_days_to_first_order

FROM proddb.fionafan.guest_vs_onboarding_august_combined
GROUP BY cohort_type, platform, rau_segment

ORDER BY analysis_period, cohort_type, platform, rau_segment;

-- Display final results
SELECT * FROM proddb.fionafan.guest_vs_onboarding_august_final_analysis order by all;

