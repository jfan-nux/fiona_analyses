-- Post-Exposure Outcomes Table
-- Base: All users from experiment exposures table
-- Captures outcomes after experiment exposure at device_id/user_id level
-- Includes: reonboarding completion (notification view), opt-in status, and order metrics

CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes AS (
WITH
-- 1. Did reonboarding (saw notification page)
notification_view AS (
    SELECT DISTINCT
        REPLACE(LOWER(CASE WHEN dd_device_id LIKE 'dx_%' THEN dd_device_id ELSE 'dx_'||dd_device_id END), '-') AS dd_device_id_filtered,
        1 AS did_reonboarding
    FROM iguazu.consumer.M_onboarding_page_view_ice
    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-01-01'
        AND lower(onboarding_type) = 'resurrected_user'
        AND page = 'notification'
),

-- 2. Current push opt-in status
push_optins AS (
    SELECT
        consumer_id,DEVICE_ID,REPLACE(LOWER(CASE WHEN DEVICE_ID LIKE 'dx_%' THEN DEVICE_ID ELSE 'dx_'||DEVICE_ID END), '-') AS dd_device_id_filtered,
        CASE WHEN system_level_status = 'on' THEN 1 ELSE 0 END AS push_system_optin,
        CASE WHEN doordash_offers_status = 'on' THEN 1 ELSE 0 END AS push_doordash_offers_optin,
        CASE WHEN order_updates_status = 'on' THEN 1 ELSE 0 END AS push_order_updates_optin
    FROM edw.consumer.dimension_consumer_device_push_settings_scd3

    WHERE scd_current_record 
),

-- 3. Current SMS opt-in status
sms_optins AS (
    SELECT
        consumer_id,

        CASE WHEN marketing_status = 'on' THEN 1 ELSE 0 END AS sms_marketing_optin,
        CASE WHEN order_updates_status = 'on' THEN 1 ELSE 0 END AS sms_order_updates_optin
    FROM edw.consumer.dimension_consumer_sms_settings_scd3
    WHERE scd_current_record
),

-- 4. Post-exposure orders
orders AS (
    SELECT
        REPLACE(LOWER(CASE WHEN dd_device_id LIKE 'dx_%' THEN dd_device_id ELSE 'dx_'||dd_device_id END), '-') AS dd_device_id_filtered,
        COUNT(DISTINCT delivery_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_first_ordercart = 1 THEN delivery_id END) AS new_cx_orders,
        SUM(gov / 100.0) AS total_gov,
        SUM(variable_profit / 100.0) AS total_variable_profit,
        AVG(gov / 100.0) AS avg_order_value,
        MIN(CONVERT_TIMEZONE('UTC',timezone, created_at)::date) AS first_order_date,
        MAX(CONVERT_TIMEZONE('UTC',timezone, created_at)::date) AS last_order_date
    FROM dimension_deliveries
    WHERE is_filtered_core = 1
        AND CONVERT_TIMEZONE('UTC',timezone, created_at)::date >= '2025-09-01'
    GROUP BY 1
),

outcomes_combined AS (
SELECT
    exp.bucket_key AS dd_device_id,
    REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') AS dd_device_id_filtered,
    exp.consumer_id AS user_id,
    exp.exposure_time,
    date(exp.exposure_time) AS exposure_day,
    exp.tag,
    exp.result,
    
    -- ======================
    -- REONBOARDING (1/0 marker for notification page view)
    -- ======================
    COALESCE(nv.did_reonboarding, 0) AS did_reonboarding,
    
    -- ======================
    -- OPT-IN STATUS (current as of today)
    -- ======================
    -- Push notifications
    COALESCE(po.push_system_optin, 0) AS push_system_optin,
    COALESCE(po.push_doordash_offers_optin, 0) AS push_doordash_offers_optin,
    COALESCE(po.push_order_updates_optin, 0) AS push_order_updates_optin,
    
    -- SMS notifications
    COALESCE(so.sms_marketing_optin, 0) AS sms_marketing_optin,
    COALESCE(so.sms_order_updates_optin, 0) AS sms_order_updates_optin,
    
    -- ======================
    -- ORDER METRICS (post-exposure)
    -- ======================
    -- Order counts
    COALESCE(o.total_orders, 0) AS total_orders_post_exposure,
    COALESCE(o.new_cx_orders, 0) AS new_cx_orders_post_exposure,
    
    -- Binary: did they order?
    CASE WHEN COALESCE(o.total_orders, 0) > 0 THEN 1 ELSE 0 END AS has_order_post_exposure,
    
    -- Order value metrics
    COALESCE(o.total_gov, 0) AS total_gov_post_exposure,
    COALESCE(o.total_variable_profit, 0) AS total_vp_post_exposure,
    COALESCE(o.avg_order_value, 0) AS avg_order_value,
    
    -- Order dates and timing
    o.first_order_date AS first_order_date_post_exposure,
    o.last_order_date AS last_order_date_post_exposure,
    CASE WHEN o.first_order_date IS NOT NULL 
         THEN DATEDIFF('day', date(exp.exposure_time), o.first_order_date)
         ELSE NULL END AS days_to_first_order

FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp

-- Left join reonboarding (notification view)
LEFT JOIN notification_view nv
    ON REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') = nv.dd_device_id_filtered

-- Left join push opt-ins (current status) - join on filtered device_id
LEFT JOIN push_optins po
    ON REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') = po.dd_device_id_filtered

-- Left join SMS opt-ins (current status) - join on consumer_id
LEFT JOIN sms_optins so
    ON exp.consumer_id = so.consumer_id

-- Left join orders
LEFT JOIN orders o
    ON REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') = o.dd_device_id_filtered
)

-- Deduplicate: Keep one row per dd_device_id
SELECT *
FROM outcomes_combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY exposure_time) = 1
);

-- Summary statistics
SELECT 
    'Total Exposed Devices' AS metric,
    COUNT(*) AS count,
    NULL AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes

UNION ALL

SELECT 
    'Did Reonboarding (notification_view = 1)' AS metric,
    SUM(did_reonboarding) AS count,
    AVG(did_reonboarding) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes

UNION ALL

SELECT 
    'Opted into Push System' AS metric,
    SUM(push_system_optin) AS count,
    AVG(push_system_optin) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes

UNION ALL

SELECT 
    'Opted into SMS Marketing' AS metric,
    SUM(sms_marketing_optin) AS count,
    AVG(sms_marketing_optin) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes

UNION ALL

SELECT 
    'Ordered Post-Exposure' AS metric,
    SUM(has_order_post_exposure) AS count,
    AVG(has_order_post_exposure) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes;

-- Check for duplicates
SELECT 
    dd_device_id,
    user_id,
    COUNT(*) AS row_count
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Order metrics by reonboarding status
SELECT 
    did_reonboarding,
    tag,
    result,
    COUNT(DISTINCT dd_device_id) AS devices,
    SUM(has_order_post_exposure) AS ordered_devices,
    AVG(has_order_post_exposure) * 100.0 AS order_rate_pct,
    AVG(total_orders_post_exposure) AS avg_orders_per_device,
    AVG(total_gov_post_exposure) AS avg_gov_per_device,
    AVG(total_vp_post_exposure) AS avg_vp_per_device
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes
GROUP BY 1, 2, 3
ORDER BY 2, 3, 1;

-- ======================
-- MASTER FEATURE TABLE
-- Combines pre-exposure features and post-exposure outcomes
-- Deduplicated at device_id level
-- ======================
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_master_features AS (
WITH combined AS (
SELECT
    -- Base identifiers from post-exposure (ensures all exposed users included)
    post.dd_device_id,
    post.dd_device_id_filtered,
    post.user_id,
    post.exposure_time,
    post.exposure_day,
    post.tag,
    post.result,
    
    -- ======================
    -- PRE-EXPOSURE FEATURES
    -- ======================
    -- SCD status at exposure
    pre.scd_consumer_id,
    pre.scd_start_date,
    pre.scd_end_date,
    pre.signup_date,
    pre.first_order_date,
    pre.last_order_date,
    pre.lifestage,
    pre.lifestage_bucket,
    pre.experience,
    pre.business_vertical_line,
    pre.country_id,
    pre.region_name,
    pre.submarket_id,
    pre.tenure_days_at_exposure,
    pre.days_since_first_order,
    pre.days_since_last_order,
    
    -- Funnel metrics (pre-exposure)
    pre.store_content_visitor_count,
    pre.has_store_content_visit,
    pre.days_before_exposure_store_content,
    pre.store_page_visitor_count,
    pre.has_store_page_visit,
    pre.days_before_exposure_store_page,
    pre.order_cart_visitor_count,
    pre.has_order_cart_visit,
    pre.days_before_exposure_order_cart,
    pre.checkout_visitor_count,
    pre.has_checkout_visit,
    pre.days_before_exposure_checkout,
    pre.purchaser_count,
    pre.has_purchase,
    pre.days_before_exposure_purchase,
    pre.platform,
    pre.app_version,
    pre.urban_type,
    pre.is_dashpass,
    pre.first_event_date,
    pre.last_event_date,
    pre.num_event_days,
    
    -- Merchant affinity (pre-exposure)
    pre.merchant_total_orders_ytd,
    pre.unique_merchants_ytd,
    pre.merchant_total_spend_ytd,
    pre.unique_verticals_ytd,
    pre.num_favorite_merchants,
    pre.top_merchant_id,
    pre.top_merchant_name,
    pre.top_merchant_vertical,
    pre.top_merchant_orders,
    pre.top_merchant_pct_orders,
    pre.max_merchant_order_concentration,
    pre.avg_merchant_order_share,
    pre.merchant_diversity,
    pre.user_ordering_type,
    
    -- Order frequency patterns (pre-exposure)
    pre.frequency_total_orders_ytd,
    pre.distinct_order_days,
    pre.unique_stores,
    pre.frequency_total_spend_ytd,
    pre.ytd_period_days,
    pre.ytd_period_weeks,
    pre.avg_orders_per_week,
    pre.avg_orders_per_month,
    pre.avg_days_between_orders,
    pre.avg_days_between_orders_actual,
    pre.median_days_between_orders,
    pre.min_gap_between_orders,
    pre.max_gap_between_orders,
    pre.stddev_gap_between_orders,
    pre.longest_streak_weeks,
    pre.orders_in_longest_streak,
    pre.number_of_streaks,
    pre.orders_0_7d,
    pre.orders_8_14d,
    pre.orders_15_30d,
    pre.orders_31_60d,
    pre.orders_61_90d,
    pre.orders_91_180d,
    pre.orders_180plus_d,
    pre.active_weeks,
    pre.pct_weeks_active,
    pre.frequency_bucket,
    pre.regularity_pattern,
    pre.orders_early_period,
    pre.orders_late_period,
    pre.activity_trend,
    pre.pre_exposure_engagement_level,
    pre.days_since_last_funnel_action,
    pre.is_consistent_orderer,
    
    -- ======================
    -- POST-EXPOSURE OUTCOMES
    -- ======================
    -- Reonboarding
    post.did_reonboarding,
    
    -- Opt-in status (current)
    post.push_system_optin,
    post.push_doordash_offers_optin,
    post.push_order_updates_optin,
    post.sms_marketing_optin,
    post.sms_order_updates_optin,
    
    -- Order metrics (post-exposure)
    post.total_orders_post_exposure,
    post.new_cx_orders_post_exposure,
    post.has_order_post_exposure,
    post.total_gov_post_exposure,
    post.total_vp_post_exposure,
    post.avg_order_value,
    post.first_order_date_post_exposure,
    post.last_order_date_post_exposure,
    post.days_to_first_order

FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes post

LEFT JOIN proddb.fionafan.cx_ios_reonboarding_pre_exposure_features pre
    ON post.dd_device_id = pre.dd_device_id
    AND post.user_id = pre.user_id
)

-- Deduplicate: Keep one row per dd_device_id
SELECT *
FROM combined
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY exposure_time) = 1
);

select dd_device_id, user_id, count(1) from proddb.fionafan.cx_ios_reonboarding_master_features group by all having count(1) > 1;
-- Master table summary
SELECT 
    'Total Devices in Master Table' AS metric,
    COUNT(*) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features

UNION ALL

SELECT 
    'Devices with Pre-Exposure Features' AS metric,
    COUNT(*) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features
WHERE lifestage IS NOT NULL

UNION ALL

SELECT 
    'Devices that Did Reonboarding' AS metric,
    SUM(did_reonboarding) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features

UNION ALL

SELECT 
    'Devices that Ordered Post-Exposure' AS metric,
    SUM(has_order_post_exposure) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features;

-- Check for duplicates in master table
SELECT 
    dd_device_id,
    user_id,
    COUNT(*) AS row_count
FROM proddb.fionafan.cx_ios_reonboarding_master_features
GROUP BY ALL
HAVING COUNT(*) > 1;

-- ============================================
-- USER-LEVEL TABLES (CONSUMER_ID GRANULARITY)
-- ============================================

-- ======================
-- POST-EXPOSURE OUTCOMES - USER LEVEL
-- Base: All users from experiment exposures table, aggregated to consumer_id level
-- ======================
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level AS (
WITH
-- 1. Did reonboarding (saw notification page)
notification_view AS (
    SELECT DISTINCT
        consumer_id,
        1 AS did_reonboarding
    FROM iguazu.consumer.M_onboarding_page_view_ice
    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-01-01'
        AND lower(onboarding_type) = 'resurrected_user'
        AND page = 'notification'
),

-- 2. Current push opt-in status (aggregate to user level if multiple devices)
push_optins AS (
    SELECT
        consumer_id,
        MAX(CASE WHEN system_level_status = 'on' THEN 1 ELSE 0 END) AS push_system_optin,
        MAX(CASE WHEN doordash_offers_status = 'on' THEN 1 ELSE 0 END) AS push_doordash_offers_optin,
        MAX(CASE WHEN order_updates_status = 'on' THEN 1 ELSE 0 END) AS push_order_updates_optin
    FROM edw.consumer.dimension_consumer_device_push_settings_scd3
    WHERE scd_current_record 
    GROUP BY consumer_id
),

-- 3. Current SMS opt-in status
sms_optins AS (
    SELECT
        consumer_id,
        CASE WHEN marketing_status = 'on' THEN 1 ELSE 0 END AS sms_marketing_optin,
        CASE WHEN order_updates_status = 'on' THEN 1 ELSE 0 END AS sms_order_updates_optin
    FROM edw.consumer.dimension_consumer_sms_settings_scd3
    WHERE scd_current_record
),

-- 4. Post-exposure orders (aggregate to consumer_id level)
orders AS (
    SELECT
        creator_id as consumer_id,
        COUNT(DISTINCT delivery_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_first_ordercart = 1 THEN delivery_id END) AS new_cx_orders,
        SUM(gov / 100.0) AS total_gov,
        SUM(variable_profit / 100.0) AS total_variable_profit,
        AVG(gov / 100.0) AS avg_order_value,
        MIN(CONVERT_TIMEZONE('UTC',timezone, created_at)::date) AS first_order_date,
        MAX(CONVERT_TIMEZONE('UTC',timezone, created_at)::date) AS last_order_date
    FROM dimension_deliveries
    WHERE is_filtered_core = 1
        AND CONVERT_TIMEZONE('UTC',timezone, created_at)::date >= '2025-09-01'
    GROUP BY creator_id
),

outcomes_combined AS (
SELECT
    exp.consumer_id AS user_id,
    MIN(exp.exposure_time) AS exposure_time,
    MIN(date(exp.exposure_time)) AS exposure_day,
    MIN(exp.tag) AS tag,
    MIN(exp.result) AS result,
    
    -- ======================
    -- REONBOARDING (1/0 marker for notification page view)
    -- ======================
    MAX(COALESCE(nv.did_reonboarding, 0)) AS did_reonboarding,
    
    -- ======================
    -- OPT-IN STATUS (current as of today)
    -- ======================
    -- Push notifications
    MAX(COALESCE(po.push_system_optin, 0)) AS push_system_optin,
    MAX(COALESCE(po.push_doordash_offers_optin, 0)) AS push_doordash_offers_optin,
    MAX(COALESCE(po.push_order_updates_optin, 0)) AS push_order_updates_optin,
    
    -- SMS notifications
    MAX(COALESCE(so.sms_marketing_optin, 0)) AS sms_marketing_optin,
    MAX(COALESCE(so.sms_order_updates_optin, 0)) AS sms_order_updates_optin,
    
    -- ======================
    -- ORDER METRICS (post-exposure)
    -- ======================
    -- Order counts
    COALESCE(MAX(o.total_orders), 0) AS total_orders_post_exposure,
    COALESCE(MAX(o.new_cx_orders), 0) AS new_cx_orders_post_exposure,
    
    -- Binary: did they order?
    CASE WHEN COALESCE(MAX(o.total_orders), 0) > 0 THEN 1 ELSE 0 END AS has_order_post_exposure,
    
    -- Order value metrics
    COALESCE(MAX(o.total_gov), 0) AS total_gov_post_exposure,
    COALESCE(MAX(o.total_variable_profit), 0) AS total_vp_post_exposure,
    COALESCE(MAX(o.avg_order_value), 0) AS avg_order_value,
    
    -- Order dates and timing
    MAX(o.first_order_date) AS first_order_date_post_exposure,
    MAX(o.last_order_date) AS last_order_date_post_exposure,
    CASE WHEN MAX(o.first_order_date) IS NOT NULL 
         THEN DATEDIFF('day', MIN(date(exp.exposure_time)), MAX(o.first_order_date))
         ELSE NULL END AS days_to_first_order

FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp

-- Left join reonboarding (notification view)
LEFT JOIN notification_view nv
    ON exp.consumer_id = nv.consumer_id

-- Left join push opt-ins (current status)
LEFT JOIN push_optins po
    ON exp.consumer_id = po.consumer_id

-- Left join SMS opt-ins (current status)
LEFT JOIN sms_optins so
    ON exp.consumer_id = so.consumer_id

-- Left join orders
LEFT JOIN orders o
    ON exp.consumer_id::varchar = o.consumer_id::varchar

WHERE exp.consumer_id IS NOT NULL
GROUP BY exp.consumer_id
)

SELECT *
FROM outcomes_combined
);

-- Summary statistics - User Level
SELECT 
    'Total Exposed Users' AS metric,
    COUNT(*) AS count,
    NULL AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level

UNION ALL

SELECT 
    'Did Reonboarding (notification_view = 1)' AS metric,
    SUM(did_reonboarding) AS count,
    AVG(did_reonboarding) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level

UNION ALL

SELECT 
    'Opted into Push System' AS metric,
    SUM(push_system_optin) AS count,
    AVG(push_system_optin) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level

UNION ALL

SELECT 
    'Opted into SMS Marketing' AS metric,
    SUM(sms_marketing_optin) AS count,
    AVG(sms_marketing_optin) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level

UNION ALL

SELECT 
    'Ordered Post-Exposure' AS metric,
    SUM(has_order_post_exposure) AS count,
    AVG(has_order_post_exposure) * 100.0 AS rate
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level;

-- Check for duplicates - User Level
SELECT 
    user_id,
    COUNT(*) AS row_count
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Order metrics by reonboarding status - User Level
SELECT 
    did_reonboarding,
    tag,
    result,
    COUNT(DISTINCT user_id) AS users,
    SUM(has_order_post_exposure) AS ordered_users,
    AVG(has_order_post_exposure) * 100.0 AS order_rate_pct,
    AVG(total_orders_post_exposure) AS avg_orders_per_user,
    AVG(total_gov_post_exposure) AS avg_gov_per_user,
    AVG(total_vp_post_exposure) AS avg_vp_per_user
FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level
GROUP BY 1, 2, 3
ORDER BY 2, 3, 1;

-- ======================
-- MASTER FEATURE TABLE - USER LEVEL
-- Combines pre-exposure features and post-exposure outcomes at consumer_id level
-- ======================
CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_master_features_user_level AS (
WITH combined AS (
SELECT
    -- Base identifiers from post-exposure (ensures all exposed users included)
    post.user_id,
    post.exposure_time,
    post.exposure_day,
    post.tag,
    post.result,
    
    -- ======================
    -- PRE-EXPOSURE FEATURES
    -- ======================
    -- SCD status at exposure
    pre.scd_consumer_id,
    pre.scd_start_date,
    pre.scd_end_date,
    pre.signup_date,
    pre.first_order_date,
    pre.last_order_date,
    pre.lifestage,
    pre.lifestage_bucket,
    pre.experience,
    pre.business_vertical_line,
    pre.country_id,
    pre.region_name,
    pre.submarket_id,
    pre.tenure_days_at_exposure,
    pre.days_since_first_order,
    pre.days_since_last_order,
    
    -- Funnel metrics (pre-exposure)
    pre.store_content_visitor_count,
    pre.has_store_content_visit,
    pre.days_before_exposure_store_content,
    pre.store_page_visitor_count,
    pre.has_store_page_visit,
    pre.days_before_exposure_store_page,
    pre.order_cart_visitor_count,
    pre.has_order_cart_visit,
    pre.days_before_exposure_order_cart,
    pre.checkout_visitor_count,
    pre.has_checkout_visit,
    pre.days_before_exposure_checkout,
    pre.purchaser_count,
    pre.has_purchase,
    pre.days_before_exposure_purchase,
    pre.platform,
    pre.app_version,
    pre.urban_type,
    pre.is_dashpass,
    pre.first_event_date,
    pre.last_event_date,
    pre.num_event_days,
    
    -- Merchant affinity (pre-exposure)
    pre.merchant_total_orders_ytd,
    pre.unique_merchants_ytd,
    pre.merchant_total_spend_ytd,
    pre.unique_verticals_ytd,
    pre.num_favorite_merchants,
    pre.top_merchant_id,
    pre.top_merchant_name,
    pre.top_merchant_vertical,
    pre.top_merchant_orders,
    pre.top_merchant_pct_orders,
    pre.max_merchant_order_concentration,
    pre.avg_merchant_order_share,
    pre.merchant_diversity,
    pre.user_ordering_type,
    
    -- Order frequency patterns (pre-exposure)
    pre.frequency_total_orders_ytd,
    pre.distinct_order_days,
    pre.unique_stores,
    pre.frequency_total_spend_ytd,
    pre.ytd_period_days,
    pre.ytd_period_weeks,
    pre.avg_orders_per_week,
    pre.avg_orders_per_month,
    pre.avg_days_between_orders,
    pre.avg_days_between_orders_actual,
    pre.median_days_between_orders,
    pre.min_gap_between_orders,
    pre.max_gap_between_orders,
    pre.stddev_gap_between_orders,
    pre.longest_streak_weeks,
    pre.orders_in_longest_streak,
    pre.number_of_streaks,
    pre.orders_0_7d,
    pre.orders_8_14d,
    pre.orders_15_30d,
    pre.orders_31_60d,
    pre.orders_61_90d,
    pre.orders_91_180d,
    pre.orders_180plus_d,
    pre.active_weeks,
    pre.pct_weeks_active,
    pre.frequency_bucket,
    pre.regularity_pattern,
    pre.orders_early_period,
    pre.orders_late_period,
    pre.activity_trend,
    pre.pre_exposure_engagement_level,
    pre.days_since_last_funnel_action,
    pre.is_consistent_orderer,
    
    -- ======================
    -- POST-EXPOSURE OUTCOMES
    -- ======================
    -- Reonboarding
    post.did_reonboarding,
    
    -- Opt-in status (current)
    post.push_system_optin,
    post.push_doordash_offers_optin,
    post.push_order_updates_optin,
    post.sms_marketing_optin,
    post.sms_order_updates_optin,
    
    -- Order metrics (post-exposure)
    post.total_orders_post_exposure,
    post.new_cx_orders_post_exposure,
    post.has_order_post_exposure,
    post.total_gov_post_exposure,
    post.total_vp_post_exposure,
    post.avg_order_value,
    post.first_order_date_post_exposure,
    post.last_order_date_post_exposure,
    post.days_to_first_order

FROM proddb.fionafan.cx_ios_reonboarding_post_exposure_outcomes_user_level post

LEFT JOIN proddb.fionafan.cx_ios_reonboarding_pre_exposure_features_user_level pre
    ON post.user_id = pre.user_id
)

SELECT *
FROM combined
);

-- Check for duplicates - Master User Level
SELECT 
    user_id,
    COUNT(*) AS row_count
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Master table summary - User Level
SELECT 
    'Total Users in Master Table' AS metric,
    COUNT(*) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level

UNION ALL

SELECT 
    'Users with Pre-Exposure Features' AS metric,
    COUNT(*) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
WHERE lifestage IS NOT NULL

UNION ALL

SELECT 
    'Users that Did Reonboarding' AS metric,
    SUM(did_reonboarding) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level

UNION ALL

SELECT 
    'Users that Ordered Post-Exposure' AS metric,
    SUM(has_order_post_exposure) AS count
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level;
;

