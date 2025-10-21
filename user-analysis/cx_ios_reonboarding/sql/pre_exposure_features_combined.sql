-- Comprehensive Pre-Exposure Feature Table
-- Base: All users from experiment exposures table
-- Left joins all pre-exposure behavioral features at user_id/device_id level
-- Includes: SCD status, funnel metrics, merchant affinity, and order frequency patterns

CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_exposure_features AS (
WITH combined_features AS (
SELECT 
    -- ======================
    -- 1. IDENTIFIERS & BASIC INFO
    -- ======================
    exp.bucket_key AS dd_device_id,
    REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') as dd_device_id_filtered,
    exp.consumer_id AS user_id,
    exp.exposure_time,
    exp.tag,
    exp.result,
    
    -- ======================
    -- 2. STATUS AT EXPOSURE (from SCD)
    -- ======================
    date(exp.exposure_time) AS exposure_day,
    scd.scd_consumer_id,
    scd.scd_start_date,
    scd.scd_end_date,
    scd.signup_date,
    scd.first_order_date,
    scd.last_order_date,
    scd.lifestage,
    scd.lifestage_bucket,
    scd.experience,
    scd.business_vertical_line,
    scd.country_id,
    scd.region_name,
    scd.submarket_id,
    
    -- Tenure metrics (handle 9999-12-31 placeholder dates)
    CASE 
        WHEN scd.signup_date IS NULL OR scd.signup_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.signup_date, DATE(exp.exposure_time))
    END AS tenure_days_at_exposure,
    
    CASE 
        WHEN scd.first_order_date IS NULL OR scd.first_order_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.first_order_date, DATE(exp.exposure_time))
    END AS days_since_first_order,
    
    CASE 
        WHEN scd.last_order_date IS NULL OR scd.last_order_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.last_order_date, DATE(exp.exposure_time))
    END AS days_since_last_order,
    
    -- ======================
    -- 3. PREVIOUS FUNNEL METRICS (from pre_churn_1)
    -- ======================
    -- Store content page
    f.store_content_visitor_count,
    f.has_store_content_visit,
    f.days_before_exposure_store_content,
    
    -- Store page
    f.store_page_visitor_count,
    f.has_store_page_visit,
    f.days_before_exposure_store_page,
    
    -- Order cart page
    f.order_cart_visitor_count,
    f.has_order_cart_visit,
    f.days_before_exposure_order_cart,
    
    -- Checkout page
    f.checkout_visitor_count,
    f.has_checkout_visit,
    f.days_before_exposure_checkout,
    
    -- Purchase
    f.purchaser_count,
    f.has_purchase,
    f.days_before_exposure_purchase,
    
    -- Additional context
    f.platform,
    f.app_version,
    f.urban_type,
    f.is_dashpass,
    f.first_event_date,
    f.last_event_date,
    f.num_event_days,
    
    -- ======================
    -- 4. MERCHANT AFFINITY METRICS (from pre_churn_3)
    -- ======================
    mb.total_orders_ytd AS merchant_total_orders_ytd,
    mb.unique_merchants_ytd,
    mb.total_spend_ytd AS merchant_total_spend_ytd,
    mb.unique_verticals_ytd,
    mb.num_favorite_merchants,
    mb.top_merchant_id,
    mb.top_merchant_name,
    mb.top_merchant_vertical,
    mb.top_merchant_orders,
    mb.top_merchant_pct_orders,
    mb.max_merchant_order_concentration,
    mb.avg_merchant_order_share,
    mb.merchant_diversity,
    mb.user_ordering_type,
    
    -- ======================
    -- 5. ORDER FREQUENCY PATTERNS (from pre_churn_4)
    -- ======================
    freq.total_orders_ytd AS frequency_total_orders_ytd,
    freq.distinct_order_days,
    freq.unique_stores,
    freq.total_spend_ytd AS frequency_total_spend_ytd,
    freq.ytd_period_days,
    freq.ytd_period_weeks,
    freq.avg_orders_per_week,
    freq.avg_orders_per_month,
    freq.avg_days_between_orders,
    freq.avg_days_between_orders_actual,
    freq.median_days_between_orders,
    freq.min_gap_between_orders,
    freq.max_gap_between_orders,
    freq.stddev_gap_between_orders,
    freq.longest_streak_weeks,
    freq.orders_in_longest_streak,
    freq.number_of_streaks,
    
    -- Orders by time before exposure
    freq.orders_0_7d,
    freq.orders_8_14d,
    freq.orders_15_30d,
    freq.orders_31_60d,
    freq.orders_61_90d,
    freq.orders_91_180d,
    freq.orders_180plus_d,
    
    -- Activity metrics
    freq.active_weeks,
    freq.pct_weeks_active,
    freq.frequency_bucket,
    freq.regularity_pattern,
    freq.orders_early_period,
    freq.orders_late_period,
    freq.activity_trend,
    
    -- ======================
    -- 6. DERIVED FEATURES
    -- ======================
    -- Engagement score (composite metric)
    CASE 
        WHEN f.has_store_content_visit = 1 AND f.has_checkout_visit = 1 THEN 'High Engagement'
        WHEN f.has_store_content_visit = 1 THEN 'Medium Engagement'
        ELSE 'Low Engagement'
    END AS pre_exposure_engagement_level,
    
    -- Recency score (most recent funnel action)
    LEAST(
        COALESCE(f.days_before_exposure_store_content, 999),
        COALESCE(f.days_before_exposure_store_page, 999),
        COALESCE(f.days_before_exposure_order_cart, 999),
        COALESCE(f.days_before_exposure_checkout, 999),
        COALESCE(f.days_before_exposure_purchase, 999)
    ) AS days_since_last_funnel_action,
    
    -- Order consistency flag
    CASE 
        WHEN freq.total_orders_ytd >= 5 AND freq.stddev_gap_between_orders <= 14 THEN 1
        ELSE 0
    END AS is_consistent_orderer

FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp

-- Join SCD status at exposure (on consumer_id)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records scd
    ON exp.consumer_id = scd.consumer_id
    -- AND exp.exposure_time = scd.exposure_time

-- Join funnel metrics (device-level)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_exposure_previous_funnel f
    ON exp.bucket_key= f.dd_device_id

-- Join merchant affinity features (user-level)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior mb
    ON exp.consumer_id = mb.consumer_id

-- Join order frequency features (user-level)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency freq
    ON exp.consumer_id = freq.consumer_id
)

-- Deduplicate: Keep one row per dd_device_id + user_id combination
SELECT *
FROM combined_features
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id, user_id ORDER BY exposure_time) = 1
);

-- Summary statistics
SELECT 
    'Total Users' AS metric,
    COUNT(DISTINCT user_id) AS count,
    COUNT(DISTINCT dd_device_id) AS unique_devices
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features

UNION ALL

SELECT 
    'Users with scd Data' AS metric,
    COUNT(DISTINCT CASE WHEN lifestage is not null THEN user_id END) AS count,
    COUNT(DISTINCT CASE WHEN lifestage is not null THEN dd_device_id END) AS unique_devices
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features

UNION ALL

SELECT 
    'Users with Funnel Data' AS metric,
    COUNT(DISTINCT CASE WHEN has_store_content_visit = 1 THEN user_id END) AS count,
    COUNT(DISTINCT CASE WHEN has_store_content_visit = 1 THEN dd_device_id END) AS unique_devices
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features

UNION ALL

SELECT 
    'Users with Order History' AS metric,
    COUNT(DISTINCT CASE WHEN frequency_total_orders_ytd > 0 THEN user_id END) AS count,
    COUNT(DISTINCT CASE WHEN frequency_total_orders_ytd > 0 THEN dd_device_id END) AS unique_devices
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features

UNION ALL

SELECT 
    'Users with Merchant Affinity Data' AS metric,
    COUNT(DISTINCT CASE WHEN unique_merchants_ytd > 0 THEN user_id END) AS count,
    COUNT(DISTINCT CASE WHEN unique_merchants_ytd > 0 THEN dd_device_id END) AS unique_devices
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features;

-- Check for duplicates
SELECT 
    dd_device_id,
    user_id,
    COUNT(*) AS row_count
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Feature completeness check
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT dd_device_id) AS unique_devices,
    COUNT(DISTINCT user_id) AS unique_users,
    
    -- SCD features
    COUNT(lifestage) AS has_lifestage,
    COUNT(lifestage) * 100.0 / COUNT(*) AS pct_has_lifestage,
    
    -- Funnel features
    SUM(has_store_content_visit) AS users_with_store_visits,
    SUM(has_store_content_visit) * 100.0 / COUNT(*) AS pct_with_store_visits,
    
    -- Merchant features
    COUNT(unique_merchants_ytd) AS has_merchant_data,
    COUNT(unique_merchants_ytd) * 100.0 / COUNT(*) AS pct_has_merchant_data,
    
    -- Frequency features
    COUNT(frequency_total_orders_ytd) AS has_frequency_data,
    COUNT(frequency_total_orders_ytd) * 100.0 / COUNT(*) AS pct_has_frequency_data
    
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features;


select * from proddb.fionafan.cx_ios_reonboarding_pre_exposure_features where store_page_visitor_count is null limit 10;

select b.is_guest, count(1) from
(select distinct user_id from proddb.fionafan.cx_ios_reonboarding_pre_exposure_features where store_page_visitor_count is null ) a
left join dimension_consumer b on a.user_id::varchar = b.user_id::varchar group by all;

select a.* from
(select distinct user_id from proddb.fionafan.cx_ios_reonboarding_pre_exposure_features where store_page_visitor_count is null ) a
left join dimension_consumer b on a.user_id::varchar = b.user_id::varchar where b.is_guest = 0 limit 10;


select * from proddb.public.fact_unique_visitors_full_pt where user_id = '78740921' limit 10;
select * from proddb.fionafan.cx_ios_reonboarding_experiment_exposures where consumer_id = '78740921' limit 10;


select case when c.user_id is not null then 1 end, count(distinct a.user_id), count(distinct a.dd_device_id)
from
(select distinct user_id, dd_device_id from proddb.fionafan.cx_ios_reonboarding_pre_exposure_features where store_page_visitor_count is null ) a
left join dimension_consumer b on a.user_id::varchar = b.user_id::varchar 
left join (select distinct dd_device_id, user_id from proddb.public.fact_unique_visitors_full_pt ) c on a.user_id::varchar = c.user_id::varchar
where b.is_guest = 0 or b.is_guest is null
group by all;




CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_pre_exposure_features_user_level AS (
WITH combined_features AS (
SELECT 
    -- ======================
    -- 1. IDENTIFIERS & BASIC INFO
    -- ======================
    exp.bucket_key AS dd_device_id,
    REPLACE(LOWER(CASE WHEN exp.bucket_key LIKE 'dx_%' THEN exp.bucket_key ELSE 'dx_'||exp.bucket_key END), '-') as dd_device_id_filtered,
    exp.consumer_id AS user_id,
    exp.exposure_time,
    exp.tag,
    exp.result,
    
    -- ======================
    -- 2. STATUS AT EXPOSURE (from SCD)
    -- ======================
    date(exp.exposure_time) AS exposure_day,
    scd.scd_consumer_id,
    scd.scd_start_date,
    scd.scd_end_date,
    scd.signup_date,
    scd.first_order_date,
    scd.last_order_date,
    scd.lifestage,
    scd.lifestage_bucket,
    scd.experience,
    scd.business_vertical_line,
    scd.country_id,
    scd.region_name,
    scd.submarket_id,
    
    -- Tenure metrics (handle 9999-12-31 placeholder dates)
    CASE 
        WHEN scd.signup_date IS NULL OR scd.signup_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.signup_date, DATE(exp.exposure_time))
    END AS tenure_days_at_exposure,
    
    CASE 
        WHEN scd.first_order_date IS NULL OR scd.first_order_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.first_order_date, DATE(exp.exposure_time))
    END AS days_since_first_order,
    
    CASE 
        WHEN scd.last_order_date IS NULL OR scd.last_order_date = '9999-12-31' THEN NULL
        ELSE DATEDIFF('day', scd.last_order_date, DATE(exp.exposure_time))
    END AS days_since_last_order,
    
    -- ======================
    -- 3. PREVIOUS FUNNEL METRICS (from user-level funnel table)
    -- ======================
    -- Store content page
    f.store_content_visitor_count,
    f.has_store_content_visit,
    f.days_before_exposure_store_content,
    
    -- Store page
    f.store_page_visitor_count,
    f.has_store_page_visit,
    f.days_before_exposure_store_page,
    
    -- Order cart page
    f.order_cart_visitor_count,
    f.has_order_cart_visit,
    f.days_before_exposure_order_cart,
    
    -- Checkout page
    f.checkout_visitor_count,
    f.has_checkout_visit,
    f.days_before_exposure_checkout,
    
    -- Purchase
    f.purchaser_count,
    f.has_purchase,
    f.days_before_exposure_purchase,
    
    -- Additional context
    f.platform,
    f.app_version,
    f.urban_type,
    f.is_dashpass,
    f.first_event_date,
    f.last_event_date,
    f.num_event_days,
    
    -- ======================
    -- 4. MERCHANT AFFINITY METRICS (from pre_churn_3)
    -- ======================
    mb.total_orders_ytd AS merchant_total_orders_ytd,
    mb.unique_merchants_ytd,
    mb.total_spend_ytd AS merchant_total_spend_ytd,
    mb.unique_verticals_ytd,
    mb.num_favorite_merchants,
    mb.top_merchant_id,
    mb.top_merchant_name,
    mb.top_merchant_vertical,
    mb.top_merchant_orders,
    mb.top_merchant_pct_orders,
    mb.max_merchant_order_concentration,
    mb.avg_merchant_order_share,
    mb.merchant_diversity,
    mb.user_ordering_type,
    
    -- ======================
    -- 5. ORDER FREQUENCY PATTERNS (from pre_churn_4)
    -- ======================
    freq.total_orders_ytd AS frequency_total_orders_ytd,
    freq.distinct_order_days,
    freq.unique_stores,
    freq.total_spend_ytd AS frequency_total_spend_ytd,
    freq.ytd_period_days,
    freq.ytd_period_weeks,
    freq.avg_orders_per_week,
    freq.avg_orders_per_month,
    freq.avg_days_between_orders,
    freq.avg_days_between_orders_actual,
    freq.median_days_between_orders,
    freq.min_gap_between_orders,
    freq.max_gap_between_orders,
    freq.stddev_gap_between_orders,
    freq.longest_streak_weeks,
    freq.orders_in_longest_streak,
    freq.number_of_streaks,
    
    -- Orders by time before exposure
    freq.orders_0_7d,
    freq.orders_8_14d,
    freq.orders_15_30d,
    freq.orders_31_60d,
    freq.orders_61_90d,
    freq.orders_91_180d,
    freq.orders_180plus_d,
    
    -- Activity metrics
    freq.active_weeks,
    freq.pct_weeks_active,
    freq.frequency_bucket,
    freq.regularity_pattern,
    freq.orders_early_period,
    freq.orders_late_period,
    freq.activity_trend,
    
    -- ======================
    -- 6. DERIVED FEATURES
    -- ======================
    -- Engagement score (composite metric)
    CASE 
        WHEN f.has_store_content_visit = 1 AND f.has_checkout_visit = 1 THEN 'High Engagement'
        WHEN f.has_store_content_visit = 1 THEN 'Medium Engagement'
        ELSE 'Low Engagement'
    END AS pre_exposure_engagement_level,
    
    -- Recency score (most recent funnel action)
    LEAST(
        COALESCE(f.days_before_exposure_store_content, 999),
        COALESCE(f.days_before_exposure_store_page, 999),
        COALESCE(f.days_before_exposure_order_cart, 999),
        COALESCE(f.days_before_exposure_checkout, 999),
        COALESCE(f.days_before_exposure_purchase, 999)
    ) AS days_since_last_funnel_action,
    
    -- Order consistency flag
    CASE 
        WHEN freq.total_orders_ytd >= 5 AND freq.stddev_gap_between_orders <= 14 THEN 1
        ELSE 0
    END AS is_consistent_orderer

FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp

-- Join SCD status at exposure (on consumer_id)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_exposure_scd_all_records scd
    ON exp.consumer_id = scd.consumer_id

-- Join funnel metrics (USER-LEVEL - join on consumer_id = user_id)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_exposure_previous_funnel_user_level f
    ON exp.consumer_id::varchar = f.user_id::varchar

-- Join merchant affinity features (user-level)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_user_merchant_behavior mb
    ON exp.consumer_id = mb.consumer_id

-- Join order frequency features (user-level)
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_pre_churn_order_frequency freq
    ON exp.consumer_id = freq.consumer_id
)

-- Deduplicate: Keep one row per dd_device_id + user_id combination
SELECT *
FROM combined_features
QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id, user_id ORDER BY exposure_time) = 1
);

-- Summary statistics for user-level features table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT dd_device_id) AS unique_devices,
    COUNT(DISTINCT user_id) AS unique_users,
    

    COUNT(lifestage) * 100.0 / COUNT(*) AS pct_has_lifestage,
    
    -- Funnel features
    SUM(has_store_content_visit) AS users_with_store_visits,
    SUM(has_store_content_visit) * 100.0 / COUNT(*) AS pct_with_store_visits,
    
    -- Merchant features
    COUNT(unique_merchants_ytd) AS has_merchant_data,
    COUNT(unique_merchants_ytd) * 100.0 / COUNT(*) AS pct_has_merchant_data,
    
    -- Frequency features
    COUNT(frequency_total_orders_ytd) AS has_frequency_data,
    COUNT(frequency_total_orders_ytd) * 100.0 / COUNT(*) AS pct_has_frequency_data
    
FROM proddb.fionafan.cx_ios_reonboarding_pre_exposure_features_user_level;
