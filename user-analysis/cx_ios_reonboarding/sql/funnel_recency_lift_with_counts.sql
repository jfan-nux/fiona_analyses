-- Order rate lift by funnel recency with counts for statistical testing
WITH bucketed_data AS (
    SELECT 
        user_id,
        tag,
        has_order_post_exposure,
        -- Bucket all funnel stages with same bins
        CASE 
            WHEN days_before_exposure_store_content IS NULL THEN 'No visit'
            WHEN days_before_exposure_store_content <= 90 THEN '0-90 days'
            WHEN days_before_exposure_store_content <= 120 THEN '90-120 days'
            WHEN days_before_exposure_store_content <= 150 THEN '120-150 days'
            WHEN days_before_exposure_store_content <= 180 THEN '150-180 days'
            WHEN days_before_exposure_store_content <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS store_content_bucket,
        CASE 
            WHEN days_before_exposure_store_page IS NULL THEN 'No visit'
            WHEN days_before_exposure_store_page <= 90 THEN '0-90 days'
            WHEN days_before_exposure_store_page <= 120 THEN '90-120 days'
            WHEN days_before_exposure_store_page <= 150 THEN '120-150 days'
            WHEN days_before_exposure_store_page <= 180 THEN '150-180 days'
            WHEN days_before_exposure_store_page <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS store_page_bucket,
        CASE 
            WHEN days_before_exposure_order_cart IS NULL THEN 'No visit'
            WHEN days_before_exposure_order_cart <= 90 THEN '0-90 days'
            WHEN days_before_exposure_order_cart <= 120 THEN '90-120 days'
            WHEN days_before_exposure_order_cart <= 150 THEN '120-150 days'
            WHEN days_before_exposure_order_cart <= 180 THEN '150-180 days'
            WHEN days_before_exposure_order_cart <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS order_cart_bucket,
        CASE 
            WHEN days_before_exposure_checkout IS NULL THEN 'No visit'
            WHEN days_before_exposure_checkout <= 90 THEN '0-90 days'
            WHEN days_before_exposure_checkout <= 120 THEN '90-120 days'
            WHEN days_before_exposure_checkout <= 150 THEN '120-150 days'
            WHEN days_before_exposure_checkout <= 180 THEN '150-180 days'
            WHEN days_before_exposure_checkout <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS checkout_bucket,
        CASE 
            WHEN days_before_exposure_purchase IS NULL THEN 'No visit'
            WHEN days_before_exposure_purchase <= 90 THEN '0-90 days'
            WHEN days_before_exposure_purchase <= 120 THEN '90-120 days'
            WHEN days_before_exposure_purchase <= 150 THEN '120-150 days'
            WHEN days_before_exposure_purchase <= 180 THEN '150-180 days'
            WHEN days_before_exposure_purchase <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS purchase_bucket
    FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
),

-- Calculate stats for store_content
store_content_stats AS (
    SELECT 
        store_content_bucket AS recency_bucket,
        COUNT(DISTINCT CASE WHEN tag = 'control' THEN user_id END) AS sc_control_count,
        AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END) AS sc_control_rate,
        COUNT(DISTINCT CASE WHEN tag = 'treatment' THEN user_id END) AS sc_treatment_count,
        AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) AS sc_treatment_rate
    FROM bucketed_data
    GROUP BY store_content_bucket
),

-- Calculate stats for store_page
store_page_stats AS (
    SELECT 
        store_page_bucket AS recency_bucket,
        COUNT(DISTINCT CASE WHEN tag = 'control' THEN user_id END) AS sp_control_count,
        AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END) AS sp_control_rate,
        COUNT(DISTINCT CASE WHEN tag = 'treatment' THEN user_id END) AS sp_treatment_count,
        AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) AS sp_treatment_rate
    FROM bucketed_data
    GROUP BY store_page_bucket
),

-- Calculate stats for order_cart
order_cart_stats AS (
    SELECT 
        order_cart_bucket AS recency_bucket,
        COUNT(DISTINCT CASE WHEN tag = 'control' THEN user_id END) AS oc_control_count,
        AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END) AS oc_control_rate,
        COUNT(DISTINCT CASE WHEN tag = 'treatment' THEN user_id END) AS oc_treatment_count,
        AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) AS oc_treatment_rate
    FROM bucketed_data
    GROUP BY order_cart_bucket
),

-- Calculate stats for checkout
checkout_stats AS (
    SELECT 
        checkout_bucket AS recency_bucket,
        COUNT(DISTINCT CASE WHEN tag = 'control' THEN user_id END) AS ch_control_count,
        AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END) AS ch_control_rate,
        COUNT(DISTINCT CASE WHEN tag = 'treatment' THEN user_id END) AS ch_treatment_count,
        AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) AS ch_treatment_rate
    FROM bucketed_data
    GROUP BY checkout_bucket
),

-- Calculate stats for purchase
purchase_stats AS (
    SELECT 
        purchase_bucket AS recency_bucket,
        COUNT(DISTINCT CASE WHEN tag = 'control' THEN user_id END) AS pu_control_count,
        AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END) AS pu_control_rate,
        COUNT(DISTINCT CASE WHEN tag = 'treatment' THEN user_id END) AS pu_treatment_count,
        AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) AS pu_treatment_rate
    FROM bucketed_data
    GROUP BY purchase_bucket
)

-- Final output: Join all stats together
SELECT
    COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket, 
             ch.recency_bucket, pu.recency_bucket) AS recency_bucket,
    -- Store Content
    sc.sc_control_count,
    sc.sc_control_rate,
    sc.sc_treatment_count,
    sc.sc_treatment_rate,
    -- Store Page
    sp.sp_control_count,
    sp.sp_control_rate,
    sp.sp_treatment_count,
    sp.sp_treatment_rate,
    -- Order Cart
    oc.oc_control_count,
    oc.oc_control_rate,
    oc.oc_treatment_count,
    oc.oc_treatment_rate,
    -- Checkout
    ch.ch_control_count,
    ch.ch_control_rate,
    ch.ch_treatment_count,
    ch.ch_treatment_rate,
    -- Purchase
    pu.pu_control_count,
    pu.pu_control_rate,
    pu.pu_treatment_count,
    pu.pu_treatment_rate
FROM store_content_stats sc
FULL OUTER JOIN store_page_stats sp ON sc.recency_bucket = sp.recency_bucket
FULL OUTER JOIN order_cart_stats oc ON COALESCE(sc.recency_bucket, sp.recency_bucket) = oc.recency_bucket
FULL OUTER JOIN checkout_stats ch ON COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket) = ch.recency_bucket
FULL OUTER JOIN purchase_stats pu ON COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket, ch.recency_bucket) = pu.recency_bucket
ORDER BY 
    CASE 
        WHEN recency_bucket = '0-90 days' THEN 1
        WHEN recency_bucket = '90-120 days' THEN 2
        WHEN recency_bucket = '120-150 days' THEN 3
        WHEN recency_bucket = '150-180 days' THEN 4
        WHEN recency_bucket = '180-365 days' THEN 5
        WHEN recency_bucket = '365+ days' THEN 6
        WHEN recency_bucket = 'No visit' THEN 7
    END;

