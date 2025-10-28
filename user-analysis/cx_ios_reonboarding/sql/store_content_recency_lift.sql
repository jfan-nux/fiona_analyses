WITH threshold AS (
    SELECT 120 AS days_threshold  -- <<< CHANGE THIS VALUE TO TUNE
),
base AS (
    SELECT 
        CASE 
            WHEN days_before_exposure_store_content > t.days_threshold 
                THEN 'More than ' || t.days_threshold || ' days'
            ELSE t.days_threshold || ' days or less'
        END AS store_content_recency,
        tag,
        COUNT(DISTINCT user_id) AS user_count,
        AVG(has_order_post_exposure) AS order_rate
    FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
    CROSS JOIN threshold t
    WHERE days_before_exposure_store_content > 90
    GROUP BY 
        CASE 
            WHEN days_before_exposure_store_content > t.days_threshold 
                THEN 'More than ' || t.days_threshold || ' days'
            ELSE t.days_threshold || ' days or less'
        END,
        tag
),
pivot_data AS (
    SELECT
        store_content_recency,
        MAX(CASE WHEN tag = 'control' THEN user_count END) AS control_count,
        MAX(CASE WHEN tag = 'control' THEN order_rate END) AS control_rate,
        MAX(CASE WHEN tag = 'treatment' THEN user_count END) AS treatment_count,
        MAX(CASE WHEN tag = 'treatment' THEN order_rate END) AS treatment_rate
    FROM base
    GROUP BY store_content_recency
)
SELECT
    store_content_recency,
    control_count,
    ROUND(control_rate * 100, 2) AS control_pct,
    treatment_count,
    ROUND(treatment_rate * 100, 2) AS treatment_pct,
    ROUND((treatment_rate - control_rate) * 100, 2) AS absolute_lift_pp
FROM pivot_data
ORDER BY store_content_recency DESC;

