-- Evaluate NV Propensity Model Performance by User Segment
-- Model Table: proddb.ml.fact_cx_cross_vertical_propensity_scores_v1
-- Score: CAF_CS_PROPENSITY_SCORE_28D_GROCERY
-- Active Date: 2025-10-01
-- Evaluation Period: 30 days after active_date (2025-10-02 to 2025-10-31)

-- Step 1: Create user-level table with propensity scores and user segments
CREATE OR REPLACE TABLE proddb.fionafan.nv_propensity_model_evaluation AS (

WITH propensity_scores AS (
    -- Get propensity scores for active_date = 2025-10-01
    SELECT 
        CONSUMER_ID,
        ACTIVE_DATE,
        CAF_CS_PROPENSITY_SCORE_28D_GROCERY
    FROM proddb.ml.fact_cx_cross_vertical_propensity_scores_v1
    WHERE ACTIVE_DATE = '2025-10-01'
        AND CAF_CS_PROPENSITY_SCORE_28D_GROCERY IS NOT NULL
),

user_segments AS (
    -- Get user segments from growth accounting table
    -- Filter for Active, Non-Purchaser, and New Cx users where active_date falls within SCD period
    SELECT 
        ps.CONSUMER_ID,
        ps.ACTIVE_DATE,
        ps.CAF_CS_PROPENSITY_SCORE_28D_GROCERY,
        scd.LIFESTAGE,
        scd.LIFESTAGE_BUCKET,
        scd.SCD_START_DATE,
        scd.SCD_END_DATE,
        scd.FIRST_ORDER_DATE,
        scd.LAST_ORDER_DATE,
        -- Categorize user segment
        CASE 
            WHEN scd.LIFESTAGE ILIKE '%Active%' THEN 'Active'
            WHEN scd.LIFESTAGE ILIKE '%Non-Purchaser%' THEN 'Non-Purchaser'
            WHEN scd.LIFESTAGE ILIKE '%New Cx%' THEN 'New Cx'
            ELSE 'Other'
        END AS user_segment
    FROM propensity_scores ps
    LEFT JOIN edw.growth.consumer_growth_accounting_scd3 scd
        ON ps.CONSUMER_ID = scd.CONSUMER_ID
        AND ps.ACTIVE_DATE BETWEEN scd.SCD_START_DATE AND scd.SCD_END_DATE
    WHERE (scd.LIFESTAGE ILIKE '%Active%' 
        OR scd.LIFESTAGE ILIKE '%Non-Purchaser%' 
        OR scd.LIFESTAGE ILIKE '%New Cx%')
),

nv_orders AS (
    -- Check if users made NV orders within 30 days after active_date
    SELECT DISTINCT 
        us.CONSUMER_ID,
        CASE WHEN dd.creator_id IS NOT NULL THEN TRUE ELSE FALSE END AS made_nv_order,
        MIN(dd.active_date) AS first_nv_order_date
    FROM user_segments us
    LEFT JOIN (
        SELECT DISTINCT 
            creator_id,
            active_date,
            nv_vertical_name
        FROM proddb.public.dimension_deliveries
        WHERE is_filtered_core = 1
            AND active_date BETWEEN '2025-10-02' AND '2025-10-31'  -- 30 days after 2025-10-01
            AND nv_vertical_name IS NOT NULL  -- Only NV orders
    ) dd
        ON us.CONSUMER_ID = dd.creator_id
    GROUP BY ALL
),

score_deciles AS (
    -- Calculate decile buckets for the propensity scores
    SELECT 
        us.*,
        nv.made_nv_order,
        nv.first_nv_order_date,
        NTILE(10) OVER (ORDER BY us.CAF_CS_PROPENSITY_SCORE_28D_GROCERY ASC) AS score_decile
    FROM user_segments us
    LEFT JOIN nv_orders nv
        ON us.CONSUMER_ID = nv.CONSUMER_ID
)

SELECT * FROM score_deciles
);

-- Validation: Check table size and distribution
SELECT 
    'Total Records' AS metric,
    COUNT(*) AS count,
    COUNT(DISTINCT CONSUMER_ID) AS unique_consumers
FROM proddb.fionafan.nv_propensity_model_evaluation;

SELECT 
    'User Segment Distribution' AS metric,
    user_segment,
    COUNT(*) AS user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM proddb.fionafan.nv_propensity_model_evaluation
GROUP BY ALL
ORDER BY user_count DESC;


-- Step 2: Analyze model performance by decile and user segment

-- Overall performance by decile
SELECT 
    score_decile,
    COUNT(DISTINCT CONSUMER_ID) AS total_users,
    SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) AS nv_converters,
    ROUND(SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT CONSUMER_ID), 2) AS conversion_rate,
    MIN(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS min_score,
    MAX(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS max_score,
    AVG(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS avg_score,
    MEDIAN(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS median_score
FROM proddb.fionafan.nv_propensity_model_evaluation
GROUP BY ALL
ORDER BY score_decile;


-- Performance by user segment
SELECT 
    user_segment,
    COUNT(DISTINCT CONSUMER_ID) AS total_users,
    SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) AS nv_converters,
    ROUND(SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT CONSUMER_ID), 2) AS conversion_rate,
    AVG(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS avg_score,
    MEDIAN(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS median_score
FROM proddb.fionafan.nv_propensity_model_evaluation
GROUP BY ALL
ORDER BY conversion_rate DESC;


-- Performance by decile AND user segment (detailed breakdown)
SELECT 
    score_decile,
    user_segment,
    COUNT(DISTINCT CONSUMER_ID) AS total_users,
    SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) AS nv_converters,
    ROUND(SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT CONSUMER_ID), 2) AS conversion_rate,
    AVG(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS avg_score,
    MIN(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS min_score,
    MAX(CAF_CS_PROPENSITY_SCORE_28D_GROCERY) AS max_score
FROM proddb.fionafan.nv_propensity_model_evaluation
GROUP BY ALL
ORDER BY score_decile, user_segment;


-- Pivot view: Conversion rate by decile across user segments
SELECT 
    score_decile,
    COUNT(DISTINCT CONSUMER_ID) AS total_users,
    -- Active users
    SUM(CASE WHEN user_segment = 'Active' THEN 1 ELSE 0 END) AS active_users,
    ROUND(SUM(CASE WHEN user_segment = 'Active' AND made_nv_order THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN user_segment = 'Active' THEN 1 ELSE 0 END), 0), 2) AS active_conversion_rate,
    -- Non-Purchaser users
    SUM(CASE WHEN user_segment = 'Non-Purchaser' THEN 1 ELSE 0 END) AS non_purchaser_users,
    ROUND(SUM(CASE WHEN user_segment = 'Non-Purchaser' AND made_nv_order THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN user_segment = 'Non-Purchaser' THEN 1 ELSE 0 END), 0), 2) AS non_purchaser_conversion_rate,
    -- New Cx users
    SUM(CASE WHEN user_segment = 'New Cx' THEN 1 ELSE 0 END) AS new_cx_users,
    ROUND(SUM(CASE WHEN user_segment = 'New Cx' AND made_nv_order THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN user_segment = 'New Cx' THEN 1 ELSE 0 END), 0), 2) AS new_cx_conversion_rate,
    -- Overall
    ROUND(SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT CONSUMER_ID), 2) AS overall_conversion_rate
FROM proddb.fionafan.nv_propensity_model_evaluation
GROUP BY ALL
ORDER BY score_decile;


-- Model lift analysis: Compare top deciles vs bottom deciles
WITH decile_groups AS (
    SELECT 
        user_segment,
        CASE 
            WHEN score_decile <= 3 THEN 'Bottom 30% (Deciles 1-3)'
            WHEN score_decile >= 8 THEN 'Top 30% (Deciles 8-10)'
            ELSE 'Middle 40% (Deciles 4-7)'
        END AS score_group,
        COUNT(DISTINCT CONSUMER_ID) AS total_users,
        SUM(CASE WHEN made_nv_order THEN 1 ELSE 0 END) AS nv_converters
    FROM proddb.fionafan.nv_propensity_model_evaluation
    GROUP BY ALL
)

SELECT 
    user_segment,
    score_group,
    total_users,
    nv_converters,
    ROUND(nv_converters * 100.0 / total_users, 2) AS conversion_rate,
    ROUND(nv_converters * 100.0 / SUM(nv_converters) OVER (PARTITION BY user_segment), 2) AS pct_of_conversions
FROM decile_groups
ORDER BY user_segment, score_group;

select * from proddb.fionafan.all_user_notifications_base 
where campaign_name = 'reserved_for_notification_v2_do_not_use' 