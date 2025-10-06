-- Power Analysis for Experiment Duration
-- Goal: Determine how long to run experiment to detect 3k MAU effect size
-- Author: Fiona Fan
-- Date: 2025-01-05

-- ============================================================================
-- STEP 1: Calculate Daily Enrollment Rate (Past 30 Days)
-- ============================================================================

WITH enrollment_rate AS (
    select
        event_date::date as enrollment_date,
        count(distinct user_id) as daily_enrollments
    from proddb.public.fact_unique_visitors_full_pt
    where  event_date between DATEADD('day', -30, current_date) AND DATEADD('day', -1, current_date)
    and user_id is not null
    group by all
),

-- ============================================================================
-- STEP 2: Calculate Historical MAU Baseline and Variance
-- ============================================================================

historical_baseline AS (
    SELECT 
        ROUND(AVG(daily_enrollments), 0) AS avg_daily_enrollments,
        ROUND(STDDEV_SAMP(daily_enrollments), 2) AS std_daily_enrollments,
        COUNT(*) AS days_observed
    FROM enrollment_rate
),

-- Get baseline MAU rate from recent cohorts using rolling 28-day window
mau_baseline AS (
    SELECT 
        -- Enrollment data (top of funnel from fact_unique_visitors_full_pt, dedup to first visit per user in cohort window)
        COUNT(DISTINCT e.user_id) AS total_enrolled,
        
        -- MAU calculation: users who placed an order within 28 days of their first visit
        COUNT(DISTINCT CASE 
            WHEN o.delivery_id IS NOT NULL THEN e.user_id
            END) AS mau_count,
        
        -- MAU rate: percentage of enrolled users who became MAU within 28 days
        COUNT(DISTINCT CASE 
            WHEN o.delivery_id IS NOT NULL THEN e.user_id
            END) * 1.0 / NULLIF(COUNT(DISTINCT e.user_id), 0) AS baseline_mau_rate
        
    FROM (
        SELECT 
            user_id,
            MIN(event_date)::date AS enrollment_date
        FROM proddb.public.fact_unique_visitors_full_pt
        WHERE event_date BETWEEN DATEADD('day', -56, current_date) AND DATEADD('day', -28, current_date)
          AND user_id IS NOT NULL
        GROUP BY user_id
    ) e
    LEFT JOIN (
        -- Orders placed within 28 days of enrollment
        SELECT 
            creator_id consumer_id,
            delivery_id,
            convert_timezone('UTC','America/Los_Angeles', created_at)::date AS order_date
        FROM dimension_deliveries
        WHERE is_filtered_core = 1
          -- Look at orders from the enrollment period through to recent dates
          AND convert_timezone('UTC','America/Los_Angeles', created_at) 
              BETWEEN DATEADD('day', -84, current_date) AND DATEADD('day', -1, current_date)
    ) o 
      ON o.consumer_id = e.user_id 
     -- Key constraint: order must be within 28 days of enrollment
     AND o.order_date BETWEEN e.enrollment_date AND DATEADD('day', 28, e.enrollment_date)
),

-- ============================================================================
-- STEP 3: Power Analysis Calculations
-- ============================================================================

power_calculation AS (
    SELECT 
        h.avg_daily_enrollments,
        h.std_daily_enrollments,
        h.days_observed,
        
        m.total_enrolled,
        m.mau_count,
        ROUND(m.baseline_mau_rate, 4) AS baseline_mau_rate,
        
        -- Power analysis parameters
        19000 AS target_effect_size_absolute,  -- 250 MAU increase
        ROUND(19000 / NULLIF(m.mau_count, 0), 4) AS target_effect_size_relative,
        
        -- Estimated variance for MAU (binomial approximation)
        ROUND(m.baseline_mau_rate * (1 - m.baseline_mau_rate), 6) AS estimated_mau_variance,
        
        -- Standard alpha and beta
        0.05 AS alpha,  -- 5% significance level
        0.80 AS power,  -- 80% power
        1.96 AS z_alpha,  -- Critical value for alpha=0.05 (two-tailed)
        0.84 AS z_beta   -- Critical value for beta=0.20 (power=0.80)
        
    FROM historical_baseline h
    CROSS JOIN mau_baseline m
),

-- ============================================================================
-- STEP 4: Sample Size and Duration Calculations
-- ============================================================================

sample_size_calculation AS (
    SELECT 
        *,
        
        -- Required sample size per arm (for binomial/proportion test)
        -- n = 2 * (z_alpha + z_beta)^2 * p(1-p) / (effect_size)^2
        -- where p is baseline rate and effect_size is relative change
        CASE 
            WHEN target_effect_size_relative > 0 AND estimated_mau_variance > 0 THEN
                CEIL(2 * POWER(z_alpha + z_beta, 2) * estimated_mau_variance / POWER(target_effect_size_relative, 2))
            ELSE NULL
        END AS required_sample_size_per_arm,
        
        -- Alternative calculation using absolute effect size
        -- For comparing two proportions: n = 2 * (z_alpha + z_beta)^2 * p(1-p) / (p1-p2)^2
        CASE 
            WHEN target_effect_size_absolute > 0 AND total_enrolled > 0 THEN
                CEIL(2 * POWER(z_alpha + z_beta, 2) * estimated_mau_variance / 
                     POWER(target_effect_size_absolute * 1.0 / total_enrolled, 2))
            ELSE NULL
        END AS required_sample_size_per_arm_absolute,
        
        -- Total sample size (both arms)
        CASE 
            WHEN target_effect_size_relative > 0 AND estimated_mau_variance > 0 THEN
                CEIL(4 * POWER(z_alpha + z_beta, 2) * estimated_mau_variance / POWER(target_effect_size_relative, 2))
            ELSE NULL
        END AS total_required_sample_size
        
    FROM power_calculation
),

-- ============================================================================
-- STEP 5: Duration Recommendations
-- ============================================================================

duration_recommendations AS (
    SELECT 
        *,
        
        -- Days to reach required sample size
        CASE 
            WHEN avg_daily_enrollments > 0 AND required_sample_size_per_arm > 0 THEN
                CEIL(required_sample_size_per_arm * 1.0 / avg_daily_enrollments)
            ELSE NULL
        END AS days_to_reach_sample_size,
        
        -- Add buffer for MAU measurement (28 days after enrollment ends)
        CASE 
            WHEN avg_daily_enrollments > 0 AND required_sample_size_per_arm > 0 THEN
                CEIL(required_sample_size_per_arm * 1.0 / avg_daily_enrollments) + 28
            ELSE NULL
        END AS total_experiment_duration_days,
        
        -- Alternative scenarios with different daily enrollment rates
        CASE 
            WHEN required_sample_size_per_arm > 0 THEN
                CEIL(required_sample_size_per_arm * 1.0 / (avg_daily_enrollments * 1.5))
            ELSE NULL
        END AS days_if_50pct_higher_enrollment,
        
        CASE 
            WHEN required_sample_size_per_arm > 0 THEN
                CEIL(required_sample_size_per_arm * 1.0 / (avg_daily_enrollments * 0.8))
            ELSE NULL
        END AS days_if_20pct_lower_enrollment
        
    FROM sample_size_calculation
)

-- ============================================================================
-- FINAL RESULTS
-- ============================================================================

SELECT 
    '=== ENROLLMENT METRICS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit
    
UNION ALL

SELECT 
    NULL AS section,
    'Average Daily Enrollments (Past 30 Days)' AS metric,
    avg_daily_enrollments AS value,
    'users/day' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Std Dev Daily Enrollments' AS metric,
    std_daily_enrollments AS value,
    'users/day' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    '=== BASELINE MAU METRICS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit

UNION ALL

SELECT 
    NULL AS section,
    'Historical MAU Count (28-56 days ago cohort)' AS metric,
    mau_count AS value,
    'MAU' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Baseline MAU Rate' AS metric,
    ROUND(baseline_mau_rate * 100, 2) AS value,
    '%' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    '=== POWER ANALYSIS PARAMETERS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit

UNION ALL

SELECT 
    NULL AS section,
    'Target Effect Size (Absolute)' AS metric,
    target_effect_size_absolute AS value,
    'MAU increase' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Target Effect Size (Relative)' AS metric,
    ROUND(target_effect_size_relative * 100, 2) AS value,
    '% increase' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Significance Level (Alpha)' AS metric,
    alpha * 100 AS value,
    '%' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Statistical Power' AS metric,
    power * 100 AS value,
    '%' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    '=== SAMPLE SIZE REQUIREMENTS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit

UNION ALL

SELECT 
    NULL AS section,
    'Required Sample Size Per Arm' AS metric,
    required_sample_size_per_arm AS value,
    'users' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Total Required Sample Size' AS metric,
    total_required_sample_size AS value,
    'users' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    '=== DURATION RECOMMENDATIONS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit

UNION ALL

SELECT 
    NULL AS section,
    'Days to Reach Sample Size (Enrollment)' AS metric,
    days_to_reach_sample_size AS value,
    'days' AS unit
FROM duration_recommendations

UNION ALL
SELECT 
    NULL AS section,
    'Total Experiment Duration (w/ MAU measurement)' AS metric,
    total_experiment_duration_days AS value,
    'days' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Estimated End Date' AS metric,
    NULL AS value,
    DATEADD('day', total_experiment_duration_days, current_date)::varchar AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    '=== SCENARIO ANALYSIS ===' AS section,
    NULL AS metric,
    NULL AS value,
    NULL AS unit

UNION ALL

SELECT 
    NULL AS section,
    'Days if 50% Higher Enrollment' AS metric,
    days_if_50pct_higher_enrollment + 28 AS value,
    'days total' AS unit
FROM duration_recommendations

UNION ALL

SELECT 
    NULL AS section,
    'Days if 20% Lower Enrollment' AS metric,
    days_if_20pct_lower_enrollment + 28 AS value,
    'days total' AS unit
FROM duration_recommendations

ORDER BY 
    CASE 
        WHEN section IS NOT NULL THEN 1 
        ELSE 2 
    END,
    metric;
