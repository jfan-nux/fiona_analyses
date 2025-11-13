-- Second Session Timing Distribution Analysis
-- Purpose: Analyze when second sessions occur relative to exposure date (onboarding)
-- Shows distribution across weeks 1-4 and beyond

-- ============================================================================
-- Distribution by Week
-- ============================================================================

WITH second_session_timing AS (
    SELECT 
        consumer_id,
        cohort_type,
        exposure_time,
        second_session_ts,
        DATEDIFF(day, exposure_time, second_session_ts) as days_to_second_session,
        CASE 
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 0 AND 6 THEN 'Week 1 (Days 0-6)'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 7 AND 13 THEN 'Week 2 (Days 7-13)'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 14 AND 20 THEN 'Week 3 (Days 14-20)'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 21 AND 27 THEN 'Week 4 (Days 21-27)'
            ELSE 'Beyond Week 4 (28+ days)'
        END as week_bucket,
        CASE 
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 0 AND 6 THEN 1
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 7 AND 13 THEN 2
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 14 AND 20 THEN 3
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 21 AND 27 THEN 4
            ELSE 5
        END as week_number
    FROM proddb.fionafan.all_user_sessions_enriched
    WHERE second_session_id IS NOT NULL
        AND second_session_ts IS NOT NULL
        AND exposure_time IS NOT NULL
)
SELECT 
    week_bucket,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    MIN(days_to_second_session) as min_days,
    MAX(days_to_second_session) as max_days,
    ROUND(AVG(days_to_second_session), 1) as avg_days,
    ROUND(MEDIAN(days_to_second_session), 1) as median_days
FROM second_session_timing
GROUP BY week_bucket, week_number
ORDER BY week_number;


-- ============================================================================
-- Distribution by Cohort Type
-- ============================================================================

WITH second_session_timing AS (
    SELECT 
        consumer_id,
        cohort_type,
        exposure_time,
        second_session_ts,
        DATEDIFF(day, exposure_time, second_session_ts) as days_to_second_session,
        CASE 
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 0 AND 6 THEN 'Week 1'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 7 AND 13 THEN 'Week 2'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 14 AND 20 THEN 'Week 3'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 21 AND 27 THEN 'Week 4'
            ELSE 'Beyond Week 4'
        END as week_bucket
    FROM proddb.fionafan.all_user_sessions_enriched
    WHERE second_session_id IS NOT NULL
        AND second_session_ts IS NOT NULL
        AND exposure_time IS NOT NULL
)
SELECT 
    cohort_type,
    week_bucket,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_within_cohort,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_overall
FROM second_session_timing
GROUP BY cohort_type, week_bucket
ORDER BY cohort_type, 
    CASE week_bucket
        WHEN 'Week 1' THEN 1
        WHEN 'Week 2' THEN 2
        WHEN 'Week 3' THEN 3
        WHEN 'Week 4' THEN 4
        ELSE 5
    END;


-- ============================================================================
-- Summary: Sessions in Weeks 1-2 vs Weeks 3-4
-- ============================================================================

WITH second_session_timing AS (
    SELECT 
        consumer_id,
        DATEDIFF(day, exposure_time, second_session_ts) as days_to_second_session,
        CASE 
            WHEN DATEDIFF(day, exposure_time, second_session_ts) < 14 THEN 'Weeks 1-2 (Keep)'
            WHEN DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 14 AND 27 THEN 'Weeks 3-4 (Filter Out)'
            ELSE 'Beyond Week 4'
        END as grouping
    FROM proddb.fionafan.all_user_sessions_enriched
    WHERE second_session_id IS NOT NULL
        AND second_session_ts IS NOT NULL
        AND exposure_time IS NOT NULL
)
SELECT 
    grouping,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM second_session_timing
GROUP BY grouping
ORDER BY 
    CASE grouping
        WHEN 'Weeks 1-2 (Keep)' THEN 1
        WHEN 'Weeks 3-4 (Filter Out)' THEN 2
        ELSE 3
    END;


-- ============================================================================
-- Detailed Distribution by Day
-- ============================================================================

WITH second_session_timing AS (
    SELECT 
        DATEDIFF(day, exposure_time, second_session_ts) as days_to_second_session
    FROM proddb.fionafan.all_user_sessions_enriched
    WHERE second_session_id IS NOT NULL
        AND second_session_ts IS NOT NULL
        AND exposure_time IS NOT NULL
        AND DATEDIFF(day, exposure_time, second_session_ts) BETWEEN 0 AND 27
)
SELECT 
    days_to_second_session as day,
    COUNT(*) as session_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    SUM(COUNT(*)) OVER (ORDER BY days_to_second_session ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_count,
    ROUND(SUM(COUNT(*) * 100.0) OVER (ORDER BY days_to_second_session ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COUNT(*)) OVER (), 2) as cumulative_percentage
FROM second_session_timing
GROUP BY days_to_second_session
ORDER BY days_to_second_session;

