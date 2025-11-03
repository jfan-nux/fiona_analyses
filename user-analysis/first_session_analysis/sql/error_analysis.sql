-- Error Event Analysis
-- Question 1: How many sessions have errors by cohort_type and session_type
-- Question 2: For error events, what are the discovery surfaces/features involved

-- ============================================================================
-- QUESTION 1: Error Rate by Cohort and Session Type
-- ============================================================================

-- Session-level error analysis
SELECT 
  cohort_type,
  session_type,
  COUNT(*) as total_sessions,
  
  -- Error counts
  SUM(error_had_any) as sessions_with_error,
  SUM(error_event_count) as total_error_events,
  
  -- Percentages
  ROUND(100.0 * SUM(error_had_any) / COUNT(*), 2) as pct_sessions_with_error,
  
  -- Average errors per session (all sessions)
  ROUND(AVG(error_event_count), 2) as avg_errors_per_session,
  
  -- Average errors per session (only sessions with errors)
  ROUND(AVG(CASE WHEN error_had_any = 1 THEN error_event_count END), 2) as avg_errors_per_error_session,
  
  -- Median errors per session (only sessions with errors)
  MEDIAN(CASE WHEN error_had_any = 1 THEN error_event_count END) as median_errors_per_error_session

FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- ============================================================================
-- QUESTION 2: Error Event Details - Discovery Surface and Feature
-- ============================================================================

-- Get event-level details for all error events
WITH error_events AS (
  SELECT 
    cohort_type,
    session_type,
    user_id,
    dd_device_id,
    event_date,
    session_num,
    timestamp,
    event,
    event_type,
    discovery_surface,
    discovery_feature,
    detail,
    store_id,
    store_name
  FROM proddb.fionafan.all_user_sessions_with_events
  WHERE event_type = 'error'
)
, result as (
SELECT 
  cohort_type,
  session_type,
  discovery_surface,
  discovery_feature,
  COUNT(*) as error_event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_errors_in_group,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_all_errors,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as sessions_with_this_error,
  COUNT(DISTINCT user_id) as users_with_this_error

FROM error_events
GROUP BY cohort_type, session_type, discovery_surface, discovery_feature
ORDER BY cohort_type, session_type, error_event_count DESC)
select * from result where pct_of_errors_in_group >= 1;



-- ============================================================================
-- Additional: Error Event Details by Event Name
-- ============================================================================

WITH error_events AS (
  SELECT 
    cohort_type,
    session_type,
    user_id,
    dd_device_id,
    event_date,
    session_num,
    event,
    discovery_surface,
    discovery_feature,
    detail
  FROM proddb.fionafan.all_user_sessions_with_events
  WHERE event_type = 'error'
)

SELECT 
  cohort_type,
  session_type,
  event,
  COUNT(*) as error_event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_errors_in_group,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as sessions_with_this_error,
  COUNT(DISTINCT user_id) as users_with_this_error

FROM error_events
GROUP BY cohort_type, session_type, event
ORDER BY cohort_type, session_type, error_event_count DESC
LIMIT 50;


-- ============================================================================
-- Additional: Error Detail Field Analysis
-- ============================================================================

WITH error_events AS (
  SELECT 
    cohort_type,
    session_type,
    user_id,
    dd_device_id,
    event_date,
    session_num,
    event,
    detail
  FROM proddb.fionafan.all_user_sessions_with_events
  WHERE event_type = 'error'
)

SELECT 
  cohort_type,
  session_type,
  detail,
  COUNT(*) as error_event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_errors_in_group,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as sessions_with_this_error

FROM error_events
WHERE detail IS NOT NULL
GROUP BY cohort_type, session_type, detail
ORDER BY cohort_type, session_type, error_event_count DESC
LIMIT 50;


-- ============================================================================
-- Additional: Combined Error Context (Event + Surface + Feature)
-- ============================================================================

WITH error_events AS (
  SELECT 
    cohort_type,
    session_type,
    user_id,
    dd_device_id,
    event_date,
    session_num,
    event,
    discovery_surface,
    discovery_feature,
    detail
  FROM proddb.fionafan.all_user_sessions_with_events
  WHERE event_type = 'error'
)

SELECT 
  cohort_type,
  session_type,
  event,
  discovery_surface,
  discovery_feature,
  COUNT(*) as error_event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_errors_in_group,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as sessions_with_this_error,
  COUNT(DISTINCT user_id) as users_with_this_error

FROM error_events
GROUP BY cohort_type, session_type, event, discovery_surface, discovery_feature
ORDER BY cohort_type, session_type, error_event_count DESC
LIMIT 100;


-- ============================================================================
-- Additional: Error Rate by Funnel Progression Stage
-- ============================================================================

-- Analyze if errors correlate with funnel stage
SELECT 
  cohort_type,
  session_type,
  
  -- Funnel stage classification
  CASE 
    WHEN funnel_converted_bool = 1 THEN '5. Converted'
    WHEN funnel_reached_checkout_bool = 1 THEN '4. Reached Checkout'
    WHEN funnel_reached_cart_bool = 1 THEN '3. Reached Cart'
    WHEN funnel_reached_store_bool = 1 THEN '2. Reached Store Page'
    WHEN impression_had_any = 1 THEN '1. Had Impressions Only'
    ELSE '0. Other'
  END as funnel_stage,
  
  COUNT(*) as total_sessions,
  SUM(error_had_any) as sessions_with_error,
  ROUND(100.0 * SUM(error_had_any) / COUNT(*), 2) as pct_sessions_with_error,
  ROUND(AVG(error_event_count), 2) as avg_errors_per_session

FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type, session_type, funnel_stage
ORDER BY cohort_type, session_type, funnel_stage;

