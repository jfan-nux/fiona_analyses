-- Analysis of sessions with store page visit (store_page_load) and add to cart
-- Question 1: Compare session characteristics for sessions with vs without add to cart
-- Question 2: Analyze events between store page load and add to cart

-- ============================================================================
-- QUESTION 1: Session Length and Store Impression Comparison
-- ============================================================================

-- Compare sessions that have store page visit + add to cart vs store page visit only
WITH session_classification AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    
    -- Session metrics
    session_duration_seconds,
    total_events,
    
    -- Impression metrics
    impression_event_count,
    impression_unique_stores,
    
    -- Funnel metrics
    funnel_reached_store_bool,
    funnel_num_store_page,
    funnel_had_add,
    funnel_num_adds,
    
    -- Classify sessions
    CASE 
      WHEN funnel_reached_store_bool = 1 AND funnel_had_add = 1 THEN 'Has Store Page + Add to Cart'
      WHEN funnel_reached_store_bool = 1 AND funnel_had_add = 0 THEN 'Has Store Page Only'
      ELSE 'Other'
    END AS session_category
    
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1  -- Must have at least reached store page
)

SELECT 
  cohort_type,
  session_category,
  COUNT(*) as total_sessions,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort,
  
  -- Session duration
  ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_sec,
  ROUND(MEDIAN(session_duration_seconds), 2) as median_session_duration_sec,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY session_duration_seconds) as p25_session_duration_sec,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY session_duration_seconds) as p75_session_duration_sec,
  
  -- Store impressions
  ROUND(AVG(impression_unique_stores), 2) as avg_unique_stores_impressed,
  ROUND(MEDIAN(impression_unique_stores), 2) as median_unique_stores_impressed,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY impression_unique_stores) as p25_unique_stores,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY impression_unique_stores) as p75_unique_stores,
  
  -- Impression event counts
  ROUND(AVG(impression_event_count), 2) as avg_impression_events,
  ROUND(MEDIAN(impression_event_count), 2) as median_impression_events,
  
  -- Total events
  ROUND(AVG(total_events), 2) as avg_total_events,
  ROUND(MEDIAN(total_events), 2) as median_total_events

FROM session_classification
WHERE session_category IN ('Has Store Page + Add to Cart', 'Has Store Page Only')
GROUP BY cohort_type, session_category
ORDER BY cohort_type, session_category;


-- ============================================================================
-- QUESTION 2: Events Between First Store Page Load and Add to Cart
-- ============================================================================

-- Step 1: Identify sessions with both store page load and add to cart
WITH sessions_with_both AS (
  SELECT DISTINCT
    user_id,
    dd_device_id,
    event_date,
    session_num
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1 
    AND funnel_had_add = 1
),

-- Step 2: Get event-level data for these sessions
session_events AS (
  SELECT 
    e.cohort_type,
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.event,
    e.discovery_surface,
    e.discovery_feature,
    e.store_id,
    
    -- Flag store page load events
    CASE WHEN e.event_type = 'funnel' AND LOWER(e.event) LIKE '%store_page_load%' THEN 1 ELSE 0 END as is_store_page_load,
    
    -- Flag add to cart events
    CASE WHEN e.event_type = 'funnel' AND (LOWER(e.event) LIKE '%add_item%' OR e.event IN ('action_add_item','action_quick_add_item')) THEN 1 ELSE 0 END as is_add_to_cart,
    
    -- Flag store impression events
    CASE WHEN e.event_type = 'store_impression' THEN 1 ELSE 0 END as is_store_impression
    
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

-- Step 3: Find first occurrence of each event type per session
first_events AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE WHEN is_store_page_load = 1 THEN timestamp END) as first_store_page_load_ts,
    MIN(CASE WHEN is_add_to_cart = 1 THEN timestamp END) as first_add_to_cart_ts
  FROM session_events
  GROUP BY user_id, dd_device_id, event_date, session_num
),

-- Step 4: Get events that occur between first store page load and first add to cart
events_in_between AS (
  SELECT
    e.cohort_type,
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.event,
    e.discovery_surface,
    e.discovery_feature,
    e.is_store_impression,
    f.first_store_page_load_ts,
    f.first_add_to_cart_ts,
    DATEDIFF(second, f.first_store_page_load_ts, f.first_add_to_cart_ts) as seconds_between
  FROM session_events e
  INNER JOIN first_events f
    ON e.user_id = f.user_id
    AND e.dd_device_id = f.dd_device_id
    AND e.event_date = f.event_date
    AND e.session_num = f.session_num
  WHERE e.timestamp > f.first_store_page_load_ts
    AND e.timestamp < f.first_add_to_cart_ts
    AND f.first_store_page_load_ts IS NOT NULL
    AND f.first_add_to_cart_ts IS NOT NULL
),

-- Event breakdown
result as (
  SELECT 
    cohort_type,
    event_type,
    event,
    COUNT(*) as event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_events,
    COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as sessions_with_event,
    ROUND(AVG(seconds_between), 2) as avg_seconds_available_for_event,
    
    -- Store impression specific metrics
    SUM(is_store_impression) as store_impression_count,
    ROUND(100.0 * SUM(is_store_impression) / COUNT(*), 2) as pct_store_impressions
  
  FROM events_in_between
  GROUP BY cohort_type, event_type, event
  ORDER BY cohort_type, event_count DESC
)

select * from result where pct_of_events >= 1;


-- Summary statistics for time between first store page load and add to cart
WITH sessions_with_both AS (
  SELECT DISTINCT
    user_id,
    dd_device_id,
    event_date,
    session_num
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1 
    AND funnel_had_add = 1
),

session_events AS (
  SELECT 
    e.cohort_type,
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    
    CASE WHEN e.event_type = 'funnel' AND LOWER(e.event) LIKE '%store_page_load%' THEN 1 ELSE 0 END as is_store_page_load,
    CASE WHEN e.event_type = 'funnel' AND (LOWER(e.event) LIKE '%add_item%' OR e.event IN ('action_add_item','action_quick_add_item')) THEN 1 ELSE 0 END as is_add_to_cart,
    CASE WHEN e.event_type = 'store_impression' THEN 1 ELSE 0 END as is_store_impression
    
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

first_events AS (
  SELECT
    cohort_type,
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE WHEN is_store_page_load = 1 THEN timestamp END) as first_store_page_load_ts,
    MIN(CASE WHEN is_add_to_cart = 1 THEN timestamp END) as first_add_to_cart_ts
  FROM session_events
  GROUP BY cohort_type, user_id, dd_device_id, event_date, session_num
),

events_in_between AS (
  SELECT
    e.cohort_type,
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    COUNT(*) as events_between,
    SUM(e.is_store_impression) as store_impressions_between,
    f.first_store_page_load_ts,
    f.first_add_to_cart_ts,
    DATEDIFF(second, f.first_store_page_load_ts, f.first_add_to_cart_ts) as seconds_between
  FROM session_events e
  INNER JOIN first_events f
    ON e.user_id = f.user_id
    AND e.dd_device_id = f.dd_device_id
    AND e.event_date = f.event_date
    AND e.session_num = f.session_num
  WHERE e.timestamp > f.first_store_page_load_ts
    AND e.timestamp < f.first_add_to_cart_ts
    AND f.first_store_page_load_ts IS NOT NULL
    AND f.first_add_to_cart_ts IS NOT NULL
  GROUP BY e.cohort_type, e.user_id, e.dd_device_id, e.event_date, e.session_num, 
           f.first_store_page_load_ts, f.first_add_to_cart_ts
)

SELECT
  cohort_type,
  COUNT(*) as total_sessions,
  
  -- Time metrics
  ROUND(AVG(seconds_between), 2) as avg_seconds_between,
  ROUND(MEDIAN(seconds_between), 2) as median_seconds_between,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY seconds_between) as p25_seconds_between,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY seconds_between) as p75_seconds_between,
  MIN(seconds_between) as min_seconds_between,
  MAX(seconds_between) as max_seconds_between,
  
  -- Event count metrics
  ROUND(AVG(events_between), 2) as avg_events_between,
  ROUND(MEDIAN(events_between), 2) as median_events_between,
  
  -- Store impression metrics
  ROUND(AVG(store_impressions_between), 2) as avg_store_impressions_between,
  ROUND(MEDIAN(store_impressions_between), 2) as median_store_impressions_between,
  
  -- Sessions with no events in between
  SUM(CASE WHEN events_between = 0 THEN 1 ELSE 0 END) as sessions_with_zero_events_between,
  ROUND(100.0 * SUM(CASE WHEN events_between = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_sessions_zero_events

FROM events_in_between
GROUP BY cohort_type
ORDER BY cohort_type;


-- ============================================================================
-- Additional: Discovery surface/feature usage between store page load and add to cart
-- ============================================================================

WITH sessions_with_both AS (
  SELECT DISTINCT
    user_id,
    dd_device_id,
    event_date,
    session_num
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1 
    AND funnel_had_add = 1
),

session_events AS (
  SELECT 
    e.cohort_type,
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.discovery_surface,
    e.discovery_feature,
    
    CASE WHEN e.event_type = 'funnel' AND LOWER(e.event) LIKE '%store_page_load%' THEN 1 ELSE 0 END as is_store_page_load,
    CASE WHEN e.event_type = 'funnel' AND (LOWER(e.event) LIKE '%add_item%' OR e.event IN ('action_add_item','action_quick_add_item')) THEN 1 ELSE 0 END as is_add_to_cart
    
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

first_events AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE WHEN is_store_page_load = 1 THEN timestamp END) as first_store_page_load_ts,
    MIN(CASE WHEN is_add_to_cart = 1 THEN timestamp END) as first_add_to_cart_ts
  FROM session_events
  GROUP BY user_id, dd_device_id, event_date, session_num
),

events_in_between AS (
  SELECT
    e.cohort_type,
    e.discovery_surface,
    e.discovery_feature
  FROM session_events e
  INNER JOIN first_events f
    ON e.user_id = f.user_id
    AND e.dd_device_id = f.dd_device_id
    AND e.event_date = f.event_date
    AND e.session_num = f.session_num
  WHERE e.timestamp > f.first_store_page_load_ts
    AND e.timestamp < f.first_add_to_cart_ts
    AND f.first_store_page_load_ts IS NOT NULL
    AND f.first_add_to_cart_ts IS NOT NULL
)

SELECT
  cohort_type,
  discovery_surface,
  discovery_feature,
  COUNT(*) as event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_events
FROM events_in_between
WHERE discovery_surface IS NOT NULL OR discovery_feature IS NOT NULL
GROUP BY cohort_type, discovery_surface, discovery_feature
ORDER BY cohort_type, event_count DESC
LIMIT 50;

