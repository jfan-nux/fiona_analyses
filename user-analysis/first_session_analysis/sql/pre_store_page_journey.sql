-- Analysis of journey BEFORE reaching first store page
-- For sessions that reached a store page
-- Shows: impressions viewed, time to store page, NV presence, discovery patterns

-- Query 1: Summary statistics - What happens before first store page?
WITH sessions_with_store_page AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_store_page,
    attribution_seconds_to_first_card_click,
    impression_event_count,
    attribution_event_count,
    funnel_reached_store_bool,
    store_had_nv
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1
    AND funnel_seconds_to_store_page IS NOT NULL
),

events_before_store_page AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.event,
    e.store_id,
    e.store_name,
    e.discovery_feature,
    e.discovery_surface,
    s.cohort_type,
    s.session_type,
    s.days_since_onboarding,
    s.funnel_seconds_to_store_page,
    DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) AS seconds_from_session_start
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_store_page s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
  WHERE DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) < s.funnel_seconds_to_store_page
),

journey_metrics AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_store_page,
    -- Count events before store page
    COUNT(*) AS total_events_before,
    COUNT(CASE WHEN event_type = 'store_impression' THEN 1 END) AS impressions_before,
    COUNT(CASE WHEN event_type = 'attribution' THEN 1 END) AS attribution_clicks_before,
    COUNT(CASE WHEN event_type ILIKE '%action%' THEN 1 END) AS actions_before,
    -- Unique stores viewed before store page
    COUNT(DISTINCT CASE WHEN event_type IN ('store_impression', 'attribution') THEN store_id END) AS unique_stores_viewed_before,
    -- Discovery patterns
    LISTAGG(DISTINCT discovery_feature, '|') AS discovery_features_used
  FROM events_before_store_page
  GROUP BY user_id, dd_device_id, event_date, session_num, cohort_type, session_type, days_since_onboarding, funnel_seconds_to_store_page
)

SELECT 
  cohort_type,
  session_type,
  COUNT(*) as sessions_analyzed,
  
  -- Time to first store page
  ROUND(AVG(funnel_seconds_to_store_page), 2) as avg_seconds_to_store_page,
  ROUND(MEDIAN(funnel_seconds_to_store_page), 2) as median_seconds_to_store_page,
  ROUND(STDDEV(funnel_seconds_to_store_page), 2) as std_seconds_to_store_page,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY funnel_seconds_to_store_page) as p25_seconds,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY funnel_seconds_to_store_page) as p75_seconds,
  
  -- Events before store page
  ROUND(AVG(total_events_before), 2) as avg_events_before,
  ROUND(MEDIAN(total_events_before), 2) as median_events_before,
  ROUND(AVG(impressions_before), 2) as avg_impressions_before,
  ROUND(MEDIAN(impressions_before), 2) as median_impressions_before,
  ROUND(AVG(attribution_clicks_before), 2) as avg_attribution_clicks_before,
  ROUND(MEDIAN(attribution_clicks_before), 2) as median_attribution_clicks_before,
  ROUND(AVG(actions_before), 2) as avg_actions_before,
  
  -- Stores viewed before store page
  ROUND(AVG(unique_stores_viewed_before), 2) as avg_unique_stores_viewed,
  ROUND(MEDIAN(unique_stores_viewed_before), 2) as median_unique_stores_viewed,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unique_stores_viewed_before) as p75_unique_stores_viewed,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY unique_stores_viewed_before) as p95_unique_stores_viewed
  
FROM journey_metrics
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- Query 2: First store page NV percentage
-- What % of first store pages reached are NV merchants?
WITH first_store_pages AS (
  SELECT
    s.user_id,
    s.dd_device_id,
    s.event_date,
    s.session_num,
    s.cohort_type,
    s.session_type,
    s.days_since_onboarding,
    s.funnel_seconds_to_store_page,
    -- Get the first store_page_load event
    e.store_id as first_store_id,
    e.store_name as first_store_name,
    e.discovery_feature as first_store_discovery_feature,
    e.discovery_surface as first_store_discovery_surface
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
  INNER JOIN proddb.fionafan.all_user_sessions_with_events e
    ON s.user_id = e.user_id
    AND s.dd_device_id = e.dd_device_id
    AND s.event_date = e.event_date
    AND s.session_num = e.session_num
  WHERE s.funnel_reached_store_bool = 1
    AND e.event_type = 'funnel'
    AND e.event ILIKE '%store_page_load%'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.user_id, s.dd_device_id, s.event_date, s.session_num 
    ORDER BY e.timestamp
  ) = 1
),

first_store_with_nv AS (
  SELECT
    f.*,
    CASE WHEN ds.NV_ORG IS NOT NULL OR ds.NV_VERTICAL_NAME IS NOT NULL 
         OR ds.NV_BUSINESS_LINE IS NOT NULL OR ds.NV_BUSINESS_SUB_TYPE IS NOT NULL 
      THEN 1 ELSE 0 END AS first_store_is_nv,
    ds.TIER_LEVEL as first_store_tier,
    ds.PRIMARY_TAG_NAME as first_store_tag,
    ds.PRIMARY_CATEGORY_NAME as first_store_category
  FROM first_store_pages f
  LEFT JOIN edw.merchant.dimension_store ds
    ON f.first_store_id = ds.store_ID
)

SELECT 
  cohort_type,
  session_type,
  COUNT(*) as total_sessions_with_store_page,
  SUM(first_store_is_nv) as first_store_is_nv_count,
  ROUND(100.0 * SUM(first_store_is_nv) / COUNT(*), 2) as pct_first_store_is_nv,
  
  -- Time to first store page by NV status
  ROUND(AVG(CASE WHEN first_store_is_nv = 1 THEN funnel_seconds_to_store_page END), 2) as avg_seconds_when_nv,
  ROUND(AVG(CASE WHEN first_store_is_nv = 0 THEN funnel_seconds_to_store_page END), 2) as avg_seconds_when_not_nv,
  
  -- Most common discovery sources for first store
  MODE(first_store_discovery_feature) as most_common_discovery_feature,
  MODE(first_store_discovery_surface) as most_common_discovery_surface
  
FROM first_store_with_nv
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- Query 3: First store tier and category distribution
SELECT 
  cohort_type,
  first_store_tier,
  first_store_category,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort,
  AVG(first_store_is_nv) as avg_is_nv,
  ROUND(AVG(funnel_seconds_to_store_page), 2) as avg_seconds_to_reach
FROM first_store_with_nv
WHERE first_store_category IS NOT NULL
GROUP BY cohort_type, first_store_tier, first_store_category
QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_type ORDER BY COUNT(*) DESC) <= 15
ORDER BY cohort_type, session_count DESC;


-- Query 4: Event patterns before reaching store page
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1
    AND funnel_seconds_to_store_page IS NOT NULL
  ORDER BY RANDOM()
  LIMIT 1000  -- Sample for analysis
),

events_with_timing AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.event,
    e.store_id,
    e.discovery_feature,
    e.discovery_surface,
    MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num) AS session_start_ts,
    s.cohort_type,
    s.session_type,
    s.funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_sample s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
)

SELECT 
  cohort_type,
  event_type,
  event,
  COUNT(*) as event_count,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as unique_sessions,
  ROUND(AVG(DATEDIFF(second, session_start_ts, timestamp)), 2) as avg_seconds_when_occurred
FROM events_with_timing
WHERE DATEDIFF(second, session_start_ts, timestamp) < funnel_seconds_to_store_page
GROUP BY cohort_type, event_type, event
QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_type ORDER BY COUNT(*) DESC) <= 20
ORDER BY cohort_type, event_count DESC;


-- Query 5: Distribution of time to first store page
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN funnel_seconds_to_store_page < 5 THEN '< 5 sec'
    WHEN funnel_seconds_to_store_page < 10 THEN '5-10 sec'
    WHEN funnel_seconds_to_store_page < 30 THEN '10-30 sec'
    WHEN funnel_seconds_to_store_page < 60 THEN '30-60 sec'
    WHEN funnel_seconds_to_store_page < 120 THEN '1-2 min'
    WHEN funnel_seconds_to_store_page < 300 THEN '2-5 min'
    ELSE '5+ min'
  END as time_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_group
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE funnel_reached_store_bool = 1
  AND funnel_seconds_to_store_page IS NOT NULL
GROUP BY cohort_type, session_type, time_bucket
ORDER BY cohort_type, session_type,
  CASE time_bucket
    WHEN '< 5 sec' THEN 1
    WHEN '5-10 sec' THEN 2
    WHEN '10-30 sec' THEN 3
    WHEN '30-60 sec' THEN 4
    WHEN '1-2 min' THEN 5
    WHEN '2-5 min' THEN 6
    ELSE 7
  END;


-- Query 6: Discovery feature usage before reaching store page
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1
    AND funnel_seconds_to_store_page IS NOT NULL
),

events_with_timing AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.discovery_feature,
    e.discovery_surface,
    MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num) AS session_start_ts,
    e.timestamp,
    s.cohort_type,
    s.session_type,
    s.funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_sample s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
  WHERE e.event_type IN ('store_impression', 'attribution')
    AND DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) < s.funnel_seconds_to_store_page
)

SELECT 
  cohort_type,
  session_type,
  discovery_feature,
  discovery_surface,
  COUNT(*) as event_count,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as unique_sessions,
  ROUND(100.0 * COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) / 
    SUM(COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num))) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions
FROM events_with_timing
GROUP BY cohort_type, session_type, discovery_feature, discovery_surface
QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_type, session_type ORDER BY COUNT(*) DESC) <= 10
ORDER BY cohort_type, session_type, event_count DESC;


-- Query 7: Stores viewed count distribution before first store page
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN unique_stores_viewed_before = 0 THEN '0 stores'
    WHEN unique_stores_viewed_before = 1 THEN '1 store'
    WHEN unique_stores_viewed_before BETWEEN 2 AND 3 THEN '2-3 stores'
    WHEN unique_stores_viewed_before BETWEEN 4 AND 5 THEN '4-5 stores'
    WHEN unique_stores_viewed_before BETWEEN 6 AND 10 THEN '6-10 stores'
    WHEN unique_stores_viewed_before BETWEEN 11 AND 20 THEN '11-20 stores'
    ELSE '20+ stores'
  END as stores_viewed_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_group,
  ROUND(AVG(funnel_seconds_to_store_page), 2) as avg_time_to_store_page
FROM journey_metrics
GROUP BY cohort_type, session_type, stores_viewed_bucket
ORDER BY cohort_type, session_type,
  CASE stores_viewed_bucket
    WHEN '0 stores' THEN 1
    WHEN '1 store' THEN 2
    WHEN '2-3 stores' THEN 3
    WHEN '4-5 stores' THEN 4
    WHEN '6-10 stores' THEN 5
    WHEN '11-20 stores' THEN 6
    ELSE 7
  END;


-- Query 8: Impressions vs Attribution clicks before store page
SELECT 
  cohort_type,
  session_type,
  impressions_before,
  attribution_clicks_before,
  COUNT(*) as session_count,
  ROUND(AVG(funnel_seconds_to_store_page), 2) as avg_seconds_to_store_page,
  ROUND(AVG(unique_stores_viewed_before), 2) as avg_unique_stores
FROM journey_metrics
WHERE impressions_before <= 20 AND attribution_clicks_before <= 10  -- Filter outliers for readability
GROUP BY cohort_type, session_type, impressions_before, attribution_clicks_before
HAVING COUNT(*) >= 10  -- Only show common patterns
ORDER BY cohort_type, session_type, session_count DESC
LIMIT 100;


-- Query 9: Early vs Late store page reach - conversion comparison
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN funnel_seconds_to_store_page < 10 THEN 'Fast (< 10s)'
    WHEN funnel_seconds_to_store_page < 30 THEN 'Medium (10-30s)'
    ELSE 'Slow (30s+)'
  END as speed_to_store_page,
  COUNT(*) as session_count,
  
  -- Stores viewed
  ROUND(AVG(unique_stores_viewed_before), 2) as avg_stores_viewed,
  
  -- Conversion metrics
  ROUND(100.0 * SUM(CASE WHEN funnel_had_add = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_add,
  ROUND(100.0 * SUM(CASE WHEN funnel_had_order = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_order,
  ROUND(100.0 * SUM(CASE WHEN funnel_converted_bool = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_converted
  
FROM (
  SELECT
    jm.*,
    f.funnel_had_add,
    f.funnel_had_order,
    f.funnel_converted_bool
  FROM journey_metrics jm
  INNER JOIN proddb.fionafan.all_user_sessions_with_events_features_gen f
    ON jm.user_id = f.user_id
    AND jm.dd_device_id = f.dd_device_id
    AND jm.event_date = f.event_date
    AND jm.session_num = f.session_num
)
GROUP BY cohort_type, session_type, speed_to_store_page
ORDER BY cohort_type, session_type, 
  CASE speed_to_store_page
    WHEN 'Fast (< 10s)' THEN 1
    WHEN 'Medium (10-30s)' THEN 2
    ELSE 3
  END;


-- Query 10: Sample event sequences before store page (for detailed inspection)
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_reached_store_bool = 1
    AND funnel_seconds_to_store_page IS NOT NULL
  ORDER BY RANDOM()
  LIMIT 50
),

events_with_timing AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.timestamp,
    e.event_type,
    e.event,
    e.store_id,
    e.store_name,
    e.discovery_feature,
    e.discovery_surface,
    MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num) AS session_start_ts,
    s.cohort_type,
    s.session_type,
    s.days_since_onboarding,
    s.funnel_seconds_to_store_page
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_sample s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
)

SELECT
  cohort_type,
  session_type,
  user_id,
  dd_device_id,
  event_date,
  session_num,
  days_since_onboarding,
  funnel_seconds_to_store_page,
  timestamp,
  DATEDIFF(second, session_start_ts, timestamp) AS seconds_from_session_start,
  ROW_NUMBER() OVER (PARTITION BY user_id, dd_device_id, event_date, session_num ORDER BY timestamp) AS event_sequence,
  event_type,
  event,
  store_id,
  store_name,
  discovery_feature,
  discovery_surface
FROM events_with_timing
WHERE DATEDIFF(second, session_start_ts, timestamp) < funnel_seconds_to_store_page
ORDER BY 
  user_id,
  dd_device_id,
  event_date,
  session_num,
  timestamp;



