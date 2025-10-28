-- Analysis of journey from First Add to Checkout
-- For sessions that have both funnel add and checkout events
-- Shows: timing, store browsing, tab switching, additional adds, funnel events

-- Query 1: Summary statistics - What happens between add and checkout?
WITH sessions_with_both AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_first_add,
    funnel_seconds_to_checkout,
    funnel_num_adds,
    funnel_had_order,
    funnel_converted_bool,
    -- Time from first add to checkout
    funnel_seconds_to_checkout - funnel_seconds_to_first_add AS seconds_from_add_to_checkout
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_had_add = 1 
    AND funnel_reached_checkout_bool = 1
    AND funnel_seconds_to_first_add IS NOT NULL
    AND funnel_seconds_to_checkout IS NOT NULL
),

events_in_journey AS (
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
    s.funnel_seconds_to_first_add,
    s.funnel_seconds_to_checkout,
    s.seconds_from_add_to_checkout,
    -- Calculate seconds from session start
    DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) AS seconds_from_session_start,
    -- Flag if this event is between first add and checkout
    CASE 
      WHEN DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) > s.funnel_seconds_to_first_add
      AND DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) < s.funnel_seconds_to_checkout
      THEN 1 
      ELSE 0 
    END AS is_between_add_and_checkout
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

journey_metrics AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    seconds_from_add_to_checkout,
    -- Count different event types between add and checkout
    COUNT(CASE WHEN is_between_add_and_checkout = 1 THEN 1 END) AS events_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'store_impression' THEN 1 END) AS impressions_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'attribution' THEN 1 END) AS attribution_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'funnel' THEN 1 END) AS funnel_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type ILIKE '%action%' THEN 1 END) AS action_between,
    -- Specific funnel events
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'funnel' AND event ILIKE '%add_item%' THEN 1 END) AS additional_adds_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'funnel' AND event ILIKE '%store_page_load%' THEN 1 END) AS store_page_loads_between,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'funnel' AND event ILIKE '%order_cart%' THEN 1 END) AS cart_page_loads_between,
    -- Tab switching
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event = 'm_select_tab - explore' THEN 1 END) AS explore_tab_switches,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event = 'm_select_tab - me' THEN 1 END) AS me_tab_switches,
    COUNT(CASE WHEN is_between_add_and_checkout = 1 AND event ILIKE 'm_select_tab%' THEN 1 END) AS total_tab_switches,
    -- Unique stores viewed between add and checkout
    COUNT(DISTINCT CASE WHEN is_between_add_and_checkout = 1 AND event_type IN ('store_impression', 'attribution', 'funnel') THEN store_id END) AS unique_stores_between,
    -- Flag: had any store impressions between add and checkout
    MAX(CASE WHEN is_between_add_and_checkout = 1 AND event_type = 'store_impression' THEN 1 ELSE 0 END) AS had_impressions_between
  FROM events_in_journey
  GROUP BY user_id, dd_device_id, event_date, session_num, cohort_type, session_type, seconds_from_add_to_checkout
)

SELECT 
  cohort_type,
  session_type,
  COUNT(*) as sessions_analyzed,
  
  -- Timing from add to checkout
  ROUND(AVG(seconds_from_add_to_checkout), 2) as avg_seconds_add_to_checkout,
  ROUND(MEDIAN(seconds_from_add_to_checkout), 2) as median_seconds_add_to_checkout,
  ROUND(STDDEV(seconds_from_add_to_checkout), 2) as std_seconds_add_to_checkout,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY seconds_from_add_to_checkout) as p25_seconds,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY seconds_from_add_to_checkout) as p75_seconds,
  
  -- Events between add and checkout
  ROUND(AVG(events_between), 2) as avg_events_between,
  ROUND(MEDIAN(events_between), 2) as median_events_between,
  
  -- Store impressions between add and checkout
  ROUND(100.0 * SUM(had_impressions_between) / COUNT(*), 2) as pct_sessions_with_impressions_between,
  ROUND(AVG(impressions_between), 2) as avg_impressions_between,
  ROUND(MEDIAN(impressions_between), 2) as median_impressions_between,
  ROUND(AVG(CASE WHEN had_impressions_between = 1 THEN impressions_between END), 2) as avg_impressions_when_present,
  
  -- Attribution clicks between
  ROUND(AVG(attribution_between), 2) as avg_attribution_between,
  ROUND(MEDIAN(attribution_between), 2) as median_attribution_between,
  
  -- Funnel events between
  ROUND(AVG(funnel_between), 2) as avg_funnel_between,
  ROUND(AVG(additional_adds_between), 2) as avg_additional_adds,
  ROUND(MEDIAN(additional_adds_between), 2) as median_additional_adds,
  ROUND(AVG(store_page_loads_between), 2) as avg_store_page_loads,
  ROUND(AVG(cart_page_loads_between), 2) as avg_cart_page_loads,
  
  -- Tab switching behavior
  ROUND(100.0 * SUM(CASE WHEN explore_tab_switches > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_switched_to_explore,
  ROUND(AVG(explore_tab_switches), 2) as avg_explore_tab_switches,
  ROUND(100.0 * SUM(CASE WHEN me_tab_switches > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_switched_to_me,
  ROUND(AVG(total_tab_switches), 2) as avg_total_tab_switches,
  
  -- Action events
  ROUND(AVG(action_between), 2) as avg_action_between,
  
  -- Stores viewed between add and checkout
  ROUND(AVG(unique_stores_between), 2) as avg_unique_stores_between,
  ROUND(MEDIAN(unique_stores_between), 2) as median_unique_stores_between,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unique_stores_between) as p75_unique_stores_between,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY unique_stores_between) as p95_unique_stores_between
  
FROM journey_metrics
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- Query 2: Total adds per session (how many adds total?)
SELECT 
  cohort_type,
  session_type,
  funnel_num_adds as total_adds_in_session,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions,
  -- Conversion by number of adds
  ROUND(100.0 * SUM(CASE WHEN funnel_had_order = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_order,
  ROUND(100.0 * SUM(funnel_converted_bool) / COUNT(*), 2) as pct_converted
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE funnel_had_add = 1
  AND funnel_reached_checkout_bool = 1
  AND funnel_num_adds <= 20  -- Filter extreme outliers
GROUP BY cohort_type, session_type, funnel_num_adds
ORDER BY cohort_type, session_type, funnel_num_adds;


-- Query 3: Event patterns between add and checkout
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    funnel_seconds_to_first_add,
    funnel_seconds_to_checkout
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_had_add = 1
    AND funnel_reached_checkout_bool = 1
    AND funnel_seconds_to_first_add IS NOT NULL
    AND funnel_seconds_to_checkout IS NOT NULL
  ORDER BY RANDOM()
  LIMIT 2000  -- Sample for analysis
),

events_with_timing AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    e.event_type,
    e.event,
    e.discovery_feature,
    MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num) AS session_start_ts,
    e.timestamp,
    s.cohort_type,
    s.session_type,
    s.funnel_seconds_to_first_add,
    s.funnel_seconds_to_checkout
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_sample s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

event_summary AS (
  SELECT 
    cohort_type,
    session_type,
    event_type,
    event,
    COUNT(*) as event_count,
    COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as unique_sessions
  FROM events_with_timing
  WHERE DATEDIFF(second, session_start_ts, timestamp) > funnel_seconds_to_first_add
    AND DATEDIFF(second, session_start_ts, timestamp) < funnel_seconds_to_checkout
  GROUP BY cohort_type, session_type, event_type, event
),

cohort_totals AS (
  SELECT
    cohort_type,
    session_type,
    SUM(event_count) as total_events,
    SUM(unique_sessions) as total_sessions
  FROM event_summary
  GROUP BY cohort_type, session_type
)

SELECT 
  e.cohort_type,
  e.session_type,
  e.event_type,
  e.event,
  e.event_count,
  ROUND(100.0 * e.event_count / t.total_events, 2) as pct_of_events,
  e.unique_sessions,
  ROUND(100.0 * e.unique_sessions / t.total_sessions, 2) as pct_of_sessions
FROM event_summary e
INNER JOIN cohort_totals t 
  ON e.cohort_type = t.cohort_type 
  AND e.session_type = t.session_type
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.cohort_type, e.session_type ORDER BY e.event_count DESC) <= 20
ORDER BY e.cohort_type, e.session_type, e.event_count DESC;


-- Query 4: Distribution of time from add to checkout
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN seconds_from_add_to_checkout < 5 THEN '< 5 sec'
    WHEN seconds_from_add_to_checkout < 10 THEN '5-10 sec'
    WHEN seconds_from_add_to_checkout < 30 THEN '10-30 sec'
    WHEN seconds_from_add_to_checkout < 60 THEN '30-60 sec'
    WHEN seconds_from_add_to_checkout < 120 THEN '1-2 min'
    WHEN seconds_from_add_to_checkout < 300 THEN '2-5 min'
    ELSE '5+ min'
  END as time_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_group
FROM sessions_with_both
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


-- Query 5: Store browsing between add and checkout
WITH sessions_with_both AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    funnel_seconds_to_first_add,
    funnel_seconds_to_checkout,
    funnel_num_adds,
    funnel_converted_bool
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_had_add = 1 
    AND funnel_reached_checkout_bool = 1
    AND funnel_seconds_to_first_add IS NOT NULL
    AND funnel_seconds_to_checkout IS NOT NULL
),

store_browsing_metrics AS (
  SELECT
    e.user_id,
    e.dd_device_id,
    e.event_date,
    e.session_num,
    s.cohort_type,
    s.session_type,
    s.funnel_num_adds,
    s.funnel_converted_bool,
    -- Store impressions between add and checkout
    COUNT(CASE WHEN event_type = 'store_impression' 
      AND DATEDIFF(second, MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num), e.timestamp) > s.funnel_seconds_to_first_add
      AND DATEDIFF(second, MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num), e.timestamp) < s.funnel_seconds_to_checkout
      THEN 1 END) AS impressions_between,
    COUNT(DISTINCT CASE WHEN event_type IN ('store_impression', 'attribution')
      AND DATEDIFF(second, MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num), e.timestamp) > s.funnel_seconds_to_first_add
      AND DATEDIFF(second, MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num), e.timestamp) < s.funnel_seconds_to_checkout
      THEN store_id END) AS unique_stores_between
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
  GROUP BY e.user_id, e.dd_device_id, e.event_date, e.session_num, s.cohort_type, s.session_type, s.funnel_num_adds, s.funnel_converted_bool
)

SELECT 
  cohort_type,
  session_type,
  
  -- Store impression presence
  COUNT(*) as total_sessions,
  SUM(CASE WHEN impressions_between > 0 THEN 1 ELSE 0 END) as sessions_with_impressions_between,
  ROUND(100.0 * SUM(CASE WHEN impressions_between > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_impressions_between,
  
  -- Store impression counts
  ROUND(AVG(impressions_between), 2) as avg_impressions_between,
  ROUND(MEDIAN(impressions_between), 2) as median_impressions_between,
  ROUND(AVG(CASE WHEN impressions_between > 0 THEN impressions_between END), 2) as avg_impressions_when_present,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY impressions_between) as p75_impressions,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY impressions_between) as p95_impressions,
  
  -- Unique stores viewed
  ROUND(AVG(unique_stores_between), 2) as avg_unique_stores_between,
  ROUND(MEDIAN(unique_stores_between), 2) as median_unique_stores_between,
  
  -- Total adds in session
  ROUND(AVG(funnel_num_adds), 2) as avg_total_adds_in_session,
  ROUND(MEDIAN(funnel_num_adds), 2) as median_total_adds_in_session,
  
  -- Conversion
  ROUND(100.0 * SUM(funnel_converted_bool) / COUNT(*), 2) as pct_converted
  
FROM store_browsing_metrics
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- Query 6: Tab switching analysis
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN explore_tab_switches = 0 AND me_tab_switches = 0 AND total_tab_switches = 0 THEN 'No tab switching'
    WHEN explore_tab_switches > 0 THEN 'Switched to Explore'
    WHEN me_tab_switches > 0 THEN 'Switched to Me'
    WHEN total_tab_switches > 0 THEN 'Other tab switching'
    ELSE 'No tab switching'
  END as tab_behavior,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions,
  ROUND(AVG(seconds_from_add_to_checkout), 2) as avg_time_to_checkout,
  ROUND(AVG(impressions_between), 2) as avg_impressions,
  ROUND(AVG(additional_adds_between), 2) as avg_additional_adds
FROM journey_metrics
GROUP BY cohort_type, session_type, tab_behavior
ORDER BY cohort_type, session_type, session_count DESC;


-- Query 7: Additional adds distribution
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN additional_adds_between = 0 THEN '0 additional adds'
    WHEN additional_adds_between = 1 THEN '1 additional add'
    WHEN additional_adds_between BETWEEN 2 AND 3 THEN '2-3 adds'
    WHEN additional_adds_between BETWEEN 4 AND 5 THEN '4-5 adds'
    ELSE '6+ adds'
  END as additional_adds_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions,
  ROUND(AVG(seconds_from_add_to_checkout), 2) as avg_time_to_checkout,
  ROUND(AVG(impressions_between), 2) as avg_impressions,
  ROUND(AVG(unique_stores_between), 2) as avg_unique_stores
FROM journey_metrics
GROUP BY cohort_type, session_type, additional_adds_bucket
ORDER BY cohort_type, session_type,
  CASE additional_adds_bucket
    WHEN '0 additional adds' THEN 1
    WHEN '1 additional add' THEN 2
    WHEN '2-3 adds' THEN 3
    WHEN '4-5 adds' THEN 4
    ELSE 5
  END;


-- Query 8: Stores viewed vs conversion rate
SELECT 
  cohort_type,
  session_type,
  CASE 
    WHEN unique_stores_between = 0 THEN '0 stores'
    WHEN unique_stores_between = 1 THEN '1 store'
    WHEN unique_stores_between BETWEEN 2 AND 3 THEN '2-3 stores'
    WHEN unique_stores_between BETWEEN 4 AND 5 THEN '4-5 stores'
    WHEN unique_stores_between BETWEEN 6 AND 10 THEN '6-10 stores'
    ELSE '10+ stores'
  END as stores_viewed_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions,
  ROUND(AVG(seconds_from_add_to_checkout), 2) as avg_time_to_checkout,
  ROUND(AVG(additional_adds_between), 2) as avg_additional_adds
FROM journey_metrics
GROUP BY cohort_type, session_type, stores_viewed_bucket
ORDER BY cohort_type, session_type,
  CASE stores_viewed_bucket
    WHEN '0 stores' THEN 1
    WHEN '1 store' THEN 2
    WHEN '2-3 stores' THEN 3
    WHEN '4-5 stores' THEN 4
    WHEN '6-10 stores' THEN 5
    ELSE 6
  END;


-- Query 9: Event-level details for sample sessions (between add and checkout)
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    days_since_onboarding,
    funnel_seconds_to_first_add,
    funnel_seconds_to_checkout,
    funnel_seconds_to_checkout - funnel_seconds_to_first_add AS journey_duration
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE funnel_had_add = 1
    AND funnel_reached_checkout_bool = 1
    AND funnel_seconds_to_first_add IS NOT NULL
    AND funnel_seconds_to_checkout IS NOT NULL
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
    s.journey_duration,
    s.funnel_seconds_to_first_add,
    s.funnel_seconds_to_checkout
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
  journey_duration,
  timestamp,
  DATEDIFF(second, session_start_ts, timestamp) AS seconds_from_session_start,
  ROW_NUMBER() OVER (PARTITION BY user_id, dd_device_id, event_date, session_num ORDER BY timestamp) AS sequence_in_journey,
  event_type,
  event,
  store_id,
  store_name,
  discovery_feature,
  discovery_surface
FROM events_with_timing
WHERE DATEDIFF(second, session_start_ts, timestamp) > funnel_seconds_to_first_add
  AND DATEDIFF(second, session_start_ts, timestamp) < funnel_seconds_to_checkout
ORDER BY 
  user_id,
  dd_device_id,
  event_date,
  session_num,
  timestamp;

