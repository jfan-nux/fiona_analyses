-- Analysis of journey from Attribution Click to Add-to-Cart
-- For sessions that have both attribution events and funnel adds
-- Shows: timing, stores viewed, events in between

-- Step 1: Calculate first non-search add timing
WITH events_with_session_start AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    timestamp,
    event_type,
    event,
    DISCOVERY_SURFACE,
    DISCOVERY_FEATURE,
    MIN(timestamp) OVER (PARTITION BY user_id, dd_device_id, event_date, session_num) AS session_start_ts
  FROM proddb.fionafan.all_user_sessions_with_events
),

first_non_search_add AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE 
      WHEN event_type = 'funnel' 
        AND (event ILIKE '%add_item%' OR event IN ('action_add_item', 'action_quick_add_item'))
        AND DISCOVERY_SURFACE <> 'Search Tab'
        AND DISCOVERY_FEATURE <> 'Core Search'
      THEN DATEDIFF(second, session_start_ts, timestamp)
      ELSE NULL 
    END) AS funnel_seconds_to_first_non_search_add
  FROM events_with_session_start
  GROUP BY user_id, dd_device_id, event_date, session_num
),

sessions_with_both AS (
  SELECT 
    f.user_id,
    f.dd_device_id,
    f.event_date,
    f.session_num,
    f.cohort_type,
    f.session_type,
    f.days_since_onboarding,
    f.attribution_seconds_to_first_card_click,
    nsa.funnel_seconds_to_first_non_search_add AS funnel_seconds_to_first_add,
    f.attribution_num_card_clicks,
    f.funnel_num_adds,
    -- Time from first attribution to first non-search add
    nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click AS seconds_from_click_to_add
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
  INNER JOIN first_non_search_add nsa
    ON f.user_id = nsa.user_id
    AND f.dd_device_id = nsa.dd_device_id
    AND f.event_date = nsa.event_date
    AND f.session_num = nsa.session_num
  WHERE f.attribution_had_any = 1 
    AND f.funnel_had_add = 1
    AND f.attribution_seconds_to_first_card_click IS NOT NULL
    AND nsa.funnel_seconds_to_first_non_search_add IS NOT NULL
),

-- Step 2: Get event-level details for these sessions
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
    s.attribution_seconds_to_first_card_click,
    s.funnel_seconds_to_first_add,
    s.seconds_from_click_to_add,
    -- Calculate seconds from session start
    DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) AS seconds_from_session_start,
    -- Flag if this event is between first click and first add
    CASE 
      WHEN DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) >= s.attribution_seconds_to_first_card_click
      AND DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) <= s.funnel_seconds_to_first_add
      THEN 1 
      ELSE 0 
    END AS is_between_click_and_add
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

-- Step 3: Calculate metrics for events between click and add
journey_metrics AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    seconds_from_click_to_add,
    -- Count different event types between click and add
    COUNT(CASE WHEN is_between_click_and_add = 1 THEN 1 END) AS events_between_click_add,
    COUNT(CASE WHEN is_between_click_and_add = 1 AND event_type = 'store_impression' THEN 1 END) AS impressions_between,
    COUNT(CASE WHEN is_between_click_and_add = 1 AND event_type = 'attribution' THEN 1 END) AS attribution_between,
    COUNT(CASE WHEN is_between_click_and_add = 1 AND event_type = 'funnel' THEN 1 END) AS funnel_between,
    COUNT(CASE WHEN is_between_click_and_add = 1 AND event_type ILIKE '%action%' THEN 1 END) AS action_between,
    -- Unique stores viewed between click and add
    COUNT(DISTINCT CASE WHEN is_between_click_and_add = 1 AND event_type IN ('store_impression', 'attribution', 'funnel') THEN store_id END) AS unique_stores_between,
    -- Most common event types
    LISTAGG(DISTINCT CASE WHEN is_between_click_and_add = 1 THEN event_type END, '|') AS event_types_in_journey
  FROM events_in_journey
  GROUP BY user_id, dd_device_id, event_date, session_num, cohort_type, session_type, seconds_from_click_to_add
)

-- Summary statistics
SELECT 
  cohort_type,
  session_type,
  COUNT(*) as sessions_analyzed,
  
  -- Timing from click to add
  ROUND(AVG(seconds_from_click_to_add), 2) as avg_seconds_click_to_add,
  ROUND(MEDIAN(seconds_from_click_to_add), 2) as median_seconds_click_to_add,
  ROUND(STDDEV(seconds_from_click_to_add), 2) as std_seconds_click_to_add,
  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY seconds_from_click_to_add) as p25_seconds_click_to_add,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY seconds_from_click_to_add) as p75_seconds_click_to_add,
  
  -- Events between click and add
  ROUND(AVG(events_between_click_add), 2) as avg_events_between,
  ROUND(MEDIAN(events_between_click_add), 2) as median_events_between,
  ROUND(AVG(impressions_between), 2) as avg_impressions_between,
  ROUND(MEDIAN(impressions_between), 2) as median_impressions_between,
  ROUND(AVG(attribution_between), 2) as avg_attribution_between,
  ROUND(MEDIAN(attribution_between), 2) as median_attribution_between,
  ROUND(AVG(funnel_between), 2) as avg_funnel_between,
  ROUND(MEDIAN(funnel_between), 2) as median_funnel_between,
  ROUND(AVG(action_between), 2) as avg_action_between,
  ROUND(MEDIAN(action_between), 2) as median_action_between,
  
  -- Stores viewed between click and add
  ROUND(AVG(unique_stores_between), 2) as avg_unique_stores_between,
  ROUND(MEDIAN(unique_stores_between), 2) as median_unique_stores_between,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY unique_stores_between) as p75_unique_stores_between,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY unique_stores_between) as p95_unique_stores_between
  
FROM journey_metrics
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


WITH events_with_session_start AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    timestamp,
    event_type,
    event,
    DISCOVERY_SURFACE,
    DISCOVERY_FEATURE,
    MIN(timestamp) OVER (PARTITION BY user_id, dd_device_id, event_date, session_num) AS session_start_ts
  FROM proddb.fionafan.all_user_sessions_with_events
),

first_non_search_add AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE 
      WHEN event_type = 'funnel' 
        AND (event ILIKE '%add_item%' OR event IN ('action_add_item', 'action_quick_add_item'))
        AND DISCOVERY_SURFACE <> 'Search Tab'
        AND DISCOVERY_FEATURE <> 'Core Search'
      THEN DATEDIFF(second, session_start_ts, timestamp)
      ELSE NULL 
    END) AS funnel_seconds_to_first_non_search_add
  FROM events_with_session_start
  GROUP BY user_id, dd_device_id, event_date, session_num
),

sessions_with_both AS (
  SELECT 
    f.user_id,
    f.dd_device_id,
    f.event_date,
    f.session_num,
    f.cohort_type,
    f.session_type,
    f.attribution_seconds_to_first_card_click,
    nsa.funnel_seconds_to_first_non_search_add AS funnel_seconds_to_first_add
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
  INNER JOIN first_non_search_add nsa
    ON f.user_id = nsa.user_id
    AND f.dd_device_id = nsa.dd_device_id
    AND f.event_date = nsa.event_date
    AND f.session_num = nsa.session_num
  WHERE f.attribution_had_any = 1 
    AND f.funnel_had_add = 1
    AND f.attribution_seconds_to_first_card_click IS NOT NULL
    AND nsa.funnel_seconds_to_first_non_search_add IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.cohort_type ORDER BY RANDOM()) <= 500  -- Sample 500 per cohort
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
    e.discovery_feature,
    s.cohort_type,
    s.session_type,
    DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) AS seconds_from_session_start,
    s.attribution_seconds_to_first_card_click,
    s.funnel_seconds_to_first_add,
    CASE 
      WHEN DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) >= s.attribution_seconds_to_first_card_click
      AND DATEDIFF(second, 
        MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
        e.timestamp
      ) <= s.funnel_seconds_to_first_add
      THEN 1 
      ELSE 0 
    END AS is_between
  FROM proddb.fionafan.all_user_sessions_with_events e
  INNER JOIN sessions_with_both s
    ON e.user_id = s.user_id
    AND e.dd_device_id = s.dd_device_id
    AND e.event_date = s.event_date
    AND e.session_num = s.session_num
),

event_summary AS (
  SELECT 
    cohort_type,
    event_type,
    event,
    COUNT(*) as event_count,
    COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as unique_sessions
  FROM events_in_journey
  WHERE is_between = 1
  GROUP BY cohort_type, event_type, event
),

cohort_event_totals AS (
  SELECT
    cohort_type,
    SUM(event_count) as total_events
  FROM event_summary
  GROUP BY cohort_type
),

cohort_session_totals AS (
  SELECT
    cohort_type,
    COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as total_sessions
  FROM events_in_journey
  WHERE is_between = 1
  GROUP BY cohort_type
)

SELECT 
  e.cohort_type,
  e.event_type,
  e.event,
  e.event_count,
  ROUND(100.0 * e.event_count / te.total_events, 2) as pct_of_events,
  e.unique_sessions,
  ROUND(100.0 * e.unique_sessions / ts.total_sessions, 2) as pct_of_sessions
FROM event_summary e
INNER JOIN cohort_event_totals te ON e.cohort_type = te.cohort_type
INNER JOIN cohort_session_totals ts ON e.cohort_type = ts.cohort_type
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.cohort_type ORDER BY e.event_count DESC) <= 20
ORDER BY e.cohort_type, e.event_count DESC;


-- Query 3: Distribution of time from click to add by cohort (non-search adds only)
WITH events_with_session_start AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    timestamp,
    event_type,
    event,
    DISCOVERY_SURFACE,
    DISCOVERY_FEATURE,
    MIN(timestamp) OVER (PARTITION BY user_id, dd_device_id, event_date, session_num) AS session_start_ts
  FROM proddb.fionafan.all_user_sessions_with_events
),

first_non_search_add AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE 
      WHEN event_type = 'funnel' 
        AND (event ILIKE '%add_item%' OR event IN ('action_add_item', 'action_quick_add_item'))
        AND DISCOVERY_SURFACE <> 'Search Tab'
        AND DISCOVERY_FEATURE <> 'Core Search'
      THEN DATEDIFF(second, session_start_ts, timestamp)
      ELSE NULL 
    END) AS funnel_seconds_to_first_non_search_add
  FROM events_with_session_start
  GROUP BY user_id, dd_device_id, event_date, session_num
)

SELECT 
  f.cohort_type,
  CASE 
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 5 THEN '< 5 sec'
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 10 THEN '5-10 sec'
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 30 THEN '10-30 sec'
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 60 THEN '30-60 sec'
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 120 THEN '1-2 min'
    WHEN nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click < 300 THEN '2-5 min'
    ELSE '5+ min'
  END as time_bucket,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.cohort_type), 2) as pct_of_cohort
FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
INNER JOIN first_non_search_add nsa
  ON f.user_id = nsa.user_id
  AND f.dd_device_id = nsa.dd_device_id
  AND f.event_date = nsa.event_date
  AND f.session_num = nsa.session_num
WHERE f.attribution_had_any = 1 
  AND f.funnel_had_add = 1
  AND f.attribution_seconds_to_first_card_click IS NOT NULL
  AND nsa.funnel_seconds_to_first_non_search_add IS NOT NULL
GROUP BY f.cohort_type, time_bucket
ORDER BY f.cohort_type, 
  CASE time_bucket
    WHEN '< 5 sec' THEN 1
    WHEN '5-10 sec' THEN 2
    WHEN '10-30 sec' THEN 3
    WHEN '30-60 sec' THEN 4
    WHEN '1-2 min' THEN 5
    WHEN '2-5 min' THEN 6
    ELSE 7
  END;


-- Query 4: Event-level details for each session (between click and add)
-- Shows the actual events in order for each session
WITH sessions_sample AS (
  SELECT 
    user_id,
    dd_device_id,
    event_date,
    session_num,
    cohort_type,
    session_type,
    attribution_seconds_to_first_card_click,
    funnel_seconds_to_first_add,
    funnel_seconds_to_first_add - attribution_seconds_to_first_card_click AS seconds_from_click_to_add
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen
  WHERE attribution_had_any = 1 
    AND funnel_had_add = 1
    AND attribution_seconds_to_first_card_click IS NOT NULL
    AND funnel_seconds_to_first_add IS NOT NULL
  ORDER BY RANDOM()
  LIMIT 100  -- Sample 100 sessions for detailed inspection
)

SELECT
  s.cohort_type,
  s.session_type,
  s.user_id,
  s.dd_device_id,
  s.event_date,
  s.session_num,
  s.seconds_from_click_to_add,
  e.timestamp,
  DATEDIFF(second, 
    MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
    e.timestamp
  ) AS seconds_from_session_start,
  e.event_type,
  e.event,
  e.store_id,
  e.store_name,
  e.discovery_feature,
  e.discovery_surface,
  ROW_NUMBER() OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num ORDER BY e.timestamp) AS event_sequence,
  CASE 
    WHEN DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) >= s.attribution_seconds_to_first_card_click
    AND DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) <= s.funnel_seconds_to_first_add
    THEN 'BETWEEN_CLICK_AND_ADD'
    WHEN DATEDIFF(second, 
      MIN(e.timestamp) OVER (PARTITION BY e.user_id, e.dd_device_id, e.event_date, e.session_num),
      e.timestamp
    ) < s.attribution_seconds_to_first_card_click
    THEN 'BEFORE_CLICK'
    ELSE 'AFTER_ADD'
  END AS event_stage
FROM proddb.fionafan.all_user_sessions_with_events e
INNER JOIN sessions_sample s
  ON e.user_id = s.user_id
  AND e.dd_device_id = s.dd_device_id
  AND e.event_date = s.event_date
  AND e.session_num = s.session_num
ORDER BY 
  s.cohort_type,
  s.user_id,
  s.dd_device_id,
  s.event_date,
  s.session_num,
  e.timestamp;


-- Query 5: Events ONLY between first click and first add (filtered view)
-- For each session, shows only the events that occurred in the click-to-add journey
WITH events_with_session_start AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    timestamp,
    event_type,
    event,
    DISCOVERY_SURFACE,
    DISCOVERY_FEATURE,
    MIN(timestamp) OVER (PARTITION BY user_id, dd_device_id, event_date, session_num) AS session_start_ts
  FROM proddb.fionafan.all_user_sessions_with_events
),

first_non_search_add AS (
  SELECT
    user_id,
    dd_device_id,
    event_date,
    session_num,
    MIN(CASE 
      WHEN event_type = 'funnel' 
        AND (event ILIKE '%add_item%' OR event IN ('action_add_item', 'action_quick_add_item'))
        AND DISCOVERY_SURFACE <> 'Search Tab'
        AND DISCOVERY_FEATURE <> 'Core Search'
      THEN DATEDIFF(second, session_start_ts, timestamp)
      ELSE NULL 
    END) AS funnel_seconds_to_first_non_search_add
  FROM events_with_session_start
  GROUP BY user_id, dd_device_id, event_date, session_num
),

sessions_sample AS (
  SELECT 
    f.user_id,
    f.dd_device_id,
    f.event_date,
    f.session_num,
    f.cohort_type,
    f.session_type,
    f.days_since_onboarding,
    f.attribution_seconds_to_first_card_click,
    nsa.funnel_seconds_to_first_non_search_add AS funnel_seconds_to_first_add,
    nsa.funnel_seconds_to_first_non_search_add - f.attribution_seconds_to_first_card_click AS journey_duration_seconds
  FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
  INNER JOIN first_non_search_add nsa
    ON f.user_id = nsa.user_id
    AND f.dd_device_id = nsa.dd_device_id
    AND f.event_date = nsa.event_date
    AND f.session_num = nsa.session_num
  WHERE f.attribution_had_any = 1 
    AND f.funnel_had_add = 1
    AND f.attribution_seconds_to_first_card_click IS NOT NULL
    AND nsa.funnel_seconds_to_first_non_search_add IS NOT NULL
  ORDER BY RANDOM()
  LIMIT 50  -- Sample 50 sessions for detailed inspection
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
    s.journey_duration_seconds,
    s.attribution_seconds_to_first_card_click,
    s.funnel_seconds_to_first_add
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
  journey_duration_seconds,
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
WHERE DATEDIFF(second, session_start_ts, timestamp) >= attribution_seconds_to_first_card_click
  AND DATEDIFF(second, session_start_ts, timestamp) <= funnel_seconds_to_first_add
ORDER BY 
  user_id,
  dd_device_id,
  event_date,
  session_num,
  timestamp;

