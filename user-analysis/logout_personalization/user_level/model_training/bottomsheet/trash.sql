
create or replace table proddb.fionafan.raw_web_click_data as (

    WITH store_clicks AS (
  SELECT
    dd_session_id,
    dd_device_id,
    consumer_id,
    platform,
    event_timestamp,
    store_id,
    LOWER(COALESCE(feature, page)) AS feature_norm
  FROM proddb.public.fact_cx_card_click
  WHERE
    event_timestamp::DATE BETWEEN DATEADD(day, -180, DATE '2025-06-10') AND DATEADD(day, -1, DATE '2025-06-10')
    AND (platform ILIKE 'desktop' OR platform ILIKE 'mobile')  -- desktop + mweb only
),
item_clicks AS (
  SELECT
    dd_session_id,
    dd_device_id,
    consumer_id,
    platform,
    event_timestamp,
    store_id,
    LOWER(COALESCE(feature, feature_detailed, page)) AS feature_norm
  FROM proddb.public.fact_item_card_click
  WHERE
    event_timestamp::DATE BETWEEN DATEADD(day, -180, DATE '2025-06-10') AND DATEADD(day, -1, DATE '2025-06-10')
    AND (platform ILIKE 'desktop' OR platform ILIKE 'mobile')  -- desktop + mweb only
),
all_clicks AS (
  SELECT dd_session_id, dd_device_id, consumer_id, platform, event_timestamp, store_id, feature_norm
  FROM store_clicks
  UNION ALL
  SELECT dd_session_id, dd_device_id, consumer_id, platform, event_timestamp, store_id, feature_norm
  FROM item_clicks
),
agg_all AS (
  SELECT
    dd_session_id,
    dd_device_id,
    COUNT(*) AS total_clicks_all
  FROM all_clicks
  GROUP BY 1,2
),
agg_search AS (
  SELECT
    dd_session_id,
    dd_device_id,
    COUNT(*) AS clicks_on_search,
    COUNT(DISTINCT store_id) AS stores_clicked_on_search
  FROM all_clicks
  WHERE feature_norm ILIKE '%search%'
  GROUP BY 1,2
)
SELECT
  COALESCE(a.dd_session_id, s.dd_session_id) AS dd_session_id,
  COALESCE(a.dd_device_id, s.dd_device_id) AS dd_device_id,
  COALESCE(a.total_clicks_all, 0) AS total_clicks_all_desktop_mweb,
  COALESCE(s.clicks_on_search, 0) AS clicks_on_search_desktop_mweb,
  CASE
    WHEN COALESCE(a.total_clicks_all, 0) = 0 THEN 0
    ELSE COALESCE(s.clicks_on_search, 0) / a.total_clicks_all::FLOAT
  END AS share_of_clicks_on_search_desktop_mweb,
  COALESCE(s.stores_clicked_on_search, 0) AS stores_clicked_on_search_desktop_mweb
FROM agg_all a
FULL OUTER JOIN agg_search s
  ON a.dd_session_id = s.dd_session_id
 AND a.dd_device_id = s.dd_device_id

);


create or replace table proddb.fionafan.raw_mobile_click_data as (

WITH store_clicks AS (
  SELECT
    dd_session_id,
    dd_device_id,
    consumer_id,
    platform,
    event_timestamp,
    store_id,
    LOWER(COALESCE(feature, page)) AS feature_norm
  FROM proddb.public.fact_cx_card_click
  WHERE
    event_timestamp::DATE BETWEEN DATEADD(day, -180, DATE '2025-06-10') AND DATEADD(day, -1, DATE '2025-06-10')
    AND platform IN ('ios','android')  -- native mobile app only
),
item_clicks AS (
  SELECT
    dd_session_id,
    dd_device_id,
    consumer_id,
    platform,
    event_timestamp,
    store_id,
    LOWER(COALESCE(feature, feature_detailed, page)) AS feature_norm
  FROM proddb.public.fact_item_card_click
  WHERE
    event_timestamp::DATE BETWEEN DATEADD(day, -180, DATE '2025-06-10') AND DATEADD(day, -1, DATE '2025-06-10')
    AND platform IN ('ios','android')  -- native mobile app only
),
all_clicks AS (
  SELECT dd_session_id, dd_device_id, consumer_id, platform, event_timestamp, store_id, feature_norm
  FROM store_clicks
  UNION ALL
  SELECT dd_session_id, dd_device_id, consumer_id, platform, event_timestamp, store_id, feature_norm
  FROM item_clicks
),
agg_all AS (
  SELECT
    dd_session_id,
    dd_device_id,
    COUNT(*) AS total_clicks_all_mobile_app
  FROM all_clicks
  GROUP BY 1,2
),
agg_search AS (
  SELECT
    dd_session_id,
    dd_device_id,
    COUNT(*) AS clicks_on_search_mobile_app,
    COUNT(DISTINCT store_id) AS stores_clicked_on_search_mobile_app
  FROM all_clicks
  WHERE feature_norm ILIKE '%search%'
  GROUP BY 1,2
)
SELECT
  COALESCE(a.dd_session_id, s.dd_session_id) AS dd_session_id,
  COALESCE(a.dd_device_id, s.dd_device_id) AS dd_device_id,
  COALESCE(a.total_clicks_all_mobile_app, 0) AS total_clicks_all_mobile_app,
  COALESCE(s.clicks_on_search_mobile_app, 0) AS clicks_on_search_mobile_app,
  CASE
    WHEN COALESCE(a.total_clicks_all_mobile_app, 0) = 0 THEN 0
    ELSE COALESCE(s.clicks_on_search_mobile_app, 0) / a.total_clicks_all_mobile_app::FLOAT
  END AS share_of_clicks_on_search_mobile_app,
  COALESCE(s.stores_clicked_on_search_mobile_app, 0) AS stores_clicked_on_search_mobile_app
FROM agg_all a
FULL OUTER JOIN agg_search s
  ON a.dd_session_id = s.dd_session_id
 AND a.dd_device_id = s.dd_device_id
);


