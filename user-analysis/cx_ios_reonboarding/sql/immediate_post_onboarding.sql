-- % of distinct devices that land on Explore/Home within 24h after exposure
-- Fast-running (last 7 days). For a longer range, run in weekly chunks and UNION ALL + re-aggregate.
create or replace table proddb.fionafan.immediate_post_onboarding_impression as (
WITH
  date_bounds AS (
    SELECT
      '2025-09-08' AS start_date,
      '2025-11-03' AS end_date
  ),
  exposure AS (
select * from proddb.fionafan.cx_ios_reonboarding_experiment_exposures
  ),
  device_list AS (
    SELECT DISTINCT
      dd_device_id_filtered
    FROM
      exposure
  ),
  impressions_sub AS (
    SELECT
      replace(
        lower(
          CASE
            WHEN DD_DEVICE_ID ILIKE 'dx_%' THEN DD_DEVICE_ID
            ELSE 'dx_' || DD_DEVICE_ID
          END
        ),
        '-'
      ) AS dd_device_id_filtered,
      EVENT_TIME_UTC AS event_ts,
      EVENT_DATE
    FROM
      edw.consumer.fact_consumer_impressions_extended
      CROSS JOIN date_bounds b
    WHERE
      EVENT_DATE BETWEEN b.start_date AND b.end_date
      AND (
        PAGE ILIKE '%home%'
        OR PAGE ILIKE '%explore%'
      )
      AND replace(
        lower(
          CASE
            WHEN DD_DEVICE_ID ILIKE 'dx_%' THEN DD_DEVICE_ID
            ELSE 'dx_' || DD_DEVICE_ID
          END
        ),
        '-'
      ) IN (
        SELECT
          dd_device_id_filtered
        FROM
          device_list
      )
  )

select * from impressions_sub
);

  denom AS (
    SELECT
      COUNT(DISTINCT dd_device_id_filtered) AS devices_exposed
    FROM
      exposure
  ),
  num AS (
    SELECT
      COUNT(DISTINCT e.dd_device_id_filtered) AS devices_landed_explore_24h
    FROM
      exposure e
      JOIN impressions_sub i ON i.dd_device_id_filtered = e.dd_device_id_filtered
      AND i.EVENT_DATE BETWEEN e.exposure_date AND DATEADD(day, 1, e.exposure_date)
      AND i.event_ts >= e.exposure_time
      AND i.event_ts < e.exposure_time + INTERVAL '24 HOURS'
  )
SELECT
  d.devices_exposed,
  n.devices_landed_explore_24h,
  n.devices_landed_explore_24h / NULLIF(d.devices_exposed, 0) AS pct_landed_explore_24h
FROM
  denom d
  CROSS JOIN num n;




-- Opted-in but NOT exposed users in AU/NZ/CA
-- Unify Iguazu sources into one: source ∈ {'prompt_page','onboarding','no_match'}
-- dd_platform comes from the respective source table when available
WITH
  exposure AS (
    SELECT DISTINCT
      ee.BUCKET_KEY::VARCHAR AS consumer_id_v
    FROM
      proddb.public.fact_dedup_experiment_exposure ee
    WHERE
      ee.EXPERIMENT_NAME = 'engagement_milestone_ms1'
      AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ee.EXPOSURE_TIME) BETWEEN '2025-07-01' AND CURRENT_DATE
      AND ee.TAG IN ('Treatment-A', 'control')
  ),
  opted_in AS (
    SELECT
      s.CONSUMER_ID,
      dc.DEFAULT_COUNTRY
    FROM
      edw.consumer.dimension_consumer_sms_settings_scd3 s
      JOIN edw.consumer.dimension_consumers dc ON s.CONSUMER_ID = dc.CONSUMER_ID
    WHERE
      s.SCD_CURRENT_RECORD = TRUE
      AND CURRENT_DATE >= s.SCD_START_DATE
      AND (
        s.SCD_END_DATE IS NULL
        OR CURRENT_DATE < s.SCD_END_DATE
      )
      AND s.MARKETING_STATUS ILIKE 'on'
      AND dc.DEFAULT_COUNTRY IN ('Australia', 'New Zealand', 'Canada')
  ),
  not_exposed_opted AS (
    SELECT
      o.*
    FROM
      opted_in o
      LEFT JOIN exposure e ON e.consumer_id_v = o.CONSUMER_ID::VARCHAR
    WHERE
      e.consumer_id_v IS NULL
  ),
  -- Union both Iguazu sources with a source label; pick latest-ever per consumer
  iguazu_events AS (
    SELECT
      m.CONSUMER_ID,
      m.DD_PLATFORM,
      m.RECEIVED_AT,
      'prompt_page' AS source
    FROM
      iguazu.consumer.m_marketing_sms_prompt_page_click_ice m
    UNION ALL
    SELECT
      o.CONSUMER_ID,
      o.DD_PLATFORM,
      o.RECEIVED_AT,
      'onboarding' AS source
    FROM
      iguazu.consumer.m_onboarding_page_click_ice o
    WHERE
      o.PAGE = 'marketingSMS'
  ),
  latest_event AS (
    SELECT
      CONSUMER_ID,
      DD_PLATFORM,
      source
    FROM
      (
        SELECT
          ie.*,
          ROW_NUMBER() OVER (
            PARTITION BY
              ie.CONSUMER_ID
            ORDER BY
              ie.RECEIVED_AT DESC
          ) AS rn
        FROM
          iguazu_events ie
      )
    WHERE
      rn = 1
  ),
  enriched AS (
    SELECT
      ne.DEFAULT_COUNTRY AS country,
      ne.CONSUMER_ID,
      COALESCE(le.source, 'no_match') AS source,
      COALESCE(le.DD_PLATFORM, 'no match') AS dd_platform
    FROM
      not_exposed_opted ne
      LEFT JOIN latest_event le ON le.CONSUMER_ID = ne.CONSUMER_ID
  )
SELECT
  country,
  source,
  dd_platform,
  COUNT(DISTINCT consumer_id) AS consumers
FROM
  enriched
GROUP BY
  country,
  source,
  dd_platform
ORDER BY
  country,
  source,
  dd_platform;
