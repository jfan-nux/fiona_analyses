SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-08-11';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

-- Prior engagement analysis using all events table (pre-exposure)
-- Creates a table with last event recency and window flags (7d/30d/90d/180d/365d)
CREATE OR REPLACE TABLE proddb.fionafan.REONBOARDING_PRIOR_ENGAGEMENT_STATUS AS (
WITH exposure AS (
    SELECT  
        ee.tag,
        ee.result,
        ee.bucket_key,
        REPLACE(LOWER(CASE WHEN bucket_key LIKE 'dx_%' THEN bucket_key
                     ELSE 'dx_'||bucket_key END), '-') AS dd_device_id_filtered,
        segment,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS exposure_day,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS exposure_time,
        MAX(ee.custom_attributes:userId) AS user_id
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
    AND experiment_version::INT = $version
    AND segment = $segment
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY all
)
,
events_l60d AS (
    SELECT 
        ed.dd_device_id_filtered,
        ev.timestamp::date AS event_day,
        COUNT(*) AS event_count,
        MAX(ev.timestamp) AS max_event_ts
    FROM tyleranderson.events_all ev
    INNER JOIN (
        SELECT DISTINCT dd_device_id_filtered FROM exposure
    ) ed
      ON ed.dd_device_id_filtered = REPLACE(LOWER(CASE WHEN ev.dd_device_id LIKE 'dx_%' THEN ev.dd_device_id
                                                  ELSE 'dx_'||ev.dd_device_id END), '-')
    WHERE ev.timestamp::date 
          BETWEEN DATEADD('day', -60, $start_date)::date AND $end_date
    GROUP BY 1,2
)
,
prior_events AS (
    SELECT 
        e.tag,
        e.result,
        e.dd_device_id_filtered,
        e.user_id,
        e.exposure_day,
        e.exposure_time,
        MAX(CASE 
            WHEN (ey.event_day < e.exposure_day) 
                 OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time)
            THEN ey.event_day ELSE NULL END) AS last_event_date,
        COALESCE(SUM(CASE 
            WHEN (ey.event_day < e.exposure_day) 
                 OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time)
            THEN ey.event_count ELSE 0 END), 0) AS total_events_last_2y,
        COALESCE(MAX(CASE WHEN ((ey.event_day < e.exposure_day) OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time))
                               AND ey.event_day >= DATEADD('day', -7, e.exposure_day) THEN 1 ELSE 0 END), 0) AS any_event_7d,
        COALESCE(MAX(CASE WHEN ((ey.event_day < e.exposure_day) OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time))
                               AND ey.event_day >= DATEADD('day', -30, e.exposure_day) THEN 1 ELSE 0 END), 0) AS any_event_30d,
        COALESCE(MAX(CASE WHEN ((ey.event_day < e.exposure_day) OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time))
                               AND ey.event_day >= DATEADD('day', -90, e.exposure_day) THEN 1 ELSE 0 END), 0) AS any_event_90d,
        COALESCE(MAX(CASE WHEN ((ey.event_day < e.exposure_day) OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time))
                               AND ey.event_day >= DATEADD('day', -180, e.exposure_day) THEN 1 ELSE 0 END), 0) AS any_event_180d,
        COALESCE(MAX(CASE WHEN ((ey.event_day < e.exposure_day) OR (ey.event_day = e.exposure_day AND ey.max_event_ts < e.exposure_time))
                               AND ey.event_day >= DATEADD('day', -365, e.exposure_day) THEN 1 ELSE 0 END), 0) AS any_event_365d
    FROM exposure e
    LEFT JOIN events_l60d ey
        ON e.dd_device_id_filtered = ey.dd_device_id_filtered
    GROUP BY 1,2,3,4,5,6
)
,
final_analysis AS (
    SELECT 
        pe.tag,
        pe.result,
        pe.dd_device_id_filtered,
        pe.user_id,
        pe.exposure_day,
        pe.exposure_time,
        pe.last_event_date,
        pe.total_events_last_2y,
        pe.any_event_7d,
        pe.any_event_30d,
        pe.any_event_90d,
        pe.any_event_180d,
        pe.any_event_365d,
        CASE 
            WHEN pe.last_event_date IS NULL THEN NULL
            ELSE DATEDIFF('day', pe.last_event_date, pe.exposure_day)
        END AS days_since_last_event,
        CASE 
            WHEN pe.last_event_date IS NULL THEN 'No logged events since ' || TO_VARCHAR(DATEADD('day', -60, $start_date)::date, 'YYYY-MM-DD')
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) = 0 THEN 'Today'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) BETWEEN 1 AND 7 THEN '1-7 days ago'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) BETWEEN 8 AND 30 THEN '8-30 days ago'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) BETWEEN 31 AND 90 THEN '31-90 days ago'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) BETWEEN 91 AND 180 THEN '91-180 days ago'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) BETWEEN 181 AND 365 THEN '181-365 days ago'
            WHEN DATEDIFF('day', pe.last_event_date, pe.exposure_day) > 365 THEN 'More than 365 days ago'
            ELSE 'Other'
        END AS event_recency_bucket
    FROM prior_events pe
)
SELECT * FROM final_analysis
);


-- Summary by treatment/control and event recency buckets
SELECT 
    event_recency_bucket,
    COUNT(*) AS user_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS user_count_pct,
    AVG(days_since_last_event) AS avg_days_since_last_event,
    MEDIAN(days_since_last_event) AS median_days_since_last_event
FROM proddb.fionafan.REONBOARDING_PRIOR_ENGAGEMENT_STATUS
GROUP BY all
ORDER BY 
    CASE 
        WHEN event_recency_bucket = 'Today' THEN 1
        WHEN event_recency_bucket = '1-7 days ago' THEN 2
        WHEN event_recency_bucket = '8-30 days ago' THEN 3
        WHEN event_recency_bucket = '31-90 days ago' THEN 4
        WHEN event_recency_bucket = '91-180 days ago' THEN 5
        WHEN event_recency_bucket = '181-365 days ago' THEN 6
        WHEN event_recency_bucket = 'More than 365 days ago' THEN 7
        WHEN event_recency_bucket LIKE 'No logged events since %' THEN 8
        ELSE 9
    END;


-- Window flag coverage by tag
SELECT 
    tag,
    COUNT(DISTINCT dd_device_id_filtered) AS exposure_devices,
    SUM(any_event_7d) AS engaged_7d,
    SUM(any_event_30d) AS engaged_30d,
    SUM(any_event_90d) AS engaged_90d,
    SUM(any_event_180d) AS engaged_180d,
    SUM(any_event_365d) AS engaged_365d
FROM proddb.fionafan.REONBOARDING_PRIOR_ENGAGEMENT_STATUS
GROUP BY all
ORDER BY tag DESC;


-- Quick sanity checks
SELECT *
FROM proddb.fionafan.REONBOARDING_PRIOR_ENGAGEMENT_STATUS

WHERE event_recency_bucket = '1-7 days ago'
LIMIT 100;


select event_recency_bucket, count(1) from proddb.fionafan.REONBOARDING_PRIOR_ENGAGEMENT_STATUS group by 1;