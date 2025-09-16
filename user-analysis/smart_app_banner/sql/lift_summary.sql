-- Smart App Banner: Treatment vs Control lift summary (skip CUPED)
-- Final output columns: metric, exposure_treatment, exposure_control, treatment_value, control_value, lift

-- Parameters (adjust as needed)
SET exp_name = 'smart_app_banner_detection';
SET start_date = '2025-09-15';
SET end_date = CURRENT_DATE;
SET version = 1;

WITH 
-- Exposure with both IDs, ported from explore.sql
exposure AS (
  SELECT DISTINCT 
      ee.tag AS experiment_group,
      TO_CHAR(ee.bucket_key) AS bucket_key,
      REPLACE(LOWER(CASE WHEN ee.bucket_key LIKE 'dx_%' THEN ee.bucket_key ELSE 'dx_'||ee.bucket_key END), '-') AS dd_device_id_filtered,
      CASE WHEN CAST(ee.custom_attributes:consumer_id AS VARCHAR) NOT LIKE 'dx_%' THEN CAST(ee.custom_attributes:consumer_id AS VARCHAR) ELSE NULL END AS consumer_id,
      MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)::date) AS day,
      MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)) AS first_exposure_time
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = $exp_name
    AND ee.experiment_version::INT = $version
    AND ee.segment = 'Users'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)::date BETWEEN $start_date AND $end_date
  GROUP BY 1,2,3,4
),
adjust_links AS (
  -- Straight-to-app deep links (same as adjust_links_straight_to_app in explore.sql)
  SELECT DISTINCT 
    REPLACE(LOWER(CASE WHEN context_device_id LIKE 'dx_%' THEN context_device_id ELSE 'dx_'||context_device_id END), '-') AS app_device_id,
    CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day,
    REPLACE(
      LOWER(
        CASE WHEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1) LIKE 'dx_%'
             THEN SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1)
             ELSE 'dx_'||SPLIT_PART(SPLIT_PART(deep_link_url,'dd_device_id%3D',2),'%',1)
        END
      ), '-'
    ) AS mweb_id
  FROM iguazu.server_events_production.m_deep_link
  WHERE deep_link_url LIKE '%device_id%'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date BETWEEN $start_date AND $end_date
),
exposure_with_both_ids_device AS (
  SELECT DISTINCT e.*, ac.app_device_id
  FROM exposure e
  JOIN adjust_links ac
    ON e.dd_device_id_filtered = ac.mweb_id
   AND e.day <= ac.day
),
exposure_with_both_ids_consumer AS (
  SELECT DISTINCT e.*, ac.app_device_id
  FROM exposure e
  JOIN adjust_links ac
    ON e.consumer_id = ac.mweb_id
   AND e.day <= ac.day
),
app_exposure_with_both_ids AS (
  SELECT * FROM exposure_with_both_ids_consumer
  UNION ALL
  SELECT * FROM exposure_with_both_ids_device
),
exposure_with_both_ids AS (
  SELECT e.*, ac.app_device_id
  FROM exposure e
  LEFT JOIN app_exposure_with_both_ids ac
    ON e.dd_device_id_filtered = ac.dd_device_id_filtered
),
exposure_both AS (
  SELECT * FROM exposure_with_both_ids WHERE app_device_id IS NOT NULL
),
-- Debug counts by group (optional)
-- select experiment_group, count(1) from exposure_both group by all;
-- Metric 1: cx_app_quality_action_load_latency_web (rate = latency events / action load duration events)
m1_events AS (
  SELECT *
  FROM metrics_repo.public.cx_app_quality_action_load_duration
  WHERE CAST(event_ts AS DATE) BETWEEN $start_date AND $end_date
),
m1_by_group AS (
  SELECT 
    e.experiment_group,
    COUNT(m.web_action_load_latency_id)                           AS nume,
    IFF(COUNT(m.web_action_load_duration_id)=0,NULL,COUNT(m.web_action_load_duration_id)) AS deno,
    COUNT(DISTINCT e.bucket_key)                                  AS exposure_cnt
  FROM exposure_both e
  LEFT JOIN m1_events m
    ON TO_CHAR(m.device_id) = e.bucket_key
       AND e.first_exposure_time <= DATEADD(SECOND, 10, m.event_ts)
  GROUP BY ALL
),
m1_agg AS (
  SELECT 
    'cx_app_quality_action_load_latency_web' AS metric,
    MAX(CASE WHEN experiment_group='treatment' THEN exposure_cnt END) AS exposure_treatment,
    MAX(CASE WHEN experiment_group='control'   THEN exposure_cnt END) AS exposure_control,
    NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN deno END),0) AS treatment_value,
    NULLIF(MAX(CASE WHEN experiment_group='control'   THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='control'   THEN deno END),0) AS control_value
  FROM m1_by_group
  GROUP BY metric
),

-- Metric 2: cx_app_quality_crash_web (rate = crash events / page load duration events)
m2_crash AS (
  SELECT *
  FROM metrics_repo.public.cx_app_quality_crash_event_optimized
  WHERE CAST(event_ts AS DATE) BETWEEN $start_date AND $end_date
    AND LOWER(platform) IN ('mobile','desktop')
),
m2_page AS (
  SELECT *
  FROM metrics_repo.public.cx_app_quality_page_load_duration_optimized
  WHERE CAST(event_ts AS DATE) BETWEEN $start_date AND $end_date
    AND LOWER(platform) IN ('mobile','desktop')
),
m2_by_group AS (
  SELECT 
    e.experiment_group,
    COUNT(c.crash_id) AS nume,
    IFF(COUNT(p.page_load_duration_id)=0,NULL,COUNT(p.page_load_duration_id)) AS deno,
    COUNT(DISTINCT e.bucket_key) AS exposure_cnt
  FROM exposure_both e
  LEFT JOIN m2_crash c
    ON TO_CHAR(c.device_id) = e.bucket_key
       AND e.first_exposure_time <= DATEADD(SECOND, 10, c.event_ts)
  LEFT JOIN m2_page p
    ON TO_CHAR(p.device_id) = e.bucket_key
       AND e.first_exposure_time <= DATEADD(SECOND, 10, p.event_ts)
  GROUP BY ALL
),
m2_agg AS (
  SELECT 
    'cx_app_quality_crash_web' AS metric,
    MAX(CASE WHEN experiment_group='treatment' THEN exposure_cnt END) AS exposure_treatment,
    MAX(CASE WHEN experiment_group='control'   THEN exposure_cnt END) AS exposure_control,
    NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN deno END),0) AS treatment_value,
    NULLIF(MAX(CASE WHEN experiment_group='control'   THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='control'   THEN deno END),0) AS control_value
  FROM m2_by_group
  GROUP BY metric
),

-- Metric 3: cx_app_quality_page_action_error_web (rate = error actions / page actions)
m3_events AS (
  SELECT *
  FROM metrics_repo.public.cx_app_quality_page_action_error
  WHERE CAST(event_ts AS DATE) BETWEEN $start_date AND $end_date
),
m3_by_group AS (
  SELECT 
    e.experiment_group,
    COUNT(m.web_error_action_load_id) AS nume,
    IFF(COUNT(m.web_cx_action_load_id)=0,NULL,COUNT(m.web_cx_action_load_id)) AS deno,
    COUNT(DISTINCT e.bucket_key) AS exposure_cnt
  FROM exposure_both e
  LEFT JOIN m3_events m
    ON TO_CHAR(m.device_id) = e.bucket_key
       AND e.first_exposure_time <= DATEADD(SECOND, 10, m.event_ts)
  GROUP BY ALL
),
m3_agg AS (
  SELECT 
    'cx_app_quality_page_action_error_web' AS metric,
    MAX(CASE WHEN experiment_group='treatment' THEN exposure_cnt END) AS exposure_treatment,
    MAX(CASE WHEN experiment_group='control'   THEN exposure_cnt END) AS exposure_control,
    NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN deno END),0) AS treatment_value,
    NULLIF(MAX(CASE WHEN experiment_group='control'   THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='control'   THEN deno END),0) AS control_value
  FROM m3_by_group
  GROUP BY metric
),

-- Metric 4: cx_app_quality_page_load_error_web (rate = page load errors / page loads)
m4_events AS (
  SELECT *
  FROM metrics_repo.public.cx_app_quality_page_load_error_optimized
  WHERE CAST(event_ts AS DATE) BETWEEN $start_date AND $end_date
),
m4_by_group AS (
  SELECT 
    e.experiment_group,
    COUNT(m.error_page_load_id) AS nume,
    IFF(COUNT(m.cx_page_load_id)=0,NULL,COUNT(m.cx_page_load_id)) AS deno,
    COUNT(DISTINCT e.bucket_key) AS exposure_cnt
  FROM exposure_both e
  LEFT JOIN m4_events m
    ON TO_CHAR(m.device_id) = e.bucket_key
       AND e.first_exposure_time <= DATEADD(SECOND, 10, m.event_ts)
  GROUP BY ALL
),
m4_agg AS (
  SELECT 
    'cx_app_quality_page_load_error_web' AS metric,
    MAX(CASE WHEN experiment_group='treatment' THEN exposure_cnt END) AS exposure_treatment,
    MAX(CASE WHEN experiment_group='control'   THEN exposure_cnt END) AS exposure_control,
    NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='treatment' THEN deno END),0) AS treatment_value,
    NULLIF(MAX(CASE WHEN experiment_group='control'   THEN nume END),0)
      / NULLIF(MAX(CASE WHEN experiment_group='control'   THEN deno END),0) AS control_value
  FROM m4_by_group
  GROUP BY metric
)

SELECT 
  metric,
  exposure_treatment,
  exposure_control,
  treatment_value,
  control_value,
  IFF(control_value IS NULL, NULL, (treatment_value - control_value)/NULLIF(control_value,0)) AS lift
FROM (
  SELECT * FROM m1_agg
  UNION ALL
  SELECT * FROM m2_agg
  UNION ALL
  SELECT * FROM m3_agg
  UNION ALL
  SELECT * FROM m4_agg
) x
ORDER BY metric;


