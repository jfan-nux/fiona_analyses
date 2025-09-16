WITH exposure AS (
SELECT distinct ee.tag
              , ee.bucket_key
              , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
              , case when cast(custom_attributes:consumer_id as varchar) not like 'dx_%' then cast(custom_attributes:consumer_id as varchar) else null end as consumer_id
              , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = $exp_name
AND experiment_version = $version
and segment = 'Users'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY 1,2,3,4
)

, login_success_overall AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , SOCIAL_PROVIDER AS Source
from segment_events_RAW.consumer_production.social_login_success
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
AND SOCIAL_PROVIDER IN ('google-plus','facebook','apple')

UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'email' AS source
from segment_events_RAW.consumer_production.doordash_login_success  
WHERE  convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
 
UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_known' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
 AND type = 'login'
 AND sub_Type = 'bypass_login_wrong_credentials'
 AND bypass_login_category = 'bypass_login_magiclink'

UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_unknown' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
 AND type = 'login'
 AND sub_Type = 'bypass_login_wrong_credentials'
 AND bypass_login_category = 'bypass_login_unknown'


UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_option_known' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
 AND type = 'login'
 AND sub_Type = 'bypass_login_option'
 AND BYPASS_LOGIN_CATEGORY = 'bypass_login_magiclink'
 
UNION 

SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'bypass_login_option_unknown' AS source
from segment_events_raw.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
 AND type = 'login'
 AND sub_Type = 'bypass_login_option'
 AND BYPASS_LOGIN_CATEGORY = 'bypass_login_unknown' 


UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'guided_login' AS source
from segment_events_RAW.consumer_production.be_login_success  
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
AND sub_type = 'guided_login_v2'
)


, signup_success_overall  AS
( SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , SOCIAL_PROVIDER AS Source
from segment_events_RAW.consumer_production.social_login_new_user 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
AND SOCIAL_PROVIDER IN ('google-plus','facebook','apple')

UNION 

SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                         else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , convert_timezone('UTC','America/Los_Angeles',timestamp)::date AS day
       , 'email' AS source
from segment_events_RAW.consumer_production.doordash_signup_success 
WHERE convert_timezone('UTC','America/Los_Angeles',timestamp) BETWEEN $start_date AND $end_date
)

, adjust_links_straight_to_app AS (
  SELECT 
    DISTINCT 
    replace(lower(CASE WHEN context_device_id like 'dx_%' then context_device_id else 'dx_'||context_device_id end), '-') AS app_device_id
    ,convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp)::date AS day
    ,replace(lower(CASE WHEN split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) like 'dx_%' then split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) else 'dx_'||split_part(split_part(DEEP_LINK_URL,'dd_device_id%3D',2),'%',1) end), '-') as mweb_id
  FROM iguazu.server_events_production.m_deep_link
  WHERE DEEP_LINK_URL like '%device_id%'
    AND convert_timezone('UTC','America/Los_Angeles',iguazu_timestamp) BETWEEN $start_date AND $end_date
)

, adjust_link_app_store AS (  
SELECT distinct app_device_id
, day
, CASE WHEN mweb_id like 'dx_%' then replace(lower(CASE WHEN mweb_id like 'dx_%' then mweb_id else 'dx_'||mweb_id end), '-') else mweb_id end as mweb_id
from (
SELECT distinct replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id else 'dx_'||dd_device_id end), '-') AS app_device_id
, split_part(split_part(event_properties,'web_consumer_id%3D',2),'%',1) as mweb_id
-- , split_part(split_part(event_properties, 'adjust_source%3D',2),'%',1) as adjust_source
-- , split_part(split_part(event_properties, 'pageType%3D',2),'%',1) as page_type
, event_date as day 
FROM edw.growth.fact_singular_mobile_events 
WHERE 1=1
    AND event_properties LIKE '%web_consumer_id%'
and event_date BETWEEN $start_date AND $end_date
order by event_date desc
)
)

, adjust_links AS (
  SELECT * FROM adjust_links_straight_to_app a 
  -- UNION ALL 
  -- SELECT * FROM adjust_link_app_store
)

----- Including both app_device_id and mobile_device_id, some mobile_device_id doesn't have corresponding apple_device_id
, exposure_with_both_ids_device as (
  SELECT DISTINCT e.*
    , ac.app_device_id
  FROM exposure e
    JOIN adjust_links ac 
    ON e.dd_device_ID_filtered = ac.mweb_id 
 --   OR e.consumer_id = ac.mweb_id
    AND e.day <= ac.day
)

, exposure_with_both_ids_consumer as (
  SELECT DISTINCT e.*
    , ac.app_device_id
  FROM exposure e
    JOIN adjust_links ac 
    ON e.consumer_id = ac.mweb_id
    AND e.day <= ac.day
)

, app_exposure_with_both_ids as (
select *
from exposure_with_both_ids_consumer
union all 
select * 
from exposure_with_both_ids_device
)

, exposure_with_both_ids as (
select e.*
    , ac.app_device_id
FROM exposure e
LEFT JOIN app_exposure_with_both_ids ac 
ON e.dd_device_ID_filtered = ac.dd_device_ID_filtered 
);



--cx_app_quality_action_load_latency_web
WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_action_load_duration /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_action_load_duration AS cx_app_quality_action_load_duration
    WHERE
      CAST(cx_app_quality_action_load_duration.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
  ), cx_app_quality_action_load_duration_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(web_action_load_latency_ID) AS cx_app_quality_action_load_latency_web_numerator,
      COUNT(*) AS cx_app_quality_action_load_latency_web_numerator_count,
      IFF(COUNT(web_action_load_duration_ID) = 0, NULL, COUNT(web_action_load_duration_ID)) AS cx_app_quality_action_load_latency_web_denominator,
      COUNT(*) AS cx_app_quality_action_load_latency_web_denominator_count
    FROM exposure
    LEFT JOIN cx_app_quality_action_load_duration
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_action_load_duration.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, cx_app_quality_action_load_duration.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_numerator_count,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_denominator,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_denominator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-12' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-30' AS DATE))) AS num_days
  FROM cx_app_quality_action_load_duration_NonWindow_exp_metrics
), global_lift AS (
  WITH cx_app_quality_action_load_duration AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_action_load_duration AS cx_app_quality_action_load_duration
    WHERE
      CAST(cx_app_quality_action_load_duration.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
  ), cx_app_quality_action_load_duration_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(web_action_load_latency_ID) AS global_numerator,
      IFF(COUNT(web_action_load_duration_ID) = 0, NULL, COUNT(web_action_load_duration_ID)) AS global_denominator
    FROM cx_app_quality_action_load_duration
  )
  SELECT
    global_denominator,
    global_numerator
  FROM cx_app_quality_action_load_duration_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_action_load_duration AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_action_load_duration AS cx_app_quality_action_load_duration
    WHERE
      CAST(cx_app_quality_action_load_duration.event_ts AS DATE) BETWEEN '2025-09-05' AND '2025-09-11'
  ), cx_app_quality_action_load_duration_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(web_action_load_latency_ID) AS cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
      COUNT(*) AS cx_app_quality_action_load_latency_web_cuped_7_days_numerator_count,
      IFF(COUNT(web_action_load_duration_ID) = 0, NULL, COUNT(web_action_load_duration_ID)) AS cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
      COUNT(*) AS cx_app_quality_action_load_latency_web_cuped_7_days_denominator_count
    FROM exposure
    JOIN cx_app_quality_action_load_duration
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_action_load_duration.device_id)
    GROUP BY ALL
  )
  SELECT
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_cuped_7_days_numerator_count,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
    cx_app_quality_action_load_duration_NonWindow_exp_metrics.cx_app_quality_action_load_latency_web_cuped_7_days_denominator_count,
    bucket_key
  FROM cx_app_quality_action_load_duration_NonWindow_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(cx_app_quality_action_load_latency_web_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    CAST(COALESCE(cx_app_quality_action_load_latency_web_cuped_7_days_denominator, -1) AS FLOAT) AS cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
    metric_data.cx_app_quality_action_load_latency_web_numerator,
    metric_data.cx_app_quality_action_load_latency_web_denominator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT cx_app_quality_action_load_latency_web_numerator IS NULL
    AND NOT cx_app_quality_action_load_latency_web_denominator IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_action_load_latency_web_numerator) AS mean_cx_app_quality_action_load_latency_web_numerator, /* Metric moments */
  AVG(cx_app_quality_action_load_latency_web_denominator) AS mean_cx_app_quality_action_load_latency_web_denominator,
  VARIANCE(cx_app_quality_action_load_latency_web_numerator) AS var_cx_app_quality_action_load_latency_web_numerator,
  VARIANCE(cx_app_quality_action_load_latency_web_denominator) AS var_cx_app_quality_action_load_latency_web_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_denominator,
  AVG(cx_app_quality_action_load_latency_web_cuped_7_days_numerator) AS mean_cx_app_quality_action_load_latency_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_action_load_latency_web_cuped_7_days_denominator) AS mean_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_action_load_latency_web_numerator) AS mean_cx_app_quality_action_load_latency_web_numerator, /* Metric moments */
  AVG(cx_app_quality_action_load_latency_web_denominator) AS mean_cx_app_quality_action_load_latency_web_denominator,
  VARIANCE(cx_app_quality_action_load_latency_web_numerator) AS var_cx_app_quality_action_load_latency_web_numerator,
  VARIANCE(cx_app_quality_action_load_latency_web_denominator) AS var_cx_app_quality_action_load_latency_web_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_denominator,
  AVG(cx_app_quality_action_load_latency_web_cuped_7_days_numerator) AS mean_cx_app_quality_action_load_latency_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_action_load_latency_web_cuped_7_days_denominator) AS mean_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_cuped_7_days_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_numerator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_numerator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_action_load_latency_web_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_action_load_latency_web_denominator,
    cx_app_quality_action_load_latency_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_action_load_latency_web_denominator_cx_app_quality_action_load_latency_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL;

-- cx_app_quality_crash_web
WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_crash_event_optimized /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_crash_event_optimized AS cx_app_quality_crash_event_optimized
    WHERE
      CAST(cx_app_quality_crash_event_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_duration_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_duration_optimized AS cx_app_quality_page_load_duration_optimized
    WHERE
      CAST(cx_app_quality_page_load_duration_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_crash_event_optimized_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(crash_id) AS cx_app_quality_crash_web_numerator,
      COUNT(*) AS cx_app_quality_crash_web_numerator_count
    FROM exposure
    LEFT JOIN cx_app_quality_crash_event_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_crash_event_optimized.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, cx_app_quality_crash_event_optimized.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  ), cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      IFF(COUNT(page_load_duration_ID) = 0, NULL, COUNT(page_load_duration_ID)) AS cx_app_quality_crash_web_denominator,
      COUNT(*) AS cx_app_quality_crash_web_denominator_count
    FROM exposure
    LEFT JOIN cx_app_quality_page_load_duration_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_load_duration_optimized.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, cx_app_quality_page_load_duration_optimized.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_app_quality_crash_event_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_numerator,
    cx_app_quality_crash_event_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_numerator_count,
    cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_denominator,
    cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_denominator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-12' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-30' AS DATE))) AS num_days
  FROM cx_app_quality_crash_event_optimized_NonWindow_exp_metrics
  NATURAL FULL JOIN cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics
), global_lift AS (
  WITH cx_app_quality_crash_event_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_crash_event_optimized AS cx_app_quality_crash_event_optimized
    WHERE
      CAST(cx_app_quality_crash_event_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_duration_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_duration_optimized AS cx_app_quality_page_load_duration_optimized
    WHERE
      CAST(cx_app_quality_page_load_duration_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_crash_event_optimized_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(crash_id) AS global_numerator
    FROM cx_app_quality_crash_event_optimized
  ), cx_app_quality_page_load_duration_optimized_NonWindow_global_metrics AS (
    SELECT
      IFF(COUNT(page_load_duration_ID) = 0, NULL, COUNT(page_load_duration_ID)) AS global_denominator
    FROM cx_app_quality_page_load_duration_optimized
  )
  SELECT
    global_denominator,
    global_numerator
  FROM cx_app_quality_crash_event_optimized_NonWindow_global_metrics
  NATURAL FULL JOIN cx_app_quality_page_load_duration_optimized_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_crash_event_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_crash_event_optimized AS cx_app_quality_crash_event_optimized
    WHERE
      CAST(cx_app_quality_crash_event_optimized.event_ts AS DATE) BETWEEN '2025-09-05' AND '2025-09-11'
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_duration_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_duration_optimized AS cx_app_quality_page_load_duration_optimized
    WHERE
      CAST(cx_app_quality_page_load_duration_optimized.event_ts AS DATE) BETWEEN '2025-09-05' AND '2025-09-11'
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_crash_event_optimized_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(crash_id) AS cx_app_quality_crash_web_cuped_7_days_numerator,
      COUNT(*) AS cx_app_quality_crash_web_cuped_7_days_numerator_count
    FROM exposure
    JOIN cx_app_quality_crash_event_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_crash_event_optimized.device_id)
    GROUP BY ALL
  ), cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      IFF(COUNT(page_load_duration_ID) = 0, NULL, COUNT(page_load_duration_ID)) AS cx_app_quality_crash_web_cuped_7_days_denominator,
      COUNT(*) AS cx_app_quality_crash_web_cuped_7_days_denominator_count
    FROM exposure
    JOIN cx_app_quality_page_load_duration_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_load_duration_optimized.device_id)
    GROUP BY ALL
  )
  SELECT
    cx_app_quality_crash_event_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_cuped_7_days_numerator,
    cx_app_quality_crash_event_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_cuped_7_days_numerator_count,
    cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_cuped_7_days_denominator,
    cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics.cx_app_quality_crash_web_cuped_7_days_denominator_count,
    bucket_key
  FROM cx_app_quality_crash_event_optimized_NonWindow_exp_metrics
  NATURAL FULL JOIN cx_app_quality_page_load_duration_optimized_NonWindow_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(cx_app_quality_crash_web_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS cx_app_quality_crash_web_cuped_7_days_numerator,
    CAST(COALESCE(cx_app_quality_crash_web_cuped_7_days_denominator, -1) AS FLOAT) AS cx_app_quality_crash_web_cuped_7_days_denominator,
    metric_data.cx_app_quality_crash_web_numerator,
    metric_data.cx_app_quality_crash_web_denominator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT cx_app_quality_crash_web_numerator IS NULL
    AND NOT cx_app_quality_crash_web_denominator IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_crash_web_numerator) AS mean_cx_app_quality_crash_web_numerator, /* Metric moments */
  AVG(cx_app_quality_crash_web_denominator) AS mean_cx_app_quality_crash_web_denominator,
  VARIANCE(cx_app_quality_crash_web_numerator) AS var_cx_app_quality_crash_web_numerator,
  VARIANCE(cx_app_quality_crash_web_denominator) AS var_cx_app_quality_crash_web_denominator,
  COVAR_SAMP(cx_app_quality_crash_web_numerator, cx_app_quality_crash_web_denominator) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_denominator,
  AVG(cx_app_quality_crash_web_cuped_7_days_numerator) AS mean_cx_app_quality_crash_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_crash_web_cuped_7_days_denominator) AS mean_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_numerator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_numerator_cx_app_quality_crash_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_denominator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_denominator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_numerator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_numerator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_numerator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_crash_web_numerator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_denominator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_denominator_cx_app_quality_crash_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_crash_web_denominator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_denominator_cx_app_quality_crash_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_crash_web_numerator) AS mean_cx_app_quality_crash_web_numerator, /* Metric moments */
  AVG(cx_app_quality_crash_web_denominator) AS mean_cx_app_quality_crash_web_denominator,
  VARIANCE(cx_app_quality_crash_web_numerator) AS var_cx_app_quality_crash_web_numerator,
  VARIANCE(cx_app_quality_crash_web_denominator) AS var_cx_app_quality_crash_web_denominator,
  COVAR_SAMP(cx_app_quality_crash_web_numerator, cx_app_quality_crash_web_denominator) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_denominator,
  AVG(cx_app_quality_crash_web_cuped_7_days_numerator) AS mean_cx_app_quality_crash_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_crash_web_cuped_7_days_denominator) AS mean_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_numerator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_numerator_cx_app_quality_crash_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_denominator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_denominator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_cuped_7_days_numerator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_cuped_7_days_numerator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_numerator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_crash_web_numerator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_numerator_cx_app_quality_crash_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_crash_web_denominator,
    cx_app_quality_crash_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_crash_web_denominator_cx_app_quality_crash_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_crash_web_denominator,
    cx_app_quality_crash_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_crash_web_denominator_cx_app_quality_crash_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL;


-- cx_app_quality_page_action_error_web
WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_page_action_error /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_action_error AS cx_app_quality_page_action_error
    WHERE
      CAST(cx_app_quality_page_action_error.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
  ), cx_app_quality_page_action_error_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(web_error_action_load_ID) AS cx_app_quality_page_action_error_web_numerator,
      COUNT(*) AS cx_app_quality_page_action_error_web_numerator_count,
      IFF(COUNT(web_cx_action_load_ID) = 0, NULL, COUNT(web_cx_action_load_ID)) AS cx_app_quality_page_action_error_web_denominator,
      COUNT(*) AS cx_app_quality_page_action_error_web_denominator_count
    FROM exposure
    LEFT JOIN cx_app_quality_page_action_error
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_action_error.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, cx_app_quality_page_action_error.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_numerator_count,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_denominator,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_denominator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-12' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-30' AS DATE))) AS num_days
  FROM cx_app_quality_page_action_error_NonWindow_exp_metrics
), global_lift AS (
  WITH cx_app_quality_page_action_error AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_action_error AS cx_app_quality_page_action_error
    WHERE
      CAST(cx_app_quality_page_action_error.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
  ), cx_app_quality_page_action_error_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(web_error_action_load_ID) AS global_numerator,
      IFF(COUNT(web_cx_action_load_ID) = 0, NULL, COUNT(web_cx_action_load_ID)) AS global_denominator
    FROM cx_app_quality_page_action_error
  )
  SELECT
    global_denominator,
    global_numerator
  FROM cx_app_quality_page_action_error_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_page_action_error AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_action_error AS cx_app_quality_page_action_error
    WHERE
      CAST(cx_app_quality_page_action_error.event_ts AS DATE) BETWEEN '2025-09-05' AND '2025-09-11'
  ), cx_app_quality_page_action_error_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(web_error_action_load_ID) AS cx_app_quality_page_action_error_web_cuped_7_days_numerator,
      COUNT(*) AS cx_app_quality_page_action_error_web_cuped_7_days_numerator_count,
      IFF(COUNT(web_cx_action_load_ID) = 0, NULL, COUNT(web_cx_action_load_ID)) AS cx_app_quality_page_action_error_web_cuped_7_days_denominator,
      COUNT(*) AS cx_app_quality_page_action_error_web_cuped_7_days_denominator_count
    FROM exposure
    JOIN cx_app_quality_page_action_error
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_action_error.device_id)
    GROUP BY ALL
  )
  SELECT
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_cuped_7_days_numerator_count,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_cuped_7_days_denominator,
    cx_app_quality_page_action_error_NonWindow_exp_metrics.cx_app_quality_page_action_error_web_cuped_7_days_denominator_count,
    bucket_key
  FROM cx_app_quality_page_action_error_NonWindow_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(cx_app_quality_page_action_error_web_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    CAST(COALESCE(cx_app_quality_page_action_error_web_cuped_7_days_denominator, -1) AS FLOAT) AS cx_app_quality_page_action_error_web_cuped_7_days_denominator,
    metric_data.cx_app_quality_page_action_error_web_numerator,
    metric_data.cx_app_quality_page_action_error_web_denominator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT cx_app_quality_page_action_error_web_numerator IS NULL
    AND NOT cx_app_quality_page_action_error_web_denominator IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_page_action_error_web_numerator) AS mean_cx_app_quality_page_action_error_web_numerator, /* Metric moments */
  AVG(cx_app_quality_page_action_error_web_denominator) AS mean_cx_app_quality_page_action_error_web_denominator,
  VARIANCE(cx_app_quality_page_action_error_web_numerator) AS var_cx_app_quality_page_action_error_web_numerator,
  VARIANCE(cx_app_quality_page_action_error_web_denominator) AS var_cx_app_quality_page_action_error_web_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_denominator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_denominator,
  AVG(cx_app_quality_page_action_error_web_cuped_7_days_numerator) AS mean_cx_app_quality_page_action_error_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_page_action_error_web_cuped_7_days_denominator) AS mean_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_numerator_cx_app_quality_page_action_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_denominator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_numerator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_denominator_cx_app_quality_page_action_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_denominator_cx_app_quality_page_action_error_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_page_action_error_web_numerator) AS mean_cx_app_quality_page_action_error_web_numerator, /* Metric moments */
  AVG(cx_app_quality_page_action_error_web_denominator) AS mean_cx_app_quality_page_action_error_web_denominator,
  VARIANCE(cx_app_quality_page_action_error_web_numerator) AS var_cx_app_quality_page_action_error_web_numerator,
  VARIANCE(cx_app_quality_page_action_error_web_denominator) AS var_cx_app_quality_page_action_error_web_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_denominator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_denominator,
  AVG(cx_app_quality_page_action_error_web_cuped_7_days_numerator) AS mean_cx_app_quality_page_action_error_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_page_action_error_web_cuped_7_days_denominator) AS mean_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_numerator_cx_app_quality_page_action_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_denominator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_cuped_7_days_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_cuped_7_days_numerator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_numerator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_numerator_cx_app_quality_page_action_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_action_error_web_denominator_cx_app_quality_page_action_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_action_error_web_denominator,
    cx_app_quality_page_action_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_action_error_web_denominator_cx_app_quality_page_action_error_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL;


--cx_app_quality_page_action_error_web
WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_page_load_error_optimized /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_error_optimized AS cx_app_quality_page_load_error_optimized
    WHERE
      CAST(cx_app_quality_page_load_error_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(error_page_load_ID) AS cx_app_quality_page_load_error_web_numerator,
      COUNT(*) AS cx_app_quality_page_load_error_web_numerator_count,
      IFF(COUNT(cx_page_load_ID) = 0, NULL, COUNT(cx_page_load_ID)) AS cx_app_quality_page_load_error_web_denominator,
      COUNT(*) AS cx_app_quality_page_load_error_web_denominator_count
    FROM exposure
    LEFT JOIN cx_app_quality_page_load_error_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_load_error_optimized.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, cx_app_quality_page_load_error_optimized.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_numerator_count,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_denominator,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_denominator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-12' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-30' AS DATE))) AS num_days
  FROM cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics
), global_lift AS (
  WITH cx_app_quality_page_load_error_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_error_optimized AS cx_app_quality_page_load_error_optimized
    WHERE
      CAST(cx_app_quality_page_load_error_optimized.event_ts AS DATETIME) BETWEEN CAST('2025-09-12T15:51:00' AS DATETIME) AND CAST('2025-10-30T15:51:00' AS DATETIME)
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_error_optimized_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(error_page_load_ID) AS global_numerator,
      IFF(COUNT(cx_page_load_ID) = 0, NULL, COUNT(cx_page_load_ID)) AS global_denominator
    FROM cx_app_quality_page_load_error_optimized
  )
  SELECT
    global_denominator,
    global_numerator
  FROM cx_app_quality_page_load_error_optimized_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.smart_app_banner_detection__Users_exposures
  ), cx_app_quality_page_load_error_optimized AS (
    SELECT
      *
    FROM metrics_repo.public.cx_app_quality_page_load_error_optimized AS cx_app_quality_page_load_error_optimized
    WHERE
      CAST(cx_app_quality_page_load_error_optimized.event_ts AS DATE) BETWEEN '2025-09-05' AND '2025-09-11'
      AND (
        LOWER(platform) IN ('mobile', 'desktop')
      )
  ), cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(error_page_load_ID) AS cx_app_quality_page_load_error_web_cuped_7_days_numerator,
      COUNT(*) AS cx_app_quality_page_load_error_web_cuped_7_days_numerator_count,
      IFF(COUNT(cx_page_load_ID) = 0, NULL, COUNT(cx_page_load_ID)) AS cx_app_quality_page_load_error_web_cuped_7_days_denominator,
      COUNT(*) AS cx_app_quality_page_load_error_web_cuped_7_days_denominator_count
    FROM exposure
    JOIN cx_app_quality_page_load_error_optimized
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(cx_app_quality_page_load_error_optimized.device_id)
    GROUP BY ALL
  )
  SELECT
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_cuped_7_days_numerator_count,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_cuped_7_days_denominator,
    cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics.cx_app_quality_page_load_error_web_cuped_7_days_denominator_count,
    bucket_key
  FROM cx_app_quality_page_load_error_optimized_NonWindow_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(cx_app_quality_page_load_error_web_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    CAST(COALESCE(cx_app_quality_page_load_error_web_cuped_7_days_denominator, -1) AS FLOAT) AS cx_app_quality_page_load_error_web_cuped_7_days_denominator,
    metric_data.cx_app_quality_page_load_error_web_numerator,
    metric_data.cx_app_quality_page_load_error_web_denominator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT cx_app_quality_page_load_error_web_numerator IS NULL
    AND NOT cx_app_quality_page_load_error_web_denominator IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_page_load_error_web_numerator) AS mean_cx_app_quality_page_load_error_web_numerator, /* Metric moments */
  AVG(cx_app_quality_page_load_error_web_denominator) AS mean_cx_app_quality_page_load_error_web_denominator,
  VARIANCE(cx_app_quality_page_load_error_web_numerator) AS var_cx_app_quality_page_load_error_web_numerator,
  VARIANCE(cx_app_quality_page_load_error_web_denominator) AS var_cx_app_quality_page_load_error_web_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_denominator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_denominator,
  AVG(cx_app_quality_page_load_error_web_cuped_7_days_numerator) AS mean_cx_app_quality_page_load_error_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_page_load_error_web_cuped_7_days_denominator) AS mean_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_numerator_cx_app_quality_page_load_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_denominator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_numerator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_denominator_cx_app_quality_page_load_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_denominator_cx_app_quality_page_load_error_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(cx_app_quality_page_load_error_web_numerator) AS mean_cx_app_quality_page_load_error_web_numerator, /* Metric moments */
  AVG(cx_app_quality_page_load_error_web_denominator) AS mean_cx_app_quality_page_load_error_web_denominator,
  VARIANCE(cx_app_quality_page_load_error_web_numerator) AS var_cx_app_quality_page_load_error_web_numerator,
  VARIANCE(cx_app_quality_page_load_error_web_denominator) AS var_cx_app_quality_page_load_error_web_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_denominator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_denominator,
  AVG(cx_app_quality_page_load_error_web_cuped_7_days_numerator) AS mean_cx_app_quality_page_load_error_web_cuped_7_days_numerator, /* Covariates moments */
  AVG(cx_app_quality_page_load_error_web_cuped_7_days_denominator) AS mean_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_numerator_cx_app_quality_page_load_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_denominator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_cuped_7_days_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_cuped_7_days_numerator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_numerator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_numerator_cx_app_quality_page_load_error_web_cuped_7_days_denominator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_numerator
  ) AS cov_cx_app_quality_page_load_error_web_denominator_cx_app_quality_page_load_error_web_cuped_7_days_numerator,
  COVAR_SAMP(
    cx_app_quality_page_load_error_web_denominator,
    cx_app_quality_page_load_error_web_cuped_7_days_denominator
  ) AS cov_cx_app_quality_page_load_error_web_denominator_cx_app_quality_page_load_error_web_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL