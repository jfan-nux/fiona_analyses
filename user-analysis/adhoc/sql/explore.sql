select top 10 * from METRICS_REPO.PUBLIC.final_rollout_ios_exposures;
select top 10 * from metrics_repo.public.core_quality_events;

select count(1) from 
METRICS_REPO.PUBLIC.final_rollout_ios_exposures b  
-- inner join  (select * from  metrics_repo.public.core_quality_events where event_ts between '2023-07-31' and '2023-12-01') a
-- on a.consumer_id = b.bucket_key
inner join metrics_repo.public.growth_notif_cx_dimensions_scd_consumer_id c 
ON to_varchar(b.bucket_key) = to_varchar(c.consumer_id)
      AND c.start_timestamp < DATEADD(SECOND, 180, b.first_exposure_time)
;
-- and a.event_ts between b.first_exposure_time and b.first_exposure_time + interval '10 seconds';

WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.final_rollout_ios_exposures
  ), core_quality_events /* Fetch metric events and dimensions */ AS (
    SELECT
      *
    FROM metrics_repo.public.core_quality_events AS core_quality_events
    WHERE
      CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2023-07-31T00:00:00' AS DATETIME) AND CAST('2023-12-01T00:00:00' AS DATETIME)
  ), growth_notif_cx_dimensions AS (
    SELECT
      *
    FROM metrics_repo.public.growth_notif_cx_dimensions_scd_consumer_id AS growth_notif_cx_dimensions
  ), core_quality_events_NonWindow_exp_events /* Join exposures with events */ AS (
    SELECT
      exposure.*,
      core_quality_events.*,
      growth_notif_cx_dimensions.cx_lifestage AS growth_notif_cx_dimensions_cx_lifestage
    FROM exposure
    LEFT JOIN core_quality_events
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.consumer_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)
    /* join with dimensions if exist */
    LEFT JOIN growth_notif_cx_dimensions
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(growth_notif_cx_dimensions.consumer_id)
      AND growth_notif_cx_dimensions.start_timestamp < DATEADD(SECOND, 180, exposure.first_exposure_time)
      AND DATEADD(SECOND, 180, exposure.first_exposure_time) <= growth_notif_cx_dimensions.end_timestamp
    WHERE
      1 = 1
  ), core_quality_events_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      bucket_key,
      experiment_group,
      growth_notif_cx_dimensions_cx_lifestage AS cx_lifestage,
      SUM(is_nd_excl_fraud) AS core_quality_nd_excl_fraud_numerator,
      COUNT(is_nd_excl_fraud) AS core_quality_nd_excl_fraud_numerator_count,
      SUM(is_nd_excl_fraud_denom) AS core_quality_nd_excl_fraud_denominator,
      COUNT(is_nd_excl_fraud_denom) AS core_quality_nd_excl_fraud_denominator_count
    FROM core_quality_events_NonWindow_exp_events
    GROUP BY ALL
    HAVING
      NOT core_quality_nd_excl_fraud_numerator IS NULL
      AND NOT core_quality_nd_excl_fraud_denominator IS NULL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_lifestage,
    IFF(
      core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_numerator IS NULL,
      0,
      core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_numerator
    ) AS core_quality_nd_excl_fraud_numerator,
    core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_numerator_count,
    IFF(
      core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_denominator IS NULL,
      0,
      core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_denominator
    ) AS core_quality_nd_excl_fraud_denominator,
    core_quality_events_NonWindow_exp_metrics.core_quality_nd_excl_fraud_denominator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2023-07-31' AS DATE), LEAST(CURRENT_DATE, CAST('2023-12-01' AS DATE))) AS num_days
  FROM core_quality_events_NonWindow_exp_metrics
), global_lift AS (
  WITH core_quality_events AS (
    SELECT
      *
    FROM metrics_repo.public.core_quality_events AS core_quality_events
    WHERE
      CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2023-07-31T00:00:00' AS DATETIME) AND CAST('2023-12-01T00:00:00' AS DATETIME)
  ), core_quality_events_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      SUM(is_nd_excl_fraud) AS global_numerator,
      SUM(is_nd_excl_fraud_denom) AS global_denominator
    FROM core_quality_events
  )
  SELECT
    global_denominator,
    global_numerator
  FROM core_quality_events_NonWindow_global_metrics
), raw_dimension_data AS (
  SELECT
    metric_data.bucket_key,
    metric_data.experiment_group,
    metric_data.cx_lifestage,
    metric_data.core_quality_nd_excl_fraud_numerator,
    metric_data.core_quality_nd_excl_fraud_denominator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  WHERE
    NOT core_quality_nd_excl_fraud_numerator IS NULL
    AND NOT core_quality_nd_excl_fraud_denominator IS NULL
), all_variants_moments AS (
  SELECT
    *
  FROM (
    WITH processed_dimension_data AS (
      SELECT
        *
      FROM raw_dimension_data
      WHERE
        1 = 1
    ), bucket_key_level_data_w_dimension_filtered AS (
      SELECT
        *
      FROM processed_dimension_data /* Skip dedupe for ITEMIZED */
    ), dimension_moments_data AS (
      SELECT
        'cx_lifestage' || '_' || COALESCE(TO_CHAR(cx_lifestage), 'NULL') AS variant_name,
        'all_data' AS experiment_group,
        COUNT(*) AS data_count,
        AVG(core_quality_nd_excl_fraud_numerator) AS mean_core_quality_nd_excl_fraud_numerator, /* Metric moments */
        AVG(core_quality_nd_excl_fraud_denominator) AS mean_core_quality_nd_excl_fraud_denominator,
        VARIANCE(core_quality_nd_excl_fraud_numerator) AS var_core_quality_nd_excl_fraud_numerator,
        VARIANCE(core_quality_nd_excl_fraud_denominator) AS var_core_quality_nd_excl_fraud_denominator,
        COVAR_SAMP(core_quality_nd_excl_fraud_numerator, core_quality_nd_excl_fraud_denominator) AS cov_core_quality_nd_excl_fraud_numerator_core_quality_nd_excl_fraud_denominator
      FROM bucket_key_level_data_w_dimension_filtered
      GROUP BY ALL
      UNION
      SELECT
        'cx_lifestage' || '_' || COALESCE(TO_CHAR(cx_lifestage), 'NULL') AS variant_name,
        experiment_group,
        COUNT(*) AS data_count,
        AVG(core_quality_nd_excl_fraud_numerator) AS mean_core_quality_nd_excl_fraud_numerator, /* Metric moments */
        AVG(core_quality_nd_excl_fraud_denominator) AS mean_core_quality_nd_excl_fraud_denominator,
        VARIANCE(core_quality_nd_excl_fraud_numerator) AS var_core_quality_nd_excl_fraud_numerator,
        VARIANCE(core_quality_nd_excl_fraud_denominator) AS var_core_quality_nd_excl_fraud_denominator,
        COVAR_SAMP(core_quality_nd_excl_fraud_numerator, core_quality_nd_excl_fraud_denominator) AS cov_core_quality_nd_excl_fraud_numerator_core_quality_nd_excl_fraud_denominator
      FROM bucket_key_level_data_w_dimension_filtered
      GROUP BY ALL
    )
    SELECT
      *
    FROM dimension_moments_data
    QUALIFY
      RANK() OVER (PARTITION BY experiment_group ORDER BY variant_name DESC) <= 30
  )
)
SELECT
  *
FROM all_variants_moments
CROSS JOIN global_lift
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY variant_name, experiment_group ORDER BY 1) = 1;



  SELECT DISTINCT
  cast(iguazu_timestamp as date) AS day,
  consumer_id,
  DD_DEVICE_ID,
  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID else 'dx_'||DD_DEVICE_ID end), '-') as dd_device_id_filtered,
  dd_platform,
  lower(onboarding_type) as onboarding_type,
  promo_title,
  'start_page' as onboarding_page
FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice

WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -14 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
  AND ((lower(onboarding_type) = 'new_user') OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user'))


SET analysis_name='cx_ios_guest_conversion_fix__Users';
SET var_name='control';

SELECT 
  a.dimension_value AS Lifestage,
  a.exposures AS Exposures,
  a.metric_impact_absolute AS Impact_Abs,
  a.metric_value AS control_Rate,
  (a.exposures*a.metric_value)::integer AS control_mau
FROM dimension_experiment_analysis_results a 

INNER JOIN 
dimension_experiment_analyses b 
ON a.analysis_name=b.analysis_name
WHERE TRUE 
AND metric_name='consumers_mau'
AND dimension_name='cx_lifestage'
AND dimension_value IS NOT NULL
AND metric_impact_absolute IS NOT NULL
AND b.health_check_result_detailed:imbalance::VARCHAR='PASSED'
AND b.health_check_result_detailed:flicker::VARCHAR='PASSED'
AND a.analysis_name=$analysis_name
-- AND a.variant_name='control'
ORDER BY dimension_value;

select count(1)
from  dimension_experiment_analysis_results a 
INNER JOIN 
dimension_experiment_analyses b 
WHERE TRUE 
AND metric_name='consumers_mau'
AND dimension_name='cx_lifestage'
AND dimension_value IS NOT NULL
AND metric_impact_absolute IS NOT NULL
AND a.analysis_name='cx_ios_guest_conversion_fix';
-- AND a.variant_name=$var_name

-- Lifestage split percentage by exposure date using growth accounting table
SET exp_name = 'cx_ios_guest_conversion_fix';
SET start_date = '2025-09-29';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

WITH exposure AS (
    SELECT 
        ee.tag,
        ee.result,
        ee.custom_attributes:userId::VARCHAR AS consumer_id,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day,
        MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS EXPOSURE_TIME,
        
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
        AND experiment_version::INT = $version
        AND segment = $segment
        AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY all
),

exposures_with_lifestage AS (
    SELECT 

        exp.consumer_id,
        exp.EXPOSURE_TIME,
        exp.day AS exposure_date,
        scd.LIFESTAGE
    FROM exposure exp
    LEFT JOIN EDW.GROWTH.CONSUMER_GROWTH_ACCOUNTING_SCD3 scd
        ON exp.consumer_id = TO_CHAR(scd.CONSUMER_ID)
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', scd.SCD_START_DATE)::DATE <= exp.day
        AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', scd.SCD_END_DATE)::DATE >= exp.day
)

SELECT 
    -- exposure_date,
    LIFESTAGE,
    COUNT(*) AS exposure_count,
    COUNT(*) / SUM(COUNT(*)) OVER () AS pct_overall
FROM exposures_with_lifestage
GROUP BY LIFESTAGE
ORDER BY LIFESTAGE;
