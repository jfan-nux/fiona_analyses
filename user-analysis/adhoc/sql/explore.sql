select top 10 * from METRICS_REPO.PUBLIC.final_rollout_ios_exposures;
select top 10 * from metrics_repo.public.core_quality_events;

select count(1) from (select * from  metrics_repo.public.core_quality_events where event_ts between '2023-07-31' and '2023-12-01') a
inner join  METRICS_REPO.PUBLIC.final_rollout_ios_exposures b  
on a.consumer_id = b.bucket_key
inner join metrics_repo.public.growth_notif_cx_dimensions_scd_consumer_id c 
ON TO_CHAR(b.bucket_key) = TO_CHAR(c.consumer_id)
      AND c.start_timestamp < DATEADD(SECOND, 180, b.first_exposure_time)
;
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
  ROW_NUMBER() OVER (PARTITION BY variant_name, experiment_group ORDER BY 1) = 1