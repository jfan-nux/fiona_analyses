WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_guest_conversion_fix__Users_exposures
  ), consumer_volume_curie /* Fetch metric events */ AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATETIME) BETWEEN DATEADD(DAY, -28, '2025-09-29T19:02:00') AND CAST('2025-10-31T19:00:00' AS DATETIME)
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(DISTINCT active_consumer_volume_curie) AS consumers_mau_numerator,
      COUNT(*) AS consumers_mau_numerator_count
    FROM exposure
    LEFT JOIN consumer_volume_curie
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(consumer_volume_curie.device_id)
      AND consumer_volume_curie.dte BETWEEN DATEADD(DAY, -28, '2025-10-21') AND DATEADD(DAY, -1, '2025-10-21')
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_numerator,
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_numerator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-29' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-31' AS DATE))) AS num_days
  FROM consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics
), global_lift AS (
  WITH consumer_volume_curie AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATETIME) BETWEEN DATEADD(DAY, -28, '2025-09-29T19:02:00') AND CAST('2025-10-31T19:00:00' AS DATETIME)
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(DISTINCT TO_CHAR(consumer_volume_curie.device_id)) AS global_denominator,
      COUNT(DISTINCT active_consumer_volume_curie) AS global_numerator
    FROM consumer_volume_curie
  )
  SELECT
    global_denominator,
    global_numerator
  FROM consumer_volume_curie_ROLLING_28_1_DAY_True_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_guest_conversion_fix__Users_exposures
  ), consumer_volume_curie AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATE) BETWEEN DATEADD(DAY, -28, '2025-08-25') AND '2025-08-31'
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(DISTINCT active_consumer_volume_curie) AS consumers_mau_cuped_7_days_numerator,
      COUNT(*) AS consumers_mau_cuped_7_days_numerator_count
    FROM exposure
    JOIN consumer_volume_curie
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(consumer_volume_curie.device_id)
    GROUP BY ALL
  )
  SELECT
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_cuped_7_days_numerator,
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_cuped_7_days_numerator_count,
    bucket_key
  FROM consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(consumers_mau_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS consumers_mau_cuped_7_days_numerator,
    metric_data.consumers_mau_numerator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT consumers_mau_numerator IS NULL AND NOT entity_unit_distinct IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(consumers_mau_numerator) AS mean_consumers_mau_numerator, /* Metric moments */
  AVG(entity_unit_distinct) AS mean_consumers_mau_denominator,
  VARIANCE(consumers_mau_numerator) AS var_consumers_mau_numerator,
  VARIANCE(entity_unit_distinct) AS var_consumers_mau_denominator,
  COVAR_SAMP(consumers_mau_numerator, entity_unit_distinct) AS cov_consumers_mau_numerator_consumers_mau_denominator,
  AVG(consumers_mau_cuped_7_days_numerator) AS mean_consumers_mau_cuped_7_days_numerator, /* Covariates moments */
  AVG(entity_unit_distinct) AS mean_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_cuped_7_days_numerator, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_cuped_7_days_numerator_consumers_mau_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_consumers_mau_cuped_7_days_denominator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_cuped_7_days_numerator, entity_unit_distinct) AS cov_consumers_mau_cuped_7_days_numerator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_numerator, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_numerator_consumers_mau_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(consumers_mau_numerator, entity_unit_distinct) AS cov_consumers_mau_numerator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(entity_unit_distinct, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_denominator_consumers_mau_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_consumers_mau_denominator_consumers_mau_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(consumers_mau_numerator) AS mean_consumers_mau_numerator, /* Metric moments */
  AVG(entity_unit_distinct) AS mean_consumers_mau_denominator,
  VARIANCE(consumers_mau_numerator) AS var_consumers_mau_numerator,
  VARIANCE(entity_unit_distinct) AS var_consumers_mau_denominator,
  COVAR_SAMP(consumers_mau_numerator, entity_unit_distinct) AS cov_consumers_mau_numerator_consumers_mau_denominator,
  AVG(consumers_mau_cuped_7_days_numerator) AS mean_consumers_mau_cuped_7_days_numerator, /* Covariates moments */
  AVG(entity_unit_distinct) AS mean_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_cuped_7_days_numerator, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_cuped_7_days_numerator_consumers_mau_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_consumers_mau_cuped_7_days_denominator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_cuped_7_days_numerator, entity_unit_distinct) AS cov_consumers_mau_cuped_7_days_numerator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(consumers_mau_numerator, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_numerator_consumers_mau_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(consumers_mau_numerator, entity_unit_distinct) AS cov_consumers_mau_numerator_consumers_mau_cuped_7_days_denominator,
  COVAR_SAMP(entity_unit_distinct, consumers_mau_cuped_7_days_numerator) AS cov_consumers_mau_denominator_consumers_mau_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_consumers_mau_denominator_consumers_mau_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL;

