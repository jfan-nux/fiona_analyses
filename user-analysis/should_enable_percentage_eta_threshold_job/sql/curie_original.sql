WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.should_enable_percentage_eta_threshold_job_ios_exposures

  ), consumer_volume_curie /* Fetch metric events and dimensions */ AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATETIME) BETWEEN DATEADD(DAY, -28, '2025-09-10T00:00:00') AND CAST('2025-10-15T00:00:00' AS DATETIME)
  ), growth_notif_cx_dimensions AS (
    SELECT
      *
    FROM metrics_repo.public.growth_notif_cx_dimensions_scd_consumer_id AS growth_notif_cx_dimensions
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_exp_events /* Join exposures with events */ AS (
    SELECT
      exposure.*,
      consumer_volume_curie.*,
      growth_notif_cx_dimensions.cx_lifestage AS growth_notif_cx_dimensions_cx_lifestage
    FROM exposure
    LEFT JOIN consumer_volume_curie
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(consumer_volume_curie.consumer_id)
      AND consumer_volume_curie.dte BETWEEN DATEADD(DAY, -28, '2025-09-22') AND DATEADD(DAY, -1, '2025-09-22')
    /* join with dimensions if exist */
    LEFT JOIN growth_notif_cx_dimensions
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(growth_notif_cx_dimensions.consumer_id)
      AND growth_notif_cx_dimensions.start_timestamp < DATEADD(SECOND, 180, exposure.first_exposure_time)
      AND DATEADD(SECOND, 180, exposure.first_exposure_time) <= growth_notif_cx_dimensions.end_timestamp
    WHERE
      1 = 1
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      bucket_key,
      experiment_group,
      growth_notif_cx_dimensions_cx_lifestage AS cx_lifestage,
      COUNT(DISTINCT active_consumer_volume_curie) AS consumers_mau_numerator,
      COUNT(*) AS consumers_mau_numerator_count
    FROM consumer_volume_curie_ROLLING_28_1_DAY_True_exp_events
    GROUP BY ALL
    HAVING
      NOT consumers_mau_numerator IS NULL
  )
  SELECT
    bucket_key,
    experiment_group,
    cx_lifestage,
    IFF(
      consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_numerator IS NULL,
      0,
      consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_numerator
    ) AS consumers_mau_numerator,
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_numerator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-10' AS DATE), LEAST(CURRENT_DATE, CAST('2025-10-15' AS DATE))) AS num_days
  FROM consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics
), global_lift AS (
  WITH consumer_volume_curie AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATETIME) BETWEEN DATEADD(DAY, -28, '2025-09-10T00:00:00') AND CAST('2025-10-15T00:00:00' AS DATETIME)
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(DISTINCT TO_CHAR(consumer_volume_curie.consumer_id)) AS global_denominator,
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
    FROM METRICS_REPO.PUBLIC.should_enable_percentage_eta_threshold_job_ios_exposures
  ), consumer_volume_curie AS (
    SELECT
      *
    FROM proddb.public.firefly_consumer_volume_curie_measure AS consumer_volume_curie
    WHERE
      CAST(consumer_volume_curie.dte AS DATE) BETWEEN DATEADD(DAY, -28, '2025-08-06') AND '2025-08-12'
  ), growth_notif_cx_dimensions AS (
    SELECT
      *
    FROM metrics_repo.public.growth_notif_cx_dimensions_scd_consumer_id AS growth_notif_cx_dimensions
  ), consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      bucket_key,
      cx_lifestage,
      COUNT(DISTINCT active_consumer_volume_curie) AS consumers_mau_cuped_7_days_numerator,
      COUNT(*) AS consumers_mau_cuped_7_days_numerator_count
    FROM (
      SELECT
        growth_notif_cx_dimensions.cx_lifestage,
        consumer_volume_curie.active_consumer_volume_curie,
        TO_CHAR(exposure.bucket_key) AS bucket_key
      FROM exposure
      JOIN consumer_volume_curie
        ON TO_CHAR(exposure.bucket_key) = TO_CHAR(consumer_volume_curie.consumer_id)
      /* join with dimensions if exist */
      LEFT JOIN growth_notif_cx_dimensions
        ON TO_CHAR(exposure.bucket_key) = TO_CHAR(growth_notif_cx_dimensions.consumer_id)
    )
    GROUP BY ALL
  )
  SELECT
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_cuped_7_days_numerator,
    consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics.consumers_mau_cuped_7_days_numerator_count,
    cx_lifestage,
    bucket_key
  FROM consumer_volume_curie_ROLLING_28_1_DAY_True_exp_metrics
), raw_dimension_data AS (
  SELECT
    metric_data.bucket_key,
    metric_data.experiment_group,
    metric_data.cx_lifestage,
    CAST(COALESCE(consumers_mau_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS consumers_mau_cuped_7_days_numerator,
    metric_data.consumers_mau_numerator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
    AND metric_data.cx_lifestage = cuped_data_lag_7.cx_lifestage
  WHERE
    NOT consumers_mau_numerator IS NULL AND NOT entity_unit_distinct IS NULL
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
      FROM bucket_key_level_data_w_dimension_filtered
      GROUP BY ALL
      UNION
      SELECT
        'cx_lifestage' || '_' || COALESCE(TO_CHAR(cx_lifestage), 'NULL') AS variant_name,
        experiment_group,
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