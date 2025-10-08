WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_reonboarding__Users_exposures
  ), onboarding_flow_view_click /* Fetch metric events and dimensions */ AS (
    SELECT
      *
    FROM metrics_repo.public.onboarding_flow_view_click AS onboarding_flow_view_click
    WHERE
      CAST(onboarding_flow_view_click.event_ts AS DATETIME) BETWEEN CAST('2025-09-08T20:04:00' AS DATETIME) AND CAST('2025-11-03T20:04:00' AS DATETIME)
  ), onboarding_type_dimension AS (
    SELECT
      *
    FROM metrics_repo.public.onboarding_type_dimension AS onboarding_type_dimension
  ), onboarding_flow_view_click_NonWindow_exp_events /* Join exposures with events */ AS (
    SELECT
      exposure.*,
      onboarding_flow_view_click.*,
      onboarding_type_dimension.dim_onboarding_type AS onboarding_type_dimension_dim_onboarding_type
    FROM exposure
    LEFT JOIN onboarding_flow_view_click
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(onboarding_flow_view_click.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, onboarding_flow_view_click.event_ts)
    /* join with dimensions if exist */
    LEFT JOIN onboarding_type_dimension
      ON TO_CHAR(onboarding_flow_view_click.onboarding_type) = TO_CHAR(onboarding_type_dimension.onboarding_type)
    WHERE
      1 = 1
  ), onboarding_flow_view_click_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      bucket_key,
      experiment_group,
      onboarding_type_dimension_dim_onboarding_type AS dim_onboarding_type,
      COUNT(DISTINCT page_end_page_click_device_id) AS nux_onboarding_page_end_page_click_numerator,
      COUNT(*) AS nux_onboarding_page_end_page_click_numerator_count
    FROM onboarding_flow_view_click_NonWindow_exp_events
    GROUP BY ALL
    HAVING
      NOT nux_onboarding_page_end_page_click_numerator IS NULL
  )
  SELECT
    bucket_key,
    experiment_group,
    dim_onboarding_type,
    IFF(
      onboarding_flow_view_click_NonWindow_exp_metrics.nux_onboarding_page_end_page_click_numerator IS NULL,
      0,
      onboarding_flow_view_click_NonWindow_exp_metrics.nux_onboarding_page_end_page_click_numerator
    ) AS nux_onboarding_page_end_page_click_numerator,
    onboarding_flow_view_click_NonWindow_exp_metrics.nux_onboarding_page_end_page_click_numerator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-08' AS DATE), LEAST(CURRENT_DATE, CAST('2025-11-03' AS DATE))) AS num_days
  FROM onboarding_flow_view_click_NonWindow_exp_metrics
), global_lift AS (
  WITH onboarding_flow_view_click AS (
    SELECT
      *
    FROM metrics_repo.public.onboarding_flow_view_click AS onboarding_flow_view_click
    WHERE
      CAST(onboarding_flow_view_click.event_ts AS DATETIME) BETWEEN CAST('2025-09-08T20:04:00' AS DATETIME) AND CAST('2025-11-03T20:04:00' AS DATETIME)
  ), onboarding_flow_view_click_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(DISTINCT TO_CHAR(onboarding_flow_view_click.device_id)) AS global_denominator,
      COUNT(DISTINCT page_end_page_click_device_id) AS global_numerator
    FROM onboarding_flow_view_click
  )
  SELECT
    global_denominator,
    global_numerator
  FROM onboarding_flow_view_click_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_reonboarding__Users_exposures
  ), onboarding_flow_view_click AS (
    SELECT
      *
    FROM metrics_repo.public.onboarding_flow_view_click AS onboarding_flow_view_click
    WHERE
      CAST(onboarding_flow_view_click.event_ts AS DATE) BETWEEN '2025-09-01' AND '2025-09-07'
  ), onboarding_type_dimension AS (
    SELECT
      *
    FROM metrics_repo.public.onboarding_type_dimension AS onboarding_type_dimension
  ), onboarding_flow_view_click_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      bucket_key,
      dim_onboarding_type,
      COUNT(DISTINCT page_end_page_click_device_id) AS nux_onboarding_page_end_page_click_cuped_7_days_numerator,
      COUNT(*) AS nux_onboarding_page_end_page_click_cuped_7_days_numerator_count
    FROM (
      SELECT
        onboarding_type_dimension.dim_onboarding_type,
        onboarding_flow_view_click.page_end_page_click_device_id,
        TO_CHAR(exposure.bucket_key) AS bucket_key
      FROM exposure
      JOIN onboarding_flow_view_click
        ON TO_CHAR(exposure.bucket_key) = TO_CHAR(onboarding_flow_view_click.device_id)
      /* join with dimensions if exist */
      LEFT JOIN onboarding_type_dimension
        ON TO_CHAR(onboarding_flow_view_click.onboarding_type) = TO_CHAR(onboarding_type_dimension.onboarding_type)
    )
    GROUP BY ALL
  )
  SELECT
    onboarding_flow_view_click_NonWindow_exp_metrics.nux_onboarding_page_end_page_click_cuped_7_days_numerator,
    onboarding_flow_view_click_NonWindow_exp_metrics.nux_onboarding_page_end_page_click_cuped_7_days_numerator_count,
    dim_onboarding_type,
    bucket_key
  FROM onboarding_flow_view_click_NonWindow_exp_metrics
), raw_dimension_data AS (
  SELECT
    metric_data.bucket_key,
    metric_data.experiment_group,
    metric_data.dim_onboarding_type,
    CAST(COALESCE(nux_onboarding_page_end_page_click_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS nux_onboarding_page_end_page_click_cuped_7_days_numerator,
    metric_data.nux_onboarding_page_end_page_click_numerator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
    AND metric_data.dim_onboarding_type = cuped_data_lag_7.dim_onboarding_type
  WHERE
    NOT nux_onboarding_page_end_page_click_numerator IS NULL
    AND NOT entity_unit_distinct IS NULL
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
        AND (
          raw_dimension_data.dim_onboarding_type IN ('new_user', 'resurrected_user')
        )
    ), bucket_key_level_data_w_dimension_filtered AS (
      SELECT
        *
      FROM processed_dimension_data /* Skip dedupe for ITEMIZED */
    ), bucket_key_level_data_global_denominator AS (
      SELECT
        bucket_key,
        experiment_group,
        SUM(nux_onboarding_page_end_page_click_numerator) AS nux_onboarding_page_end_page_click_numerator,
        SUM(nux_onboarding_page_end_page_click_cuped_7_days_numerator) AS nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        1 AS entity_unit_distinct
      FROM raw_dimension_data
      WHERE
        1 = 1
      GROUP BY ALL
    ), global_denominator_moments_info AS (
      SELECT
        'all_data' AS experiment_group,
        COUNT(*) AS global_denominator_count,
        AVG(entity_unit_distinct) AS global_mean_nux_onboarding_page_end_page_click_denominator, /* Metric denominator global moments */
        VARIANCE(entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator,
        AVG(entity_unit_distinct) AS global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator, /* Covariates denominator global moments */
        COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator /* Metric Covariates denominator global cross-moments */
      FROM bucket_key_level_data_global_denominator
      GROUP BY ALL
      UNION
      SELECT
        experiment_group,
        COUNT(*) AS global_denominator_count,
        AVG(entity_unit_distinct) AS global_mean_nux_onboarding_page_end_page_click_denominator, /* Metric denominator global moments */
        VARIANCE(entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator,
        AVG(entity_unit_distinct) AS global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator, /* Covariates denominator global moments */
        COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator /* Metric Covariates denominator global cross-moments */
      FROM bucket_key_level_data_global_denominator
      GROUP BY ALL
    ), dimension_moments_data AS (
      SELECT
        'dim_onboarding_type' || '_' || COALESCE(TO_CHAR(dim_onboarding_type), 'NULL') AS variant_name,
        'all_data' AS experiment_group,
        ANY_VALUE(global_denominator_moments_info.global_denominator_count) AS data_count,
        COUNT(*) /* local moments used to compute global moments */ / ANY_VALUE(global_denominator_moments_info.global_denominator_count) AS sample_ratio,
        AVG(num_data.nux_onboarding_page_end_page_click_numerator) AS local_mean_nux_onboarding_page_end_page_click_numerator, /* Metric local moments */
        AVG(den_data.entity_unit_distinct) AS local_mean_nux_onboarding_page_end_page_click_denominator,
        VARIANCE(num_data.nux_onboarding_page_end_page_click_numerator) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator,
        AVG(num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator) AS local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator, /* Covariates local moments */
        AVG(den_data.entity_unit_distinct) AS local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        COVAR_SAMP(den_data.entity_unit_distinct, den_data.entity_unit_distinct) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator, /* Metric Covariates denominator local cross-moments */
        COVAR_SAMP(den_data.entity_unit_distinct, den_data.entity_unit_distinct) AS local_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          den_data.entity_unit_distinct,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator,
        local_mean_nux_onboarding_page_end_page_click_numerator /* global moments */ /* Metric moments */ * sample_ratio AS mean_nux_onboarding_page_end_page_click_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
        ) AS mean_nux_onboarding_page_end_page_click_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * local_mean_nux_onboarding_page_end_page_click_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator
        ) AS cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * (
          local_mean_nux_onboarding_page_end_page_click_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator,
        local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator /* Covariates moments */ * sample_ratio AS mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * (
          local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator /* Metric Covariates cross-moments */ * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * (
          local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * (
          local_mean_nux_onboarding_page_end_page_click_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
      FROM bucket_key_level_data_w_dimension_filtered AS num_data
      LEFT JOIN global_denominator_moments_info
        ON global_denominator_moments_info.experiment_group = 'all_data'
      LEFT JOIN bucket_key_level_data_global_denominator AS den_data
        ON den_data.bucket_key = num_data.bucket_key
      GROUP BY ALL
      UNION
      SELECT
        'dim_onboarding_type' || '_' || COALESCE(TO_CHAR(dim_onboarding_type), 'NULL') AS variant_name,
        num_data.experiment_group,
        ANY_VALUE(global_denominator_moments_info.global_denominator_count) AS data_count,
        COUNT(*) /* local moments used to compute global moments */ / ANY_VALUE(global_denominator_moments_info.global_denominator_count) AS sample_ratio,
        AVG(num_data.nux_onboarding_page_end_page_click_numerator) AS local_mean_nux_onboarding_page_end_page_click_numerator, /* Metric local moments */
        AVG(den_data.entity_unit_distinct) AS local_mean_nux_onboarding_page_end_page_click_denominator,
        VARIANCE(num_data.nux_onboarding_page_end_page_click_numerator) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator,
        AVG(num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator) AS local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator, /* Covariates local moments */
        AVG(den_data.entity_unit_distinct) AS local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        COVAR_SAMP(den_data.entity_unit_distinct, den_data.entity_unit_distinct) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator, /* Metric Covariates denominator local cross-moments */
        COVAR_SAMP(den_data.entity_unit_distinct, den_data.entity_unit_distinct) AS local_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          num_data.nux_onboarding_page_end_page_click_numerator,
          den_data.entity_unit_distinct
        ) AS local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        COVAR_SAMP(
          den_data.entity_unit_distinct,
          num_data.nux_onboarding_page_end_page_click_cuped_7_days_numerator
        ) AS local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator,
        local_mean_nux_onboarding_page_end_page_click_numerator /* global moments */ /* Metric moments */ * sample_ratio AS mean_nux_onboarding_page_end_page_click_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
        ) AS mean_nux_onboarding_page_end_page_click_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * local_mean_nux_onboarding_page_end_page_click_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator
        ) AS cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * (
          local_mean_nux_onboarding_page_end_page_click_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator,
        local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator /* Covariates moments */ * sample_ratio AS mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * (
          local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator /* Metric Covariates cross-moments */ * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * sample_ratio * (
          1 - sample_ratio
        ) AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
        local_cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_numerator * (
          local_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
        local_cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator * sample_ratio + local_mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator * (
          local_mean_nux_onboarding_page_end_page_click_denominator - ANY_VALUE(
            global_denominator_moments_info.global_mean_nux_onboarding_page_end_page_click_denominator
          )
        ) * sample_ratio AS cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator,
        ANY_VALUE(
          global_denominator_moments_info.global_cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
        ) AS cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
      FROM bucket_key_level_data_w_dimension_filtered AS num_data
      LEFT JOIN global_denominator_moments_info
        ON num_data.experiment_group = global_denominator_moments_info.experiment_group
      LEFT JOIN bucket_key_level_data_global_denominator AS den_data
        ON den_data.experiment_group = num_data.experiment_group
        AND den_data.bucket_key = num_data.bucket_key
      GROUP BY ALL
    )
    SELECT
      variant_name,
      experiment_group,
      data_count,
      mean_nux_onboarding_page_end_page_click_numerator,
      mean_nux_onboarding_page_end_page_click_denominator,
      cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_numerator,
      cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_denominator,
      cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_denominator,
      mean_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
      mean_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
      cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
      cov_nux_onboarding_page_end_page_click_cuped_7_days_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
      cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
      cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_numerator,
      cov_nux_onboarding_page_end_page_click_numerator_nux_onboarding_page_end_page_click_cuped_7_days_denominator,
      cov_nux_onboarding_page_end_page_click_cuped_7_days_numerator_nux_onboarding_page_end_page_click_denominator,
      cov_nux_onboarding_page_end_page_click_denominator_nux_onboarding_page_end_page_click_cuped_7_days_denominator
    FROM dimension_moments_data
  )
)
SELECT
  *
FROM all_variants_moments
CROSS JOIN global_lift
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY variant_name, experiment_group ORDER BY 1) = 1;



select case when dd_device_id is null then 0 else 1 end as dd_device_id, count(1) 
from dimension_deliveries where ACTIVE_DATE>='2025-09-04' group by all;

 select * from segment_events_raw.consumer_production.order_cart_submit_received limit 10;
select case when dd_device_id is null then 0 else 1 end as dd_device_id, count(distinct dd_delivery_id) 
from segment_events_raw.consumer_production.order_cart_submit_received 

where TIMESTAMP>= '2025-09-04' 
group by all;


select case when a.dd_device_id is null then 0 else 1 end as order_cart,
case when dd.dd_device_id is null then 0 else 1 end as delivery,
 count(distinct delivery_id) 

FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
    ON a.order_cart_id = dd.order_cart_id
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at)>= '2025-09-04' 
    group by all;
select max(created_at) from segment_events_raw.consumer_production.order_cart_submit_received;
select 26570065 / (26570065+156075831);
select 180286988 / (180286988+21126009);



select case when dd_device_id is null then 0 else 1 end as dd_device_id, count(distinct delivery_id) 
from edw.finance.fact_deliveries where is_filtered_core = 1 and ACTIVE_DATE>='2025-09-04' group by all;