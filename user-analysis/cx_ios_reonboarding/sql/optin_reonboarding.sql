WITH metric_data AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_reonboarding__Users_exposures
  ), push_email_sms_optin /* Fetch metric events */ AS (
    SELECT
      *
    FROM metrics_repo.public.push_email_sms_optin AS push_email_sms_optin
    WHERE
      CAST(push_email_sms_optin.event_ts AS DATETIME) BETWEEN CAST('2025-09-08T18:19:00' AS DATETIME) AND CAST('2025-11-18T18:19:00' AS DATETIME)
      AND (
        event_ts >= CURRENT_DATE - 1
      )
  ), push_email_sms_optin_NonWindow_exp_metrics /* Aggregate events and compute metrics and covariates */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      exposure.experiment_group,
      COUNT(DISTINCT push_optin_overall_device_id) AS optin_push_overall_device_id_numerator,
      COUNT(*) AS optin_push_overall_device_id_numerator_count
    FROM exposure
    LEFT JOIN push_email_sms_optin
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(push_email_sms_optin.device_id)
      AND exposure.first_exposure_time <= DATEADD(SECOND, 10, push_email_sms_optin.event_ts)
    WHERE
      1 = 1
    GROUP BY ALL
  )
  SELECT
    bucket_key,
    experiment_group,
    push_email_sms_optin_NonWindow_exp_metrics.optin_push_overall_device_id_numerator,
    push_email_sms_optin_NonWindow_exp_metrics.optin_push_overall_device_id_numerator_count,
    1 AS entity_unit_distinct,
    DATEDIFF(DAY, CAST('2025-09-08' AS DATE), LEAST(CURRENT_DATE, CAST('2025-11-18' AS DATE))) AS num_days
  FROM push_email_sms_optin_NonWindow_exp_metrics
), global_lift AS (
  WITH push_email_sms_optin AS (
    SELECT
      *
    FROM metrics_repo.public.push_email_sms_optin AS push_email_sms_optin
    WHERE
      CAST(push_email_sms_optin.event_ts AS DATETIME) BETWEEN CAST('2025-09-08T18:19:00' AS DATETIME) AND CAST('2025-11-18T18:19:00' AS DATETIME)
      AND (
        event_ts >= CURRENT_DATE - 1
      )
  ), push_email_sms_optin_NonWindow_global_metrics /* aggregate numerator and denominator for global lift */ AS (
    SELECT
      COUNT(DISTINCT TO_CHAR(push_email_sms_optin.device_id)) AS global_denominator,
      COUNT(DISTINCT push_optin_overall_device_id) AS global_numerator
    FROM push_email_sms_optin
  )
  SELECT
    global_denominator,
    global_numerator
  FROM push_email_sms_optin_NonWindow_global_metrics
), cuped_data_lag_7 AS (
  /* Fetch experiment exposures */
  WITH exposure AS (
    SELECT
      *
    FROM METRICS_REPO.PUBLIC.cx_ios_reonboarding__Users_exposures
  ), push_email_sms_optin AS (
    SELECT
      *
    FROM metrics_repo.public.push_email_sms_optin AS push_email_sms_optin
    WHERE
      CAST(push_email_sms_optin.event_ts AS DATE) BETWEEN '2025-09-16' AND '2025-09-22'
      AND (
        event_ts >= CURRENT_DATE - 1
      )
  ), push_email_sms_optin_NonWindow_exp_metrics /* Aggregate events and compute metrics */ AS (
    SELECT
      TO_CHAR(exposure.bucket_key) AS bucket_key,
      COUNT(DISTINCT push_optin_overall_device_id) AS optin_push_overall_device_id_cuped_7_days_numerator,
      COUNT(*) AS optin_push_overall_device_id_cuped_7_days_numerator_count
    FROM exposure
    JOIN push_email_sms_optin
      ON TO_CHAR(exposure.bucket_key) = TO_CHAR(push_email_sms_optin.device_id)
    GROUP BY ALL
  )
  SELECT
    push_email_sms_optin_NonWindow_exp_metrics.optin_push_overall_device_id_cuped_7_days_numerator,
    push_email_sms_optin_NonWindow_exp_metrics.optin_push_overall_device_id_cuped_7_days_numerator_count,
    bucket_key
  FROM push_email_sms_optin_NonWindow_exp_metrics
), all_data AS (
  SELECT
    metric_data.bucket_key AS bucket_key,
    experiment_group,
    CAST(COALESCE(optin_push_overall_device_id_cuped_7_days_numerator, -1) /* covariates and cuped aggreates */ AS FLOAT) AS optin_push_overall_device_id_cuped_7_days_numerator,
    metric_data.optin_push_overall_device_id_numerator,
    metric_data.entity_unit_distinct,
    metric_data.num_days
  FROM metric_data
  LEFT JOIN cuped_data_lag_7
    ON metric_data.bucket_key = cuped_data_lag_7.bucket_key
  WHERE
    NOT optin_push_overall_device_id_numerator IS NULL
    AND NOT entity_unit_distinct IS NULL
)
SELECT
  'all_data' AS experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(optin_push_overall_device_id_numerator) AS mean_optin_push_overall_device_id_numerator, /* Metric moments */
  AVG(entity_unit_distinct) AS mean_optin_push_overall_device_id_denominator,
  VARIANCE(optin_push_overall_device_id_numerator) AS var_optin_push_overall_device_id_numerator,
  VARIANCE(entity_unit_distinct) AS var_optin_push_overall_device_id_denominator,
  COVAR_SAMP(optin_push_overall_device_id_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_denominator,
  AVG(optin_push_overall_device_id_cuped_7_days_numerator) AS mean_optin_push_overall_device_id_cuped_7_days_numerator, /* Covariates moments */
  AVG(entity_unit_distinct) AS mean_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(
    optin_push_overall_device_id_cuped_7_days_numerator,
    optin_push_overall_device_id_cuped_7_days_numerator
  ) AS cov_optin_push_overall_device_id_cuped_7_days_numerator_optin_push_overall_device_id_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_optin_push_overall_device_id_cuped_7_days_denominator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(optin_push_overall_device_id_cuped_7_days_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_cuped_7_days_numerator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(
    optin_push_overall_device_id_numerator,
    optin_push_overall_device_id_cuped_7_days_numerator
  ) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(optin_push_overall_device_id_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(entity_unit_distinct, optin_push_overall_device_id_cuped_7_days_numerator) AS cov_optin_push_overall_device_id_denominator_optin_push_overall_device_id_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_optin_push_overall_device_id_denominator_optin_push_overall_device_id_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
UNION
SELECT
  experiment_group,
  ANY_VALUE(global_lift.global_numerator) AS global_numerator,
  ANY_VALUE(global_lift.global_denominator) AS global_denominator,
  COUNT(*) AS data_count,
  AVG(optin_push_overall_device_id_numerator) AS mean_optin_push_overall_device_id_numerator, /* Metric moments */
  AVG(entity_unit_distinct) AS mean_optin_push_overall_device_id_denominator,
  VARIANCE(optin_push_overall_device_id_numerator) AS var_optin_push_overall_device_id_numerator,
  VARIANCE(entity_unit_distinct) AS var_optin_push_overall_device_id_denominator,
  COVAR_SAMP(optin_push_overall_device_id_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_denominator,
  AVG(optin_push_overall_device_id_cuped_7_days_numerator) AS mean_optin_push_overall_device_id_cuped_7_days_numerator, /* Covariates moments */
  AVG(entity_unit_distinct) AS mean_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(
    optin_push_overall_device_id_cuped_7_days_numerator,
    optin_push_overall_device_id_cuped_7_days_numerator
  ) AS cov_optin_push_overall_device_id_cuped_7_days_numerator_optin_push_overall_device_id_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_optin_push_overall_device_id_cuped_7_days_denominator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(optin_push_overall_device_id_cuped_7_days_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_cuped_7_days_numerator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(
    optin_push_overall_device_id_numerator,
    optin_push_overall_device_id_cuped_7_days_numerator
  ) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_cuped_7_days_numerator, /* Metric Covariates cross-moments */
  COVAR_SAMP(optin_push_overall_device_id_numerator, entity_unit_distinct) AS cov_optin_push_overall_device_id_numerator_optin_push_overall_device_id_cuped_7_days_denominator,
  COVAR_SAMP(entity_unit_distinct, optin_push_overall_device_id_cuped_7_days_numerator) AS cov_optin_push_overall_device_id_denominator_optin_push_overall_device_id_cuped_7_days_numerator,
  COVAR_SAMP(entity_unit_distinct, entity_unit_distinct) AS cov_optin_push_overall_device_id_denominator_optin_push_overall_device_id_cuped_7_days_denominator
FROM all_data
CROSS JOIN global_lift
GROUP BY ALL