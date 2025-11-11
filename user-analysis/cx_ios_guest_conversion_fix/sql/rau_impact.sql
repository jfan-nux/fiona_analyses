SET exp_name = 'engagement_milestone_ms1';
SET start_date = '2025-09-29';
SET end_date = '2025-10-31';
SET segment = 'Users';

--------------------- experiment exposure
WITH exposure_order_cart AS (
  SELECT DISTINCT custom_attributes:userId::varchar AS consumer_id
        --    , convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date AS day
           , tag
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE experiment_name = 'cx_ios_guest_conversion_fix'
  AND segment = 'Users'
  AND convert_timezone('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN '2025-09-29' AND '2025-10-31'
)
, exposure_rau AS (

  SELECT DISTINCT bucket_key AS consumer_id
        --    , convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date AS day
           , tag
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE experiment_name = 'engagement_milestone_ms1'
  AND convert_timezone('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN '2025-09-29' AND '2025-10-31'
  and tag in ('Treatment-A', 'control')
)
, exposure AS (
  SELECT a.consumer_id
        , a.tag as order_cart_tag
        , b.tag as rau_tag
  FROM exposure_order_cart a
  INNER JOIN exposure_rau b
    ON a.consumer_id = b.consumer_id
)
, errors AS (
  SELECT DISTINCT consumer_id
    --   , convert_timezone('UTC','America/Los_Angeles', iguazu_sent_at)::date AS day
      , name as error_type
  FROM  IGUAZU.CONSUMER.CX_PAGE_LOAD
  WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_sent_at) BETWEEN '2025-09-29' AND '2025-10-31'
  AND PLATFORM = 'ios'
  AND IS_SUCCESS = false
)

, daily_errors AS (
  SELECT exposure.rau_tag, exposure.order_cart_tag, errors.error_type
          , COUNT(errors.consumer_id) AS total_errors
  FROM errors
  JOIN exposure
    ON errors.consumer_id = exposure.consumer_id
  GROUP BY 1,2,3
)

, daily_exposure AS (
  SELECT rau_tag, order_cart_tag
          , COUNT(DISTINCT consumer_id) AS total_exposures
  FROM exposure
  GROUP BY 1,2
)

SELECT
  daily_errors.rau_tag,
  daily_errors.order_cart_tag,
  daily_errors.error_type,
  -- Cast to a floating point number for the division
  daily_errors.total_errors,
  daily_exposure.total_exposures,
  CAST(daily_errors.total_errors AS REAL) / daily_exposure.total_exposures AS daily_error_rate
FROM daily_errors
JOIN daily_exposure 
  ON daily_errors.rau_tag = daily_exposure.rau_tag
  AND daily_errors.order_cart_tag = daily_exposure.order_cart_tag
ORDER BY 1,2,3;



