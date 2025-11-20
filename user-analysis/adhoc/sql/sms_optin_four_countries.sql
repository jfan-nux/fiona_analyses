WITH
  consumers_in_scope AS (
    SELECT
      dc.consumer_id,
      CASE UPPER(dc.default_country)
        WHEN 'UNITED STATES' THEN 'US'
        WHEN 'CANADA' THEN 'CA'
        WHEN 'AUSTRALIA' THEN 'AU'
        WHEN 'NEW ZEALAND' THEN 'NZ'
      END AS country_code
    FROM
      edw.consumer.dimension_consumers dc
    WHERE
      dc.experience ILIKE 'doordash'
      AND UPPER(dc.default_country) IN (
        'UNITED STATES',
        'CANADA',
        'AUSTRALIA',
        'NEW ZEALAND'
      )
  ),
  ga_current AS (
    SELECT
      g.consumer_id,
      g.lifestage
    FROM
      edw.growth.consumer_growth_accounting_scd3 g
    WHERE
      g.scd_current_record = TRUE
      AND g.experience ILIKE 'doordash'
      AND UPPER(TRIM(g.business_vertical_line)) = 'OVERALL'
  ),
  sms_current AS (
    SELECT
      s.consumer_id,
      LOWER(s.order_updates_status) AS order_updates_status,
      LOWER(s.marketing_status) AS marketing_status
    FROM
      edw.consumer.dimension_consumer_sms_settings_scd3 s
    WHERE
      s.scd_current_record = TRUE
      AND s.experience ILIKE 'doordash'
  )
SELECT
  base.country_code,
  base.lifestage,
  COUNT(DISTINCT base.consumer_id) AS consumers_total,
  COUNT(
    DISTINCT CASE
      WHEN (
        -- sms.order_updates_status = 'on'
        sms.marketing_status = 'on'
      ) THEN base.consumer_id
    END
  ) AS consumers_sms_opt_in,
  ROUND(
    COUNT(
      DISTINCT CASE
        WHEN (
          -- sms.order_updates_status = 'on'
          sms.marketing_status = 'on'
        ) THEN base.consumer_id
      END
    ) / NULLIF(COUNT(DISTINCT base.consumer_id), 0)::FLOAT,
    4
  ) AS sms_opt_in_rate
FROM
  (
    SELECT
      c.consumer_id,
      c.country_code,
      g.lifestage
    FROM
      consumers_in_scope c
      JOIN ga_current g ON g.consumer_id = c.consumer_id
  ) base
  LEFT JOIN sms_current sms ON sms.consumer_id = base.consumer_id
GROUP BY
  base.country_code,
  base.lifestage
ORDER BY
  base.country_code,
  base.lifestage;




-- Overlap of SMS-opted-in population with exposure to engagement_milestone_ms1
WITH
  exposure AS (
    SELECT DISTINCT
      ee.BUCKET_KEY::VARCHAR AS consumer_id_v
    FROM
      proddb.public.fact_dedup_experiment_exposure ee
    WHERE
      ee.EXPERIMENT_NAME = 'engagement_milestone_ms1'
      AND CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', ee.EXPOSURE_TIME) BETWEEN '2025-07-01' AND '2025-10-31'
      AND ee.TAG IN ('Treatment-A', 'control')
  ),
  opted_in AS (
    -- Current snapshot of consumers opted into any SMS type (marketing or order updates)
    SELECT
      s.CONSUMER_ID,
      dc.DEFAULT_COUNTRY
    FROM
      edw.consumer.dimension_consumer_sms_settings_scd3 s

      JOIN edw.consumer.dimension_consumers dc ON s.CONSUMER_ID = dc.CONSUMER_ID
    WHERE
      s.SCD_CURRENT_RECORD = TRUE
      AND CURRENT_DATE >= s.SCD_START_DATE
      AND (
        s.SCD_END_DATE IS NULL
        OR CURRENT_DATE < s.SCD_END_DATE
      )
      AND (
        -- s.ORDER_UPDATES_STATUS ILIKE 'on'
        s.MARKETING_STATUS ILIKE 'on'
      )
      AND dc.DEFAULT_COUNTRY IN ('Australia', 'New Zealand', 'Canada')
  ),
  exposed_opted AS (
    SELECT
      o.DEFAULT_COUNTRY AS country,
      COUNT(DISTINCT o.CONSUMER_ID) AS exposed_and_opted_in
    FROM
      opted_in o
      JOIN exposure e ON e.consumer_id_v = o.CONSUMER_ID::VARCHAR
    GROUP BY
      o.DEFAULT_COUNTRY
  ),
  opted_pop AS (
    SELECT
      DEFAULT_COUNTRY AS country,
      COUNT(DISTINCT CONSUMER_ID) AS opted_in_total
    FROM
      opted_in
    GROUP BY
      DEFAULT_COUNTRY
  )
SELECT
  p.country,
  p.opted_in_total,
  COALESCE(e.exposed_and_opted_in, 0) AS exposed_and_opted_in,
  CASE
    WHEN p.opted_in_total > 0 THEN COALESCE(e.exposed_and_opted_in, 0)::FLOAT / p.opted_in_total
    ELSE NULL
  END AS overlap_ratio
FROM
  opted_pop p
  LEFT JOIN exposed_opted e ON p.country = e.country
ORDER BY
  overlap_ratio DESC NULLS LAST;
