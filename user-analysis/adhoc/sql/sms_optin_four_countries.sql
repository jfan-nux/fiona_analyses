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
        sms.order_updates_status = 'on'
        OR sms.marketing_status = 'on'
      ) THEN base.consumer_id
    END
  ) AS consumers_sms_opt_in,
  ROUND(
    COUNT(
      DISTINCT CASE
        WHEN (
          sms.order_updates_status = 'on'
          OR sms.marketing_status = 'on'
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
