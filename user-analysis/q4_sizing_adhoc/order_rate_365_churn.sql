-- Count consumers who haven't ordered in 365+ days (includes never-ordered)
-- Filters: DoorDash experience, non-guest, not blacklisted, non-internal emails, US only
-- Data sources: proddb.public.dimension_consumer, proddb.public.dimension_deliveries (core orders)

WITH base_consumers AS (
  SELECT CAST(id AS NUMBER) AS consumer_id
  FROM proddb.public.dimension_consumer
  WHERE experience = 'doordash'
    AND is_guest = FALSE
    AND is_blacklisted = FALSE
    AND sanitized_email NOT LIKE '%@doordash.com'
    AND default_country_id IN (1) -- US
)
, recent_orderers AS (
  SELECT DISTINCT creator_id AS consumer_id
  FROM proddb.public.dimension_deliveries
  WHERE is_filtered_core = TRUE
    AND created_at::date >= DATEADD(day, -365, CURRENT_DATE)
)
, no_recent AS (
  SELECT b.consumer_id
  FROM base_consumers b
  LEFT JOIN recent_orderers r USING (consumer_id)
  WHERE r.consumer_id IS NULL
)
SELECT 
  bc.total_consumers,
  nr.no_recent_consumers,
  ROUND(nr.no_recent_consumers / NULLIF(bc.total_consumers, 0) * 100, 2) AS pct_no_order_365d
FROM (SELECT COUNT(*) AS total_consumers FROM base_consumers) bc
CROSS JOIN (SELECT COUNT(*) AS no_recent_consumers FROM no_recent) nr;

-- Notes:
-- - Change the country filter if you need non-US or global results
-- - This counts anyone with zero core orders in the last 365 days (including never-ordered)

-------------------------------------------------------------------------
-- Build master orders-by-consumer-by-day table for 2024-06-30 to 2025-09-30
-- Then derive weekly cohorts from 2025-06-30 (Mon) through end of Aug 2025
-------------------------------------------------------------------------

-- 1) Orders per consumer per day (only days with orders are stored)
CREATE OR REPLACE TABLE proddb.fionafan.consumer_day_orders_20240630_20250930 AS
WITH deliveries AS (
    SELECT
        dd.creator_id AS consumer_id,
        dd.active_date AS order_date,
        dd.delivery_id
    FROM proddb.public.dimension_deliveries dd
    WHERE dd.is_filtered_core = TRUE
      AND dd.country_id = 1 -- US
      AND dd.active_date BETWEEN '2024-06-30'::date AND '2025-09-30'::date
)
SELECT
    consumer_id,
    order_date,
    COUNT(DISTINCT delivery_id) AS orders_on_day
FROM deliveries
GROUP BY all
ORDER BY consumer_id, order_date;

select lifestage_bucket, lifestage, count(1) from edw.growth.consumer_growth_accounting_scd3 where scd_end_date = '2025-09-18' group by all;
-- Weekly engagement aggregates from fact_unique_visitors_full_pt, 2025-06-30 to 2025-08-31
CREATE OR REPLACE TABLE proddb.fionafan.events_weekly_20250630_20250831 AS
SELECT
    DATE_TRUNC('week', v.event_date) AS week_start,
    DATEADD('day', 6, DATE_TRUNC('week', v.event_date)) AS week_end,
    v.user_id,
    1 AS had_app_open_week,
    MAX(CASE WHEN COALESCE(v.unique_store_content_page_visitor, 0) = 1 THEN 1 ELSE 0 END) AS had_store_content_impr_week,
    MAX(CASE WHEN COALESCE(v.unique_store_page_visitor, 0) = 1 THEN 1 ELSE 0 END) AS had_store_page_visit_week
FROM proddb.public.fact_unique_visitors_full_pt v
WHERE v.event_date BETWEEN '2025-06-30'::date AND '2025-08-31'::date
GROUP BY all;

-- 2) Weekly cohort: as of each Monday, count consumers with no orders in prior 365d
--    and how many of those converted within the week (Mon-Sun), weeks through 2025-08-31
CREATE OR REPLACE TABLE proddb.fionafan.weekly_no_order_365d_and_conversions_20250630_20250831 AS
WITH weeks AS (
    SELECT
        DATEADD('day', 7 * seq4(), '2025-06-30'::date) AS week_start,
        LEAST(DATEADD('day', 6 + 7 * seq4(), '2025-06-30'::date), '2025-08-31'::date) AS week_end
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
    WHERE week_start <= '2025-08-31'::date
)
, base_consumers AS (
    SELECT
        CAST(id AS NUMBER) AS consumer_id,
        user_id,
        created_date
    FROM proddb.public.dimension_consumer
    WHERE experience = 'doordash'
      AND is_guest = FALSE
      AND is_blacklisted = FALSE
      AND sanitized_email NOT LIKE '%@doordash.com'
      AND default_country_id IN (1) -- US
)
, no_recent_by_week AS (
    SELECT
        w.week_start,
        w.week_end,
        COUNT(*) AS no_order_365d_count
    FROM base_consumers bc
    CROSS JOIN weeks w
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o
      ON o.consumer_id = bc.consumer_id
     AND o.order_date BETWEEN DATEADD('day', -365, w.week_start) AND DATEADD('day', -1, w.week_start)
    WHERE bc.created_date <= DATEADD('day', -365, w.week_start)
      AND o.consumer_id IS NULL
    GROUP BY all
)
, converted_by_week AS (
    SELECT
        w.week_start,
        w.week_end,
        COUNT(DISTINCT bc.consumer_id) AS converted_count,
        SUM(COALESCE(o_week.orders_on_day, 0)) AS total_orders_during_week
    FROM base_consumers bc
    CROSS JOIN weeks w
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o_prior
      ON o_prior.consumer_id = bc.consumer_id
     AND o_prior.order_date BETWEEN DATEADD('day', -365, w.week_start) AND DATEADD('day', -1, w.week_start)
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o_week
      ON o_week.consumer_id = bc.consumer_id
     AND o_week.order_date BETWEEN w.week_start AND w.week_end
    WHERE bc.created_date <= DATEADD('day', -365, w.week_start)
      AND o_prior.consumer_id IS NULL
      AND o_week.consumer_id IS NOT NULL
    GROUP BY all
)
, engagement_by_week AS (
    SELECT
        w.week_start,
        w.week_end,
        COUNT(DISTINCT CASE WHEN ev.had_app_open_week = 1 THEN bc.consumer_id END) AS cohort_app_open_any_count,
        COUNT(DISTINCT CASE WHEN ev.had_store_content_impr_week = 1 THEN bc.consumer_id END) AS cohort_store_content_impr_any_count,
        COUNT(DISTINCT CASE WHEN ev.had_store_page_visit_week = 1 THEN bc.consumer_id END) AS cohort_store_page_visit_any_count
    FROM base_consumers bc
    CROSS JOIN weeks w
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o
      ON o.consumer_id = bc.consumer_id
     AND o.order_date BETWEEN DATEADD('day', -365, w.week_start) AND DATEADD('day', -1, w.week_start)
    LEFT JOIN proddb.fionafan.events_weekly_20250630_20250831 ev
      ON ev.user_id = bc.consumer_id
     AND ev.week_start = w.week_start
    WHERE bc.created_date <= DATEADD('day', -365, w.week_start)
      AND o.consumer_id IS NULL
    GROUP BY all
)
SELECT
    n.week_start,
    n.week_end,
    n.no_order_365d_count,
    COALESCE(c.converted_count, 0) AS converted_during_week_count,
    COALESCE(c.total_orders_during_week, 0) AS total_orders_during_week,
    ROUND(COALESCE(c.converted_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS conversion_rate,
    ROUND(COALESCE(c.total_orders_during_week, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS order_rate,
    COALESCE(e.cohort_app_open_any_count, 0) AS cohort_app_open_any_count,
    COALESCE(e.cohort_store_content_impr_any_count, 0) AS cohort_store_content_impr_any_count,
    COALESCE(e.cohort_store_page_visit_any_count, 0) AS cohort_store_page_visit_any_count,
    ROUND(COALESCE(e.cohort_app_open_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS app_open_rate,
    ROUND(COALESCE(e.cohort_store_content_impr_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS store_content_impr_rate,
    ROUND(COALESCE(e.cohort_store_page_visit_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS store_page_visit_rate
FROM no_recent_by_week n
LEFT JOIN converted_by_week c
  ON n.week_start = c.week_start
 AND n.week_end   = c.week_end
LEFT JOIN engagement_by_week e
  ON n.week_start = e.week_start
 AND n.week_end   = e.week_end
ORDER BY week_start;

-- Preview results
SELECT *
FROM proddb.fionafan.weekly_no_order_365d_and_conversions_20250630_20250831
ORDER BY week_start;

-------------------------------------------------------------------------
-- Monthly views
-- 1) Monthly engagement aggregates from fact_unique_visitors_full_pt
-- 2) Monthly cohort for no-orders-in-365d and conversions
-- 3) Monthly cohorts by lifestage (New Cx, Non-Purchaser)
-------------------------------------------------------------------------

-- 1) Events monthly
CREATE OR REPLACE TABLE proddb.fionafan.events_monthly_202507_202508 AS
SELECT
    DATE_TRUNC('month', v.event_date) AS month_start,
    DATEADD('day', -1, DATEADD('month', 1, DATE_TRUNC('month', v.event_date))) AS month_end,
    v.user_id,
    1 AS had_app_open_month,
    MAX(CASE WHEN COALESCE(v.unique_store_content_page_visitor, 0) = 1 THEN 1 ELSE 0 END) AS had_store_content_impr_month,
    MAX(CASE WHEN COALESCE(v.unique_store_page_visitor, 0) = 1 THEN 1 ELSE 0 END) AS had_store_page_visit_month
FROM proddb.public.fact_unique_visitors_full_pt v
WHERE v.event_date BETWEEN '2025-07-01'::date AND '2025-08-31'::date
GROUP BY all;

-- 2) Monthly no-order-365d cohort and conversions
CREATE OR REPLACE TABLE proddb.fionafan.monthly_no_order_365d_and_conversions_202507_202508 AS
WITH months AS (
    SELECT
        DATEADD('month', seq4(), '2025-07-01'::date) AS month_start,
        LEAST(
          DATEADD('day', -1, DATEADD('month', 1, DATEADD('month', seq4(), '2025-07-01'::date))),
          '2025-08-31'::date
        ) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
    WHERE month_start <= '2025-08-01'::date
)
, base_consumers AS (
    SELECT
        CAST(id AS NUMBER) AS consumer_id,
        user_id,
        created_date
    FROM proddb.public.dimension_consumer
    WHERE experience = 'doordash'
      AND is_guest = FALSE
      AND is_blacklisted = FALSE
      AND sanitized_email NOT LIKE '%@doordash.com'
      AND default_country_id IN (1) -- US
)
, no_recent_by_month AS (
    SELECT
        m.month_start,
        m.month_end,
        COUNT(*) AS no_order_365d_count
    FROM base_consumers bc
    CROSS JOIN months m
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o
      ON o.consumer_id = bc.consumer_id
     AND o.order_date BETWEEN DATEADD('day', -365, m.month_start) AND DATEADD('day', -1, m.month_start)
    WHERE bc.created_date <= DATEADD('day', -365, m.month_start)
      AND o.consumer_id IS NULL
    GROUP BY all
)
, converted_by_month AS (
    SELECT
        m.month_start,
        m.month_end,
        COUNT(DISTINCT bc.consumer_id) AS converted_count,
        SUM(COALESCE(o_month.orders_on_day, 0)) AS total_orders_during_month
    FROM base_consumers bc
    CROSS JOIN months m
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o_prior
      ON o_prior.consumer_id = bc.consumer_id
     AND o_prior.order_date BETWEEN DATEADD('day', -365, m.month_start) AND DATEADD('day', -1, m.month_start)
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o_month
      ON o_month.consumer_id = bc.consumer_id
     AND o_month.order_date BETWEEN m.month_start AND m.month_end
    WHERE bc.created_date <= DATEADD('day', -365, m.month_start)
      AND o_prior.consumer_id IS NULL
      AND o_month.consumer_id IS NOT NULL
    GROUP BY all
)
, engagement_by_month AS (
    SELECT
        m.month_start,
        m.month_end,
        COUNT(DISTINCT CASE WHEN evm.had_app_open_month = 1 THEN bc.consumer_id END) AS cohort_app_open_any_count,
        COUNT(DISTINCT CASE WHEN evm.had_store_content_impr_month = 1 THEN bc.consumer_id END) AS cohort_store_content_impr_any_count,
        COUNT(DISTINCT CASE WHEN evm.had_store_page_visit_month = 1 THEN bc.consumer_id END) AS cohort_store_page_visit_any_count
    FROM base_consumers bc
    CROSS JOIN months m
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o
      ON o.consumer_id = bc.consumer_id
     AND o.order_date BETWEEN DATEADD('day', -365, m.month_start) AND DATEADD('day', -1, m.month_start)
    LEFT JOIN proddb.fionafan.events_monthly_202507_202508 evm
      ON evm.user_id = bc.consumer_id
     AND evm.month_start = m.month_start
    WHERE bc.created_date <= DATEADD('day', -365, m.month_start)
      AND o.consumer_id IS NULL
    GROUP BY all
)
SELECT
    n.month_start,
    n.month_end,
    n.no_order_365d_count,
    COALESCE(c.converted_count, 0) AS converted_during_month_count,
    COALESCE(c.total_orders_during_month, 0) AS total_orders_during_month,
    ROUND(COALESCE(c.converted_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS conversion_rate,
    ROUND(COALESCE(c.total_orders_during_month, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS order_rate,
    COALESCE(e.cohort_app_open_any_count, 0) AS cohort_app_open_any_count,
    COALESCE(e.cohort_store_content_impr_any_count, 0) AS cohort_store_content_impr_any_count,
    COALESCE(e.cohort_store_page_visit_any_count, 0) AS cohort_store_page_visit_any_count,
    ROUND(COALESCE(e.cohort_app_open_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS app_open_rate,
    ROUND(COALESCE(e.cohort_store_content_impr_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS store_content_impr_rate,
    ROUND(COALESCE(e.cohort_store_page_visit_any_count, 0) / NULLIF(n.no_order_365d_count, 0), 4) AS store_page_visit_rate
FROM no_recent_by_month n
LEFT JOIN converted_by_month c
  ON n.month_start = c.month_start
 AND n.month_end   = c.month_end
LEFT JOIN engagement_by_month e
  ON n.month_start = e.month_start
 AND n.month_end   = e.month_end
ORDER BY month_start;

-- 3) Monthly lifestage cohorts (New Cx, Non-Purchaser)
CREATE OR REPLACE TABLE proddb.fionafan.monthly_lifestage_engagement_and_conversions_202507_202508 AS
WITH months AS (
    SELECT
        DATEADD('month', seq4(), '2025-07-01'::date) AS month_start,
        LEAST(
          DATEADD('day', -1, DATEADD('month', 1, DATEADD('month', seq4(), '2025-07-01'::date))),
          '2025-08-31'::date
        ) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
    WHERE month_start <= '2025-08-01'::date
)
, base_consumers AS (
    SELECT
        CAST(id AS NUMBER) AS consumer_id,
        user_id
    FROM proddb.public.dimension_consumer
    WHERE experience = 'doordash'
      AND is_guest = FALSE
      AND is_blacklisted = FALSE
      AND sanitized_email NOT LIKE '%@doordash.com'
      AND default_country_id IN (1)
)
, scd_active AS (
    SELECT
        s.consumer_id,
        s.lifestage,
        s.scd_start_date,
        s.scd_end_date
    FROM edw.growth.consumer_growth_accounting_scd3 s
    WHERE s.lifestage IN ('New Cx', 'Non-Purchaser')
)
, cohort_by_month AS (
    SELECT
        m.month_start,
        m.month_end,
        min(a.lifestage) AS cohort_lifestage,
        bc.consumer_id,
        bc.user_id
    FROM months m
    JOIN scd_active a
      ON m.month_start BETWEEN a.scd_start_date AND COALESCE(a.scd_end_date, '9999-12-31')
    JOIN base_consumers bc
      ON bc.consumer_id = a.consumer_id
    group by all
)
, cohort_size AS (
    SELECT
        month_start,
        month_end,
        cohort_lifestage,
        COUNT(DISTINCT consumer_id) AS cohort_size
    FROM cohort_by_month
    GROUP BY all
)
, cohort_conversions AS (
    SELECT
        cbm.month_start,
        cbm.month_end,
        cbm.cohort_lifestage,
        COUNT(DISTINCT cbm.consumer_id) AS converted_count,
        SUM(COALESCE(o_month.orders_on_day, 0)) AS total_orders_during_month
    FROM cohort_by_month cbm
    LEFT JOIN proddb.fionafan.consumer_day_orders_20240630_20250930 o_month
      ON o_month.consumer_id = cbm.consumer_id
     AND o_month.order_date BETWEEN cbm.month_start AND cbm.month_end
    WHERE o_month.consumer_id IS NOT NULL
    GROUP BY all
)
, cohort_engagement AS (
    SELECT
        cbm.month_start,
        cbm.month_end,
        cbm.cohort_lifestage,
        COUNT(DISTINCT CASE WHEN evm.had_app_open_month = 1 THEN cbm.consumer_id END) AS cohort_app_open_any_count,
        COUNT(DISTINCT CASE WHEN evm.had_store_content_impr_month = 1 THEN cbm.consumer_id END) AS cohort_store_content_impr_any_count,
        COUNT(DISTINCT CASE WHEN evm.had_store_page_visit_month = 1 THEN cbm.consumer_id END) AS cohort_store_page_visit_any_count
    FROM cohort_by_month cbm
    LEFT JOIN proddb.fionafan.events_monthly_202507_202508 evm
      ON evm.user_id = cbm.consumer_id
     AND evm.month_start = cbm.month_start
    GROUP BY all
)
SELECT
    sz.month_start,
    sz.month_end,
    sz.cohort_lifestage,
    sz.cohort_size,
    COALESCE(cv.converted_count, 0) AS converted_during_month_count,
    COALESCE(cv.total_orders_during_month, 0) AS total_orders_during_month,
    ROUND(COALESCE(cv.converted_count, 0) / NULLIF(sz.cohort_size, 0), 4) AS conversion_rate,
    ROUND(COALESCE(cv.total_orders_during_month, 0) / NULLIF(sz.cohort_size, 0), 4) AS order_rate,
    COALESCE(eg.cohort_app_open_any_count, 0) AS cohort_app_open_any_count,
    COALESCE(eg.cohort_store_content_impr_any_count, 0) AS cohort_store_content_impr_any_count,
    COALESCE(eg.cohort_store_page_visit_any_count, 0) AS cohort_store_page_visit_any_count,
    ROUND(COALESCE(eg.cohort_app_open_any_count, 0) / NULLIF(sz.cohort_size, 0), 4) AS app_open_rate,
    ROUND(COALESCE(eg.cohort_store_content_impr_any_count, 0) / NULLIF(sz.cohort_size, 0), 4) AS store_content_impr_rate,
    ROUND(COALESCE(eg.cohort_store_page_visit_any_count, 0) / NULLIF(sz.cohort_size, 0), 4) AS store_page_visit_rate
FROM cohort_size sz
LEFT JOIN cohort_conversions cv
  ON sz.month_start = cv.month_start
 AND sz.month_end   = cv.month_end
 AND sz.cohort_lifestage = cv.cohort_lifestage
LEFT JOIN cohort_engagement eg
  ON sz.month_start = eg.month_start
 AND sz.month_end   = eg.month_end
 AND sz.cohort_lifestage = eg.cohort_lifestage
ORDER BY month_start, cohort_lifestage;

-- Previews
SELECT * FROM proddb.fionafan.monthly_no_order_365d_and_conversions_202507_202508 ORDER BY month_start;
SELECT * FROM proddb.fionafan.monthly_lifestage_engagement_and_conversions_202507_202508 ORDER BY month_start, cohort_lifestage;

-- Combined monthly cohort summary (union of 365 no order cohort and lifestage cohorts)
CREATE OR REPLACE TABLE proddb.fionafan.monthly_cohort_summary_202507_202508 AS
SELECT
    n.month_start,
    n.month_end,
    '365 no order' AS cohort_name,
    n.no_order_365d_count AS cohort_size,
    n.cohort_app_open_any_count AS app_open_count,
    n.cohort_store_content_impr_any_count AS store_content_impr_count,
    n.cohort_store_page_visit_any_count AS store_page_visit_count,
    n.converted_during_month_count AS converted_count,
    n.total_orders_during_month AS total_orders_during_month,
    ROUND(n.cohort_app_open_any_count / NULLIF(n.no_order_365d_count, 0), 4) AS visit_rate,
    ROUND(n.converted_during_month_count / NULLIF(n.cohort_app_open_any_count, 0), 4) AS conversion_rate,
    ROUND(n.total_orders_during_month / NULLIF(n.no_order_365d_count, 0), 4) AS order_rate,
    ROUND(n.cohort_store_content_impr_any_count / NULLIF(n.no_order_365d_count, 0), 4) AS store_content_impr_rate,
    ROUND(n.cohort_store_page_visit_any_count / NULLIF(n.no_order_365d_count, 0), 4) AS store_page_visit_rate
FROM proddb.fionafan.monthly_no_order_365d_and_conversions_202507_202508 n
UNION ALL
SELECT
    l.month_start,
    l.month_end,
    l.cohort_lifestage AS cohort_name,
    l.cohort_size AS cohort_size,
    l.cohort_app_open_any_count AS app_open_count,
    l.cohort_store_content_impr_any_count AS store_content_impr_count,
    l.cohort_store_page_visit_any_count AS store_page_visit_count,
    l.converted_during_month_count AS converted_count,
    l.total_orders_during_month AS total_orders_during_month,
    ROUND(l.cohort_app_open_any_count / NULLIF(l.cohort_size, 0), 4) AS visit_rate,
    ROUND(l.converted_during_month_count / NULLIF(l.cohort_app_open_any_count, 0), 4) AS conversion_rate,
    ROUND(l.total_orders_during_month / NULLIF(l.cohort_size, 0), 4) AS order_rate,
    ROUND(l.cohort_store_content_impr_any_count / NULLIF(l.cohort_size, 0), 4) AS store_content_impr_rate,
    ROUND(l.cohort_store_page_visit_any_count / NULLIF(l.cohort_size, 0), 4) AS store_page_visit_rate
FROM proddb.fionafan.monthly_lifestage_engagement_and_conversions_202507_202508 l;

-- Preview combined summary
SELECT * FROM proddb.fionafan.monthly_cohort_summary_202507_202508 ORDER BY month_start, cohort_name;

-- Examples: New Cx in August who converted but had no app_open that month
WITH params AS (
    SELECT DATE('2025-08-01') AS month_start, DATE('2025-08-31') AS month_end
)
, base_consumers AS (
    SELECT CAST(id AS NUMBER) AS consumer_id, user_id
    FROM proddb.public.dimension_consumer
    WHERE experience = 'doordash'
      AND is_guest = FALSE
      AND is_blacklisted = FALSE
      AND sanitized_email NOT LIKE '%@doordash.com'
      AND default_country_id IN (1)
)
, new_cx_aug AS (
    SELECT bc.consumer_id, bc.user_id
    FROM edw.growth.consumer_growth_accounting_scd3 s
    JOIN base_consumers bc ON bc.consumer_id = s.consumer_id
    CROSS JOIN params p
    WHERE s.lifestage = 'New Cx'
      AND p.month_start BETWEEN s.scd_start_date AND COALESCE(s.scd_end_date, '9999-12-31')
)
, converters_aug AS (
    SELECT DISTINCT o.consumer_id
    FROM proddb.fionafan.consumer_day_orders_20240630_20250930 o
    CROSS JOIN params p
    WHERE o.order_date BETWEEN p.month_start AND p.month_end
)
, app_open_aug AS (
    SELECT DISTINCT v.user_id
    FROM proddb.public.fact_unique_visitors_full_pt v

    CROSS JOIN params p
    WHERE v.event_date BETWEEN p.month_start AND p.month_end
)
SELECT
    n.consumer_id,
    n.user_id
FROM new_cx_aug n
JOIN converters_aug c
  ON c.consumer_id = n.consumer_id
LEFT JOIN app_open_aug a
  ON a.user_id = n.user_id
WHERE a.user_id IS NULL
LIMIT 200;

