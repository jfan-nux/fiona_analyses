/*
Purpose: For store-item pairs that are marked as "most liked" in m_card_view badges,
         compute the distribution of core deliveries in the past 30, 60, 90, 120, 150, 180 days.

Notes:
- Source events table: iguazu.consumer.m_card_view
- Badge filter: badges contains 'most liked'
- Container filter: container in ('recommended_items_for_you', 'popular-items')
- Only include rows with non-null store_id and item_id
- Core deliveries source: proddb.public.dimension_deliveries (is_filtered_core = 1 and exclude shipping)
- Item-level mapping: edw.merchant.fact_merchant_order_items
- Use group by all per workspace rules
*/
create or replace table proddb.fionafan.m_card_view_most_liked_items as (
    SELECT DISTINCT
        to_number(store_id) AS store_id,
        item_id
    FROM iguazu.consumer.m_card_view
    WHERE timestamp >= '2025-09-08'::date - 90
      AND store_id IS NOT NULL
      AND item_id  IS NOT NULL
      AND (
            LOWER(badges) LIKE '%most liked%'
          OR LOWER(badges_text) LIKE '%most liked%'
      )
      AND container IN ('recommended_items_for_you', 'popular-items')
      group by all
      );

CREATE OR REPLACE TABLE proddb.fionafan.m_card_view_most_liked_deliveries AS
WITH
/* 1) Store-item pairs from m_card_view with 'most liked' badge */
most_liked_pairs AS (
select * from proddb.fionafan.m_card_view_most_liked_items
),

/* 2) Core deliveries table restricted to last 180 days for performance */
deliveries_180d AS (
    SELECT
        delivery_id,
        active_date
    FROM proddb.public.dimension_deliveries
    WHERE is_filtered_core = 1
      AND NVL(fulfillment_type, '') NOT IN ('shipping')
      AND active_date >= DATEADD('day', -180, CURRENT_DATE)
),

/* 3) Item rows for those deliveries (only those in last 180d) */
items_180d AS (
    SELECT
        f.delivery_id,
        f.store_id,
        f.item_id,
        d.active_date
    FROM edw.merchant.fact_merchant_order_items f
    JOIN deliveries_180d d
      ON d.delivery_id = f.delivery_id
),

/* 4) Aggregate deliveries at store-item for 30/60/90/120/150/180d windows */
deliveries_by_window AS (
    SELECT
        i.store_id,
        i.item_id,
        COUNT(DISTINCT CASE WHEN i.active_date >= DATEADD('day', -30, CURRENT_DATE) THEN i.delivery_id END) AS deliveries_30d,
        COUNT(DISTINCT CASE WHEN i.active_date >= DATEADD('day', -60, CURRENT_DATE) THEN i.delivery_id END) AS deliveries_60d,
        COUNT(DISTINCT CASE WHEN i.active_date >= DATEADD('day', -90, CURRENT_DATE) THEN i.delivery_id END) AS deliveries_90d,
        COUNT(DISTINCT CASE WHEN i.active_date >= DATEADD('day', -120, CURRENT_DATE) THEN i.delivery_id END) AS deliveries_120d,
        COUNT(DISTINCT CASE WHEN i.active_date >= DATEADD('day', -150, CURRENT_DATE) THEN i.delivery_id END) AS deliveries_150d,
        COUNT(DISTINCT i.delivery_id) AS deliveries_180d
    FROM items_180d i
    GROUP BY all
)

/* 5) Final output: only store-item pairs that were flagged as most liked */
SELECT
    p.store_id,
    p.item_id,
    COALESCE(d.deliveries_30d, 0) AS deliveries_30d,
    COALESCE(d.deliveries_60d, 0) AS deliveries_60d,
    COALESCE(d.deliveries_90d, 0) AS deliveries_90d,
    COALESCE(d.deliveries_120d, 0) AS deliveries_120d,
    COALESCE(d.deliveries_150d, 0) AS deliveries_150d,
    COALESCE(d.deliveries_180d, 0) AS deliveries_180d
FROM most_liked_pairs p
LEFT JOIN deliveries_by_window d
  ON p.store_id = d.store_id
 AND p.item_id  = d.item_id
GROUP BY all
ORDER BY store_id, item_id;

/* Optional: histogram distributions across windows */
-- Example: bucket deliveries_30d into ranges
-- SELECT
--   CASE
--     WHEN deliveries_30d = 0 THEN '0'
--     WHEN deliveries_30d BETWEEN 1 AND 5 THEN '1-5'
--     WHEN deliveries_30d BETWEEN 6 AND 10 THEN '6-10'
--     WHEN deliveries_30d BETWEEN 11 AND 20 THEN '11-20'
--     WHEN deliveries_30d BETWEEN 21 AND 50 THEN '21-50'
--     ELSE '50+'
--   END AS deliveries_30d_bucket,
--   COUNT(1) AS num_store_items
-- FROM (
--   SELECT
--     p.store_id,
--     p.item_id,
--     COALESCE(d.deliveries_30d, 0) AS deliveries_30d
--   FROM most_liked_pairs p
--   LEFT JOIN deliveries_by_window d
--     ON p.store_id = d.store_id
--    AND p.item_id  = d.item_id
-- ) x
-- GROUP BY all
-- ORDER BY deliveries_30d_bucket;
