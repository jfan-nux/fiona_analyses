/*
Purpose: For all store-item pairs, compute the number of core deliveries in the
         previous 30/60/90/120/150/180 days strictly before 2025-09-08.
         No "most liked" filter.

Notes:
- Core deliveries source: proddb.public.dimension_deliveries (is_filtered_core = 1 and exclude shipping)
- Item-level mapping: edw.merchant.fact_merchant_order_items
- Windows: [anchor_date - X, anchor_date) for X in {30,60,90,120,150,180}, anchor_date = '2025-09-08'
- Only include rows with non-null store_id and item_id
- Additional filter per request: d.nv_vertical_name IS NULL
- Use group by all per workspace rules
*/

CREATE OR REPLACE TABLE proddb.fionafan.store_item_deliveries_90d_pre_20250908 AS
WITH
anchor AS (
    SELECT '2025-09-08'::date AS anchor_date
),

/* 1) Core deliveries restricted to the last 180 days before anchor_date */
deliveries_180d AS (
    SELECT
        d.delivery_id,
        d.active_date
    FROM proddb.public.dimension_deliveries d
    CROSS JOIN anchor a
    WHERE d.is_filtered_core = 1
      AND NVL(d.fulfillment_type, '') NOT IN ('shipping')
      AND d.active_date >= DATEADD('day', -180, a.anchor_date)
      AND d.active_date < a.anchor_date
      AND d.nv_vertical_name IS NULL
),

/* 2) Item rows for those deliveries */
items_180d AS (
    SELECT
        f.delivery_id,
        f.store_id,
        f.item_id,
        d.active_date
    FROM edw.merchant.fact_merchant_order_items f
    JOIN deliveries_180d d
      ON d.delivery_id = f.delivery_id
    WHERE f.store_id IS NOT NULL
      AND f.item_id  IS NOT NULL
),

/* 3) Aggregate to store-item volume over 30/60/90/120/150/180-day windows */
final AS (
    SELECT
        store_id,
        item_id,
        COUNT(DISTINCT CASE WHEN active_date >= DATEADD('day', -30,  (SELECT anchor_date FROM anchor)) THEN delivery_id END)  AS deliveries_30d,
        COUNT(DISTINCT CASE WHEN active_date >= DATEADD('day', -60,  (SELECT anchor_date FROM anchor)) THEN delivery_id END)  AS deliveries_60d,
        COUNT(DISTINCT CASE WHEN active_date >= DATEADD('day', -90,  (SELECT anchor_date FROM anchor)) THEN delivery_id END)  AS deliveries_90d,
        COUNT(DISTINCT CASE WHEN active_date >= DATEADD('day', -120, (SELECT anchor_date FROM anchor)) THEN delivery_id END)  AS deliveries_120d,
        COUNT(DISTINCT CASE WHEN active_date >= DATEADD('day', -150, (SELECT anchor_date FROM anchor)) THEN delivery_id END)  AS deliveries_150d,
        COUNT(DISTINCT delivery_id) AS deliveries_180d
    FROM items_180d
    GROUP BY all
)

SELECT *
FROM final
ORDER BY store_id, item_id;


