/*---------------------------------------------------------------
  Volume Bin Analysis: Overlap with "Most Liked" Control Items
  
  For each volume bin, calculate what percentage of store-items 
  overlap with control items that have "most liked" badges
----------------------------------------------------------------*/

WITH volume_bins AS (
    SELECT
        store_id,
        item_id,
        item_name,
        CASE 
            WHEN avg_recent_orders_volume > 0 AND avg_recent_orders_volume <= 10 THEN '10'
            WHEN avg_recent_orders_volume > 10 AND avg_recent_orders_volume <= 50 THEN '50'
            WHEN avg_recent_orders_volume > 50 AND avg_recent_orders_volume <= 100 THEN '100'
            WHEN avg_recent_orders_volume > 100 AND avg_recent_orders_volume <= 150 THEN '150'
            WHEN avg_recent_orders_volume > 150 AND avg_recent_orders_volume <= 200 THEN '200'
            WHEN avg_recent_orders_volume > 200 AND avg_recent_orders_volume <= 250 THEN '250'
            WHEN avg_recent_orders_volume > 250 AND avg_recent_orders_volume <= 300 THEN '300'
            WHEN avg_recent_orders_volume > 300 AND avg_recent_orders_volume <= 350 THEN '350'
            WHEN avg_recent_orders_volume > 350 AND avg_recent_orders_volume <= 400 THEN '400'
            WHEN avg_recent_orders_volume > 400 AND avg_recent_orders_volume <= 450 THEN '450'
            WHEN avg_recent_orders_volume > 450 AND avg_recent_orders_volume <= 500 THEN '500'
            WHEN avg_recent_orders_volume > 500 AND avg_recent_orders_volume <= 550 THEN '550'
            WHEN avg_recent_orders_volume > 550 AND avg_recent_orders_volume <= 600 THEN '600'
            WHEN avg_recent_orders_volume > 600 THEN '600+'
            ELSE NULL
        END AS volume_bin,
        avg_recent_orders_volume
    FROM proddb.fionafan.treatment_item_order_volume_info
    WHERE avg_recent_orders_volume > 0
),

most_liked_control AS (
    SELECT DISTINCT
        store_id,
        item_id
    FROM proddb.fionafan.most_liked_control_items
),

volume_bin_stats AS (
    SELECT
        vb.volume_bin,
        COUNT(*) AS total_store_items_in_bin,
        COUNT(mlc.store_id) AS overlapping_with_most_liked,
        ROUND(
            COUNT(mlc.store_id)::FLOAT / COUNT(*)::FLOAT * 100, 
            2
        ) AS pct_overlap_with_most_liked,
        AVG(vb.avg_recent_orders_volume) AS avg_volume_in_bin,
        MIN(vb.avg_recent_orders_volume) AS min_volume_in_bin,
        MAX(vb.avg_recent_orders_volume) AS max_volume_in_bin
    FROM volume_bins vb
    LEFT JOIN most_liked_control mlc 
        ON vb.store_id = mlc.store_id 
        AND vb.item_id = mlc.item_id
    WHERE vb.volume_bin IS NOT NULL
    GROUP BY vb.volume_bin
),

overall_stats AS (
    SELECT
        'OVERALL' AS volume_bin,
        COUNT(*) AS total_store_items_in_bin,
        COUNT(mlc.store_id) AS overlapping_with_most_liked,
        ROUND(
            COUNT(mlc.store_id)::FLOAT / COUNT(*)::FLOAT * 100, 
            2
        ) AS pct_overlap_with_most_liked,
        AVG(vb.avg_recent_orders_volume) AS avg_volume_in_bin,
        MIN(vb.avg_recent_orders_volume) AS min_volume_in_bin,
        MAX(vb.avg_recent_orders_volume) AS max_volume_in_bin
    FROM volume_bins vb
    LEFT JOIN most_liked_control mlc 
        ON vb.store_id = mlc.store_id 
        AND vb.item_id = mlc.item_id
    WHERE vb.volume_bin IS NOT NULL
)

SELECT * FROM volume_bin_stats
UNION ALL
SELECT * FROM overall_stats
ORDER BY 
    CASE WHEN volume_bin = 'OVERALL' THEN 1 ELSE 0 END,
    TRY_TO_NUMBER(volume_bin);
