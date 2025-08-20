/*---------------------------------------------------------------
  0. Parameters
----------------------------------------------------------------*/
-- SET start_date_items  = '2025-07-25';
-- SET end_date_items    = '2025-07-31';
-- SET start_date_deliv  = '2024-07-26';
-- SET end_date_deliv    = '2025-07-24';



/*---------------------------------------------------------------
  1. Badge buckets (store-level → volume_bin with simplified binning as VARCHAR)
     - Averaging avg_recent_orders_volume across all items per store
     - Simplified bins: 10,50,100,150,200,250,300,350,400,450,500,550,600,600+
----------------------------------------------------------------*/
WITH store_avg_volumes AS (
    SELECT
        store_id,
        AVG(avg_recent_orders_volume) AS avg_store_volume
    FROM proddb.fionafan.treatment_item_order_volume_info
    GROUP BY store_id
),

badge_bins AS (
    SELECT
        store_id,
        avg_store_volume,
        CASE 
            WHEN avg_store_volume > 0 AND avg_store_volume <= 10 THEN '10'
            WHEN avg_store_volume > 10 AND avg_store_volume <= 50 THEN '50'
            WHEN avg_store_volume > 50 AND avg_store_volume <= 100 THEN '100'
            WHEN avg_store_volume > 100 AND avg_store_volume <= 150 THEN '150'
            WHEN avg_store_volume > 150 AND avg_store_volume <= 200 THEN '200'
            WHEN avg_store_volume > 200 AND avg_store_volume <= 250 THEN '250'
            WHEN avg_store_volume > 250 AND avg_store_volume <= 300 THEN '300'
            WHEN avg_store_volume > 300 AND avg_store_volume <= 350 THEN '350'
            WHEN avg_store_volume > 350 AND avg_store_volume <= 400 THEN '400'
            WHEN avg_store_volume > 400 AND avg_store_volume <= 450 THEN '450'
            WHEN avg_store_volume > 450 AND avg_store_volume <= 500 THEN '500'
            WHEN avg_store_volume > 500 AND avg_store_volume <= 550 THEN '550'
            WHEN avg_store_volume > 550 AND avg_store_volume <= 600 THEN '600'
            WHEN avg_store_volume > 600 THEN '600+'
            ELSE NULL
        END AS volume_bin
    FROM store_avg_volumes
),

/*---------------------------------------------------------------
  2. Store-level orders in analysis window
----------------------------------------------------------------*/
store_orders AS (
    SELECT
        delivery_id,
        store_id,
        sum(subtotal) / 100.0 AS store_revenue_usd
    FROM edw.merchant.fact_merchant_order_items
    WHERE active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
    GROUP BY delivery_id, store_id
),

/*---------------------------------------------------------------
  3. Core marketplace deliveries
----------------------------------------------------------------*/
deliveries AS (
    SELECT
        delivery_id,
        creator_id AS consumer_id
    FROM proddb.public.dimension_deliveries
    WHERE active_date BETWEEN '2025-07-25' AND '2025-07-31'
      AND is_filtered_core = TRUE
      AND NVL(fulfillment_type,'') NOT IN ('shipping')
    GROUP BY ALL
),

/*---------------------------------------------------------------
  4. Exposure lookup
----------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key AS consumer_id,
           experiment_group
    FROM METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),

/*---------------------------------------------------------------
  5. Combine; keep only stores that have a badge bucket
----------------------------------------------------------------*/
joined AS (
    SELECT
        b.volume_bin,
        e.experiment_group,
        d.delivery_id,
        so.store_revenue_usd
    FROM deliveries d
    JOIN exposures e ON d.consumer_id = e.consumer_id
    JOIN store_orders so ON so.delivery_id = d.delivery_id
    JOIN badge_bins b ON b.store_id = so.store_id
),

/*---------------------------------------------------------------
  6. Metrics by (bin, group)
----------------------------------------------------------------*/
metrics AS (
    SELECT
        volume_bin,
        experiment_group,
        COUNT(DISTINCT delivery_id) AS order_count,
        SUM(store_revenue_usd) AS revenue_usd
    FROM joined
    GROUP BY 1,2
),

/*---------------------------------------------------------------
  7. Control metrics (one row per bin)
----------------------------------------------------------------*/
control AS (
    SELECT *
    FROM metrics
    WHERE experiment_group = 'control'
),

/*---------------------------------------------------------------
  8. Treatment metrics we want to compare   (icon / no-icon)
----------------------------------------------------------------*/
treatments AS (
    SELECT *
    FROM metrics
    WHERE experiment_group IN ('icon treatment','no icon treatment')
),

/*---------------------------------------------------------------
  9. Percent-diff-to-control for each treatment arm
----------------------------------------------------------------*/
bin_diffs AS (
    SELECT
        t.volume_bin,
        t.experiment_group AS treatment_arm,

        /* -------------- order_count % diff -------------- */
        CASE 
            WHEN c.order_count > 0 THEN (t.order_count::FLOAT / c.order_count) - 1.0
            ELSE NULL 
        END AS pct_diff_order_count,

        /* -------------- revenue % diff ------------------ */
        CASE 
            WHEN c.revenue_usd > 0 THEN (t.revenue_usd::FLOAT / c.revenue_usd) - 1.0
            ELSE NULL 
        END AS pct_diff_revenue_usd
    FROM treatments t
    JOIN control c USING (volume_bin)
),

/*---------------------------------------------------------------
 10.  OVERALL diff (all bins) for each treatment arm
----------------------------------------------------------------*/
overall_diffs AS (
    SELECT
        'OVERALL' AS volume_bin,
        t.experiment_group AS treatment_arm,

        CASE 
            WHEN SUM(c.order_count) > 0 THEN (SUM(t.order_count)::FLOAT / SUM(c.order_count)) - 1.0
            ELSE NULL 
        END AS pct_diff_order_count,

        CASE 
            WHEN SUM(c.revenue_usd) > 0 THEN (SUM(t.revenue_usd)::FLOAT / SUM(c.revenue_usd)) - 1.0
            ELSE NULL 
        END AS pct_diff_revenue_usd
    FROM treatments t
    JOIN control c USING (volume_bin)
    GROUP BY t.experiment_group
)

/*---------------------------------------------------------------
 11.  Final output
----------------------------------------------------------------*/
SELECT * FROM bin_diffs
UNION ALL
SELECT * FROM overall_diffs
ORDER BY
    CASE WHEN volume_bin = 'OVERALL' THEN 1 ELSE 0 END,
    TRY_TO_NUMBER(volume_bin),        -- numeric sort for 50,100,150…
    treatment_arm;
