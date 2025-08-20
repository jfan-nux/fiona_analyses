/*---------------------------------------------------------------
  0. Parameters
----------------------------------------------------------------*/
-- SET start_date_items  = '2025-07-25';
-- SET end_date_items    = '2025-07-31';
-- SET start_date_deliv  = '2024-07-26';
-- SET end_date_deliv    = '2025-07-24';



/*---------------------------------------------------------------
  1. Badge buckets (store-item level → volume_bin with simplified binning as VARCHAR)
     - Using avg_recent_orders_volume for each store_id + item_id combination
     - Extended bins: 10,50,100,150,200,250,300,350,400,450,500,550,600,650,700,750,800,850,900,950,1000,1000+
----------------------------------------------------------------*/
WITH badge_bins AS (
    SELECT
        store_id,
        item_id,
        avg_recent_orders_volume AS item_volume,
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
            WHEN avg_recent_orders_volume > 600 AND avg_recent_orders_volume <= 650 THEN '650'
            WHEN avg_recent_orders_volume > 650 AND avg_recent_orders_volume <= 700 THEN '700'
            WHEN avg_recent_orders_volume > 700 AND avg_recent_orders_volume <= 750 THEN '750'
            WHEN avg_recent_orders_volume > 750 AND avg_recent_orders_volume <= 800 THEN '800'
            WHEN avg_recent_orders_volume > 800 AND avg_recent_orders_volume <= 850 THEN '850'
            WHEN avg_recent_orders_volume > 850 AND avg_recent_orders_volume <= 900 THEN '900'
            WHEN avg_recent_orders_volume > 900 AND avg_recent_orders_volume <= 950 THEN '950'
            WHEN avg_recent_orders_volume > 950 AND avg_recent_orders_volume <= 1000 THEN '1000'
            WHEN avg_recent_orders_volume > 1000 THEN '1000+'
            ELSE NULL
        END AS volume_bin
    FROM proddb.fionafan.treatment_item_order_volume_info
),

/*---------------------------------------------------------------
  2. Store categorization (ENT vs SMB)
----------------------------------------------------------------*/
store_categories AS (
    SELECT 
        store_id::varchar AS store_id,
        CASE 
            WHEN management_type IN ('ENTERPRISE') THEN 'ENT' 
            ELSE 'SMB' 
        END AS store_type
    FROM dimension_store_ext
),

/*---------------------------------------------------------------
  3. Store-item level orders in analysis window
----------------------------------------------------------------*/
store_item_orders AS (
    SELECT
        delivery_id,
        store_id,
        item_id,
        sum(quantity) AS item_quantity,
        sum(subtotal) / 100.0 AS item_revenue_usd
    FROM edw.merchant.fact_merchant_order_items
    WHERE active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
    GROUP BY delivery_id, store_id, item_id
),

/*---------------------------------------------------------------
  4. Core marketplace deliveries
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
  5. Exposure lookup
----------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key AS consumer_id,
           experiment_group
    FROM METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),

/*---------------------------------------------------------------
  6. Combine; keep only store-item combinations that have a badge bucket
----------------------------------------------------------------*/
joined AS (
    SELECT
        b.volume_bin,
        b.store_id,
        b.item_id,
        sc.store_type,
        e.experiment_group,
        d.delivery_id,
        sio.item_quantity,
        sio.item_revenue_usd
    FROM deliveries d
    JOIN exposures e ON d.consumer_id = e.consumer_id
    JOIN store_item_orders sio ON sio.delivery_id = d.delivery_id
    JOIN badge_bins b ON b.store_id = sio.store_id AND b.item_id = sio.item_id
    JOIN store_categories sc ON sc.store_id = b.store_id
),


/*---------------------------------------------------------------
  7. Metrics by (store_type, bin, group) - Store-Item Level
----------------------------------------------------------------*/
metrics AS (
    SELECT
        store_type,
        volume_bin,
        experiment_group,
        COUNT(DISTINCT delivery_id) AS order_count,
        SUM(item_revenue_usd) AS revenue_usd
    FROM joined
    GROUP BY 1,2,3
),

/*---------------------------------------------------------------
  8. Control metrics (one row per store_type + bin)
----------------------------------------------------------------*/
control AS (
    SELECT *
    FROM metrics
    WHERE experiment_group = 'control'
),

/*---------------------------------------------------------------
  9. Treatment metrics we want to compare   (icon / no-icon)
----------------------------------------------------------------*/
treatments AS (
    SELECT *
    FROM metrics
    WHERE experiment_group IN ('icon treatment','no icon treatment')
),

/*---------------------------------------------------------------
  10. Percent-diff-to-control for each treatment arm (Store-Item Level)
----------------------------------------------------------------*/
bin_diffs AS (
    SELECT
        t.store_type,
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
    JOIN control c ON t.store_type = c.store_type AND t.volume_bin = c.volume_bin
),

/*---------------------------------------------------------------
| 11.  OVERALL diff (all bins) for each treatment arm by store type
----------------------------------------------------------------*/
overall_diffs AS (
    SELECT
        t.store_type,
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
    JOIN control c ON t.store_type = c.store_type AND t.volume_bin = c.volume_bin
    GROUP BY t.store_type, t.experiment_group
)

/*---------------------------------------------------------------
| 12.  Final output
----------------------------------------------------------------*/
SELECT * FROM bin_diffs
UNION ALL
SELECT * FROM overall_diffs
ORDER BY
    store_type,
    CASE WHEN volume_bin = 'OVERALL' THEN 1 ELSE 0 END,
    CASE 
        WHEN volume_bin = '1000+' THEN 1001  -- Sort 1000+ at the end
        ELSE TRY_TO_NUMBER(volume_bin)
    END,
    treatment_arm;
