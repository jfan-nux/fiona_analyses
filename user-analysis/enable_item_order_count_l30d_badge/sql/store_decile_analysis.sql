/*---------------------------------------------------------------
  Badge Experiment Analysis by Store Volume Deciles
  
  Purpose: Analyze enable_item_order_count_l30d_badge experiment results
           broken down by store volume deciles instead of item volume bins
  
  Store Deciles: Based on 30 days prior to 2025-07-25 (June 25 - July 24)
  Analysis Period: July 25 - July 31, 2025
----------------------------------------------------------------*/

/*---------------------------------------------------------------
  1. Store Volume Deciles (30 days prior to 2025-07-25)
----------------------------------------------------------------*/
WITH store_volume_baseline AS (
    SELECT 
        store_id,
        COUNT(*) as baseline_order_count
    FROM proddb.public.dimension_deliveries
    WHERE 1=1
        AND is_filtered_core = 1
        AND country_id = 1
        AND active_date BETWEEN '2025-06-25' AND '2025-07-24'  -- 30 days prior to 2025-07-25
        AND store_id IS NOT NULL
    GROUP BY store_id
),

store_deciles AS (
    SELECT 
        store_id,
        baseline_order_count,
        NTILE(10) OVER (ORDER BY baseline_order_count) as volume_decile,
        CONCAT('D', NTILE(10) OVER (ORDER BY baseline_order_count)) as volume_tier_label
    FROM store_volume_baseline
),

/*---------------------------------------------------------------
  2. Badge buckets (store-item → volume_bin for filtering)
     Note: We still use badge bins to filter to items that had badges,
     but analysis will be by store decile, not item volume bin
----------------------------------------------------------------*/
badge_bins AS (
    SELECT
        store_id ,
        item_id  ,
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
            WHEN avg_recent_orders_volume > 1000 THEN '1000'
            ELSE NULL
        END AS volume_bin
    FROM proddb.fionafan.treatment_item_order_volume_info
),

/*---------------------------------------------------------------
  3. Item-level rows in analysis window
----------------------------------------------------------------*/
item_orders AS (
    SELECT
        delivery_id ,
        store_id ,
        item_id ,
        sum(subtotal) / 100.0                      AS item_revenue_usd
    FROM edw.merchant.fact_merchant_order_items
    WHERE active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
    group by all
),

/*---------------------------------------------------------------
  4. Core marketplace deliveries
----------------------------------------------------------------*/
deliveries AS (
    SELECT
        delivery_id ,
        creator_id                                 AS consumer_id
    FROM proddb.public.dimension_deliveries
    WHERE  active_date BETWEEN '2025-07-24' AND '2025-08-01'
      AND  is_filtered_core = TRUE
      AND  NVL(fulfillment_type,'') NOT IN ('shipping')
      group by all
),

/*---------------------------------------------------------------
  5. Exposure lookup
----------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key        AS consumer_id ,
           experiment_group
    FROM   METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),

/*---------------------------------------------------------------
  6. Combine all data with store deciles
     Note: We filter to items that have badges but analyze by store decile
----------------------------------------------------------------*/
joined AS (
    SELECT
        sd.volume_decile ,
        sd.volume_tier_label ,
        sd.baseline_order_count ,
        b.volume_bin                             AS item_volume_bin,  -- Keep for context
        e.experiment_group ,
        d.delivery_id ,
        io.item_revenue_usd ,
        io.store_id
    FROM          deliveries  d
    JOIN          exposures   e  ON d.consumer_id  = e.consumer_id
    JOIN          item_orders io ON io.delivery_id = d.delivery_id
    JOIN          badge_bins  b  ON b.store_id     = io.store_id
                                 AND b.item_id     = io.item_id
    JOIN          store_deciles sd ON sd.store_id  = io.store_id
),

/*---------------------------------------------------------------
  7. Metrics by store volume decile and experiment group
----------------------------------------------------------------*/
metrics AS (
    SELECT
        volume_decile ,
        volume_tier_label ,
        experiment_group ,
        
        -- Core metrics
        COUNT(DISTINCT delivery_id)             AS order_count ,
        SUM(item_revenue_usd)                   AS revenue_usd ,
        
        -- Context metrics
        COUNT(DISTINCT store_id)                AS unique_stores ,
        AVG(baseline_order_count)               AS avg_store_baseline_volume ,
        MIN(baseline_order_count)               AS min_store_baseline_volume ,
        MAX(baseline_order_count)               AS max_store_baseline_volume ,
        
        -- Badge context (for reference)
        COUNT(DISTINCT item_volume_bin)         AS unique_item_volume_bins
        
    FROM joined
    GROUP BY 1,2,3
),

/*---------------------------------------------------------------
  8. Control metrics (one row per decile)
----------------------------------------------------------------*/
control AS (
    SELECT *
    FROM   metrics
    WHERE  experiment_group = 'control'
),

/*---------------------------------------------------------------
  9. Treatment metrics (icon / no-icon)
----------------------------------------------------------------*/
treatments AS (
    SELECT *
    FROM   metrics
    WHERE  experiment_group IN ('icon treatment','no icon treatment')
),

/*---------------------------------------------------------------
 10. Treatment uplift by store volume decile
----------------------------------------------------------------*/
decile_treatment_effects AS (
    SELECT
        t.volume_decile ,
        t.volume_tier_label ,
        t.experiment_group                                       AS treatment_arm ,
        
        -- Store context metrics
        t.unique_stores ,
        ROUND(t.avg_store_baseline_volume, 0)                    AS avg_store_baseline_volume ,
        t.min_store_baseline_volume ,
        t.max_store_baseline_volume ,
        t.unique_item_volume_bins ,

        -- Treatment effect calculations
        CASE 
            WHEN c.order_count > 0 THEN (t.order_count::FLOAT / c.order_count) - 1.0
            ELSE NULL 
        END                                                      AS pct_diff_order_count ,

        CASE 
            WHEN c.revenue_usd > 0 THEN (t.revenue_usd::FLOAT / c.revenue_usd) - 1.0
            ELSE NULL 
        END                                                      AS pct_diff_revenue_usd ,
        
        -- Raw metrics for context
        t.order_count                                            AS treatment_order_count ,
        c.order_count                                            AS control_order_count ,
        ROUND(t.revenue_usd, 2)                                  AS treatment_revenue_usd ,
        ROUND(c.revenue_usd, 2)                                  AS control_revenue_usd ,
        
        -- Statistical context
        ROUND((t.order_count::FLOAT / c.order_count), 3)         AS order_count_ratio ,
        ROUND((t.revenue_usd::FLOAT / c.revenue_usd), 3)         AS revenue_ratio
        
    FROM   treatments  t
    JOIN   control     c  USING (volume_decile, volume_tier_label)
),

/*---------------------------------------------------------------
| 11. Overall treatment effects (all deciles combined)
----------------------------------------------------------------*/
overall_effects AS (
    SELECT
        0                         AS volume_decile ,
        'OVERALL'                 AS volume_tier_label ,
        t.experiment_group        AS treatment_arm ,
        
        -- Context metrics for overall
        SUM(t.unique_stores)                                     AS unique_stores ,
        ROUND(AVG(t.avg_store_baseline_volume), 0)               AS avg_store_baseline_volume ,
        MIN(t.min_store_baseline_volume)                         AS min_store_baseline_volume ,
        MAX(t.max_store_baseline_volume)                         AS max_store_baseline_volume ,
        SUM(t.unique_item_volume_bins)                           AS unique_item_volume_bins ,

        -- Overall treatment effects
        CASE 
            WHEN SUM(c.order_count) > 0 THEN (SUM(t.order_count)::FLOAT / SUM(c.order_count)) - 1.0
            ELSE NULL 
        END                                                      AS pct_diff_order_count ,

        CASE 
            WHEN SUM(c.revenue_usd) > 0 THEN (SUM(t.revenue_usd)::FLOAT / SUM(c.revenue_usd)) - 1.0
            ELSE NULL 
        END                                                      AS pct_diff_revenue_usd ,
        
        -- Raw metrics
        SUM(t.order_count)                                       AS treatment_order_count ,
        SUM(c.order_count)                                       AS control_order_count ,
        ROUND(SUM(t.revenue_usd), 2)                             AS treatment_revenue_usd ,
        ROUND(SUM(c.revenue_usd), 2)                             AS control_revenue_usd ,
        
        -- Ratios
        ROUND((SUM(t.order_count)::FLOAT / SUM(c.order_count)), 3) AS order_count_ratio ,
        ROUND((SUM(t.revenue_usd)::FLOAT / SUM(c.revenue_usd)), 3) AS revenue_ratio
        
    FROM   treatments  t
    JOIN   control     c  USING (volume_decile, volume_tier_label)
    GROUP  BY t.experiment_group
)

/*---------------------------------------------------------------
| 12. Final output - Badge experiment results by store volume decile
----------------------------------------------------------------*/
SELECT * FROM decile_treatment_effects
UNION ALL
SELECT * FROM overall_effects
ORDER BY
    CASE WHEN volume_tier_label = 'OVERALL' THEN 0 ELSE 1 END ,
    volume_decile ,
    treatment_arm;
