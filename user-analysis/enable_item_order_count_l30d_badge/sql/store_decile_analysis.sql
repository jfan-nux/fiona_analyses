/*---------------------------------------------------------------
  Badge Experiment Analysis by Store Volume Deciles - STORE LEVEL
  
  Purpose: Analyze enable_item_order_count_l30d_badge experiment results
           at STORE LEVEL broken down by store volume deciles
  
  Analysis Level: Store-level (one row per delivery across all stores)
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
  2. Core marketplace deliveries with metrics
----------------------------------------------------------------*/
deliveries AS (
    SELECT
        delivery_id,
        creator_id                                 AS consumer_id,
        store_id,
        order_cart_id,
        is_first_ordercart_dd,
        subtotal,
        variable_profit,
        gov,
        created_at
    FROM proddb.public.dimension_deliveries
    WHERE  active_date BETWEEN '2025-07-25' AND '2025-07-31'
      AND  is_filtered_core = TRUE
      AND  NVL(fulfillment_type,'') NOT IN ('shipping')
),

/*---------------------------------------------------------------
  3. Exposure lookup
----------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key        AS consumer_id ,
           experiment_group
    FROM   METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),

/*---------------------------------------------------------------
  4. Combine all data with store deciles - STORE LEVEL ANALYSIS
     Note: Analyze at store/delivery level across all stores
----------------------------------------------------------------*/
joined AS (
    SELECT
        sd.volume_decile,
        sd.volume_tier_label,
        sd.baseline_order_count,
        e.experiment_group,
        d.delivery_id,
        d.store_id,
        -- Order cart metrics from dimension_deliveries  
        d.order_cart_id,
        d.is_first_ordercart_dd,
        d.subtotal / 100.0                       AS cart_subtotal_usd,
        d.variable_profit / 100.0                AS cart_variable_profit_usd,
        d.gov / 100.0                            AS cart_gov_usd
    FROM          deliveries  d
    JOIN          exposures   e  ON d.consumer_id  = e.consumer_id
    JOIN          store_deciles sd ON sd.store_id  = d.store_id
),

/*---------------------------------------------------------------
  5. Metrics by store volume decile and experiment group
----------------------------------------------------------------*/
metrics AS (
    SELECT
        volume_decile,
        volume_tier_label,
        experiment_group,
        
        -- Core metrics (store-level analysis)
        COUNT(DISTINCT delivery_id)             AS order_count,
        SUM(cart_subtotal_usd)                  AS revenue_usd,  -- Use subtotal as revenue proxy
        
        -- Cart metrics from dimension_deliveries
        COUNT(DISTINCT order_cart_id)           AS cart_submit_count,
        COUNT(DISTINCT CASE WHEN is_first_ordercart_dd = 1 THEN order_cart_id END) AS first_order_cart_submit_count,
        SUM(cart_subtotal_usd)                  AS cart_submit_subtotal_usd,
        SUM(cart_variable_profit_usd)           AS cart_submit_variable_profit_usd,
        SUM(cart_gov_usd)                       AS cart_submit_gov_usd,
        
        -- Context metrics
        COUNT(DISTINCT store_id)                AS unique_stores,
        AVG(baseline_order_count)               AS avg_store_baseline_volume,
        MIN(baseline_order_count)               AS min_store_baseline_volume,
        MAX(baseline_order_count)               AS max_store_baseline_volume,
        
        -- Store context (for reference)
        1                                       AS analysis_stores_flag  -- All stores in this analysis
        
    FROM joined
    GROUP BY 1,2,3
),

/*---------------------------------------------------------------
  6. Control metrics (one row per decile)
----------------------------------------------------------------*/
control AS (
    SELECT *
    FROM   metrics
    WHERE  experiment_group = 'control'
),

/*---------------------------------------------------------------
  7. Treatment metrics (icon / no-icon)
----------------------------------------------------------------*/
treatments AS (
    SELECT *
    FROM   metrics
    WHERE  experiment_group IN ('icon treatment','no icon treatment')
),

/*---------------------------------------------------------------
  8. Treatment uplift by store volume decile
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
        t.analysis_stores_flag ,

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
|  9. Overall treatment effects (all deciles combined)
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
        SUM(t.analysis_stores_flag)                              AS analysis_stores_count ,

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
| 10. Final output - Badge experiment results by store volume decile
----------------------------------------------------------------*/
SELECT * FROM decile_treatment_effects
UNION ALL
SELECT * FROM overall_effects
ORDER BY
    CASE WHEN volume_tier_label = 'OVERALL' THEN 0 ELSE 1 END ,
    volume_decile ,
    treatment_arm;
