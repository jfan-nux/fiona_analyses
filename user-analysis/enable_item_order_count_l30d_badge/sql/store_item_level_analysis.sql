/*---------------------------------------------------------------
  Badge Experiment Analysis by Store-Item Volume Bins
  
  Purpose: Analyze enable_item_order_count_l30d_badge experiment results
           broken down by store_id-item_id volume bins based on historical performance
  
  Historical Period: June 25 - July 24, 2025 (30 days prior to experiment)
  Analysis Period: July 25 - July 31, 2025
  Base Table: edw.merchant.fact_merchant_order_items filtered by dimension_deliveries
  
  Volume Bins: <50, 50-100, 100-200, 200-300, 300-400, 400-500, 500-600, 
               600-700, 700-800, 800-900, 900-1000, 1000+ orders
----------------------------------------------------------------*/

/*---------------------------------------------------------------
  1. Filtered fact_merchant_order_items for baseline period (30 days prior)
----------------------------------------------------------------*/
WITH baseline_item_orders AS (
    SELECT 
        delivery_id,
        store_id,
        item_id,
        quantity,
        subtotal,
        active_date_utc
    FROM edw.merchant.fact_merchant_order_items
    WHERE active_date_utc BETWEEN '2025-06-25' AND '2025-07-24'  -- 30 days prior to 2025-07-25
        AND store_id IS NOT NULL
        AND item_id IS NOT NULL
),

/*---------------------------------------------------------------
  2. Filtered dimension_deliveries for baseline period  
----------------------------------------------------------------*/
baseline_deliveries AS (
    SELECT
        delivery_id,
        creator_id,
        active_date,
        is_filtered_core,
        country_id,
        fulfillment_type
    FROM proddb.public.dimension_deliveries
    WHERE active_date BETWEEN '2025-06-25' AND '2025-07-24'
        AND is_filtered_core = 1
        AND NVL(fulfillment_type,'') NOT IN ('shipping')
),

/*---------------------------------------------------------------
  3. Store-Item Historical Volume (30 days prior to experiment start)
----------------------------------------------------------------*/
store_item_baseline AS (
    SELECT 
        foi.store_id,
        foi.item_id,
        COUNT(DISTINCT foi.delivery_id) as baseline_order_count,
        SUM(foi.quantity) as baseline_quantity,
        SUM(foi.subtotal / 100.0) as baseline_revenue_usd
    FROM baseline_item_orders foi
    INNER JOIN baseline_deliveries d ON foi.delivery_id = d.delivery_id
    GROUP BY foi.store_id, foi.item_id
),

/*---------------------------------------------------------------
  4. Store-Item Volume Bins based on historical performance
----------------------------------------------------------------*/
store_item_volume_bins AS (
    SELECT 
        store_id,
        item_id,
        baseline_order_count,
        baseline_quantity,
        baseline_revenue_usd,
        CASE 
            WHEN baseline_order_count < 50 THEN 1
            WHEN baseline_order_count >= 50 AND baseline_order_count < 100 THEN 2
            WHEN baseline_order_count >= 100 AND baseline_order_count < 200 THEN 3
            WHEN baseline_order_count >= 200 AND baseline_order_count < 300 THEN 4
            WHEN baseline_order_count >= 300 AND baseline_order_count < 400 THEN 5
            WHEN baseline_order_count >= 400 AND baseline_order_count < 500 THEN 6
            WHEN baseline_order_count >= 500 AND baseline_order_count < 600 THEN 7
            WHEN baseline_order_count >= 600 AND baseline_order_count < 700 THEN 8
            WHEN baseline_order_count >= 700 AND baseline_order_count < 800 THEN 9
            WHEN baseline_order_count >= 800 AND baseline_order_count < 900 THEN 10
            WHEN baseline_order_count >= 900 AND baseline_order_count < 1000 THEN 11
            WHEN baseline_order_count >= 1000 THEN 12
        END as volume_bin,
        CASE 
            WHEN baseline_order_count < 50 THEN 'V1_Less_Than_50'
            WHEN baseline_order_count >= 50 AND baseline_order_count < 100 THEN 'V2_50_to_100'
            WHEN baseline_order_count >= 100 AND baseline_order_count < 200 THEN 'V3_100_to_200'
            WHEN baseline_order_count >= 200 AND baseline_order_count < 300 THEN 'V4_200_to_300'
            WHEN baseline_order_count >= 300 AND baseline_order_count < 400 THEN 'V5_300_to_400'
            WHEN baseline_order_count >= 400 AND baseline_order_count < 500 THEN 'V6_400_to_500'
            WHEN baseline_order_count >= 500 AND baseline_order_count < 600 THEN 'V7_500_to_600'
            WHEN baseline_order_count >= 600 AND baseline_order_count < 700 THEN 'V8_600_to_700'
            WHEN baseline_order_count >= 700 AND baseline_order_count < 800 THEN 'V9_700_to_800'
            WHEN baseline_order_count >= 800 AND baseline_order_count < 900 THEN 'V10_800_to_900'
            WHEN baseline_order_count >= 900 AND baseline_order_count < 1000 THEN 'V11_900_to_1000'
            WHEN baseline_order_count >= 1000 THEN 'V12_1000_Plus'
        END as volume_tier_label
    FROM store_item_baseline
),

/*---------------------------------------------------------------
  5. Badge treatment info (for filtering to badged items only)
----------------------------------------------------------------*/
badge_items AS (
    SELECT DISTINCT
        store_id,
        item_id
    FROM proddb.fionafan.treatment_item_order_volume_info
),

/*---------------------------------------------------------------
  6. Filtered fact_merchant_order_items for experiment period
----------------------------------------------------------------*/
experiment_item_orders AS (
    SELECT
        delivery_id,
        store_id,
        item_id,
        quantity,
        subtotal,
        active_date_utc
    FROM edw.merchant.fact_merchant_order_items
    WHERE active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
        AND store_id IS NOT NULL
        AND item_id IS NOT NULL
),

/*---------------------------------------------------------------
  7. Filtered dimension_deliveries for experiment period
----------------------------------------------------------------*/
experiment_deliveries AS (
    SELECT
        delivery_id,
        creator_id AS consumer_id,
        active_date,
        is_filtered_core,
        country_id,
        fulfillment_type
    FROM proddb.public.dimension_deliveries
    WHERE active_date BETWEEN '2025-07-24' AND '2025-08-01'
        AND is_filtered_core = TRUE
        AND NVL(fulfillment_type,'') NOT IN ('shipping')
),

/*---------------------------------------------------------------
  8. Experiment exposures
----------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key        AS consumer_id ,
           experiment_group
    FROM   METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),


/*---------------------------------------------------------------
  9. Join all data together - experiment data with store-item volume bins
     Only include store-item pairs that had badges AND have historical data
----------------------------------------------------------------*/
joined AS (
    SELECT
        sib.volume_bin,
        sib.volume_tier_label,
        sib.baseline_order_count,
        sib.baseline_quantity,
        sib.baseline_revenue_usd,
        e.experiment_group,
        eio.store_id,
        eio.item_id,
        eio.delivery_id,
        eio.quantity,
        eio.subtotal / 100.0 AS item_revenue_usd,
        eio.active_date_utc
    FROM experiment_item_orders eio
    INNER JOIN experiment_deliveries ed ON eio.delivery_id = ed.delivery_id
    INNER JOIN exposures e ON ed.consumer_id = e.consumer_id
    INNER JOIN badge_items bi ON eio.store_id = bi.store_id AND eio.item_id = bi.item_id  -- Only badged items
    INNER JOIN store_item_volume_bins sib ON eio.store_id = sib.store_id AND eio.item_id = sib.item_id  -- Only items with historical data
),

/*---------------------------------------------------------------
 10. Aggregate metrics by volume decile and experiment group
----------------------------------------------------------------*/
bin_metrics AS (
    SELECT
        volume_bin,
        volume_tier_label,
        experiment_group,
        
        -- Core experiment metrics
        COUNT(DISTINCT delivery_id) as order_count,
        SUM(quantity) as total_quantity,
        SUM(item_revenue_usd) as revenue_usd,
        
        -- Context metrics
        COUNT(DISTINCT CONCAT(store_id, '-', item_id)) as unique_store_item_pairs,
        AVG(baseline_order_count) as avg_baseline_order_count,
        MIN(baseline_order_count) as min_baseline_order_count,
        MAX(baseline_order_count) as max_baseline_order_count,
        
        -- Additional metrics
        AVG(item_revenue_usd) as avg_revenue_per_order,
        AVG(quantity) as avg_quantity_per_order
        
    FROM joined
    GROUP BY 1,2,3
),

/*---------------------------------------------------------------
 11. Control metrics for each decile
----------------------------------------------------------------*/
control_metrics AS (
    SELECT 
        volume_bin,
        volume_tier_label,
        order_count as control_order_count,
        total_quantity as control_quantity,
        revenue_usd as control_revenue_usd,
        unique_store_item_pairs as control_store_item_pairs,
        avg_baseline_order_count,
        min_baseline_order_count,
        max_baseline_order_count,
        avg_revenue_per_order as control_avg_revenue,
        avg_quantity_per_order as control_avg_quantity
    FROM bin_metrics
    WHERE experiment_group = 'control'
),

/*---------------------------------------------------------------
 12. Treatment metrics for each decile
----------------------------------------------------------------*/
treatment_metrics AS (
    SELECT *
    FROM bin_metrics
    WHERE experiment_group IN ('icon treatment', 'no icon treatment')
),

/*---------------------------------------------------------------
| 13. Most liked overlap statistics by volume bin
----------------------------------------------------------------*/
most_liked_overlap_stats AS (
    SELECT
        sib.volume_bin,
        COUNT(*) as total_store_items_in_bin,
        COUNT(mlc.store_id) as overlapping_with_most_liked,
        ROUND(
            COUNT(mlc.store_id)::FLOAT / COUNT(*)::FLOAT * 100, 
            2
        ) as pct_overlap_with_most_liked
    FROM store_item_volume_bins sib
    LEFT JOIN proddb.fionafan.most_liked_control_items mlc 
        ON sib.store_id = mlc.store_id AND sib.item_id = mlc.item_id
    GROUP BY sib.volume_bin
    
    UNION ALL
    
    -- Overall statistics
    SELECT
        0 as volume_bin,
        COUNT(*) as total_store_items_in_bin,
        COUNT(mlc.store_id) as overlapping_with_most_liked,
        ROUND(
            COUNT(mlc.store_id)::FLOAT / COUNT(*)::FLOAT * 100, 
            2
        ) as pct_overlap_with_most_liked
    FROM store_item_volume_bins sib
    LEFT JOIN proddb.fionafan.most_liked_control_items mlc 
        ON sib.store_id = mlc.store_id AND sib.item_id = mlc.item_id
),

/*---------------------------------------------------------------
 13. Calculate treatment effects by volume decile
----------------------------------------------------------------*/
bin_treatment_effects AS (
    SELECT
        t.volume_bin,
        t.volume_tier_label,
        t.experiment_group as treatment_arm,
        
        -- Context metrics
        t.unique_store_item_pairs,
        ROUND(t.avg_baseline_order_count, 1) as avg_baseline_order_count,
        t.min_baseline_order_count,
        t.max_baseline_order_count,
        
        -- Most liked overlap statistics (from CTE)
        mlos.total_store_items_in_bin,
        mlos.overlapping_with_most_liked,
        mlos.pct_overlap_with_most_liked,
        
        -- Treatment effects (percentage differences)
        CASE 
            WHEN c.control_order_count > 0 THEN (t.order_count::FLOAT / c.control_order_count) - 1.0
            ELSE NULL 
        END as pct_diff_order_count,
        
        CASE 
            WHEN c.control_quantity > 0 THEN (t.total_quantity::FLOAT / c.control_quantity) - 1.0
            ELSE NULL 
        END as pct_diff_quantity,
        
        CASE 
            WHEN c.control_revenue_usd > 0 THEN (t.revenue_usd::FLOAT / c.control_revenue_usd) - 1.0
            ELSE NULL 
        END as pct_diff_revenue_usd,
        
        -- Raw metrics for context
        t.order_count as treatment_order_count,
        c.control_order_count,
        ROUND(t.revenue_usd, 2) as treatment_revenue_usd,
        ROUND(c.control_revenue_usd, 2) as control_revenue_usd,
        
        -- Ratios
        ROUND(t.order_count::FLOAT / NULLIF(c.control_order_count, 0), 3) as order_count_ratio,
        ROUND(t.total_quantity::FLOAT / NULLIF(c.control_quantity, 0), 3) as quantity_ratio,
        ROUND(t.revenue_usd::FLOAT / NULLIF(c.control_revenue_usd, 0), 3) as revenue_ratio
        
    FROM treatment_metrics t
    INNER JOIN control_metrics c ON t.volume_bin = c.volume_bin AND t.volume_tier_label = c.volume_tier_label
    INNER JOIN most_liked_overlap_stats mlos ON t.volume_bin = mlos.volume_bin
),

/*---------------------------------------------------------------
 14. Overall treatment effects across all deciles
----------------------------------------------------------------*/
overall_effects AS (
    SELECT
        0 as volume_bin,
        'OVERALL' as volume_tier_label,
        t.experiment_group as treatment_arm,
        
        -- Context metrics
        SUM(t.unique_store_item_pairs) as unique_store_item_pairs,
        ROUND(AVG(t.avg_baseline_order_count), 1) as avg_baseline_order_count,
        MIN(t.min_baseline_order_count) as min_baseline_order_count,
        MAX(t.max_baseline_order_count) as max_baseline_order_count,
        
        -- Most liked overlap statistics (from CTE - overall)
        mlos.total_store_items_in_bin,
        mlos.overlapping_with_most_liked,
        mlos.pct_overlap_with_most_liked,
        
        -- Overall treatment effects
        CASE 
            WHEN SUM(c.control_order_count) > 0 THEN (SUM(t.order_count)::FLOAT / SUM(c.control_order_count)) - 1.0
            ELSE NULL 
        END as pct_diff_order_count,
        
        CASE 
            WHEN SUM(c.control_quantity) > 0 THEN (SUM(t.total_quantity)::FLOAT / SUM(c.control_quantity)) - 1.0
            ELSE NULL 
        END as pct_diff_quantity,
        
        CASE 
            WHEN SUM(c.control_revenue_usd) > 0 THEN (SUM(t.revenue_usd)::FLOAT / SUM(c.control_revenue_usd)) - 1.0
            ELSE NULL 
        END as pct_diff_revenue_usd,
        
        -- Raw metrics
        SUM(t.order_count) as treatment_order_count,
        SUM(c.control_order_count) as control_order_count,
        ROUND(SUM(t.revenue_usd), 2) as treatment_revenue_usd,
        ROUND(SUM(c.control_revenue_usd), 2) as control_revenue_usd,
        
        -- Ratios
        ROUND(SUM(t.order_count)::FLOAT / NULLIF(SUM(c.control_order_count), 0), 3) as order_count_ratio,
        ROUND(SUM(t.total_quantity)::FLOAT / NULLIF(SUM(c.control_quantity), 0), 3) as quantity_ratio,
        ROUND(SUM(t.revenue_usd)::FLOAT / NULLIF(SUM(c.control_revenue_usd), 0), 3) as revenue_ratio
        
    FROM treatment_metrics t
    INNER JOIN control_metrics c ON t.volume_bin = c.volume_bin AND t.volume_tier_label = c.volume_tier_label
    INNER JOIN most_liked_overlap_stats mlos ON mlos.volume_bin = 0  -- Join to overall stats
    GROUP BY t.experiment_group, mlos.total_store_items_in_bin, mlos.overlapping_with_most_liked, mlos.pct_overlap_with_most_liked
)

/*---------------------------------------------------------------
 15. Final output - Badge experiment results by store-item volume deciles
----------------------------------------------------------------*/
SELECT * FROM bin_treatment_effects
UNION ALL
SELECT * FROM overall_effects
ORDER BY
    CASE WHEN volume_tier_label = 'OVERALL' THEN 0 ELSE 1 END,
    volume_bin,
    treatment_arm;
