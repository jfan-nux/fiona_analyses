

CREATE OR REPLACE TABLE proddb.fionafan.item_volume_by_store_item_arm_d20250725_31 AS
WITH
/*---------------------------------------------------------------------------
  1.  Valid iOS exposures for “enable_item_order_count_l30d_badge” test
----------------------------------------------------------------------------*/
exposures AS (
    SELECT
        bucket_key      AS consumer_id,
        experiment_group
    FROM METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures

),
/*---------------------------------------------------------------------------
  2.  Core marketplace deliveries in the analysis window
----------------------------------------------------------------------------*/
deliveries_dated AS (
    SELECT
        delivery_id                        AS order_id,
        store_id,
        store_name,
        creator_id                         AS consumer_id
    FROM proddb.public.dimension_deliveries
    WHERE   is_filtered_core         = 1
        AND NVL(fulfillment_type,'') NOT IN ('shipping')
        AND creator_id IS NOT NULL
        AND active_date BETWEEN '2025-07-25' AND '2025-07-31'
),
/*---------------------------------------------------------------------------
  3.  Map each order to its experiment arm
----------------------------------------------------------------------------*/
orders_with_arm AS (
    SELECT
        d.order_id,
        d.store_id,
        d.store_name,
        e.experiment_group
    FROM deliveries_dated  d
    JOIN exposures         e
      ON e.consumer_id = d.consumer_id
),
/*---------------------------------------------------------------------------
  4.  Item-level rows in the same date window
      (fact_merchant_order_items uses active_date_utc)
----------------------------------------------------------------------------*/
order_items_dated AS (
    SELECT
        f.delivery_id      AS order_id,
        f.store_id,
        f.item_id,
        f.item_name,
        f.menu_category_name,
        f.quantity,
        f.subtotal,
        f.order_item_id
    FROM edw.merchant.fact_merchant_order_items f
    WHERE f.active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
)
/*---------------------------------------------------------------------------
  5.  Combine & aggregate – one row per store × item × arm
----------------------------------------------------------------------------*/
SELECT
      o.store_id,
      o.store_name,
      i.item_id,
      i.item_name,
      -- i.menu_category_name,
      o.experiment_group,
      COUNT(DISTINCT o.order_id)                  AS order_volume,
      COUNT(1)                                    AS order_frequency,
      SUM(i.quantity)                             AS total_quantity,
      SUM(i.subtotal)                             AS total_subtotal_cents   -- in cents
FROM orders_with_arm     o
JOIN order_items_dated   i
  ON i.order_id  = o.order_id                   -- links items to the tagged orders
GROUP BY
      o.store_id,
      o.store_name,
      i.item_id,
      i.item_name,
      i.menu_category_name,
      o.experiment_group
ORDER BY
      o.store_id,
      i.item_id,
      o.experiment_group;




CREATE OR REPLACE TABLE proddb.fionafan.social_cue_m_card_click_full AS
SELECT 
  cv.*,
  e.experiment_group
FROM (
  SELECT consumer_id, page, timestamp, name, store_id, store_name, container, position, type, card_position, dd_device_id, badges, container_name, business_id, submarket_id, dd_platform
  , item_card_position,item_collection_name, item_collection_position, item_collection_type, item_id, item_name, item_score, item_star_rating, item_price, item_position, item_photo_position, item_image_url, item_is_retail
  FROM iguazu.consumer.m_card_click
  WHERE  TIMESTAMP BETWEEN '2025-07-25 00:00:00' AND '2025-07-31 23:59:59'
) cv
JOIN METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures e 
  ON cv.CONSUMER_ID = e.bucket_key;




/*==========================================================================*\
|  Delivery-status mix-shift                                               |
|  – 7-day arm metrics + 30-day & 365-day baselines                        |
|                                                                          |
|  Output: proddb.fionafan.store_item_delivery_status_by_arm_d20250725_31  |
\*==========================================================================*/

CREATE OR REPLACE TABLE proddb.fionafan.store_item_delivery_status_by_arm_d20250725_31 AS
WITH
/*---------------------------------------------------------------------------
  A.  Valid iOS exposures (experiment arms)
----------------------------------------------------------------------------*/
exposures AS (
    SELECT bucket_key  AS consumer_id,
           experiment_group
    FROM  METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures
),
/*---------------------------------------------------------------------------
  B.  7-day deliveries (core marketplace, ASAP, high-quality only)
----------------------------------------------------------------------------*/
deliveries_7d AS (
    SELECT
        dd.delivery_id,
        dd.store_id,
        dd.store_name,
        dd.creator_id                  AS consumer_id,
        dd.created_at,
        dd.first_assignment_made_time,
        dd.dasher_confirmed_time,
        dd.d2r_duration,
        dd.dasher_at_store_time,
        dd.actual_pickup_time,
        dd.dasher_wait_duration,
        dd.r2c_duration,
        dd.actual_delivery_time,
        dd.quoted_delivery_time,              -- needed for calcs
        dd.active_date
    FROM  proddb.public.dimension_deliveries dd
    WHERE dd.active_date BETWEEN '2025-07-25' AND '2025-07-31'
      AND dd.is_filtered_core        = 1            -- quality orders only
      AND dd.is_asap                 = TRUE
      AND dd.is_consumer_pickup      = FALSE
      AND NVL(dd.fulfillment_type,'') NOT IN ('shipping')
),
/*---------------------------------------------------------------------------
  C.  Tag each delivery with arm
----------------------------------------------------------------------------*/
orders_with_arm_7d AS (
    SELECT  d.*, e.experiment_group
    FROM    deliveries_7d d
    JOIN    exposures   e
          ON e.consumer_id = d.consumer_id
),
/*---------------------------------------------------------------------------
  D.  Item rows for those deliveries
----------------------------------------------------------------------------*/
items_7d AS (
    SELECT
        f.delivery_id,
        f.store_id,
        f.item_id,
        f.item_name
    FROM edw.merchant.fact_merchant_order_items f
    WHERE f.active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
),
/*---------------------------------------------------------------------------
  E.  ETA range table (quoted windows) – restrict to the 7-day set first
----------------------------------------------------------------------------*/
eta_7d AS (
    SELECT
        r.delivery_id,
        r.quoted_delivery_time_range_min,
        r.quoted_delivery_time_range_max
    FROM   EDW.LOGISTICS.DASHER_DELIVERY_ETA_RANGE r
    WHERE  r.delivery_id IN (SELECT DISTINCT delivery_id FROM deliveries_7d)
),
/*---------------------------------------------------------------------------
  F.  Combine → compute 7-day metrics per store × item × arm
----------------------------------------------------------------------------*/
status_7d AS (
    SELECT
        i.store_id,
        o.store_name,
        i.item_id,
        i.item_name,
        o.experiment_group,

        COUNT(*)                                                     AS deliveries_7d,

        /*  time-based metrics in minutes  */
        AVG(DATEDIFF('second', o.created_at,               o.first_assignment_made_time)) / 60  AS alat_min_7d,
        AVG(DATEDIFF('second', o.first_assignment_made_time, o.dasher_confirmed_time))          / 60  AS conflat_min_7d,
        AVG(o.d2r_duration) / 60                                              AS d2r_min_7d,
        AVG(DATEDIFF('second', o.dasher_at_store_time, o.actual_pickup_time)) / 60  AS baw_min_7d,
        AVG(o.dasher_wait_duration) / 60                                       AS dasher_wait_min_7d,
        AVG(o.r2c_duration) / 60                                               AS r2c_min_7d,
        AVG(DATEDIFF('second', o.created_at, o.actual_delivery_time)) / 60     AS asap_min_7d,

        /* punctuality flags (proportions) */
        AVG( CASE WHEN o.actual_delivery_time BETWEEN e.quoted_delivery_time_range_min
                                             AND     e.quoted_delivery_time_range_max THEN 1 ELSE 0 END )  AS pct_ontime_7d,
        AVG( CASE WHEN o.actual_delivery_time <  e.quoted_delivery_time_range_min  THEN 1 ELSE 0 END )      AS pct_early_7d,
        AVG( CASE WHEN o.actual_delivery_time >  e.quoted_delivery_time_range_max  THEN 1 ELSE 0 END )      AS pct_late_7d,
        AVG( CASE WHEN o.actual_delivery_time >
                        DATEADD('second', 60*20, e.quoted_delivery_time_range_max) THEN 1 ELSE 0 END )      AS pct_late20_7d
    FROM   orders_with_arm_7d         o
    JOIN   items_7d                   i  USING (delivery_id)
    JOIN   eta_7d                     e  USING (delivery_id)
    GROUP  BY
           i.store_id,
           o.store_name,
           i.item_id,
           i.item_name,
           o.experiment_group
),
/*---------------------------------------------------------------------------
  G.  30-day baseline (no arm split)  |  25-Jun-2025 → 24-Jul-2025
----------------------------------------------------------------------------*/
baseline_deliveries_30d AS (
    SELECT
        dd.delivery_id,
        dd.store_id,
        dd.created_at,
        dd.first_assignment_made_time,
        dd.dasher_confirmed_time,
        dd.d2r_duration,
        dd.dasher_at_store_time,
        dd.actual_pickup_time,
        dd.dasher_wait_duration,
        dd.r2c_duration,
        dd.actual_delivery_time,
        dd.quoted_delivery_time,
        dd.active_date
    FROM   proddb.public.dimension_deliveries dd
    WHERE  dd.active_date BETWEEN '2025-06-25' AND '2025-07-24'
      AND  dd.is_filtered_core = 1
      AND  dd.is_asap          = TRUE
      AND  dd.is_consumer_pickup = FALSE
      AND  NVL(dd.fulfillment_type,'') NOT IN ('shipping')
),
items_30d AS (
    SELECT delivery_id, store_id, item_id
    FROM   edw.merchant.fact_merchant_order_items
    WHERE  active_date_utc BETWEEN '2025-06-25' AND '2025-07-24'
),
eta_30d AS (
    SELECT delivery_id,
           quoted_delivery_time_range_min,
           quoted_delivery_time_range_max
    FROM   EDW.LOGISTICS.DASHER_DELIVERY_ETA_RANGE
    WHERE  delivery_id IN (SELECT DISTINCT delivery_id FROM baseline_deliveries_30d)
),
status_30d AS (
    SELECT
        i.store_id,
        i.item_id,
        COUNT(*)                                                  AS deliveries_30d,
        AVG(DATEDIFF('second', d.created_at, d.first_assignment_made_time))/60   AS alat_min_30d,
        AVG(DATEDIFF('second', d.first_assignment_made_time, d.dasher_confirmed_time))/60  AS conflat_min_30d,
        AVG(d.d2r_duration)/60                                    AS d2r_min_30d,
        AVG(DATEDIFF('second', d.dasher_at_store_time, d.actual_pickup_time))/60 AS baw_min_30d,
        AVG(d.dasher_wait_duration)/60                            AS dasher_wait_min_30d,
        AVG(d.r2c_duration)/60                                    AS r2c_min_30d,
        AVG(DATEDIFF('second', d.created_at, d.actual_delivery_time))/60         AS asap_min_30d,
        AVG(CASE WHEN d.actual_delivery_time BETWEEN e.quoted_delivery_time_range_min
                                        AND     e.quoted_delivery_time_range_max THEN 1 ELSE 0 END)  AS pct_ontime_30d,
        -- NEW:
        AVG(CASE WHEN d.actual_delivery_time <  e.quoted_delivery_time_range_min THEN 1 ELSE 0 END)  AS pct_early_30d,
        AVG(CASE WHEN d.actual_delivery_time >  e.quoted_delivery_time_range_max THEN 1 ELSE 0 END)  AS pct_late_30d,
        AVG(CASE WHEN d.actual_delivery_time >
                        DATEADD('second', 60*20, e.quoted_delivery_time_range_max) THEN 1 ELSE 0 END) AS pct_late20_30d
    FROM   baseline_deliveries_30d d
    JOIN   items_30d               i USING (delivery_id)
    JOIN   eta_30d                 e USING (delivery_id)
    GROUP  BY i.store_id, i.item_id
),
/*---------------------------------------------------------------------------
  H.  365-day baseline (26-Jul-2024 → 24-Jul-2025)
----------------------------------------------------------------------------*/
baseline_deliveries_365d AS (
    SELECT  *
    FROM    proddb.public.dimension_deliveries dd
    WHERE   dd.active_date BETWEEN '2024-07-26' AND '2025-07-24'
      AND   dd.is_filtered_core = 1
      AND   dd.is_asap          = TRUE
      AND   dd.is_consumer_pickup = FALSE
      AND   NVL(dd.fulfillment_type,'') NOT IN ('shipping')
),
items_365d AS (
    SELECT delivery_id, store_id, item_id
    FROM   edw.merchant.fact_merchant_order_items
    WHERE  active_date_utc BETWEEN '2024-07-26' AND '2025-07-24'
),
eta_365d AS (
    SELECT delivery_id,
           quoted_delivery_time_range_min,
           quoted_delivery_time_range_max
    FROM   EDW.LOGISTICS.DASHER_DELIVERY_ETA_RANGE
    WHERE  delivery_id IN (SELECT DISTINCT delivery_id FROM baseline_deliveries_365d)
),
status_365d AS (
    SELECT
        i.store_id,
        i.item_id,
        COUNT(*)                                                AS deliveries_365d,
        AVG(DATEDIFF('second', d.created_at, d.first_assignment_made_time))/60   AS alat_min_365d,
        AVG(DATEDIFF('second', d.first_assignment_made_time, d.dasher_confirmed_time))/60  AS conflat_min_365d,
        AVG(d.d2r_duration)/60                                  AS d2r_min_365d,
        AVG(DATEDIFF('second', d.dasher_at_store_time, d.actual_pickup_time))/60 AS baw_min_365d,
        AVG(d.dasher_wait_duration)/60                          AS dasher_wait_min_365d,
        AVG(d.r2c_duration)/60                                  AS r2c_min_365d,
        AVG(DATEDIFF('second', d.created_at, d.actual_delivery_time))/60         AS asap_min_365d,
        AVG(CASE WHEN d.actual_delivery_time BETWEEN e.quoted_delivery_time_range_min
                                        AND     e.quoted_delivery_time_range_max THEN 1 ELSE 0 END)  AS pct_ontime_365d,
        AVG(CASE WHEN d.actual_delivery_time <  e.quoted_delivery_time_range_min THEN 1 ELSE 0 END)  AS pct_early_365d,
        AVG(CASE WHEN d.actual_delivery_time >  e.quoted_delivery_time_range_max THEN 1 ELSE 0 END)  AS pct_late_365d,
        AVG(CASE WHEN d.actual_delivery_time >
                        DATEADD('second', 60*20, e.quoted_delivery_time_range_max) THEN 1 ELSE 0 END) AS pct_late20_365d
    FROM   baseline_deliveries_365d d
    JOIN   items_365d               i USING (delivery_id)
    JOIN   eta_365d                 e USING (delivery_id)
    GROUP  BY i.store_id, i.item_id
)
/*---------------------------------------------------------------------------
  I.  Assemble everything
----------------------------------------------------------------------------*/
SELECT
      s7.store_id,
      s7.store_name,
      s7.item_id,
      s7.item_name,
      s7.experiment_group,

      /* 7-day (arm) metrics */
      s7.deliveries_7d,
      s7.alat_min_7d,
      s7.conflat_min_7d,
      s7.d2r_min_7d,
      s7.baw_min_7d,
      s7.dasher_wait_min_7d,
      s7.r2c_min_7d,
      s7.asap_min_7d,
      s7.pct_ontime_7d,
      s7.pct_early_7d,
      s7.pct_late_7d,
      s7.pct_late20_7d,

      /* 30-day baseline */
      COALESCE(b30.deliveries_30d,0)           AS deliveries_30d,
      b30.alat_min_30d,
      b30.conflat_min_30d,
      b30.d2r_min_30d,
      b30.baw_min_30d,
      b30.dasher_wait_min_30d,
      b30.r2c_min_30d,
      b30.asap_min_30d,
      b30.pct_ontime_30d,
      b30.pct_early_30d,
      b30.pct_late_30d,
      b30.pct_late20_30d,

      /* 365-day baseline */
      COALESCE(b365.deliveries_365d,0)         AS deliveries_365d,
      b365.alat_min_365d,
      b365.conflat_min_365d,
      b365.d2r_min_365d,
      b365.baw_min_365d,
      b365.dasher_wait_min_365d,
      b365.r2c_min_365d,
      b365.asap_min_365d,
      b365.pct_ontime_365d,
      b365.pct_early_365d,
      b365.pct_late_365d,
      b365.pct_late20_365d
FROM   status_7d            s7
LEFT   JOIN status_30d      b30
       ON  s7.store_id = b30.store_id
       AND s7.item_id  = b30.item_id
LEFT   JOIN status_365d     b365
       ON  s7.store_id = b365.store_id
       AND s7.item_id  = b365.item_id
ORDER  BY
      s7.store_id,
      s7.item_id,
      s7.experiment_group;



/*==========================================================================*\
|  Is delivery service worse in the treatments?                             |
|                                                                           |
|  1.  Pick the restaurant’s top-5 items *within every arm* (by 7-day order |
|      volume) from …_mixshift_by_arm_d20250725_31_pct                      |
|  2.  Pull 7-day delivery-status KPIs for those items from                 |
|      …_delivery_status_by_arm_d20250725_31                                |
|  3.  Aggregate KPI’s two ways:                                            |
|        a) per-restaurant × arm (weighted by deliveries_7d)                |
|        b) overall per arm (again weighted)                                |
|  4.  Return differences vs the control arm                                |
|                                                                           |
|  Result tables:                                                           |
|    •  proddb.fionafan.store_arm_top5_delivery_kpi_d20250725_31            |
|         – restaurant-level KPI roll-ups                                   |
|    •  proddb.fionafan.arm_delivery_kpi_top5_vs_control_d20250725_31       |
|         – overall per-arm KPI & deltas vs control                         |
\*==========================================================================*/

/*==========================================================================*\
|  Arm-level delivery KPI’s – top-5 items per restaurant (by order_rank_exp) |
|  Window: 25-Jul-2025 → 31-Jul-2025                                        |
|                                                                           |
|  Result: proddb.fionafan.arm_delivery_kpi_top5_vs_control_d20250725_31     |
\*==========================================================================*/

use database proddb;
use schema fionafan;

/*──────────────── 1.  Flag every store-item as top5 / not ────────────────*/
CREATE OR REPLACE TEMP TABLE _item_flags AS
SELECT
        store_id,
        item_id,
        experiment_group,
        CASE WHEN order_rank_exp <= 5 THEN 1 ELSE 0 END AS is_top5
FROM    proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_pct;


/*──────────────── 2.  Bring KPI row for every store-item ─────────────────*/
CREATE OR REPLACE TEMP TABLE _item_kpi AS
SELECT  f.store_id,
        f.item_id,
        f.experiment_group,
        f.is_top5,

        k.pct_ontime_7d  AS pct_ontime,
        k.pct_late_7d    AS pct_late,
        k.pct_late20_7d  AS pct_late20
FROM    _item_flags          f
JOIN    proddb.fionafan.store_item_delivery_status_by_arm_d20250725_31 k
       ON k.store_id         = f.store_id
      AND k.item_id          = f.item_id
      AND k.experiment_group = f.experiment_group;


/*──────────────── 3.  Build two data sets: top5 only, overall ────────────*/
CREATE OR REPLACE TEMP TABLE _item_kpi_labeled AS
SELECT  *,                  'top5'    AS group_label
FROM    _item_kpi
WHERE   is_top5 = 1

UNION ALL

SELECT  *,                  'overall' AS group_label
FROM    _item_kpi;          -- all rows


/*──────────────── 4.  Aggregate item-level means & variances ─────────────*/
CREATE OR REPLACE TEMP TABLE _stats_by_arm_grp AS
SELECT
      group_label,
      experiment_group,
      COUNT(*)                       AS n_items,

      AVG(pct_ontime)                AS mean_pct_ontime,
      VAR_SAMP(pct_ontime)           AS var_pct_ontime,

      AVG(pct_late)                  AS mean_pct_late,
      VAR_SAMP(pct_late)             AS var_pct_late,

      AVG(pct_late20)                AS mean_pct_late20,
      VAR_SAMP(pct_late20)           AS var_pct_late20
FROM  _item_kpi_labeled
GROUP BY group_label, experiment_group;


/*──────────────── 5.  Deltas & z-stats vs control for each grouping ──────*/
CREATE OR REPLACE TABLE proddb.fionafan.arm_delivery_kpi_top5_overall_itemLevel_z_d20250725_31
AS
WITH
ctrl AS (
    SELECT * FROM _stats_by_arm_grp
    WHERE experiment_group = 'control'
),

tests AS (
    SELECT * FROM _stats_by_arm_grp
    WHERE experiment_group <> 'control'
)

SELECT
    t.group_label,             -- 'top5' or 'overall'
    t.experiment_group,        -- treatment arm
    t.n_items,                 -- # of store-items in that group & arm

    /*────────  raw means  ────────*/
    t.mean_pct_ontime,
    t.mean_pct_late,
    t.mean_pct_late20,

    /*────────  deltas vs control  ────────*/
    t.mean_pct_ontime - c.mean_pct_ontime      AS delta_pct_ontime,
    t.mean_pct_late   - c.mean_pct_late        AS delta_pct_late,
    t.mean_pct_late20 - c.mean_pct_late20      AS delta_pct_late20,

    /*────────  z-stats  (difference of means)  ────────*/
    /* on-time */
    ( t.mean_pct_ontime - c.mean_pct_ontime ) /
      NULLIF( SQRT( t.var_pct_ontime / NULLIF(t.n_items,0)
                  + c.var_pct_ontime / NULLIF(c.n_items,0) ), 0 )
        AS z_pct_ontime,

    /* late */
    ( t.mean_pct_late - c.mean_pct_late ) /
      NULLIF( SQRT( t.var_pct_late / NULLIF(t.n_items,0)
                  + c.var_pct_late / NULLIF(c.n_items,0) ), 0 )
        AS z_pct_late,

    /* late20 */
    ( t.mean_pct_late20 - c.mean_pct_late20 ) /
      NULLIF( SQRT( t.var_pct_late20 / NULLIF(t.n_items,0)
                  + c.var_pct_late20 / NULLIF(c.n_items,0) ), 0 )
        AS z_pct_late20

FROM   tests t
JOIN   ctrl  c
   ON  t.group_label = c.group_label   -- compare within same grouping
ORDER BY
      group_label,
      CASE WHEN t.experiment_group='control' THEN 0 ELSE 1 END,
      experiment_group;
select * from proddb.fionafan.arm_delivery_kpi_top5_overall_itemLevel_z_d20250725_31;
select * from proddb.fionafan.arm_delivery_kpi_top5_itemLevel_z_d20250725_31;





CREATE OR REPLACE TABLE proddb.fionafan.store_item_impression_distribution_by_arm_d20250725_31 AS
WITH
/*---------------------------------------------------------------------------
  2.  Impressions in the window  (STORE_ID & ITEM_ID present)
----------------------------------------------------------------------------*/
impressions AS (
    SELECT
        cv.consumer_id,
        cv.store_id,
        cv.store_name,
        cv.item_id,
        cv.item_name,
        cv.experiment_group,
        cv.position
    FROM proddb.fionafan.social_cue_m_card_click_full  cv
    WHERE 1=1
      -- AND cv.store_id IS NOT NULL
      -- AND cv.item_id  IS NOT NULL
)
/*---------------------------------------------------------------------------
  3.  Aggregate: impressions per store × item × arm
----------------------------------------------------------------------------*/
SELECT
      store_id,
      store_name,
      item_id,
      item_name,
      experiment_group,
      COUNT(*)  AS impressions,     -- total impressions of this item in this arm
      avg(position) as avg_item_position
FROM impressions
GROUP BY 
      store_id,
      store_name,
      item_id,
      item_name,
      experiment_group
ORDER BY
      store_id,
      item_id,
      experiment_group;


CREATE OR REPLACE TABLE proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_new AS
WITH
/*---------------------------------------------------------------------------
  1.  7-day impressions & orders (already materialised)
----------------------------------------------------------------------------*/

deliveries_baseline_exp AS (
    SELECT
        -- core_delivery_volume, delivery_id, store_id, business_id, vertical, consumer_id, device_id, is_filtered_core, is_restaurant, management_type, country_id  
        core_delivery_volume as delivery_id, experiment_group
    FROM (select core_delivery_volume, consumer_id from proddb.public.firefly_measure_financials_delivery_creation_core
    WHERE   is_filtered_core   =      'True'
        AND dte BETWEEN '2025-07-25' AND '2025-07-31') a
    inner join METRICS_REPO.PUBLIC.enable_item_order_count_l30d_badge__iOS_users_exposures e 
  ON a.CONSUMER_ID = e.bucket_key
    group by all
),
baseline_exp AS (
    SELECT
        a.store_id, a.store_name,
        a.item_id, a.item_name, experiment_group,
        COUNT(DISTINCT a.delivery_id)    AS order_volume_exp
    FROM edw.merchant.fact_merchant_order_items a
    inner join deliveries_baseline_exp b on a.delivery_id = b.delivery_id
    WHERE a.active_date_utc BETWEEN '2025-07-25' AND '2025-07-31'
    GROUP BY all
),
orders_exp AS (
    SELECT
        store_id,
        store_name,
        item_id,
        item_name,
        experiment_group,
        order_volume_exp                       -- distinct-order count 7/25-7/31
    FROM baseline_exp 
),
ranked_7d AS (      -- rank within each store × arm
    SELECT
        m.*,
        RANK() OVER (PARTITION BY store_id, experiment_group
                     ORDER BY order_volume_exp DESC)
                     AS order_rank_exp
    FROM orders_exp m
),
/*---------------------------------------------------------------------------
  2.  30-day baseline (2025-06-25 → 2025-07-24)
----------------------------------------------------------------------------*/
deliveries_baseline_30d AS (
    SELECT
        -- core_delivery_volume, delivery_id, store_id, business_id, vertical, consumer_id, device_id, is_filtered_core, is_restaurant, management_type, country_id  
        core_delivery_volume as delivery_id
    FROM proddb.public.firefly_measure_financials_delivery_creation_core
    WHERE   is_filtered_core   =      'True'
        AND dte BETWEEN '2025-06-25' AND '2025-07-24'
    group by all
),
baseline_30d AS (
    SELECT
        a.store_id,
        a.item_id,
        COUNT(DISTINCT a.delivery_id)    AS order_volume_30d
    FROM edw.merchant.fact_merchant_order_items a
    inner join deliveries_baseline_30d b on a.delivery_id = b.delivery_id
    WHERE a.active_date_utc BETWEEN '2025-06-25' AND '2025-07-24'
    GROUP BY a.store_id, a.item_id
),
baseline_30d_ranked AS (
    SELECT
        b.*,
        RANK() OVER (PARTITION BY store_id
                     ORDER BY order_volume_30d DESC) AS order_rank_30d
    FROM baseline_30d b
),
/*---------------------------------------------------------------------------
  3.  365-day baseline (2024-07-26 → 2025-07-24)
----------------------------------------------------------------------------*/
deliveries_baseline_365d AS (
    -- SELECT
    --     delivery_id                  
    -- FROM proddb.public.dimension_deliveries
    -- WHERE   is_filtered_core         = 1
    --     AND NVL(fulfillment_type,'') NOT IN ('shipping')
    --     and creator_id is not null
    --     AND active_date BETWEEN '2024-07-26' AND '2025-07-24'
    -- group by all
    SELECT
        -- core_delivery_volume, delivery_id, store_id, business_id, vertical, consumer_id, device_id, is_filtered_core, is_restaurant, management_type, country_id  
        core_delivery_volume as delivery_id
    FROM proddb.public.firefly_measure_financials_delivery_creation_core
    WHERE   is_filtered_core   =      'True'
        AND dte BETWEEN '2024-07-26' AND '2025-07-24'
    group by all
),
baseline_365d AS (
    SELECT
        a.store_id,
        a.item_id,
        COUNT(DISTINCT a.delivery_id)    AS order_volume_365d
    FROM edw.merchant.fact_merchant_order_items a
    inner join deliveries_baseline_365d b on a.delivery_id = b.delivery_id
    WHERE active_date_utc BETWEEN '2024-07-26' AND '2025-07-24'
    GROUP BY a.store_id, a.item_id
),

baseline_365d_ranked AS (
    SELECT
        b.*,
        RANK() OVER (PARTITION BY store_id
                     ORDER BY order_volume_365d DESC) AS order_rank_365d
    FROM baseline_365d b
)
/*---------------------------------------------------------------------------
  4.  Final mix-shift table
----------------------------------------------------------------------------*/
SELECT
      r.store_id,
      r.store_name,
      r.item_id,
      r.item_name,
      r.experiment_group,

      -- 7-day metrics
      r.order_volume_exp,
      r.order_rank_exp,

      -- 30-day baseline
      COALESCE(b30.order_volume_30d, 0)    AS order_volume_30d,
      b30.order_rank_30d,

      -- 365-day baseline
      COALESCE(b365.order_volume_365d, 0)  AS order_volume_365d,
      b365.order_rank_365d
FROM ranked_7d              r
LEFT JOIN baseline_30d_ranked  b30
       ON  r.store_id = b30.store_id
       AND r.item_id  = b30.item_id
LEFT JOIN baseline_365d_ranked b365
       ON  r.store_id = b365.store_id
       AND r.item_id  = b365.item_id
ORDER BY
      r.store_id,
      r.item_id,
      r.experiment_group;




/*==========================================================================*\
|  Item-mix shift – add %-of-restaurant columns & rank-agreement flags      |
|                                                                          |
|  Source table:  proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_new
|  Target table:  proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_pct
\*==========================================================================*/

CREATE OR REPLACE TABLE proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_pct AS
WITH base AS (
    SELECT
        t.*,

        /* totals needed for percentage calculations */
        SUM(order_volume_exp)  OVER (PARTITION BY store_id, experiment_group)
            AS total_volume_exp_store_arm,

        SUM(order_volume_30d)  OVER (PARTITION BY store_id)
            AS total_volume_30d_store,

        SUM(order_volume_365d) OVER (PARTITION BY store_id)
            AS total_volume_365d_store
    FROM proddb.fionafan.store_item_mixshift_by_arm_d20250725_31_new t
)
SELECT
      store_id,
      store_name,
      item_id,
      item_name,
      experiment_group,

      /* raw volumes & ranks */
      order_volume_exp,
      order_rank_exp,
      order_volume_30d,
      order_rank_30d,
      order_volume_365d,
      order_rank_365d,

      /*  NEW: share of restaurant’s volume  */
      order_volume_exp  / NULLIF(total_volume_exp_store_arm, 0)  AS pct_order_volume_exp,
      order_volume_30d  / NULLIF(total_volume_30d_store, 0)     AS pct_order_volume_30d,
      order_volume_365d / NULLIF(total_volume_365d_store, 0)    AS pct_order_volume_365d,

      /*  NEW: rank-agreement flags  */
      CASE WHEN order_rank_exp<=5 and order_rank_30d<=5  THEN 1 ELSE 0 END  AS rank_match_30d,
      CASE WHEN order_rank_exp<=5 and order_rank_365d<=5 THEN 1 ELSE 0 END  AS rank_match_365d
FROM base
ORDER BY
      store_id,
      item_id,
      experiment_group;




select experiment_group, case when lower(badges) like '%recent order%' then 'recent_order' when lower(badges) like '%most like%' then 'most_liked' else 'other' end as label, count(1) cnt, avg(CARD_POSITION) as card_position 
from  proddb.fionafan.social_cue_m_card_click_full group by all order by all;