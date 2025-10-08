-- Impact follow-ups for Special Menu Default Selection experiment
-- Questions covered:
-- 1) Pre-period volume share of POS-error items by restaurant (store_id)
-- 2) Order rate lift by restaurant cut (deck_rank) using device normalization
-- 3) Chain volume share for chains identified from stores with diff > 2

/* -------------------------------------------
   Q1. Pre-30-day item share of restaurant delivery-id volume (prior to experiment start)
   - Build item-by-restaurant delivery counts over the 30 days prior to experiment start
   - Join to top-items table and compute item share of store-level distinct delivery volume
-------------------------------------------- */
-- Store pre-30-day item-by-restaurant delivery counts
CREATE OR REPLACE TABLE proddb.fionafan.special_menu_pre30_item_delivery_volume AS (
  WITH params AS (
    SELECT TO_DATE('2025-09-16') AS exp_start_date
  ),
  pre_period AS (
    SELECT
      DATEADD(DAY, -30, exp_start_date) AS start_date,
      DATEADD(DAY, -1,  exp_start_date) AS end_date
    FROM params
  ),
  base AS (
    SELECT
      dd.store_id,
      doi.item_id,
      dd.delivery_id,
      (dd.cancelled_at IS NOT NULL) AS is_cancelled
    FROM proddb.public.dimension_order_item doi
    JOIN proddb.public.dimension_deliveries dd
      ON dd.delivery_id = doi.delivery_id
      AND dd.is_filtered_core = TRUE
    JOIN pre_period p
      ON COALESCE(dd.actual_order_place_time, dd.created_at) >= p.start_date
     AND COALESCE(dd.actual_order_place_time, dd.created_at) <  p.end_date + 1
    WHERE COALESCE(dd.actual_order_place_time, dd.created_at) IS NOT NULL
  ),
  item_counts AS (
    SELECT
      store_id,
      item_id,
      COUNT(DISTINCT CASE WHEN NOT is_cancelled THEN delivery_id END) AS item_delivery_cnt,
      COUNT(DISTINCT CASE WHEN is_cancelled THEN delivery_id END)     AS item_cancel_cnt
    FROM base
    GROUP BY ALL
  ),
  store_counts AS (
    SELECT
      store_id,
      COUNT(DISTINCT CASE WHEN NOT is_cancelled THEN delivery_id END) AS store_delivery_cnt,
      COUNT(DISTINCT CASE WHEN is_cancelled THEN delivery_id END)     AS store_cancel_cnt
    FROM base
    GROUP BY ALL
  )
  SELECT
    i.store_id,
    i.item_id,
    i.item_delivery_cnt,
    i.item_cancel_cnt,
    s.store_delivery_cnt,
    s.store_cancel_cnt
  FROM item_counts i
  JOIN store_counts s USING (store_id)
);

-- Updated Q1 result using in-table denominators (and including cancel counts)
SELECT distinct
  ti.store_id,
  ti.store_name,
  ti.item_id,
  ti.item_name,
  iv.item_delivery_cnt,
  iv.item_cancel_cnt,
  iv.store_delivery_cnt,
  iv.store_cancel_cnt,
  iv.item_delivery_cnt / NULLIF(iv.store_delivery_cnt, 0) AS item_delivery_share_pre30,
  c.diff
FROM proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_items ti

INNER JOIN proddb.fionafan.special_menu_pre30_item_delivery_volume iv
  ON ti.store_id = iv.store_id AND ti.item_id = iv.item_id
left join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores c
on ti.store_id = c.store_id
where c.diff>=2
ORDER BY diff desc, item_delivery_share_pre30 DESC NULLS LAST;


/* -------------------------------------------
   Q2. Order rate lift by restaurant cut (deck_rank)
   - Device normalization applied per workspace rule when joining
   - Consider first completed order per device after exposure within exp window
   - Order rate = orderers / exposed for each experiment_group x deck_rank
-------------------------------------------- */
WITH params AS (
  SELECT
    TO_TIMESTAMP('2025-09-16 00:00:00') AS exp_start_ts,
    TO_TIMESTAMP('2025-10-30 00:00:00') AS exp_end_ts
),
exposures AS (
  SELECT
    /* device normalization */
    REPLACE(LOWER(CASE WHEN TO_CHAR(bucket_key) LIKE 'dx_%' THEN TO_CHAR(bucket_key) ELSE 'dx_' || TO_CHAR(bucket_key) END), '-') AS dd_device_id_filtered,
    first_exposure_time,
    experiment_group
  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures
  WHERE segment = 'All Users IOS'
),
exposures_dedup AS (
  /* One row per device: earliest exposure per group in window */
  SELECT
    e.dd_device_id_filtered,
    e.experiment_group,
    MIN(e.first_exposure_time) AS first_exposure_time
  FROM exposures e
  JOIN params p
    ON e.first_exposure_time >= p.exp_start_ts
   AND e.first_exposure_time <  p.exp_end_ts
  GROUP BY ALL
),
deliveries_norm AS (
  SELECT
    /* device normalization */
    REPLACE(LOWER(CASE WHEN dd.device_id LIKE 'dx_%' THEN dd.device_id ELSE 'dx_' || dd.device_id END), '-') AS dd_device_id_filtered,
    COALESCE(dd.actual_order_place_time, dd.created_at) AS order_ts,
    dd.store_id,
    dd.cancelled_at
  FROM proddb.public.dimension_deliveries dd
  where active_date between '2025-09-15' and '2025-09-30'
  and is_filtered_core = true 
),
deliveries_after_exposure AS (
  SELECT
    ed.experiment_group,
    d.store_id,
    d.delivery_id
  FROM exposures_dedup ed
  JOIN deliveries_norm d
    ON d.dd_device_id_filtered = ed.dd_device_id_filtered
   AND d.order_ts >= ed.first_exposure_time
  JOIN params p
    ON d.order_ts >= p.exp_start_ts AND d.order_ts < p.exp_end_ts
  WHERE d.cancelled_at IS NULL
),
deliveries_with_store AS (
  SELECT
    dae.experiment_group,
    s.deck_rank,
    dae.delivery_id
  FROM deliveries_after_exposure dae
  JOIN edw.merchant.dimension_store s
    ON s.store_id = dae.store_id
),
deliveries_by_group_cut AS (
  SELECT
    experiment_group,
    deck_rank,
    COUNT(DISTINCT delivery_id) AS deliveries
  FROM deliveries_with_store
  GROUP BY ALL
),
first_orders AS (
  SELECT
    ed.dd_device_id_filtered,
    ed.experiment_group,
    d.store_id,
    d.order_ts,
    ROW_NUMBER() OVER (
      PARTITION BY ed.dd_device_id_filtered
      ORDER BY d.order_ts ASC
    ) AS rn
  FROM exposures_dedup ed
  JOIN deliveries_norm d
    ON d.dd_device_id_filtered = ed.dd_device_id_filtered
   AND d.order_ts >= ed.first_exposure_time
  JOIN params p
    ON d.order_ts >= p.exp_start_ts AND d.order_ts < p.exp_end_ts
  WHERE d.cancelled_at IS NULL
),
first_orders_kept AS (
  SELECT dd_device_id_filtered, experiment_group, store_id
  FROM first_orders
  WHERE rn = 1
),
orders_with_store AS (
  SELECT
    f.dd_device_id_filtered,
    f.experiment_group,
    s.deck_rank
  FROM first_orders_kept f
  JOIN edw.merchant.dimension_store s
    ON s.store_id = f.store_id
),
orderers_by_group_cut AS (
  SELECT
    experiment_group,
    deck_rank,
    COUNT(DISTINCT dd_device_id_filtered) AS orderers
  FROM orders_with_store
  GROUP BY ALL
),
exposed_by_group AS (
  SELECT
    experiment_group,
    COUNT(DISTINCT dd_device_id_filtered) AS exposed
  FROM exposures_dedup
  GROUP BY ALL
),
rates AS (
  SELECT
    o.deck_rank,
    o.experiment_group,
    o.orderers,
    e.exposed,
    o.orderers / NULLIF(e.exposed, 0) AS order_rate
  FROM orderers_by_group_cut o
  JOIN exposed_by_group e USING (experiment_group)
)
SELECT
  deck_rank,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN order_rate END) AS treatment_order_rate,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN order_rate END) AS control_order_rate,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN order_rate END)
    - MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN order_rate END) AS order_rate_lift,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN deliveries END) AS treatment_deliveries,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN deliveries END) AS control_deliveries,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN deliveries END)
    / NULLIF(MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN eg.exposed END), 0) AS treatment_orders_per_exposed,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN deliveries END)
    / NULLIF(MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN eg.exposed END), 0) AS control_orders_per_exposed,
  (
    MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN deliveries END)
      / NULLIF(MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN eg.exposed END), 0)
    - MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN deliveries END)
      / NULLIF(MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN eg.exposed END), 0)
  ) AS orders_per_exposed_lift
FROM rates
LEFT JOIN deliveries_by_group_cut USING (experiment_group, deck_rank)
LEFT JOIN exposed_by_group eg USING (experiment_group)
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER (ORDER BY order_rate_lift DESC NULLS LAST) <= 10
ORDER BY order_rate_lift DESC NULLS LAST;


/* -------------------------------------------
   Q2b. Order rate lift by store attribute cuts (table output)
   - Creates: proddb.fionafan.special_menu_order_rate_lift_by_store_cuts
   - Output columns: cut_column, cut_dimension, lift
-------------------------------------------- */
CREATE OR REPLACE TABLE proddb.fionafan.special_menu_order_rate_lift_by_store_cuts AS (
WITH params AS (
  SELECT
    TO_TIMESTAMP('2025-09-16 00:00:00') AS exp_start_ts,
    TO_TIMESTAMP('2025-10-30 00:00:00') AS exp_end_ts
),
exposures AS (
  SELECT
    REPLACE(LOWER(CASE WHEN TO_CHAR(bucket_key) LIKE 'dx_%' THEN TO_CHAR(bucket_key) ELSE 'dx_' || TO_CHAR(bucket_key) END), '-') AS dd_device_id_filtered,
    first_exposure_time,
    experiment_group
  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures
  WHERE segment = 'All Users IOS'
),
exposures_dedup AS (
  SELECT
    e.dd_device_id_filtered,
    e.experiment_group,
    MIN(e.first_exposure_time) AS first_exposure_time
  FROM exposures e
  JOIN params p
    ON e.first_exposure_time >= p.exp_start_ts
   AND e.first_exposure_time <  p.exp_end_ts
  GROUP BY ALL
),
deliveries_norm AS (
  SELECT
    REPLACE(LOWER(CASE WHEN dd.dd_device_id LIKE 'dx_%' THEN dd.dd_device_id ELSE 'dx_' || dd.dd_device_id END), '-') AS dd_device_id_filtered,
    COALESCE(dd.actual_order_place_time, dd.created_at) AS order_ts,
    dd.store_id,
    dd.cancelled_at,
    dd.DELIVERY_ID
  FROM proddb.public.dimension_deliveries dd
  WHERE active_date BETWEEN '2025-09-15' AND '2025-09-30'
    AND is_filtered_core = TRUE
),
deliveries_after_exposure AS (
  SELECT
    ed.experiment_group,
    d.store_id,
    d.delivery_id
  FROM exposures_dedup ed
  JOIN deliveries_norm d
    ON d.dd_device_id_filtered = ed.dd_device_id_filtered
   AND d.order_ts >= ed.first_exposure_time
  JOIN params p
    ON d.order_ts >= p.exp_start_ts AND d.order_ts < p.exp_end_ts
  WHERE d.cancelled_at IS NULL
),
deliveries_by_cut AS (
  SELECT 'is_cng' AS cut_column, COALESCE(CAST(ds.is_cng AS STRING), 'unknown') AS cut_dimension,
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id) AS deliveries
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_drive_enabled', COALESCE(CAST(ds.is_drive_enabled AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_storefront_enabled', COALESCE(CAST(ds.is_storefront_enabled AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_active_menulink', COALESCE(CAST(ds.is_active_menulink AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_virtual_brand', COALESCE(CAST(ds.is_virtual_brand AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_active_store', COALESCE(CAST(ds.is_active_store AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_pos_partner', COALESCE(CAST(ds.is_pos_partner AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_national_business', COALESCE(CAST(ds.is_national_business AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_pickup_enabled', COALESCE(CAST(ds.is_pickup_enabled AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_delivery_enabled', COALESCE(CAST(ds.is_delivery_enabled AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
--   SELECT 'is_self_fulfillement', COALESCE(CAST(ds.is_self_fulfillement AS STRING), 'unknown'),
--          dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
--   FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
--   GROUP BY ALL
--   UNION ALL
  SELECT 'is_managed_account', COALESCE(CAST(ds.is_managed_account AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_storefront_active_last_30_day', COALESCE(CAST(ds.is_storefront_active_last_30_day AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'primary_category_name', COALESCE(CAST(ds.primary_category_name AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
--   SELECT 'active_store', COALESCE(CAST(ds.active_store AS STRING), 'unknown'),
--          dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
--   FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
--   GROUP BY ALL
--   UNION ALL
  SELECT 'is_partner', COALESCE(CAST(ds.is_partner AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
--   UNION ALL
--   SELECT 'is_active_business', COALESCE(CAST(COALESCE(ds.is_active_business, ds."is _active_business") AS STRING), 'unknown'),
--          dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
--   FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
--   GROUP BY ALL
  UNION ALL
  SELECT 'is_corporate', COALESCE(CAST(ds.is_corporate AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_franchise', COALESCE(CAST(ds.is_franchise AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
  UNION ALL
  SELECT 'is_mp_enabled', COALESCE(CAST(ds.is_mp_enabled AS STRING), 'unknown'),
         dae.experiment_group, COUNT(DISTINCT dae.delivery_id)
  FROM deliveries_after_exposure dae JOIN edw.merchant.dimension_store ds ON ds.store_id = dae.store_id
  GROUP BY ALL
),
exposed_by_group AS (
  SELECT
    experiment_group,
    COUNT(DISTINCT dd_device_id_filtered) AS exposed
  FROM exposures_dedup
  GROUP BY ALL
),
rates_by_cut AS (
  SELECT
    dc.cut_column,
    dc.cut_dimension,
    dc.experiment_group,
    dc.deliveries,
    eg.exposed,
    dc.deliveries / NULLIF(eg.exposed, 0) AS order_rate
  FROM deliveries_by_cut dc
  JOIN exposed_by_group eg USING (experiment_group)
)
SELECT
  cut_column,
  cut_dimension,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN exposed   END) AS treatment_exposed,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN exposed   END) AS control_exposed,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN deliveries END) AS treatment_numerator,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN deliveries END) AS control_numerator,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN order_rate END) AS treatment_rate,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN order_rate END) AS control_rate,
  MAX(CASE WHEN LOWER(experiment_group) LIKE 'treat%'   THEN order_rate END)
    - MAX(CASE WHEN LOWER(experiment_group) LIKE 'control%' THEN order_rate END) AS lift
FROM rates_by_cut
GROUP BY ALL
);

select * from proddb.fionafan.special_menu_order_rate_lift_by_store_cuts 
where cut_column = 'is_franchise'
order by lift desc;
/* -------------------------------------------
   Q3. Chain volume share for chains from stores where diff > 2
   - Build chain candidates from store_name (strip trailing parentheses)
   - Match all stores whose names start with the candidate chain names
   - Compute share of GOV in experiment window vs overall GOV
-------------------------------------------- */

select * from dimension_store limit 10;
WITH params AS (
  SELECT
    TO_TIMESTAMP('2025-09-16 00:00:00') AS exp_start_ts,
    TO_TIMESTAMP('2025-10-30 00:00:00') AS exp_end_ts
),
top_stores AS (
  SELECT DISTINCT store_name
  FROM proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores


  WHERE diff > 2
),
chain_candidates AS (
  SELECT DISTINCT
    TRIM(REGEXP_REPLACE(store_name, '\\s*\\(.*\\)$', '')) AS chain_name
  FROM top_stores
  WHERE store_name IS NOT NULL
),
store_universe AS (
  SELECT
    ds.store_id,
    ds.name as store_name,
    cc.chain_name,
    ds.business_name
  FROM edw.merchant.dimension_store ds
  JOIN chain_candidates cc
    ON LOWER(ds.name) LIKE LOWER(cc.chain_name) || '%'
),
  store_error_counts AS (
    SELECT
      store_id,
      store_name,
      (treatment_cnt + control_cnt) AS error_cnt
    FROM proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores
    WHERE diff > 2
  ),
orders_in_window AS (
  SELECT
    dd.store_id,
    SUM(doi.unit_price * doi.order_item_quantity) / 100.0 AS gov
  FROM proddb.public.dimension_order_item doi
  JOIN proddb.public.dimension_deliveries dd
    ON dd.delivery_id = doi.delivery_id
  JOIN params p
    ON COALESCE(dd.actual_order_place_time, dd.created_at) >= p.exp_start_ts
   AND COALESCE(dd.actual_order_place_time, dd.created_at) <  p.exp_end_ts
  WHERE dd.cancelled_at IS NULL
    AND COALESCE(dd.actual_order_place_time, dd.created_at) IS NOT NULL
    AND dd.is_test = FALSE
    AND dd.is_from_store_to_us = FALSE
    AND dd.fulfillment_type NOT IN ('merchant_fleet', 'shipping')
  GROUP BY ALL
),
chain_volume AS (
  SELECT
    su.business_name,
    SUM(o.gov) AS chain_gov
  FROM store_universe su
  JOIN orders_in_window o
    ON o.store_id = su.store_id
  GROUP BY ALL
),
  chain_error_counts AS (
    SELECT
      su.business_name,
      SUM(sec.error_cnt) AS chain_error_cnt
    FROM store_universe su
    JOIN store_error_counts sec
      ON sec.store_id = su.store_id
    GROUP BY ALL
  ),
total_volume AS (
  SELECT SUM(gov) AS total_gov FROM orders_in_window
  ),
  total_errors AS (
    SELECT SUM(error_cnt) AS total_error_cnt FROM store_error_counts
)
SELECT
  c.business_name,
  c.chain_gov,
  t.total_gov,
  c.chain_gov / NULLIF(t.total_gov, 0) AS volume_share,
  coalesce(e.chain_error_cnt,0) as chain_error_cnt,
  coalesce(te.total_error_cnt,0) as total_error_cnt,
  e.chain_error_cnt / NULLIF(te.total_error_cnt, 0) AS error_share
FROM chain_volume c
CROSS JOIN total_volume t
LEFT JOIN chain_error_counts e USING (business_name)
CROSS JOIN total_errors te
ORDER BY volume_share DESC NULLS LAST;


