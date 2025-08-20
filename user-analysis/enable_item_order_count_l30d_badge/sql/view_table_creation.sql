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



CREATE OR REPLACE TABLE proddb.fionafan.treatment_item_order_volume_info AS
WITH base AS (
  SELECT
      store_id,
      item_id,
      item_name,
      experiment_group,
      badges,
      TO_NUMBER(
        REGEXP_SUBSTR(badges, '\\[(\\d+)\\+\\s*recent\\s+orders\\]', 1, 1, 'e', 1)
      ) AS recent_orders_volume
  FROM proddb.fionafan.social_cue_m_card_view_full
  WHERE store_id IS NOT NULL
    AND item_id  IS NOT NULL
    AND badges ILIKE '%recent orders%'
    AND experiment_group <> 'control'
)
SELECT
    store_id,
    item_id,
    item_name,
    -- experiment_group,
    LISTAGG(DISTINCT badges, ' | ')
      WITHIN GROUP (ORDER BY badges)                                   AS badges_list,
    LISTAGG(DISTINCT TO_VARCHAR(recent_orders_volume), ', ')
      WITHIN GROUP (ORDER BY TO_VARCHAR(recent_orders_volume))          AS recent_orders_volumes,
    AVG(recent_orders_volume)                                           AS avg_recent_orders_volume,
    COUNT(DISTINCT badges)                                              AS distinct_badge_count,
FROM base
GROUP BY store_id, item_id, item_name;


/*---------------------------------------------------------------
  Create table for control group store-items with "most liked" badges
----------------------------------------------------------------*/

CREATE OR REPLACE TABLE proddb.fionafan.most_liked_control_items AS
SELECT DISTINCT
    store_id,
    item_id,
    item_name,
    badges
FROM proddb.fionafan.social_cue_m_card_view_full
WHERE store_id IS NOT NULL
  AND item_id IS NOT NULL
  AND LOWER(badges) LIKE '%most liked%'
  AND experiment_group = 'control';

-- Check the results
SELECT 
    COUNT(*) AS total_most_liked_control_items,
    COUNT(DISTINCT store_id) AS distinct_stores,
    COUNT(DISTINCT item_id) AS distinct_items,
    COUNT(DISTINCT CONCAT(store_id, '-', item_id)) AS distinct_store_item_pairs
FROM proddb.fionafan.most_liked_control_items;



create or replace table proddb.fionafan.social_cue_m_card_click_item_card_position as (
with base as (
select experiment_group, store_id, item_id, -1 as tagged_recent_order_volume , avg(card_position) as card_position 
from proddb.fionafan.social_cue_m_card_click_full
where store_id is not null
and item_id is not null
and lower(badges) like '%most liked%'
and experiment_group = 'control'
group by all
union

select experiment_group, store_id, item_id, 
ROUND(
  AVG(
    CASE
      -- "1k+" / "12k+"
      WHEN REGEXP_SUBSTR(badges, '\\[(\\d+)k\\+\\s*recent\\s+orders\\]', 1, 1, 'e', 1) IS NOT NULL THEN
        TO_NUMBER(REGEXP_SUBSTR(badges, '\\[(\\d+)k\\+\\s*recent\\s+orders\\]', 1, 1, 'e', 1)) * 1000
      -- "1000+"
      ELSE TO_NUMBER(REGEXP_SUBSTR(badges, '\\[(\\d+)\\+\\s*recent\\s+orders\\]', 1, 1, 'e', 1))
    END
  ),
  -1
) as tagged_recent_order_volume , avg(card_position) as card_position
from proddb.fionafan.social_cue_m_card_click_full
where store_id is not null
and item_id is not null
and lower(badges) like '%recent orders%'
and experiment_group <> 'control'
group by all
),
control AS (
  SELECT
      store_id,
      item_id,
      card_position   AS control_card_position,
      tagged_recent_order_volume AS control_tagged_recent_order_volume
  FROM base
  WHERE experiment_group = 'control'
),

no_icon AS (
  SELECT
      store_id,
      item_id,
      card_position   AS no_icon_card_position,
      tagged_recent_order_volume AS no_icon_tagged_recent_order_volume
  FROM base
  WHERE experiment_group = 'no icon treatment'
),

icon AS (
  SELECT
      store_id,
      item_id,
      card_position   AS icon_card_position,
      tagged_recent_order_volume AS icon_tagged_recent_order_volume
  FROM base
  WHERE experiment_group = 'icon treatment'
)

SELECT
    COALESCE(c.store_id, n.store_id, i.store_id) AS store_id,
    COALESCE(c.item_id,  n.item_id,  i.item_id)  AS item_id,

    -- Control columns
    c.control_card_position,
    c.control_tagged_recent_order_volume,

    -- No icon treatment columns
    n.no_icon_card_position,
    n.no_icon_tagged_recent_order_volume,

    -- Icon treatment columns
    i.icon_card_position,
    i.icon_tagged_recent_order_volume

FROM control c
FULL OUTER JOIN no_icon n USING (store_id, item_id)
FULL OUTER JOIN icon   i USING (store_id, item_id)
);


select no_icon_tagged_recent_order_volume, avg(control_card_position), avg(no_icon_card_position), avg(icon_card_position), count(1), avg(case when no_icon_card_position>control_card_position then 1 else 0 end) pct_item_greater from  proddb.fionafan.social_cue_m_card_click_item_card_position 
where control_card_position is not null and no_icon_card_position is not null 
group by all;


select icon_tagged_recent_order_volume, avg(control_card_position), avg(icon_card_position), count(1), avg(case when icon_card_position>control_card_position then 1 else 0 end) pct_item_greater from  proddb.fionafan.social_cue_m_card_click_item_card_position 
where control_card_position is not null and icon_card_position is not null
group by all;


select no_icon_tagged_recent_order_volume, sum(case when control_card_position is not null then 1 else 0 end)/ count(1) as overlap from proddb.fionafan.social_cue_m_card_click_item_card_position where no_icon_card_position is not null group by all;

------------------------------------------------------------------------------------------------

WITH labeled AS (
  SELECT
    LOWER(experiment_group) AS experiment_group,
    CASE
      WHEN badges ILIKE '%most liked%'                  THEN 'most_liked'
      WHEN badges ILIKE '%recent orders%'               THEN 'recent_orders'
      WHEN badges ILIKE '%stock%'                       THEN 'stock'
      WHEN badges ILIKE '%member_pricing%'              THEN 'member_pricing'
      WHEN badges ILIKE '%dashpass_offer%'              THEN 'dashpass_offer'
      WHEN badges ILIKE '%great price%'                 THEN 'great_price'
      WHEN badges ILIKE '%top_rated_leaderboard%'       THEN 'top_rated_leaderboard'
      WHEN badges ILIKE '%type:offer%'                  THEN 'offer'
      WHEN badges ILIKE '%sponsored%'                   THEN 'sponsored'
      WHEN badges ILIKE '%farther_away%'                THEN 'farther_away'
      WHEN badges ILIKE '%reorder%'                     THEN 'reorder'
      WHEN badges ILIKE '%dashpass_exclusive_offer_locked%' THEN 'dashpass_exclusive_offer_locked'
      WHEN badges ILIKE '%buy 1%'                       THEN 'offer'
      WHEN badges ILIKE '%free%'                        THEN 'offer'
      WHEN badges ILIKE '%best_value%'                  THEN 'best_value'
      WHEN badges ILIKE '%snap%'                        THEN 'snap'
      WHEN badges ILIKE '%off%'                         THEN 'discount'
      WHEN badges ILIKE '%discount%'                    THEN 'discount'
      ELSE 'other'
    END AS badges_renamed,
    card_position
  FROM proddb.fionafan.social_cue_m_card_click_full
  WHERE card_position IS NOT NULL
),
agg AS (
  SELECT
    badges_renamed,
    experiment_group,
    AVG(card_position) AS avg_card_position,
    COUNT(*)           AS cnt
  FROM labeled
  WHERE experiment_group IN ('control', 'icon treatment')
  GROUP BY 1, 2
),
pivoted AS (
  SELECT
    badges_renamed,
    MAX(CASE WHEN experiment_group = 'control'        THEN avg_card_position END) AS control_avg_card_position,
    MAX(CASE WHEN experiment_group = 'icon treatment' THEN avg_card_position END) AS icon_avg_card_position,
    MAX(CASE WHEN experiment_group = 'control'        THEN cnt END)               AS control_cnt,
    MAX(CASE WHEN experiment_group = 'icon treatment' THEN cnt END)               AS icon_cnt
  FROM agg
  GROUP BY 1
)
SELECT
  badges_renamed,
  control_avg_card_position,
  icon_avg_card_position,
  (icon_avg_card_position - control_avg_card_position)    AS diff_signed,
  ABS(icon_avg_card_position - control_avg_card_position) AS diff_abs,
  control_cnt,
  icon_cnt
FROM pivoted
WHERE control_avg_card_position IS NOT NULL
  AND icon_avg_card_position IS NOT NULL
ORDER BY diff_abs DESC;