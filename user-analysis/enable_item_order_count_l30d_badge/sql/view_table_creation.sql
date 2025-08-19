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

