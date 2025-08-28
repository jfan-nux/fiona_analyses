



create or replace table proddb.fionafan.preference_toggle_ice_latest as (

  select consumer_id, dd_device_id, 
         TO_TIMESTAMP(max(iguazu_event_time)/1000) max_event_time, 
         listagg(entity_id, ',') within group (order by entity_id) entity_ids
  from (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
  FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 
  WHERE toggle_type = 'add' and page = 'onboarding_preference_page' 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) = 1 
  )
  group by all
  );


create or replace table proddb.fionafan.preference_experiment_data as (
SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
                , segment
                , min (custom_attributes:userId) as user_id
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'cx_mobile_onboarding_preferences'
AND ee.segment = 'iOS'
AND experiment_version::INT = 1
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-18' AND '2025-09-30'
GROUP BY 1,2,3,4,5

);

-- select user_id, count(1) cnt from proddb.fionafan.experiment_preference_events group by all having cnt>1 order by cnt desc;
select * from proddb.fionafan.experiment_preference_events where user_id = '1125900323560425' order by event_timestamp asc;



-- Create combined table with experiment exposure, preferences, and post-exposure events
CREATE OR REPLACE TABLE proddb.fionafan.experiment_preference_events AS (

WITH experiment_with_preferences AS (
  SELECT 
    exp.tag,
    exp.result,
    exp.bucket_key,
    exp.dd_device_ID_filtered,
    exp.user_id,
    exp.segment,
    exp.day,
    exp.exposure_time,
    -- Include entity_ids for treatment users, null for control
    CASE 
      WHEN exp.tag <> 'control' THEN pref.entity_ids 
      ELSE NULL 
    END AS entity_ids,
    -- Calculate effective exposure time
    CASE 
      WHEN exp.tag <> 'control' AND pref.max_event_time IS NOT NULL 
        THEN GREATEST(exp.exposure_time, pref.max_event_time)
      ELSE exp.exposure_time 
    END AS effective_exposure_time,
    pref.consumer_id,
    pref.max_event_time AS preference_time
  FROM proddb.fionafan.preference_experiment_data exp
  LEFT JOIN proddb.fionafan.preference_toggle_ice_latest pref
    ON exp.dd_device_ID_filtered = replace(lower(CASE WHEN pref.dd_device_id like 'dx_%' then pref.dd_device_id
                    else 'dx_'||pref.dd_device_id end), '-') 
),
events_with_experiment AS (
  SELECT 
    ewp.*,
    events.event_date,
    events.session_num,
    events.platform,
    events.timestamp AS event_timestamp,
    events.event_type,
    events.event,
    events.discovery_surface,
    events.discovery_feature,
    events.detail,
    events.store_id,
    events.store_name,
    events.context_timezone,
    events.context_os_version,
    events.event_rank,
    events.discovery_surface_click_attr,
    events.discovery_feature_click_attr,
    events.discovery_surface_imp_attr,
    events.discovery_feature_imp_attr,
    events.pre_purchase_flag,
    events.l91d_orders,
    events.last_order_date
  FROM experiment_with_preferences ewp
  INNER JOIN tyleranderson.events_all events
    ON lower(ewp.dd_device_ID_filtered) = replace(lower(CASE WHEN events.dd_device_id like 'dx_%' then events.dd_device_id
                      else 'dx_'||events.dd_device_id end), '-') 
    AND events.timestamp > ewp.effective_exposure_time
    AND events.event_date >= '2025-08-18'  -- Filter events after 2025-08-18
),
ranked_events AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY dd_device_ID_filtered ORDER BY event_timestamp) AS action_rank
  FROM events_with_experiment
)
SELECT * FROM ranked_events);


create or replace  table proddb.fionafan.preference_experiment_m_card_view as (
  WITH experiment_with_preferences AS (
  SELECT 
    exp.tag,
    exp.result,
    exp.bucket_key,
    exp.user_id,
    exp.dd_device_ID_filtered,
    exp.segment,
    exp.day,
    exp.exposure_time,
    -- Include entity_ids for treatment users, null for control
    CASE 
      WHEN exp.tag <> 'control' THEN pref.entity_ids 
      ELSE NULL 
    END AS entity_ids,
    -- Calculate effective exposure time
    CASE 
      WHEN exp.tag <> 'control' AND pref.max_event_time IS NOT NULL 
        THEN GREATEST(exp.exposure_time, pref.max_event_time)
      ELSE exp.exposure_time 
    END AS effective_exposure_time,
    pref.consumer_id,
    pref.max_event_time AS preference_time
  FROM proddb.fionafan.preference_experiment_data exp

  LEFT JOIN proddb.fionafan.preference_toggle_ice_latest pref


    ON exp.dd_device_ID_filtered = replace(lower(CASE WHEN pref.dd_device_id like 'dx_%' then pref.dd_device_id
                    else 'dx_'||pref.dd_device_id end), '-')
)
, m_card_view_base as (
select convert_timezone('UTC','America/Los_Angeles',received_at)::date as event_date, context_device_type as platform, user_id,consumer_id, dd_device_id, convert_timezone('UTC','America/Los_Angeles',timestamp) timestamp
, 'store_impression' as event_type ,event,

case
    when page like 'post_checkout%' then 'DoubleDash'
    when page in ('explore_page','homepage') then 'Home Page'
    when page = 'vertical_page' then 'Vertical Page'
    when page ilike ('grocery_tab%') then 'Grocery Tab'
    when page ilike ('retail_tab%') then 'Retail Tab'
    when page = 'vertical_search_page' then 'Vertical Search'
    when page in ('search_autocomplete','search_results') then 'Search Tab'
    when page = 'saved_stores_page' then 'Saved Stores Page'
    when page = 'pickup' then 'Pickup Tab'
    when page IN ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offers Tab'
    -- when container in('store','list') AND page in ('collection_page_list','collection_landing_page') AND tile_name IS NOT NULL then 'Landing Page'
    when page in ('collection_page_list','collection_landing_page') then 'Landing Page'
    when container = 'banner_carousel' then 'Home Page'
    when page ILIKE 'post_checkout%' then 'DoubleDash'
    when page = 'all_reviews' then 'All Reviews Page'
    when page = 'order_history' then 'Order History Tab' -- this is broken for iOS
    when page = 'browse_page' then 'Browse Tab'
    when page = 'open_carts_page' then 'Open Carts Page'
    when page = 'cuisine_see_all_page' then 'Cuisine See All Page'
    when page = 'checkout_aisle' then 'Checkout Aisle Page'
    -- when page = 'cuisine_filter_search_result' then 'Cuisine Search'
    -- when container = 'cluster' then 'Other Pages'
    else 'Other'--page
  end as discovery_surface,

  case
    when page ILIKE 'post_checkout%' then 'DoubleDash'
    when page = 'all_reviews' then 'Reviews'
    when container = 'collection' AND page in ('explore_page', 'vertical_page') AND tile_name IS NOT NULL then 'Collection Carousel Landing Page'
    when page = 'vertical_page' and container = 'cluster' then 'Traditional Carousel'
    when page = 'vertical_search_page' then 'Vertical Search'
    when page in ('search_autocomplete') then 'Autocomplete'
    when page in ('search_results') then 'Core Search'
    when page = 'saved_stores_page' then 'Saved Stores'
    when page = 'pickup' then 'Pickup'
    when page IN ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offers'
    when container in('store','list') AND page in ('collection_page_list','collection_landing_page') AND tile_name IS NOT NULL then 'Collection Carousel Landing Page'
    when container in('store','list') AND page in ('collection_page_list','collection_landing_page') then 'Traditional Carousel Landing Page'
    when container = 'cluster' then 'Traditional Carousel'
    when container = 'banner_carousel' then 'Banner'
    when container IN ('store', 'list', 'item:go-to-store') AND page IN ('explore_page', 'vertical_page','cuisine_see_all_page') AND list_filter ilike '%cuisine:%' then 'Cuisine Filter'
    when container in('store','list') AND page IN ('explore_page', 'vertical_page') AND list_filter IS NOT NULL then 'Pill Filter'
    when container in('store','list') AND page IN ('explore_page', 'vertical_page') then 'Home Feed'
    when container = 'announcement' AND page IN ('explore_page', 'vertical_page') then 'Announcement'
    when page = 'explore_page' AND container = 'carousel' AND lower(carousel_name) = 'cuisines' then 'Entry Point - Cuisine Filter'
    when page = 'explore_page' AND container = 'carousel' AND carousel_name = 'Verticals' then 'Entry Point - Vertical Nav'
    when page = 'order_history' then 'Order History'
    when container ilike 'collection_standard_%'then 'Traditional Carousel'
    when page in ('grocery_tab') and container = 'grid' then 'Grocery Tab - Grid'
    when page in ('retail_tab') and container = 'grid' then 'Retail Tab - Grid'
    when page in ('grocery_tab') and container IN ('store', 'list', 'item:go-to-store') then 'Grocery Tab - Feed'
    when page in ('retail_tab') and container IN ('store', 'list', 'item:go-to-store') then 'Retail Tab - Feed'
    when page in ('grocery_tab_see_all_page') then 'Grocery Tab - See All Page'
    when page in ('retail_tab_see_all_page') then 'Retail Tab - See All Page'
    when page = 'browse_page' and container in('store','list', 'item:go-to-store') and query is not null then 'Browse Tab - Search'
    when page = 'open_carts_page' then 'Open Carts Page'
    else 'Other'--container
  end as discovery_feature,

coalesce(list_filter, query, container_name) as detail, to_number(store_id) as store_id, store_name, store_type,  CONTEXT_TIMEZONE, CONTEXT_OS_VERSION, null as event_rank,
cuisine_id, cuisine_name, container, container_name, container_id, container_description, page, item_id, item_card_position, item_name, card_position, badges, badges_text, cart_id, tile_name, tile_id, card_id, card_name, dd_delivery_correlation_id
 from iguazu.consumer.m_card_view
where convert_timezone('UTC','America/Los_Angeles',received_at) >='2025-08-18'
  and store_id is not null
  and item_name is null
  and item_id is null
  and page != 'store' -- Explicitly exclude Store page as a catch-all
  -- and page not like 'post_checkout%' -- Explicitly exclude DbD pages as a catch-all
//  and context_device_type = 'ios'
and ((page != 'order_history' and context_device_type = 'ios') or context_device_type = 'android')-- since order_history impressions are broken on ios. We launch impressions after a order for some reason adn triple fire them.
)

SELECT 
  -- Experiment data
  ewp.tag,
  ewp.result,
  ewp.bucket_key,
  ewp.dd_device_ID_filtered,
  ewp.segment,
  ewp.day,
  ewp.exposure_time,
  ewp.entity_ids,
  ewp.effective_exposure_time,
  ewp.consumer_id AS exp_consumer_id,  -- Rename to avoid conflict
  ewp.preference_time,
  
  -- Event data
  events.event_date,
  events.platform,
  events.user_id,
  events.consumer_id AS event_consumer_id,  -- Rename to avoid conflict
  events.dd_device_id,
  events.timestamp AS event_timestamp,
  events.event_type,
  events.event,
  events.discovery_surface,
  events.discovery_feature,
  events.detail,
  events.store_id,
  events.store_name,
  events.store_type,
  events.CONTEXT_TIMEZONE,
  events.CONTEXT_OS_VERSION,
  events.event_rank,
  events.cuisine_id,
  events.cuisine_name,
  events.container,
  events.container_name,
  events.container_id,
  events.container_description,
  events.page,
  events.item_id,
  events.item_card_position,
  events.item_name,
  events.card_position,
  events.badges,
  events.badges_text,
  events.cart_id,
  events.tile_name,
  events.tile_id,
  events.card_id,
  events.card_name,
  events.dd_delivery_correlation_id
FROM experiment_with_preferences ewp
INNER JOIN m_card_view_base events
  ON replace(lower(CASE WHEN events.dd_device_id like 'dx_%' then events.dd_device_id
                    else 'dx_'||events.dd_device_id end), '-') = ewp.dd_device_ID_filtered
  AND events.timestamp > ewp.effective_exposure_time
  AND events.event_date >= '2025-08-18'  -- Filter events after 2025-08-18
);

select count(1), sum(case when entity_ids is not null then 1 else 0 end) from proddb.fionafan.experiment_preference_events ;

-- select date_trunc('day', iguazu_timestamp) as date, count(1) cnt 
-- FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice group by all order by 1;


-- Test the new table with action ranking
select event, discovery_surface, discovery_feature, detail, count(1) cnt
from proddb.fionafan.experiment_preference_events 
where event_timestamp >= effective_exposure_time and event_timestamp < effective_exposure_time + interval '24 hour'
and action_rank<=10
and entity_ids is not null
group by all
;

-- Create table with order information for experiment preference events population
-- Following the same pattern as the experiment analysis logic
CREATE OR REPLACE TABLE proddb.fionafan.experiment_preference_orders AS (



  WITH order_events AS (
    SELECT DISTINCT 
      a.DD_DEVICE_ID,
      replace(lower(CASE WHEN a.DD_device_id like 'dx_%' then a.DD_device_id
                  else 'dx_'||a.DD_device_id end), '-') AS dd_device_ID_filtered,
      convert_timezone('UTC','America/Los_Angeles',a.timestamp)::date as order_day,
      convert_timezone('UTC','America/Los_Angeles',a.timestamp) as order_timestamp,
      a.order_cart_id,
      dd.delivery_id,
      dd.active_date,
      dd.created_at,
      dd.actual_delivery_time,
      dd.gov * 0.01 AS gov,  -- Convert to dollars
      dd.subtotal * 0.01 AS subtotal,
      dd.total_item_count,
      dd.distinct_item_count,
      dd.is_consumer_pickup,
      dd.is_first_ordercart,
      dd.is_first_ordercart_dd,
      dd.is_subscribed_consumer,
      dd.store_id,
      dd.store_name,
      dd.variable_profit * 0.01 AS variable_profit,
      dd.tip * 0.01 AS tip,
      dd.delivery_fee * 0.01 AS delivery_fee,
      dd.service_fee * 0.01 AS service_fee
    FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd
      ON a.order_cart_id = dd.order_cart_id
      AND dd.is_filtered_core = 1
      AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-08-18' AND '2025-09-30'
    WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-08-18' AND '2025-09-30'
  ),
  
  experiment_orders AS (
    SELECT distinct
      epe.tag,
      epe.result,
      epe.dd_device_ID_filtered,
      epe.effective_exposure_time,
      epe.user_id,
      
      -- Order information
      oe.delivery_id,
      oe.order_cart_id,
      oe.order_day,
      oe.order_timestamp,
      oe.active_date,
      oe.created_at AS order_created_at,
      oe.actual_delivery_time,
      oe.gov,
      oe.subtotal,
      oe.total_item_count,
      oe.distinct_item_count,
      oe.is_consumer_pickup,
      oe.is_first_ordercart,
      oe.is_first_ordercart_dd,
      oe.is_subscribed_consumer,
      oe.store_id,
      oe.store_name,
      oe.variable_profit,
      oe.tip,
      oe.delivery_fee,
      oe.service_fee,
      
      -- Calculate time between exposure and order
      DATEDIFF('hour', epe.effective_exposure_time, oe.order_timestamp) AS hours_between_exposure_and_order,
      DATEDIFF('day', epe.effective_exposure_time, oe.order_timestamp) AS days_between_exposure_and_order,
      

    FROM (select distinct dd_device_ID_filtered, effective_exposure_time, tag, result, user_id from proddb.fionafan.experiment_preference_events) epe

    LEFT JOIN order_events oe
      ON epe.dd_device_ID_filtered = oe.dd_device_ID_filtered 
      AND epe.effective_exposure_time <= oe.order_timestamp  -- Order must be after effective exposure
  )
  
  SELECT * FROM experiment_orders
  WHERE delivery_id IS NOT NULL  -- Only include rows where we found orders
);


-- Check the new table - following the experiment analysis pattern
SELECT 
  tag, is_first_ordercart,
  COUNT(DISTINCT dd_device_ID_filtered) AS unique_users,
  COUNT(DISTINCT delivery_id) AS total_orders,
  count(1) cnt,
  AVG(total_item_count) AS avg_item_count
FROM proddb.fionafan.experiment_preference_orders
GROUP BY all
ORDER BY all;


with event_type_totals as (
  select event_type, count(1) as cnt_total
  from proddb.tyleranderson.events_all 
  where date_trunc('day',timestamp)='2025-08-01' 
  group by event_type
),
detailed_counts as (
  select event_type, case when event_type = 'error' then 'error' else event end as event
  ,event_rank, case when event_type = 'error' then 'error' else discovery_feature end as  discovery_feature, discovery_surface, count(1) cnt
  from proddb.tyleranderson.events_all 
  where date_trunc('day',timestamp)='2025-08-01' 
  group by event_type, case when event_type = 'error' then 'error' else event end, event_rank
  , case when event_type = 'error' then 'error' else discovery_feature end, discovery_surface
)
select d.event_type, d.event, d.event_rank, d.discovery_feature, d.discovery_surface, d.cnt, t.cnt_total
from detailed_counts d
join event_type_totals t on d.event_type = t.event_type
order by t.cnt_total desc, d.cnt desc;

select distinct event_type, count(1) cnt from proddb.tyleranderson.events_all where date_trunc('day',timestamp)='2025-08-01' group by all;

select event_type, event, event_rank,discovery_feature,discovery_surface,  count(1) cnt from proddb.tyleranderson.events_all 

where event_type = 'funnel' and date_trunc('day',timestamp)='2025-08-01'
group by all
order by event_rank asc, cnt desc;