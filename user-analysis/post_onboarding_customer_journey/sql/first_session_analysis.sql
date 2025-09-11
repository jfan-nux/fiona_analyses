create or replace table proddb.fionafan.preference_toggle_ice_latest as (


  select consumer_id, dd_device_id, 
         TO_TIMESTAMP(max(iguazu_event_time)/1000) max_event_time, 
         listagg(entity_id, ',') within group (order by entity_id) entity_ids
  from (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
  FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 

  WHERE toggle_type = 'add' and page = 'onboarding_preference_page' and iguazu_timestamp >= '2025-08-04'
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
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-04' AND '2025-09-30'
GROUP BY 1,2,3,4,5

);


-- First session analysis: Analyze first session behavior within 24h of effective exposure
-- Create sequential session numbering (1,2,3,4...) and filter for first session

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
    -- Aggregate entity_ids into a single list per device (in case of multiple preference rows)
    entity_ids,
    -- Calculate effective exposure time (take the latest preference time if multiple exist)
    CASE 
      WHEN exp.tag <> 'control' AND MAX(pref.max_event_time) IS NOT NULL 
        THEN GREATEST(exp.exposure_time, MAX(pref.max_event_time))
      ELSE exp.exposure_time 
    END AS effective_exposure_time,
    -- Take the first non-null consumer_id if multiple exist
    COALESCE(MIN(pref.consumer_id), exp.user_id) AS consumer_id,
    MAX(pref.max_event_time) AS preference_time
  FROM proddb.fionafan.preference_experiment_data exp
  LEFT JOIN proddb.fionafan.preference_toggle_ice_latest pref
    ON exp.dd_device_ID_filtered = replace(lower(CASE WHEN pref.dd_device_id like 'dx_%' then pref.dd_device_id
                    else 'dx_'||pref.dd_device_id end), '-') 
      and exp.user_id = pref.consumer_id
  GROUP BY 
    exp.tag,
    exp.result, 
    exp.bucket_key,
    exp.dd_device_ID_filtered,
    exp.user_id,
    exp.segment,
    exp.day,
    exp.exposure_time,
    pref.entity_ids
)
, events_with_experiment AS (
  SELECT 
    ewp.*,
    events.event_date,
    events.session_num,
    events.platform,
    events.timestamp AS event_timestamp,
    events.event AS event_type,
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
  left JOIN tyleranderson.events_all events
    ON lower(ewp.dd_device_ID_filtered) = replace(lower(CASE WHEN events.dd_device_id like 'dx_%' then events.dd_device_id
                      else 'dx_'||events.dd_device_id end), '-') 
    AND events.timestamp > ewp.exposure_time
    AND events.event_date >= '2025-08-04'  -- Filter events after 2025-08-04
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
where convert_timezone('UTC','America/Los_Angeles',received_at) >='2025-08-04'
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
  AND events.event_date >= '2025-08-04'  -- Filter events after 2025-08-04
);


select tag, count(distinct dd_device_ID_filtered) from proddb.fionafan.preference_experiment_m_card_view group by all;
select tag, count(distinct dd_device_ID_filtered) from proddb.fionafan.experiment_preference_events 
where event='m_card_view' and store_id is not null  and discovery_surface = 'Home Page'  group by all;
select tag, count(distinct dd_device_ID_filtered) from proddb.fionafan.experiment_preference_events 
where event='m_card_view' and store_id is not null  and discovery_surface = 'Home Page' group by all;
select discovery_feature, discovery_surface, tag, count(1) from proddb.fionafan.experiment_preference_events 
where event='m_card_view' and store_id is not null group by all order by all ;
select tag, count(distinct dd_device_ID_filtered) from proddb.fionafan.preference_first_session_analysis where if_exists_first_session = 1 group by all;

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
      AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-08-04' AND '2025-09-30'
    WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-08-04' AND '2025-09-30'
  ),
  
  experiment_orders AS (
    SELECT distinct
      epe.tag,
      epe.result,
      epe.dd_device_ID_filtered,
      epe.exposure_time,
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
      DATEDIFF('hour', epe.exposure_time, oe.order_timestamp) AS hours_between_exposure_and_order,
      DATEDIFF('day', epe.exposure_time, oe.order_timestamp) AS days_between_exposure_and_order,
      

    FROM (select distinct dd_device_ID_filtered, exposure_time, tag, result, user_id from proddb.fionafan.experiment_preference_events) epe

    LEFT JOIN order_events oe
      ON epe.dd_device_ID_filtered = oe.dd_device_ID_filtered 
      AND epe.exposure_time <= oe.order_timestamp  -- Order must be after effective exposure
  )
  
  SELECT * FROM experiment_orders
  WHERE delivery_id IS NOT NULL  -- Only include rows where we found orders
);

-- Step 1: Add lag values to detect session changes

-- Check total experiment population (should match source)
select tag, count(distinct dd_device_ID_filtered) 
from proddb.fionafan.experiment_preference_events 

group by all;

-- Check balanced population and engagement rates in final table
select tag, 
  count(distinct dd_device_ID_filtered) as total_users,
  sum(if_exists_first_session) as users_with_first_session,
  avg(if_exists_first_session) as first_session_engagement_rate
from proddb.fionafan.preference_first_session_analysis 
group by all;


-- select dd_device_id_filtered, max(session_num) ses from proddb.fionafan.experiment_preference_events 
-- where event_timestamp between effective_exposure_time and effective_exposure_time + interval '24 hour'
-- group by all
-- having ses>2 limit 10
-- ;

create or replace table proddb.fionafan.preference_first_session_analysis as (
WITH all_experiment_users AS (
  SELECT DISTINCT
    tag,
    result,
    dd_device_ID_filtered,
    exposure_time,
    user_id,
    entity_ids
  FROM proddb.fionafan.experiment_preference_events


),

events_with_lag AS (
  SELECT 
    tag,
    result,
    dd_device_ID_filtered,
    exposure_time,
    user_id,
    entity_ids,
    session_num,
    event_timestamp,
    event_type,
    event,
    discovery_feature,  
    discovery_surface,
    store_id,
    store_name,
    action_rank,
    -- Get previous session_num for comparison
    LAG(session_num) OVER (
      PARTITION BY dd_device_ID_filtered, exposure_time 
      ORDER BY event_timestamp
    ) AS prev_session_num
  FROM proddb.fionafan.experiment_preference_events 
  WHERE event_timestamp >= exposure_time 
    AND event_timestamp < exposure_time + interval '24 hour'
),

-- Step 2: Detect session changes and create sequential session numbers
events_with_sequential_sessions AS (
  SELECT 
    tag,
    result,
    dd_device_ID_filtered,
    exposure_time,
    user_id,
    entity_ids,
    session_num,
    event_timestamp,
    event_type,
    event,
    discovery_feature,
    discovery_surface,
    store_id,
    store_name,
    action_rank,
    -- Detect when session_num changes from previous row
    CASE WHEN prev_session_num != session_num OR prev_session_num IS NULL 
         THEN 1 ELSE 0 END AS session_change,
    -- Create sequential session numbering
    SUM(CASE WHEN prev_session_num != session_num OR prev_session_num IS NULL 
             THEN 1 ELSE 0 END) OVER (
      PARTITION BY dd_device_ID_filtered, exposure_time 
      ORDER BY event_timestamp 
      ROWS UNBOUNDED PRECEDING
    ) AS sequential_session_num
  FROM events_with_lag
)
-- Step 3: Get first session events with store dimensions
, first_session_events_base AS (
  SELECT 
    ess.tag,
    ess.result,
    ess.dd_device_ID_filtered,
    ess.exposure_time,
    ess.user_id,
    ess.entity_ids,
    ess.session_num,
    ess.event_timestamp,
    ess.event_type,
    ess.event,
    ess.discovery_feature,
    ess.discovery_surface,
    ess.store_id,
    ess.store_name,
    ess.action_rank,
    -- Store dimension fields (lowercase)
    LOWER(ds.PRIMARY_CATEGORY_NAME) AS primary_category_name,
    LOWER(ds.PRIMARY_TAG_NAME) AS primary_tag_name,
    LOWER(ds.CUISINE_TYPE) AS cuisine_type
  FROM events_with_sequential_sessions ess
  LEFT JOIN edw.merchant.dimension_store ds
    ON ess.store_id = ds.store_id
  WHERE ess.sequential_session_num = 1
),

-- Step 4: LEFT JOIN all users with their first session events (if any)
first_session_events AS (
  SELECT 
    u.tag,
    u.result,
    u.dd_device_ID_filtered,
    u.exposure_time,
    u.user_id,
    u.entity_ids,
    e.session_num,
    e.event_timestamp,
    e.event_type,
    e.event,
    e.discovery_feature,
    e.discovery_surface,
    e.store_id,
    e.store_name,
    e.action_rank,
    e.primary_category_name,
    e.primary_tag_name,
    e.cuisine_type
  FROM all_experiment_users u
  LEFT JOIN first_session_events_base e
    ON u.dd_device_ID_filtered = e.dd_device_ID_filtered
    AND u.exposure_time = e.exposure_time
)

-- Step 5: Aggregate first session metrics at device_id level
,first_session_summary AS (
  SELECT 
    tag,
    result,
    dd_device_ID_filtered,
    exposure_time,
    user_id,
    entity_ids,
    
    -- Store information (only where store_name is not null)
    LISTAGG(DISTINCT 
      CASE WHEN store_name IS NOT NULL 
           THEN store_id::TEXT 
           ELSE NULL END, 
      ';'
    ) AS store_id_list,
    
    LISTAGG(DISTINCT 
      CASE WHEN store_name IS NOT NULL 
           THEN store_name 
           ELSE NULL END, 
      ';'
    ) AS store_name_list,
    
    -- Store dimension information (lowercase, only for stores with non-null store_name)
    LISTAGG(DISTINCT 
      CASE WHEN store_name IS NOT NULL 
           THEN primary_category_name 
           ELSE NULL END, 
      ';'
    ) AS primary_category_name_list,
    
    LISTAGG(DISTINCT 
      CASE WHEN store_name IS NOT NULL 
           THEN primary_tag_name 
           ELSE NULL END, 
      ';'
    ) AS primary_tag_name_list,
    
    LISTAGG(DISTINCT 
      CASE WHEN store_name IS NOT NULL 
           THEN cuisine_type 
           ELSE NULL END, 
      ';'
    ) AS cuisine_type_list,
    
    -- Count of unique stores with non-null store_name
    COUNT(DISTINCT CASE WHEN store_name IS NOT NULL THEN store_id END) AS unique_stores_count,
    
    -- Check if user had any first session events (1 if yes, 0 if no)
    CASE WHEN COUNT(event_timestamp) > 0 THEN 1 ELSE 0 END AS if_exists_first_session,
    
    -- Check if place_order event exists in first session
    CASE WHEN MAX(CASE WHEN event LIKE '%place_order%' OR event = 'action_place_order' 
                       THEN 1 ELSE 0 END) = 1 
         THEN 1 ELSE 0 END AS if_place_order,
    
    
    
    -- Check if 'cuisine' appears in discovery_feature (case insensitive)
    CASE WHEN MAX(CASE WHEN LOWER(discovery_feature) LIKE '%cuisine%'
                       THEN 1 ELSE 0 END) = 1 
         THEN 1 ELSE 0 END AS if_cuisine_filter,
    
    -- Viewed a store card (any m_card_view with a store_id)
    CASE WHEN MAX(CASE WHEN event = 'm_card_view' AND store_id IS NOT NULL
                       THEN 1 ELSE 0 END) = 1
         THEN 1 ELSE 0 END AS if_viewed_store,
    
    -- Used search (m_card_view on search surfaces/features)
    CASE WHEN MAX(CASE WHEN event = 'm_card_view' AND (
                           LOWER(discovery_surface) LIKE '%search%'
                        OR LOWER(discovery_feature) LIKE '%search%')
                       THEN 1 ELSE 0 END) = 1
         THEN 1 ELSE 0 END AS if_used_search,
    
    -- Check if entity_ids overlap with store dimensions viewed
    CASE 
      WHEN entity_ids IS NULL THEN NULL
      WHEN MAX(CASE 
        -- Check if any entity_id appears in any of the store dimension lists
        WHEN (LOWER(entity_ids) LIKE '%' || LOWER(primary_category_name) || '%' AND primary_category_name IS NOT NULL)
          OR (LOWER(entity_ids) LIKE '%' || LOWER(primary_tag_name) || '%' AND primary_tag_name IS NOT NULL) 
          OR (LOWER(entity_ids) LIKE '%' || LOWER(cuisine_type) || '%' AND cuisine_type IS NOT NULL)
        THEN 1 ELSE 0 
      END) = 1 THEN 1 
      ELSE 0 
    END AS if_entity_store_overlap,
    
    -- Count number of impression rows where entity_ids overlap with store dimensions
    CASE 
      WHEN entity_ids IS NULL THEN NULL
      ELSE SUM(CASE 
        -- Count each row where any entity_id appears in any of the store dimensions
        WHEN (LOWER(entity_ids) LIKE '%' || LOWER(primary_category_name) || '%' AND primary_category_name IS NOT NULL)
          OR (LOWER(entity_ids) LIKE '%' || LOWER(primary_tag_name) || '%' AND primary_tag_name IS NOT NULL) 
          OR (LOWER(entity_ids) LIKE '%' || LOWER(cuisine_type) || '%' AND cuisine_type IS NOT NULL)
        THEN 1 ELSE 0 
      END)
    END AS entity_store_overlap_count,
    
    -- Additional first session metrics
    COUNT(event_timestamp) AS total_events_first_session,
    SUM(CASE WHEN event_type = 'store_impression' THEN 1 ELSE 0 END) AS total_events_store_impression,
    MIN(event_timestamp) AS first_event_timestamp,
    MAX(event_timestamp) AS last_event_timestamp,
    MAX(action_rank) AS max_action_rank_first_session
    
  FROM first_session_events
  GROUP BY tag, result, dd_device_ID_filtered, exposure_time, user_id, entity_ids
),

-- Step 6: Get order metrics within 24h of effective exposure
order_metrics_24h AS (
  SELECT 
    dd_device_ID_filtered,
    exposure_time,
    
    -- Whether any order was placed within 24h
    CASE WHEN COUNT(DISTINCT delivery_id) > 0 THEN 1 ELSE 0 END AS order_placed_24h,
    
    -- Total metrics within 24h
    SUM(gov) AS total_gov_24h,
    COUNT(DISTINCT delivery_id) AS total_orders_24h,
    
    -- Additional order details
    MIN(order_timestamp) AS first_order_timestamp_24h,
    AVG(gov) AS avg_gov_24h,
    SUM(total_item_count) AS total_items_24h
    
  FROM proddb.fionafan.experiment_preference_orders
  WHERE order_created_at >= exposure_time 
    AND order_created_at < exposure_time + interval '24 hour'
  GROUP BY dd_device_ID_filtered, exposure_time
)

-- Step 7: Final result combining first session and order data
SELECT 
  fs.tag,
  fs.result,
  fs.dd_device_ID_filtered,
  fs.exposure_time,
  fs.user_id,
  fs.entity_ids,
  
  -- First session metrics
  fs.store_id_list,
  fs.store_name_list,
  fs.primary_category_name_list,
  fs.primary_tag_name_list,
  fs.cuisine_type_list,
  fs.unique_stores_count,
  fs.if_exists_first_session,
  fs.if_place_order,
  fs.if_cuisine_filter,
  fs.if_viewed_store,
  fs.if_used_search,
  fs.if_entity_store_overlap,
  fs.entity_store_overlap_count,
  fs.total_events_first_session,
  fs.total_events_store_impression,
  fs.first_event_timestamp,
  fs.last_event_timestamp,
  fs.max_action_rank_first_session,
  
  -- Calculate first session duration in minutes
  DATEDIFF('minute', fs.first_event_timestamp, fs.last_event_timestamp) AS first_session_duration_minutes,
  
  -- Order metrics within 24h
  COALESCE(om.order_placed_24h, 0) AS order_placed_24h,
  COALESCE(om.total_gov_24h, 0) AS total_gov_24h,
  COALESCE(om.total_orders_24h, 0) AS total_orders_24h,
  COALESCE(om.avg_gov_24h, 0) AS avg_gov_24h,
  COALESCE(om.total_items_24h, 0) AS total_items_24h,
  
  -- Time to first order (if any)
  CASE WHEN om.first_order_timestamp_24h IS NOT NULL 
       THEN DATEDIFF('minute', fs.exposure_time, om.first_order_timestamp_24h)
       ELSE NULL END AS minutes_to_first_order
  
FROM first_session_summary fs
LEFT JOIN order_metrics_24h om 
  ON fs.dd_device_ID_filtered = om.dd_device_ID_filtered 
  AND fs.exposure_time = om.exposure_time
ORDER BY fs.exposure_time, fs.dd_device_ID_filtered
);

-- select tag, count(1), sum(if_place_order), sum(if_cuisine_filter), sum(order_placed_24h)
-- , sum(case when if_place_order  <> order_placed_24h then 1 else 0 end) 
-- , sum(case when if_place_order = 1 and  order_placed_24h = 0 then 1 else 0 end) 
-- , count(case when primary_category_name_list is not null then 1 end) as users_with_category_data
-- , count(case when cuisine_type_list is not null then 1 end) as users_with_cuisine_data
-- , sum(if_entity_store_overlap) as users_with_preference_store_overlap
-- , sum(entity_store_overlap_count) as total_preference_store_impression_overlaps
-- , count(case when entity_ids is not null then 1 end) as treatment_users_count
-- from proddb.fionafan.preference_first_session_analysis
-- group by all;

-- Overall comparison: Treatment vs Control order rates
SELECT 
  tag,
  COUNT(*) AS total_users,
  avg(if_exists_first_session) AS avg_if_exists_first_session,
  avg(if_cuisine_filter) AS avg_if_cuisine_filter,
  avg(if_viewed_store) AS avg_if_viewed_store,
  avg(if_used_search) AS avg_if_used_search,
  avg(if_place_order) AS avg_if_place_order,
  avg(total_events_first_session) AS avg_total_events_first_session,
  -- First session order rates
  SUM(if_place_order) AS users_place_order_first_session,
  AVG(if_place_order) AS first_session_order_rate,
  
  -- 24h order rates   
  SUM(order_placed_24h) AS users_ordered_24h,
  AVG(order_placed_24h) AS order_rate_24h,
  avg(avg_gov_24h) as avg_gov_24h,
  
  AVG(if_entity_store_overlap) AS preference_store_overlap_rate,
  -- Session length metrics
  AVG(first_session_duration_minutes) AS avg_session_length_minutes,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS median_session_length_minutes,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS p75_session_length_minutes,
  
  -- Additional engagement metrics
  AVG(total_events_first_session) AS avg_events_first_session,
  AVG(total_events_store_impression) AS avg_store_impressions_first_session
FROM proddb.fionafan.preference_first_session_analysis
where if_viewed_store = 1
GROUP BY tag
ORDER BY tag;

-- Treatment group analysis by number of entity_ids
SELECT 
  CASE 
    WHEN entity_ids IS NULL THEN 'control'
    ELSE CONCAT('treatment_', ARRAY_SIZE(SPLIT(entity_ids, ',')))
  END AS entity_count_group,
  
  COUNT(*) AS total_users,
  
  -- First session order rates
  SUM(if_place_order) AS users_place_order_first_session, 
  AVG(if_place_order) AS first_session_order_rate,
  
  -- 24h order rates
  SUM(order_placed_24h) AS users_ordered_24h,
  AVG(order_placed_24h) AS order_rate_24h,
  
  -- Session length metrics
  AVG(first_session_duration_minutes) AS avg_session_length_minutes,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS median_session_length_minutes,
  
  -- Preference-store overlap metrics (for treatment only)
  AVG(if_entity_store_overlap) AS preference_store_overlap_rate,
  AVG(entity_store_overlap_count/NULLIF(total_events_store_impression,0)) AS avg_overlap_rate_per_impression,
  
  -- Session engagement
  AVG(total_events_first_session) AS avg_events_first_session,
  AVG(total_events_store_impression) AS avg_store_impressions_first_session
  
FROM proddb.fionafan.preference_first_session_analysis

GROUP BY 
  CASE 
    WHEN entity_ids IS NULL THEN 'control'
    ELSE CONCAT('treatment_', ARRAY_SIZE(SPLIT(entity_ids, ',')))
  END
ORDER BY entity_count_group;



SELECT 
  tag,
  case when if_entity_store_overlap > 0 then 'preferenece_overlap' else 'no_overlap' end as preference_store_overlap_group,
  COUNT(*) AS total_users,
  avg(if_exists_first_session) AS avg_if_exists_first_session,
  avg(if_cuisine_filter) AS avg_if_cuisine_filter,
  avg(if_place_order) AS avg_if_place_order,
  avg(total_events_first_session) AS avg_total_events_first_session,
  -- First session order rates
  SUM(if_place_order) AS users_place_order_first_session, 
  AVG(if_place_order) AS first_session_order_rate,
  
  -- 24h order rates
  SUM(order_placed_24h) AS users_ordered_24h,
  AVG(order_placed_24h) AS order_rate_24h,
  
  -- Session length metrics
  AVG(first_session_duration_minutes) AS avg_session_length_minutes,
  -- PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS median_session_length_minutes,
  
  -- Preference-store overlap metrics (for treatment only)
  AVG(if_entity_store_overlap) AS preference_store_overlap_rate,
  
  -- Session engagement
  AVG(total_events_first_session) AS avg_events_first_session,
  AVG(total_events_store_impression) AS avg_store_impressions_first_session
  
FROM proddb.fionafan.preference_first_session_analysis
where entity_ids is not null
-- where if_exists_first_session=1
GROUP BY
all;

-- ===============================================================================================
-- PREFERENCE-STORE MATCH ANALYSIS
-- ===============================================================================================

-- SIMPLIFIED: Combined treatment and control analysis using optimized table
SELECT
  CASE
    WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
    WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
         AND (
           LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
           OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
           OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
         ) THEN 'Treatment - Expressed Match'
    WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
         THEN 'Treatment - Expressed No Match'
    WHEN pemcv.tag <> 'control' AND (pemcv.entity_ids IS NULL OR pemcv.entity_ids = '')
         THEN 'Treatment - No Expression'
    ELSE 'Other'
  END AS group_type,

  pemcv.container,
  CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END AS event_type,
  COUNT(*) AS total_events,
  ROUND(AVG(pemcv.card_position), 2) AS avg_position,
  COUNT(DISTINCT pemcv.dd_device_ID_filtered) AS unique_users,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT pemcv.dd_device_ID_filtered), 2) AS events_per_user

FROM proddb.fionafan.preference_experiment_m_card_view pemcv
LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id

WHERE
-- Include all users for complete 4-arm analysis
1=1

GROUP BY
  CASE
    WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
    WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
         AND (
           LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
           OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
           OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
         ) THEN 'Treatment - Expressed Match'
    WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
         THEN 'Treatment - Expressed No Match'
    WHEN pemcv.tag <> 'control' AND (pemcv.entity_ids IS NULL OR pemcv.entity_ids = '')
         THEN 'Treatment - No Expression'
    ELSE 'Other'
  END,
  pemcv.container,
  CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END

ORDER BY group_type, container, event_type, total_events DESC;


-- ===============================================================================================
-- BREAKDOWN BY CONTAINER: 4-ARM COMPARISON FOR EACH CONTAINER
-- ===============================================================================================

-- CREATE TEMPORARY TABLE FOR OVERALL ANALYSIS (Control + Treatment Overall)
CREATE OR REPLACE TEMPORARY TABLE arm_analysis_temp_overall AS
WITH base_overall_analysis AS (
  SELECT
    CASE
      WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
      WHEN pemcv.tag <> 'control' THEN 'Treatment Overall'
      ELSE 'Other'
    END AS experimental_arm,
    pemcv.container,
    CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END AS event_type,
    COUNT(*) AS total_events,
    ROUND(AVG(pemcv.card_position), 2) AS avg_position,
    COUNT(DISTINCT pemcv.dd_device_ID_filtered) AS unique_users
  FROM proddb.fionafan.preference_experiment_m_card_view pemcv
  LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id
  WHERE pemcv.event_date >= '2025-08-04'
  GROUP BY
    CASE
      WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
      WHEN pemcv.tag <> 'control' THEN 'Treatment Overall'
      ELSE 'Other'
    END,
    pemcv.container,
    CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END
)
SELECT
  experimental_arm,
  container,
  event_type,
  SUM(total_events) AS total_events,
  ROUND(AVG(avg_position), 2) AS avg_position,
  SUM(unique_users) AS unique_users
FROM base_overall_analysis
WHERE experimental_arm IN ('Control - No Preferences', 'Treatment Overall')
GROUP BY experimental_arm, container, event_type;

-- CREATE A TEMPORARY TABLE WITH ARM ANALYSIS FIRST (since CTE scope is limited)
CREATE OR REPLACE TEMPORARY TABLE arm_analysis_temp AS
WITH base_arm_analysis AS (
  SELECT
    CASE
      WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> '' AND (
        LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
      ) THEN 'Treatment - Expressed Match'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
      THEN 'Treatment - Expressed No Match'
      WHEN pemcv.tag <> 'control' AND (pemcv.entity_ids IS NULL OR pemcv.entity_ids = '')
      THEN 'Treatment - No Expression'
      ELSE 'Other'
    END AS experimental_arm,
    pemcv.container,
    CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END AS event_type,
    -- ADD DETAIL COLUMN - could be store category, card type, or other granularity
    COALESCE(ds.primary_category_name, 'Unknown') AS detail,
    COUNT(*) AS total_events,
    ROUND(AVG(pemcv.card_position), 2) AS avg_position,
    COUNT(DISTINCT pemcv.dd_device_ID_filtered) AS unique_users
  FROM proddb.fionafan.preference_experiment_m_card_view pemcv

  LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id
  WHERE pemcv.event_date >= '2025-08-04'
  GROUP BY ALL
)
SELECT
  experimental_arm,
  container,
  event_type,
  detail,
  SUM(total_events) AS total_events,
  ROUND(AVG(avg_position), 2) AS avg_position,
  SUM(unique_users) AS unique_users
FROM base_arm_analysis
GROUP BY experimental_arm, container, event_type, detail;


-- ===============================================================================================
-- CONTAINER BREAKDOWN: CONTROL + TREATMENT OVERALL SIDE-BY-SIDE FOR EACH CONTAINER
-- ===============================================================================================

-- NOW DO THE CONTAINER BREAKDOWN ANALYSIS
WITH arm_container_metrics AS (
  SELECT
    experimental_arm,
    container,
    SUM(total_events) AS total_events,
    ROUND(AVG(avg_position), 2) AS avg_position,
    SUM(unique_users) AS unique_users,
    ROUND(SUM(total_events) * 1.0 / SUM(unique_users), 2) AS events_per_user
  FROM arm_analysis_temp_overall
  GROUP BY experimental_arm, container
)

-- PIVOT BY CONTAINER: Show each container with metrics grouped by type (Control + Treatment Overall)
SELECT
  container,

  -- TOTAL EVENTS by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN total_events END) AS total_events_control,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN total_events END) AS total_events_treatment_overall,

  -- AVERAGE POSITION by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN avg_position END) AS avg_position_control,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN avg_position END) AS avg_position_treatment_overall,

  -- UNIQUE USERS by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN unique_users END) AS unique_users_control,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN unique_users END) AS unique_users_treatment_overall,

  -- EVENTS PER USER by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN events_per_user END) AS events_per_user_control,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN events_per_user END) AS events_per_user_treatment_overall

FROM arm_container_metrics
GROUP BY container
ORDER BY
  -- Order by total control events to see most popular containers first
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN total_events END) DESC NULLS LAST;

-- ===============================================================================================
-- CAROUSEL DETAIL BREAKDOWN: CREATE SEPARATE TEMP TABLE
-- ===============================================================================================

-- CREATE SEPARATE TEMP TABLE FOR CAROUSEL DETAIL ANALYSIS
CREATE OR REPLACE TEMPORARY TABLE carousel_detail_analysis AS
WITH carousel_base_analysis AS (
  SELECT
    CASE
      WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> '' AND (
        LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
      ) THEN 'Treatment - Expressed Match'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
      THEN 'Treatment - Expressed No Match'
      WHEN pemcv.tag <> 'control' AND (pemcv.entity_ids IS NULL OR pemcv.entity_ids = '')
      THEN 'Treatment - No Expression'
      ELSE 'Other'
    END AS experimental_arm,
    pemcv.container,
    CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END AS event_type,
    COALESCE(ds.primary_category_name, 'Unknown') AS detail,
    COUNT(*) AS total_events,
    ROUND(AVG(pemcv.card_position), 2) AS avg_position,
    COUNT(DISTINCT pemcv.dd_device_ID_filtered) AS unique_users
  FROM proddb.fionafan.preference_experiment_m_card_view pemcv
  LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id
  WHERE pemcv.event_date >= '2025-08-04'
    AND pemcv.container in ('carousel', 'cluster', 'list')  -- FILTER FOR CAROUSEL ONLY
  GROUP BY
    CASE
      WHEN pemcv.tag = 'control' THEN 'Control - No Preferences'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> '' AND (
        LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
        OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
      ) THEN 'Treatment - Expressed Match'
      WHEN pemcv.tag <> 'control' AND pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> ''
      THEN 'Treatment - Expressed No Match'
      WHEN pemcv.tag <> 'control' AND (pemcv.entity_ids IS NULL OR pemcv.entity_ids = '')
      THEN 'Treatment - No Expression'
      ELSE 'Other'
    END,
    pemcv.container,
    CASE WHEN pemcv.event_type = 'store_impression' THEN 'impression' ELSE pemcv.event_type END,
    COALESCE(ds.primary_category_name, 'Unknown')
)
SELECT
  experimental_arm,
  container,
  event_type,
  detail,
  SUM(total_events) AS total_events,
  ROUND(AVG(avg_position), 2) AS avg_position,
  SUM(unique_users) AS unique_users
FROM carousel_base_analysis
GROUP BY experimental_arm, container, event_type, detail;

-- ===============================================================================================
-- CAROUSEL BREAKDOWN: FOUR INDIVIDUAL ARMS + TREATMENT OVERALL SIDE-BY-SIDE BY DETAIL CATEGORY
-- ===============================================================================================

-- First, create the four individual arms analysis
WITH carousel_four_arms AS (
  SELECT
    experimental_arm,
    detail,
    SUM(total_events) AS total_events,
    ROUND(AVG(avg_position), 2) AS avg_position,
    SUM(unique_users) AS unique_users,
    ROUND(SUM(total_events) * 1.0 / NULLIF(SUM(unique_users), 0), 2) AS events_per_user
  FROM carousel_detail_analysis
  GROUP BY experimental_arm, detail
),

-- Second, create treatment overall by aggregating all treatment arms
carousel_treatment_overall AS (
  SELECT
    'Treatment Overall' AS experimental_arm,
    detail,
    SUM(total_events) AS total_events,
    ROUND(AVG(avg_position), 2) AS avg_position,
    SUM(unique_users) AS unique_users,
    ROUND(SUM(total_events) * 1.0 / NULLIF(SUM(unique_users), 0), 2) AS events_per_user
  FROM carousel_detail_analysis
  WHERE experimental_arm LIKE 'Treatment%'
  GROUP BY detail
),

-- Third, combine both individual arms and treatment overall
carousel_combined AS (
  SELECT * FROM carousel_four_arms
  UNION ALL
  SELECT * FROM carousel_treatment_overall
)

-- PIVOT BY DETAIL: Show each detail category with 5-arm metrics side-by-side
SELECT
  detail,

  -- TOTAL EVENTS by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN total_events END) AS total_events_control,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed Match' THEN total_events END) AS total_events_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed No Match' THEN total_events END) AS total_events_no_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - No Expression' THEN total_events END) AS total_events_no_expr,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN total_events END) AS total_events_treatment_overall,

  -- AVERAGE POSITION by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN avg_position END) AS avg_position_control,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed Match' THEN avg_position END) AS avg_position_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed No Match' THEN avg_position END) AS avg_position_no_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - No Expression' THEN avg_position END) AS avg_position_no_expr,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN avg_position END) AS avg_position_treatment_overall,

  -- UNIQUE USERS by arm
  MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN unique_users END) AS unique_users_control,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed Match' THEN unique_users END) AS unique_users_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - Expressed No Match' THEN unique_users END) AS unique_users_no_match,
  MAX(CASE WHEN experimental_arm = 'Treatment - No Expression' THEN unique_users END) AS unique_users_no_expr,
  MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN unique_users END) AS unique_users_treatment_overall,

  -- -- EVENTS PER USER by arm
  -- MAX(CASE WHEN experimental_arm = 'Control - No Preferences' THEN events_per_user END) AS events_per_user_control,
  -- MAX(CASE WHEN experimental_arm = 'Treatment - Expressed Match' THEN events_per_user END) AS events_per_user_match,
  -- MAX(CASE WHEN experimental_arm = 'Treatment - Expressed No Match' THEN events_per_user END) AS events_per_user_no_match,
  -- MAX(CASE WHEN experimental_arm = 'Treatment - No Expression' THEN events_per_user END) AS events_per_user_no_expr,
  -- MAX(CASE WHEN experimental_arm = 'Treatment Overall' THEN events_per_user END) AS events_per_user_treatment_overall

FROM carousel_combined
-- where LOWER(detail) IN (
--   'fast-food','burger','pizza','mexican','chicken','chinese','sandwich','asian',
--   'dessert','breakfast','italian','comfort-food','steak','healthy','sushi',
--   'seafood','coffee','thai','soup','indian','dineout'
-- )
GROUP BY detail
having total_events_match > 0
ORDER BY total_events_match desc;


-- ===============================================================================================
-- LIFT BY DISCOVERY FEATURE AND SURFACE (TREATMENT VS CONTROL)
-- -----------------------------------------------------------------------------------------------
-- Output columns:
--   discovery_feature, discovery_surface, treatment_val, control_val, lift, overall_users_with_view
-- Definitions:
--   - Users with view: distinct dd_device_ID_filtered with at least one m_card_view for the pair
--   - Rate per arm: users_with_view / total users in that arm (from experiment_preference_events)
--   - Lift: treatment_val / control_val - 1
--   - Ordered by overall volume = control_users_with_view + treatment_users_with_view
WITH users_per_tag AS (
  SELECT 
    tag,
    COUNT(DISTINCT dd_device_ID_filtered) AS total_users
  FROM proddb.fionafan.experiment_preference_events
  GROUP BY all
),
feature_views AS (
  SELECT 
    LOWER(discovery_feature) AS discovery_feature,
    LOWER(discovery_surface) AS discovery_surface,
    tag,
    COUNT(DISTINCT dd_device_ID_filtered) AS users_with_view
  FROM proddb.fionafan.experiment_preference_events
  WHERE event = 'm_card_view'
    AND store_id IS NOT NULL
  GROUP BY all
),
feature_rates AS (
  SELECT 
    fv.discovery_feature,
    fv.discovery_surface,
    fv.tag,
    fv.users_with_view,
    up.total_users,
    fv.users_with_view * 1.0 / NULLIF(up.total_users, 0) AS rate
  FROM feature_views fv
  JOIN users_per_tag up ON fv.tag = up.tag
),
pivoted AS (
  SELECT 
    discovery_feature,
    discovery_surface,
    MAX(CASE WHEN tag = 'control' THEN rate END) AS control_val,
    MAX(CASE WHEN tag <> 'control' THEN rate END) AS treatment_val,
    MAX(CASE WHEN tag = 'control' THEN users_with_view END) AS control_users_with_view,
    MAX(CASE WHEN tag <> 'control' THEN users_with_view END) AS treatment_users_with_view
  FROM feature_rates
  GROUP BY all
)
SELECT 
  discovery_feature,
  discovery_surface,
  treatment_val,
  control_val,
  treatment_val / NULLIF(control_val, 0) - 1 AS lift,
  COALESCE(control_users_with_view, 0) + COALESCE(treatment_users_with_view, 0) AS overall_users_with_view
FROM pivoted
ORDER BY overall_users_with_view DESC;




