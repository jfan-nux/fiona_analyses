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
  left JOIN tyleranderson.events_all events
    ON lower(ewp.dd_device_ID_filtered) = replace(lower(CASE WHEN events.dd_device_id like 'dx_%' then events.dd_device_id
                      else 'dx_'||events.dd_device_id end), '-') 
    AND events.timestamp > ewp.exposure_time
    AND events.event_date >= '2025-08-18'  -- Filter events after 2025-08-18
),
ranked_events AS (
  SELECT 
    *,
    RANK() OVER (PARTITION BY dd_device_ID_filtered ORDER BY event_timestamp) AS action_rank
  FROM events_with_experiment
)
SELECT * FROM ranked_events);


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
select count(1), sum(case when store_id_list <>'' then 1 else 0 end) from proddb.fionafan.preference_first_session_analysis where entity_ids<>'null' and entity_ids is not null;
select count(1) from proddb.fionafan.preference_first_session_analysis where tag= 'treatment';
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

select count(1), sum(if_place_order), sum(if_cuisine_filter), sum(order_placed_24h)
, sum(case when if_place_order  <> order_placed_24h then 1 else 0 end) 
, sum(case when if_place_order = 1 and  order_placed_24h = 0 then 1 else 0 end) 
, count(case when primary_category_name_list is not null then 1 end) as users_with_category_data
, count(case when cuisine_type_list is not null then 1 end) as users_with_cuisine_data
, sum(if_entity_store_overlap) as users_with_preference_store_overlap
, sum(entity_store_overlap_count) as total_preference_store_impression_overlaps
, count(case when entity_ids is not null then 1 end) as treatment_users_count
from proddb.fionafan.preference_first_session_analysis;

-- Overall comparison: Treatment vs Control order rates
SELECT 
  tag,
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
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS p75_session_length_minutes,
  
  -- Additional engagement metrics
  AVG(total_events_first_session) AS avg_events_first_session,
  AVG(total_events_store_impression) AS avg_store_impressions_first_session
FROM proddb.fionafan.preference_first_session_analysis
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
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_session_duration_minutes) AS median_session_length_minutes,
  
  -- Preference-store overlap metrics (for treatment only)
  AVG(if_entity_store_overlap) AS preference_store_overlap_rate,
  AVG(entity_store_overlap_count/NULLIF(total_events_store_impression,0)) AS avg_overlap_rate_per_impression,
  
  -- Session engagement
  AVG(total_events_first_session) AS avg_events_first_session,
  AVG(total_events_store_impression) AS avg_store_impressions_first_session
  
FROM proddb.fionafan.preference_first_session_analysis
GROUP BY 
all;
