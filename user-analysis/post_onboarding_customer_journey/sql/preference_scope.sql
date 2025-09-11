create or replace table proddb.fionafan.preference_toggle_ice_all as (


  select consumer_id, dd_device_id, 
         TO_TIMESTAMP(max(iguazu_event_time)/1000) max_event_time, 
         listagg(entity_id, ',') within group (order by entity_id) entity_ids
  from (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
  FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 

  WHERE toggle_type = 'add' 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) = 1 
  )
  group by all
  );


create or replace table proddb.fionafan.onboarded_users_july AS
(SELECT DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp) as join_time
      , cast(iguazu_timestamp as date) AS onboard_day
      , consumer_id
from iguazu.consumer.m_onboarding_start_promo_page_view_ice


WHERE convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp) BETWEEN '2025-07-01'::date AND '2025-07-31'
);

CREATE OR REPLACE TABLE proddb.fionafan.onboarded_users_july_august_orders AS (


  WITH order_events AS (
    SELECT DISTINCT 
      a.user_id,
      dd.creator_id as consumer_id,
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
      AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-07-01' AND '2025-08-31'
    WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-07-01' AND '2025-08-31'
  ),
  
  experiment_orders AS (
    SELECT distinct
      epe.join_time,
      epe.onboard_day,
      epe.consumer_id,
      
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
      DATEDIFF('hour', epe.join_time, oe.order_timestamp) AS hours_between_exposure_and_order,
      DATEDIFF('day', epe.join_time, oe.order_timestamp) AS days_between_exposure_and_order,
      

    FROM (select distinct consumer_id, join_time, onboard_day from proddb.fionafan.onboarded_users_july) epe

    LEFT JOIN order_events oe
      ON epe.consumer_id = oe.consumer_id 
      AND epe.join_time <= oe.order_timestamp  -- Order must be after effective exposure
  )
  
  SELECT * FROM experiment_orders
  WHERE delivery_id IS NOT NULL  -- Only include rows where we found orders
);

CREATE OR REPLACE TABLE proddb.fionafan.onboarded_users_events AS (

  WITH events_with_onboarding AS (
    SELECT distinct
      ou.*,
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
    FROM proddb.fionafan.onboarded_users_july ou
    LEFT JOIN tyleranderson.events_all events
        
      ON ou.dd_device_id_filtered = replace(lower(CASE WHEN events.DD_DEVICE_ID like 'dx_%' then events.DD_DEVICE_ID
                        else 'dx_'||events.DD_DEVICE_ID end), '-')
      AND events.timestamp > ou.join_time
      AND events.event_date >= '2025-07-01'
  ),
  ranked_events AS (
    SELECT 
      *,
      RANK() OVER (PARTITION BY consumer_id ORDER BY event_timestamp) AS action_rank
    FROM events_with_onboarding
  )
  SELECT * FROM ranked_events
);

select * from tyleranderson.events_all where event_date = '2025-09-03' and user_id = '1125900281188900' order by timestamp asc;
select * from iguazu.consumer.m_card_view where timestamp >= '2025-08-31' and user_id = '1125900259931897' limit 10;

select * from edw.consumer.dimension_consumers where email = 'fiona.fan@doordash.com' limit 10;




-- Analysis: Preference adoption and impact on orders/GOV
WITH consumer_order_summary AS (
  -- First aggregate orders by consumer
  SELECT 
    consumer_id,
    COUNT(DISTINCT delivery_id) AS total_orders,
    SUM(gov) AS total_gov,
    MAX(order_day) AS last_order_date
  FROM proddb.fionafan.onboarded_users_july_august_orders
  WHERE delivery_id IS NOT NULL
  GROUP BY consumer_id
),

consumer_preferences AS (
  -- Get consumers with preferences
  SELECT 
    consumer_id,
    CASE WHEN entity_ids IS NOT NULL AND entity_ids != '' THEN 1 ELSE 0 END AS has_preferences,
    LENGTH(entity_ids) - LENGTH(REPLACE(entity_ids, ',', '')) + 1 AS num_preferences
  FROM proddb.fionafan.preference_toggle_ice_all
),

all_consumers_with_orders AS (
  -- All onboarded consumers with their order data (if any) and preference status
  SELECT 
    ou.consumer_id,
    COALESCE(ous.total_orders, 0) AS total_orders,
    COALESCE(ous.total_gov, 0) AS total_gov,
    COALESCE(up.has_preferences, 0) AS has_preferences,
    ous.last_order_date
  FROM proddb.fionafan.onboarded_users_july ou
  LEFT JOIN consumer_order_summary ous ON ou.consumer_id = ous.consumer_id
  LEFT JOIN consumer_preferences up ON ou.consumer_id = up.consumer_id
)

SELECT 
  CASE WHEN has_preferences = 1 THEN 'Has Preferences' ELSE 'No Preferences' END AS preference_status,
  COUNT(consumer_id) AS total_consumers,
  COUNT(CASE WHEN total_orders > 0 THEN consumer_id END) AS consumers_with_orders,
  COUNT(CASE WHEN last_order_date >= '2025-08-31'::date - INTERVAL '30 days' THEN consumer_id END) AS mau_consumers,
  SUM(total_orders) AS total_orders,
  SUM(total_gov) AS total_gov,
  
  -- Order frequency metrics
  ROUND(SUM(total_orders) * 1.0 / COUNT(consumer_id), 2) AS avg_orders_per_consumer,
  ROUND(SUM(total_orders) * 1.0 / NULLIF(COUNT(CASE WHEN total_orders > 0 THEN consumer_id END), 0), 2) AS avg_orders_per_active_consumer,
  ROUND(SUM(total_gov) * 1.0 /  COUNT(consumer_id), 2) AS avg_gov_per_consumer,
  ROUND(SUM(total_gov) * 1.0 / NULLIF(COUNT(CASE WHEN total_orders > 0 THEN consumer_id END), 0), 2) AS avg_gov_per_active_consumer,
  
  -- Percentage breakdowns
  ROUND(COUNT(consumer_id) * 100.0 / SUM(COUNT(consumer_id)) OVER (), 2) AS pct_of_total_consumers,
  ROUND(SUM(total_orders) * 100.0 / SUM(SUM(total_orders)) OVER (), 2) AS pct_of_total_orders,
  ROUND(SUM(total_gov) * 100.0 / SUM(SUM(total_gov)) OVER (), 2) AS pct_of_total_gov,
  ROUND(COUNT(CASE WHEN last_order_date >= '2025-08-31'::date - INTERVAL '30 days' THEN consumer_id END) * 100.0 / SUM(COUNT(CASE WHEN last_order_date >= '2025-08-31'::date - INTERVAL '30 days' THEN consumer_id END)) OVER (), 2) AS pct_of_total_mau
FROM all_consumers_with_orders
GROUP BY has_preferences
ORDER BY has_preferences DESC;


-- =====================================================
-- ONBOARDED USERS EVENTS ANALYSIS
-- =====================================================

-- 1) Distribution of how many sessions there are in a day
CREATE OR REPLACE TABLE proddb.fionafan.daily_session_distribution AS (
  WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
  ),
  daily_sessions AS (
    SELECT 
      consumer_id,
      event_date,
      COUNT(DISTINCT session_num) AS sessions_per_day
    FROM dedupe_events
    GROUP BY consumer_id, event_date
  )
  
  SELECT 
    sessions_per_day,
    COUNT(*) AS frequency,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
  FROM daily_sessions
  GROUP BY sessions_per_day
  ORDER BY sessions_per_day
);
select * from  proddb.fionafan.daily_session_distribution;
-- 2) How many sessions before first order (broken down by within same day or not)
CREATE OR REPLACE TABLE proddb.fionafan.sessions_before_first_order AS (
  WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
  ),
  first_orders AS (
    SELECT 
      consumer_id,
      MIN(event_timestamp) AS first_order_timestamp,
      event_date AS first_order_date
    FROM dedupe_events
    WHERE event = 'action_place_order'
    GROUP BY consumer_id, event_date
  ),
  sessions_before_order AS (
    SELECT 
      de.consumer_id,
      de.event_date,
      de.session_num,
      fo.first_order_timestamp,
      fo.first_order_date,
      CASE WHEN de.event_date = fo.first_order_date THEN 'Same Day' ELSE 'Different Day' END AS day_type
    FROM dedupe_events de
    INNER JOIN first_orders fo ON de.consumer_id = fo.consumer_id
    WHERE de.event_timestamp < fo.first_order_timestamp
  ),
  session_counts AS (
    SELECT 
      consumer_id,
      day_type,
      COUNT(DISTINCT CONCAT(event_date, '-', session_num)) AS sessions_before_first_order
    FROM sessions_before_order
    GROUP BY consumer_id, day_type
  )
  
  SELECT 
    day_type,
    sessions_before_first_order,
    COUNT(*) AS consumers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY day_type), 2) AS pct_within_day_type
  FROM session_counts
  GROUP BY day_type, sessions_before_first_order
  ORDER BY day_type, sessions_before_first_order
);

-- 3) How many sessions before first cart event (order_cart_page_load)
CREATE OR REPLACE TABLE proddb.fionafan.sessions_before_first_cart AS (
  WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
  ),
  first_cart AS (
    SELECT 
      consumer_id,
      MIN(event_timestamp) AS first_cart_timestamp
    FROM dedupe_events
    WHERE event = 'order_cart_page_load'
    GROUP BY consumer_id
  ),
  sessions_before_cart AS (
    SELECT 
      de.consumer_id,
      COUNT(DISTINCT CONCAT(de.event_date, '-', de.session_num)) AS sessions_before_first_cart
    FROM dedupe_events de
    INNER JOIN first_cart fc ON de.consumer_id = fc.consumer_id
    WHERE de.event_timestamp < fc.first_cart_timestamp
    GROUP BY de.consumer_id
  )
  
  SELECT 
    sessions_before_first_cart,
    COUNT(*) AS consumers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
  FROM sessions_before_cart
  GROUP BY sessions_before_first_cart
  ORDER BY sessions_before_first_cart
);
select * from  proddb.fionafan.sessions_before_first_cart;
-- 4) Stores visited in sessions with and without subsequent orders
CREATE OR REPLACE TABLE proddb.fionafan.stores_visited_by_order_outcome AS (

  WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
  ),
  session_orders AS (
    SELECT 
      consumer_id,
      event_date,
      session_num,
      MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
    FROM dedupe_events
    GROUP BY consumer_id, event_date, session_num
  ),
  session_stores AS (
    SELECT 
      de.consumer_id,
      de.event_date,
      de.session_num,
      so.has_order,
      COUNT(distinct case when (de.event = 'store_page_load' AND de.store_id IS NOT NULL) or (de.event = 'm_card_click' and de.store_id is not null) then de.store_id end) AS loaded_store_count,
      COUNT(distinct de.store_id) AS viewed_stores_count
    FROM dedupe_events de
    INNER JOIN session_orders so 
      ON de.consumer_id = so.consumer_id 
      AND de.event_date = so.event_date 
      AND de.session_num = so.session_num
    WHERE ((de.event = 'store_page_load' AND de.store_id IS NOT NULL) or (de.event = 'm_card_click' and de.store_id is not null) or (de.event = 'm_card_view'))
    GROUP BY all
  )
  
  SELECT 
    CASE WHEN has_order = 1 THEN 'With Order' ELSE 'Without Order' END AS session_type,
    loaded_store_count,
    viewed_stores_count,
    COUNT(distinct consumer_id||event_date||session_num) AS sessions
  FROM session_stores
  GROUP BY all
  ORDER BY session_type DESC, loaded_store_count
);
select * from proddb.fionafan.stores_visited_by_order_outcome order by session_type, loaded_store_count, sessions;
-- 5) Session duration with and without placed orders
CREATE OR REPLACE TABLE proddb.fionafan.session_duration_by_order_outcome AS (
  WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
  ),
  session_metrics AS (
    SELECT 
      consumer_id,
      event_date,
      session_num,
      MIN(event_timestamp) AS session_start,
      MAX(event_timestamp) AS session_end,
      DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_seconds,
      MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
    FROM dedupe_events
    GROUP BY consumer_id, event_date, session_num
  ),
  duration_buckets AS (
    SELECT 
      *,
      CASE 
        WHEN session_duration_seconds <= 60 THEN '0-1 min'
        WHEN session_duration_seconds <= 300 THEN '1-5 min'
        WHEN session_duration_seconds <= 900 THEN '5-15 min'
        WHEN session_duration_seconds <= 1800 THEN '15-30 min'
        ELSE '30+ min'
      END AS duration_bucket
    FROM session_metrics
  )
  
  SELECT 
    CASE WHEN has_order = 1 THEN 'With Order' ELSE 'Without Order' END AS session_type,
    duration_bucket,
    COUNT(*) AS sessions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY has_order), 2) AS pct_within_type,
    ROUND(AVG(session_duration_seconds), 2) AS avg_duration_seconds,
    ROUND(MEDIAN(session_duration_seconds), 2) AS median_duration_seconds
  FROM duration_buckets
  GROUP BY has_order, duration_bucket
  ORDER BY has_order DESC, 
    CASE duration_bucket
      WHEN '0-1 min' THEN 1
      WHEN '1-5 min' THEN 2
      WHEN '5-15 min' THEN 3
      WHEN '15-30 min' THEN 4
      WHEN '30+ min' THEN 5
    END
);
select * from proddb.fionafan.session_duration_by_order_outcome order by session_type, duration_bucket;



-- which carosel is performing the best?
-- for people who order from your recent order, do they order the same thing?
