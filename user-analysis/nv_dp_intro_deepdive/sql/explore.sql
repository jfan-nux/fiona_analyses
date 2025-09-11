-- New users identified via onboarding start promo page view; normalized dd_device_id, join_time, day, consumer_id (Apr 2025 cohort).
create or replace table proddb.fionafan.nv_dp_new_user_table as (


SELECT  DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , iguazu_timestamp as join_time
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN '2025-04-01'::date AND '2025-04-01'::date+31
);

-- Base: all events within 30 days post-join for new users
-- All events from events_all occurring >0 to <=30 days after each user's join_time; includes session and discovery/store attributes.
create or replace table proddb.fionafan.nv_dp_new_user_all_events_30d as (
with new_users as (
  SELECT DISTINCT 
         replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                             else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
       , iguazu_timestamp as join_time
       , cast(iguazu_timestamp as date) AS day
       , consumer_id
  from iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-04-01'::date AND '2025-04-01'::date+31
)
, events as (
  select *
  from tyleranderson.events_all e
  where e.event_date between (select min(cast(join_time as date)) from new_users)
                          and (select dateadd('day', 30, max(cast(join_time as date))) from new_users)
)
select
  u.consumer_id,
  u.dd_device_id_filtered as dd_device_id,
  u.join_time,
  e.event_date,
  e.timestamp as event_timestamp,
  e.session_num,
  e.platform,
  e.event_type,
  e.event,
  e.discovery_surface,
  e.discovery_feature,
  e.detail,
  e.store_id,
  e.store_name,
  e.context_timezone,
  e.context_os_version,
  e.event_rank,
  e.discovery_surface_click_attr,
  e.discovery_feature_click_attr,
  e.discovery_surface_imp_attr,
  e.discovery_feature_imp_attr,
  e.pre_purchase_flag,
  e.l91d_orders,
  e.last_order_date
from new_users u
join events e
  on u.dd_device_id_filtered = replace(lower(CASE WHEN e.dd_device_id like 'dx_%' then e.dd_device_id
                                                  else 'dx_'||e.dd_device_id end), '-')
 and e.timestamp > u.join_time
 and e.timestamp <= dateadd('day', 30, u.join_time)
);


-- Adds sequential_session_num per user to events in 30d window; filterable to first three sessions post-join.
create or replace table proddb.fionafan.nv_dp_new_user_all_events_30d_w_session_num as (

with base as (
  select * from proddb.fionafan.nv_dp_new_user_all_events_30d
)
, events_with_lag as (
  select 
    consumer_id,
    dd_device_id,
    join_time,
    event_date,
    event_timestamp,
    session_num,
    platform,
    event_type,
    event,
    discovery_surface,
    discovery_feature,
    detail,
    store_id,
    store_name,
    context_timezone,
    context_os_version,
    event_rank,
    discovery_surface_click_attr,
    discovery_feature_click_attr,
    discovery_surface_imp_attr,
    discovery_feature_imp_attr,
    pre_purchase_flag,
    l91d_orders,
    last_order_date,
    lag(session_num) over (
      partition by dd_device_id, join_time
      order by event_timestamp
    ) as prev_session_num
  from base
)
, events_with_seq as (
  select
    *,
    case when prev_session_num is null or prev_session_num != session_num then 1 else 0 end as session_change,
    sum(case when prev_session_num is null or prev_session_num != session_num then 1 else 0 end) over (
      partition by dd_device_id, join_time
      order by event_timestamp
      rows unbounded preceding
    ) as sequential_session_num
  from events_with_lag
)
select 
  consumer_id,
  dd_device_id,
  join_time,
  event_date,
  event_timestamp,
  session_num,
  sequential_session_num,
  platform,
  event_type,
  event,
  discovery_surface,
  discovery_feature,
  detail,
  store_id,
  store_name,
  context_timezone,
  context_os_version,
  event_rank,
  discovery_surface_click_attr,
  discovery_feature_click_attr,
  discovery_surface_imp_attr,
  discovery_feature_imp_attr,
  pre_purchase_flag,
  l91d_orders,
  last_order_date
from events_with_seq
-- where sequential_session_num <= 3
);




-- Notification engagement events within 0–30 days post-join joined to new users, with lifecycle week since join.
create or replace table proddb.fionafan.nv_dp_new_user_notifications_60d as (
SELECT
  n.*, u.join_time, u.day as join_day, 
  FLOOR(DATEDIFF(day, u.join_time, n.SENT_AT_DATE) / 7) + 1 AS LIFECYCLE_WEEK
FROM edw.consumer.fact_consumer_notification_engagement n

inner join proddb.fionafan.nv_dp_new_user_table u on n.consumer_id = u.consumer_id
WHERE 1=1
  AND n.SENT_AT_DATE >= '2025-04-01'::date
  AND n.SENT_AT_DATE < '2025-04-01'::date+90
  and n.sent_at_date > u.join_time
and n.sent_at_date <= DATEADD('day', 60, u.join_time)
  );



-- Notifications within 30d enriched with Braze campaign/canvas metadata and tag-category features; includes timing features relative to join.
create or replace table proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week as (

with ct as (
    ( with base as (SELECT distinct
    *,
    CASE    WHEN lower(tag) = 'multichannel' then 'Multichannel'
        WHEN lower(tag) = 'mx' then 'Mx'
        -- New Cx
        WHEN LOWER(tag) LIKE '%new%cx%' 
            OR LOWER(tag) LIKE '%new%customer%'
            OR LOWER(tag) LIKE '%fmx%'
            OR LOWER(tag) LIKE '%fmu%'
            OR tag IN ('New Cx', 'Unified New Cx Purchaser Welcome')
            THEN 'New Customer Experience'
        WHEN lower(tag) ilike '%cx%' or lower(tag)  = 'active' then 'Cx'
        -- Cart Abandonment Flow
        WHEN LOWER(tag) LIKE '%cart%abandon%' 
            OR LOWER(tag) LIKE '%cart_abandon%'
            OR tag IN ('Cart Abandon Message Flow', 'cart_abandonment_notifcation') 
            THEN 'Cart Abandonment'
            
        -- DashPass Related
        WHEN LOWER(tag) LIKE '%dashpass%' 
            OR LOWER(tag) LIKE '%dash%pass%'
            OR tag LIKE 'DP %'
            OR tag IN ('DashPass', 'DashPass Retention', 'DashPass Adoption', 'DP Family Sharing')
            THEN 'DashPass'
            
        -- Offers and Promotions
        WHEN LOWER(tag) LIKE '%offer%' 
            OR LOWER(tag) LIKE '%promo%'
            OR LOWER(tag) LIKE '%special%'
            OR tag LIKE '%Upsell%'
            OR tag IN ('Push-DoorDash Offers', 'Special Offers', 'Promo Seekers $10off3', 'AP Intro Offers Relaunch')
            THEN 'Offers & Promos'
            
        -- New Verticals & Grocery
        WHEN LOWER(tag) LIKE '%new%vertical%' 
            OR tag IN ('Grocery', 'Convenience', 'Retail', 'Multichannel', 'ALDI', 'Coles Upsell Trigger')
            THEN 'New Verticals & Grocery'
            
        -- Churned/Dormant/Lifecycle Reengagement
        WHEN LOWER(tag) LIKE '%churn%' 
            OR LOWER(tag) LIKE '%dormant%'
            OR LOWER(tag) LIKE '%winback%'
            OR LOWER(tag) LIKE '%reengagement%'
            OR tag LIKE 'aud_%Churned'
            OR tag LIKE 'aud_%Dormant'
            OR tag IN ('CHURNED', 'Dormant Cx', 'Dormant', 'Super Churned', 'Very Churned', 'WinBack', 'Reengagement', 'DEWO')
            THEN 'Churned & Reengagement'
            
        -- Reminders & Notifications
        WHEN LOWER(tag) LIKE '%reminder%' 
            OR LOWER(tag) LIKE '%notification%'
            OR tag LIKE '%quiet%hours%'
            OR tag IN ('Push-Reminders', 'Notifications + Reminders', 'quiet-hours-exclusion-all')
            THEN 'Reminders & Notifications'
            
        
            
        -- Geographic Regions
        WHEN tag IN ('US', 'Australia', 'New Zealand', 'Canada', 'Mx') 
            OR tag LIKE 'AUS %'
            OR tag LIKE 'NZ %'
            OR tag LIKE 'CA %'
            OR tag LIKE 'CAN-%'
            OR LOWER(tag) LIKE '%australia%'
            THEN 'Geographic Targeting'
            
        -- Engagement & Recommendations
        WHEN LOWER(tag) LIKE '%engagement%'  
            OR LOWER(tag) LIKE '%recommend%'
            OR LOWER(tag) LIKE '%challenge%'
            OR tag IN ('engagement_program', 'challenges', 'Recommendations', 'Push-Recommendations')
            THEN 'Engagement & Recommendations'
            
        -- Partnerships & External
        WHEN LOWER(tag) LIKE '%partnership%'
            OR tag IN ('Lyft', 'Chase NV Sends', 'McD Upsell Trigger', 'NZ KFC DP Upsell Trigger', 'NZ BK DP Upsell Trigger')
            or lower(tag) = 'cash app pay'
            THEN 'Partnerships'
            
        -- Lifecycle & Customer Segments  
        WHEN tag LIKE 'aud_%'
            OR LOWER(tag) LIKE '%lifecycle%'
            OR tag IN ('Active Cx', 'Active', 'USLM')
            THEN 'Customer Segments'
            
        -- Communication Channels
        WHEN LOWER(tag) LIKE '%email%'
            OR LOWER(tag) LIKE '%push%'
            OR LOWER(tag) LIKE '%sms%'
            OR tag IN ('Email', 'push', 'Marketing SMS')
            THEN 'Communication Channels'
            
        -- Occasions & Seasonal
        WHEN LOWER(tag) LIKE '%occasion%'
            OR LOWER(tag) LIKE '%season%'
            OR LOWER(tag) LIKE '%school%'
            OR tag IN ('Occasions', 'Back To School', 'Verano', 'SGB')
            THEN 'Occasions & Seasonal'
            
        -- Technical & Automation
        WHEN LOWER(tag) LIKE '%batch%'
            OR LOWER(tag) LIKE '%trigger%'
            OR LOWER(tag) LIKE '%automation%'
            OR LOWER(tag) LIKE '%smart%'
            OR tag IN ('Batch', 'Trigger', 'ML Automation', 'SmartAuto')
            THEN 'Technical & Automation'
            
        -- Store & Location Specific
        WHEN LOWER(tag) LIKE '%store%'
            OR tag IN ('Store Promotions', 'Push-Store Offers')
            THEN 'Store Specific'
        when lower(tag) in ('2025','2026') then 'Occasions & Seasonal'
        -- Default for unmatched
        ELSE 'Other'
    END AS tag_category
    
FROM marketing_fivetran.braze_consumer.canvas_tag )
select canvas_id,
    LISTAGG(DISTINCT tag_category, ', ') WITHIN GROUP (ORDER BY tag_category) AS tag_categories,
    LISTAGG(DISTINCT tag, ', ') WITHIN GROUP (ORDER BY tag) AS tags,
    COUNT(DISTINCT tag) AS unique_tag_count,
    
    -- Binary columns for each tag category
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Geographic Targeting' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_geographic_targeting,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Cx' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_cx,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'New Verticals & Grocery' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_new_verticals_grocery,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Cart Abandonment' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_cart_abandonment,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Mx' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_mx,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Reminders & Notifications' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_reminders_notifications,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Multichannel' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_multichannel,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Offers & Promos' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_offers_promos,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Technical & Automation' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_technical_automation,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Communication Channels' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_communication_channels,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'New Customer Experience' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_new_customer_experience,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Engagement & Recommendations' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_engagement_recommendations,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'DashPass' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_dashpass,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Occasions & Seasonal' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_occasions_seasonal,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Other' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_other,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Churned & Reengagement' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_churned_reengagement,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Customer Segments' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_customer_segments,
    CASE WHEN COUNT(DISTINCT CASE WHEN tag_category = 'Partnerships' THEN tag END) > 0 THEN 1 ELSE 0 END AS has_partnerships
    
    from base
GROUP BY canvas_id)
),

joined as (
select distinct n.*,  
bz.campaign_id as bz_campaign_id, bz.campaign_name as bz_campaign_name, bz.canvas_step_name as bz_canvas_step_name,
bz.canvas_id as bz_canvas_id,
bz.canvas_name as bz_canvas_name,
coalesce(bz.canvas_name, bz.campaign_name) as master_campaign_name,
bz.alert_title as bz_alert_title,
bz.alert_body as bz_alert_body,
bz.external_id as bz_consumer_id,
bz.priority as bz_priority,
bz.push_type as bz_push_type,
ct.tag_categories as bz_tag_categories,
ct.tags as bz_tags,
ct.unique_tag_count as bz_unique_tag_count,
-- Binary tag category columns
ct.has_geographic_targeting,
ct.has_cx,
ct.has_new_verticals_grocery,
ct.has_cart_abandonment,
ct.has_mx,
ct.has_reminders_notifications,
ct.has_multichannel,
ct.has_offers_promos,
ct.has_technical_automation,
ct.has_communication_channels,
ct.has_new_customer_experience,
ct.has_engagement_recommendations,
ct.has_dashpass,
ct.has_occasions_seasonal,
ct.has_other,
ct.has_churned_reengagement,
ct.has_customer_segments,
ct.has_partnerships,
-- Derived timing features
DATEDIFF(hour, n.join_time, n.sent_at) AS hours_since_joined,
EXTRACT(hour FROM n.sent_at) AS send_hour_of_day
from proddb.fionafan.nv_dp_new_user_notifications_60d n
left join fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt bz on bz.dispatch_id = n.deduped_message_id and bz.external_id = n.consumer_id

left join  ct on ct.canvas_id = bz.canvas_id
)

select * from joined
);

-- Push, non-transactional notifications with campaign/device message ranking and time-since-previous metrics within 30d window.
create  or replace table proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week_ranked as (

with joined as (
select * from proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week
where 1=1
and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    and is_valid_send = 1 
    and coalesce(canvas_name, campaign_name) is not null
),
msg_times as (
  select 
    j.*,
    -- collapsed message times per campaign thread and per device thread
    MIN(j.sent_at) OVER (
      PARTITION BY j.consumer_id, j.device_id, j.master_campaign_name, j.deduped_message_id
    ) as campaign_min_sent_at,
    MIN(j.sent_at) OVER (
      PARTITION BY j.consumer_id, j.device_id, j.deduped_message_id
    ) as device_min_sent_at
  from joined j
),
ranked as (
  select 
    m.*,
    DENSE_RANK() OVER (
      PARTITION BY m.consumer_id, m.device_id, m.master_campaign_name
      ORDER BY m.campaign_min_sent_at
    ) as campaign_message_rank,
    LAG(m.campaign_min_sent_at) OVER (
      PARTITION BY m.consumer_id, m.device_id, m.master_campaign_name
      ORDER BY m.campaign_min_sent_at
    ) as prev_campaign_message_min_sent_at,
    DENSE_RANK() OVER (
      PARTITION BY m.consumer_id, m.device_id
      ORDER BY m.device_min_sent_at
    ) as device_message_rank,
    LAG(m.device_min_sent_at) OVER (
      PARTITION BY m.consumer_id, m.device_id
      ORDER BY m.device_min_sent_at
    ) as prev_device_message_min_sent_at
  from msg_times m
)

select 
  r.*, 

  -- Time since previous message (hours), message-level
  DATEDIFF(hour, r.prev_campaign_message_min_sent_at, r.campaign_min_sent_at) as hours_since_prev_message_campaign,
  DATEDIFF(hour, r.prev_device_message_min_sent_at, r.device_min_sent_at) as hours_since_prev_message_device,
  -- First..Fifth message hours since join within campaign thread (message-level)
  MAX(CASE WHEN r.campaign_message_rank = 1 THEN DATEDIFF(hour, r.join_time, r.campaign_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id, r.master_campaign_name) as msg1_hours_since_join_campaign,
  MAX(CASE WHEN r.campaign_message_rank = 2 THEN DATEDIFF(hour, r.join_time, r.campaign_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id, r.master_campaign_name) as msg2_hours_since_join_campaign,
  MAX(CASE WHEN r.campaign_message_rank = 3 THEN DATEDIFF(hour, r.join_time, r.campaign_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id, r.master_campaign_name) as msg3_hours_since_join_campaign,
  MAX(CASE WHEN r.campaign_message_rank = 4 THEN DATEDIFF(hour, r.join_time, r.campaign_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id, r.master_campaign_name) as msg4_hours_since_join_campaign,
  MAX(CASE WHEN r.campaign_message_rank = 5 THEN DATEDIFF(hour, r.join_time, r.campaign_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id, r.master_campaign_name) as msg5_hours_since_join_campaign,
  -- First..Fifth message hours since join overall (device thread)
  MAX(CASE WHEN r.device_message_rank = 1 THEN DATEDIFF(hour, r.join_time, r.device_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id) as msg1_hours_since_join_device,
  MAX(CASE WHEN r.device_message_rank = 2 THEN DATEDIFF(hour, r.join_time, r.device_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id) as msg2_hours_since_join_device,
  MAX(CASE WHEN r.device_message_rank = 3 THEN DATEDIFF(hour, r.join_time, r.device_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id) as msg3_hours_since_join_device,
  MAX(CASE WHEN r.device_message_rank = 4 THEN DATEDIFF(hour, r.join_time, r.device_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id) as msg4_hours_since_join_device,
  MAX(CASE WHEN r.device_message_rank = 5 THEN DATEDIFF(hour, r.join_time, r.device_min_sent_at) END) OVER (PARTITION BY r.consumer_id, r.device_id) as msg5_hours_since_join_device
from ranked r
);


-- Orders linked to new users with monetary and item metrics; includes timing deltas from join to order and store attributes.
CREATE OR REPLACE TABLE proddb.fionafan.nv_dp_new_user_orders AS (


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
      dd.service_fee * 0.01 AS service_fee,
      dd.NV_VERTICAL_NAME AS nv_vertical_name,
      dd.NV_ORG AS nv_org,
      dd.NV_BUSINESS_LINE AS nv_business_line,
      dd.NV_BUSINESS_SUB_TYPE AS nv_business_sub_type
    FROM segment_events_raw.consumer_production.order_cart_submit_received a
    JOIN dimension_deliveries dd

      ON a.order_cart_id = dd.order_cart_id
      AND dd.is_filtered_core = 1
      AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-04-01' AND '2025-09-12'
    WHERE convert_timezone('UTC','America/Los_Angeles',a.timestamp) BETWEEN '2025-04-01' AND '2025-09-12'
  ),
  
  experiment_orders AS (
    SELECT distinct

      epe.dd_device_ID_filtered,
      epe.join_time,
      epe.day as join_day,
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
      oe.nv_vertical_name,
      oe.nv_org,
      oe.nv_business_line,
      oe.nv_business_sub_type,
      oe.variable_profit,
      oe.tip,
      oe.delivery_fee,
      oe.service_fee,
      
      -- Calculate time between exposure and order
      DATEDIFF('hour', epe.join_time, oe.order_timestamp) AS hours_between_exposure_and_order,
      DATEDIFF('day', epe.join_time, oe.order_timestamp) AS days_between_exposure_and_order,
      

    FROM proddb.fionafan.nv_dp_new_user_table epe

    LEFT JOIN order_events oe
      ON epe.dd_device_ID_filtered = oe.dd_device_ID_filtered 
      AND epe.join_time <= oe.order_timestamp  -- Order must be after effective exposure
    
  )
  
  SELECT * FROM experiment_orders
  WHERE delivery_id IS NOT NULL  -- Only include rows where we found orders
);






-- Aggregated first-session metrics for experiment preference users plus 24h order outcomes at device-exposure level.
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

select * from edw.merchant.dimension_store where nv_org is not null order by consumer_count desc limit 1000;
