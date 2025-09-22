-- Comprehensive Analysis of Notification Campaigns: FPN Postal Service vs Braze
-- Author: Fiona Fan
-- Date: 2024-12-19
-- Purpose: Compare notification campaign performance across different sources and time windows

-- Braze message details from separate table

create or replace table proddb.fionafan.notif_new_user_table as (


SELECT  DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , iguazu_timestamp as join_time
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN '2025-07-23'::date-30 AND '2025-07-23'
);

-- select count(1) from proddb.fionafan.notif_new_user_table;
create or replace table proddb.fionafan.notif_base_table_week as (
SELECT
  n.*, u.join_time, u.day as join_day, 
  FLOOR(DATEDIFF(day, u.join_time, n.SENT_AT_DATE) / 7) + 1 AS LIFECYCLE_WEEK
FROM edw.consumer.fact_consumer_notification_engagement n

inner join proddb.fionafan.notif_new_user_table u on n.consumer_id = u.consumer_id
WHERE 1=1
  AND n.SENT_AT_DATE >= '2025-07-23'::date-30
  AND n.SENT_AT_DATE < '2025-07-23'::date+30
  and n.sent_at_date > u.join_time
and n.sent_at_date <= DATEADD('day', 30, u.join_time)
  );

create or replace table proddb.fionafan.notif_base_table_w_braze_week as (

with deliveries as (
    select 
      d.creator_id,
      MIN(d.created_at) as first_order_created_at
    from edw.finance.dimension_deliveries d
    inner join proddb.fionafan.notif_base_table_week n
      on d.creator_id = n.consumer_id
    where 1=1
      and d.is_filtered_core = 1
      and d.active_date >= '2025-07-23'::date-30
      and d.active_date < '2025-07-23'::date+30
    group by d.creator_id
),

ct as (
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
from proddb.fionafan.notif_base_table_week n
left join fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt bz on bz.dispatch_id = n.deduped_message_id and bz.external_id = n.consumer_id
left join  ct on ct.canvas_id = bz.canvas_id
left join deliveries dl on dl.creator_id = n.consumer_id and dl.first_order_created_at < n.sent_at
where 1=1
  and dl.creator_id is null
)

select * from joined
);

create  or replace table proddb.fionafan.notif_base_table_w_braze_week_ranked as (

with joined as (
select * from proddb.fionafan.notif_base_table_w_braze_week
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

select * from proddb.fionafan.notif_base_table_w_braze_week_ranked where consumer_id = '15771120';
-- Campaign-level analysis grouped by master_campaign_name
create or replace table proddb.fionafan.notif_campaign_performance as (
select notification_source,
    coalesce(canvas_name, campaign_name) as master_campaign_name,
    count(distinct deduped_message_id||consumer_id||device_id) as total_notifications,
    count(distinct consumer_id||device_id) as unique_consumer_devices,
    count(distinct deduped_message_id||consumer_id||device_id)/NULLIF(count(distinct consumer_id||device_id), 0) as avg_pushes_per_customer,
    
    -- Core engagement metrics (message-level denominator)
    count(distinct case when opened_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_rate,
    count(distinct case when open_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_within_24h_rate,
    count(distinct case when open_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_within_4h_rate,
    count(distinct case when first_session_id_after_send is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as first_session_rate,
    
    -- Unsubscribe metrics (consumer-level denominator)
    count(distinct case when unsubscribed_at is not null then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as unsubscribed_rate,
    count(distinct case when unsubscribed_at is not null and DATEDIFF(hour, sent_at, unsubscribed_at) <= 1 then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as unsubscribed_within_1h_rate,
    sum(case when unsubscribed_at is not null then DATEDIFF(minute, sent_at, unsubscribed_at) else 0 end)/NULLIF(count(case when unsubscribed_at is not null then 1 end), 0) as avg_time_to_unsubscribe_minutes,
    
    -- Uninstall metrics (consumer-level denominator)

    count(distinct case when uninstalled_at is not null then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_rate,
    count(distinct case when uninstalled_at is not null and DATEDIFF(hour, sent_at, uninstalled_at) <= 1 then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_within_1h_rate,
    -- Uninstall metrics (message-level denominator)
    count(distinct case when uninstalled_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_by_message_rate,
    count(distinct case when uninstalled_at is not null and DATEDIFF(hour, sent_at, uninstalled_at) <= 1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_by_message_1h_rate,
    sum(case when uninstalled_at is not null then DATEDIFF(minute, sent_at, uninstalled_at) else 0 end)/NULLIF(count(case when uninstalled_at is not null then 1 end), 0) as avg_time_to_uninstall_minutes,
    -- Uninstall after open metrics (consumer-level denominator)
    count(distinct case when opened_at is not null and uninstalled_at is not null then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_after_open_rate,
    count(distinct case when opened_at is not null and uninstalled_at is not null and DATEDIFF(hour, sent_at, uninstalled_at) <= 1 then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_after_open_within_1h_rate,
    -- Uninstall among non-purchasers metrics (consumer-level denominator)
    count(distinct case when ordered_at is null and uninstalled_at is not null then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_non_purchasers_rate,
    count(distinct case when order_within_1h=0 and uninstalled_at is not null and DATEDIFF(hour, sent_at, uninstalled_at) <= 1 then consumer_id end)/NULLIF(count(distinct consumer_id), 0) as uninstall_non_purchasers_within_1h_rate,
    -- Open timing metrics
    sum(case when opened_at is not null then DATEDIFF(minute, sent_at, opened_at) else 0 end)/NULLIF(count(case when opened_at is not null then 1 end), 0) as avg_time_to_open_minutes
    , count(distinct case when ordered_at is not null then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) avg_ordered

    , count(distinct case when order_within_1h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_1h
, count(distinct case when order_within_4h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_4h
, count(distinct case when order_within_24h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_24h,

    -- Tag category binary indicators
    max(has_geographic_targeting) as has_geographic_targeting,
    max(has_cx) as has_cx,
    max(has_new_verticals_grocery) as has_new_verticals_grocery,
    max(has_cart_abandonment) as has_cart_abandonment,
    max(has_mx) as has_mx,
    max(has_reminders_notifications) as has_reminders_notifications,
    max(has_multichannel) as has_multichannel,
    max(has_offers_promos) as has_offers_promos,
    max(has_technical_automation) as has_technical_automation,
    max(has_communication_channels) as has_communication_channels,
    max(GREATEST(has_new_customer_experience, case when  lower(coalesce(canvas_name, campaign_name)) ilike '%new%' or  lower(coalesce(canvas_name, campaign_name)) ilike '%welcome%' then 1 else 0 end)) as has_new_customer_experience,
    max(has_engagement_recommendations) as has_engagement_recommendations,
    max(has_dashpass) as has_dashpass,
    max(has_occasions_seasonal) as has_occasions_seasonal,
    max(has_other) as has_other,
    max(has_churned_reengagement) as has_churned_reengagement,
    max(has_customer_segments) as has_customer_segments,
    max(has_partnerships) as has_partnerships,

    -- -- New timing aggregates from notif_base_table_w_braze_week
    -- AVG(hours_since_joined) as avg_hours_since_joined,
    -- MEDIAN(hours_since_joined) as median_hours_since_joined,
    -- AVG(send_hour_of_day) as avg_send_hour_of_day,
    -- MEDIAN(send_hour_of_day) as median_send_hour_of_day,

    -- AVG(hours_since_prev_message_campaign) as avg_hours_since_prev_message_campaign,
    -- MEDIAN(hours_since_prev_message_campaign) as median_hours_since_prev_message_campaign,
    -- AVG(hours_since_prev_message_device) as avg_hours_since_prev_message_device,
    -- MEDIAN(hours_since_prev_message_device) as median_hours_since_prev_message_device,

    -- -- First..Fifth hours since join within campaign thread
    -- AVG(msg1_hours_since_join_campaign) as avg_msg1_hours_since_join_campaign,
    -- MEDIAN(msg1_hours_since_join_campaign) as median_msg1_hours_since_join_campaign,
    -- AVG(msg2_hours_since_join_campaign) as avg_msg2_hours_since_join_campaign,
    -- MEDIAN(msg2_hours_since_join_campaign) as median_msg2_hours_since_join_campaign,
    -- AVG(msg3_hours_since_join_campaign) as avg_msg3_hours_since_join_campaign,
    -- MEDIAN(msg3_hours_since_join_campaign) as median_msg3_hours_since_join_campaign,
    -- AVG(msg4_hours_since_join_campaign) as avg_msg4_hours_since_join_campaign,
    -- MEDIAN(msg4_hours_since_join_campaign) as median_msg4_hours_since_join_campaign,
    -- AVG(msg5_hours_since_join_campaign) as avg_msg5_hours_since_join_campaign,
    -- MEDIAN(msg5_hours_since_join_campaign) as median_msg5_hours_since_join_campaign,

    -- -- First..Fifth hours since join overall (device thread)
    -- AVG(msg1_hours_since_join_device) as avg_msg1_hours_since_join_device,
    -- MEDIAN(msg1_hours_since_join_device) as median_msg1_hours_since_join_device,
    -- AVG(msg2_hours_since_join_device) as avg_msg2_hours_since_join_device,
    -- MEDIAN(msg2_hours_since_join_device) as median_msg2_hours_since_join_device,
    -- AVG(msg3_hours_since_join_device) as avg_msg3_hours_since_join_device,
    -- MEDIAN(msg3_hours_since_join_device) as median_msg3_hours_since_join_device,
    -- AVG(msg4_hours_since_join_device) as avg_msg4_hours_since_join_device,
    -- MEDIAN(msg4_hours_since_join_device) as median_msg4_hours_since_join_device,
    -- AVG(msg5_hours_since_join_device) as avg_msg5_hours_since_join_device,
    -- MEDIAN(msg5_hours_since_join_device) as median_msg5_hours_since_join_device,
    
    -- Tag aggregations
    listagg(distinct bz_tag_categories, ' | ') within group (order by bz_tag_categories) as all_tag_categories,
    listagg(distinct bz_tags, ' | ') within group (order by bz_tags) as all_tags,
    
    -- Alert content aggregations
    listagg(distinct bz_alert_title, ' | ') within group (order by bz_alert_title) as all_alert_titles,
    listagg(distinct bz_alert_body, ' | ') within group (order by bz_alert_body) as all_alert_bodies,
    
    -- Additional context
    count(distinct bz_canvas_id) as unique_canvas_ids,
    count(distinct bz_campaign_id) as unique_campaign_ids,
    min(sent_at_date) as first_sent_date,
    max(sent_at_date) as last_sent_date

from proddb.fionafan.notif_base_table_w_braze_week 

where 1=1
    and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    and is_valid_send = 1 
    and coalesce(canvas_name, campaign_name) is not null
group by notification_source, coalesce(canvas_name, campaign_name)
order by total_notifications desc
)
;

select * from proddb.fionafan.notif_campaign_performance where notification_source = 'Braze' order by total_notifications desc;

-- Query to cap alert title and body character lengths
SELECT 
    master_campaign_name,
    total_notifications,
    unique_consumer_devices,
    avg_pushes_per_customer,
    open_rate,
    avg_ordered,
    uninstall_rate,
    unsubscribed_rate,
    LEFT(all_alert_titles, 10000) as all_alert_titles_capped,
    LEFT(all_alert_bodies, 20000) as all_alert_bodies_capped,
    avg_time_to_open_minutes,
    avg_time_to_uninstall_minutes,
    avg_time_to_unsubscribe_minutes,

    open_within_24h_rate,
    open_within_4h_rate,
    unsubscribed_within_1h_rate,
    
    uninstall_within_1h_rate,
    
    order_within_1h,
    order_within_4h,
    order_within_24h,
    
    has_geographic_targeting,
    has_cx,
    has_new_verticals_grocery,
    has_cart_abandonment,
    has_mx,
    has_reminders_notifications,
    has_multichannel,
    has_offers_promos,
    has_technical_automation,
    has_communication_channels,
    has_new_customer_experience,
    has_engagement_recommendations,
    has_dashpass,
    has_occasions_seasonal,
    has_other,
    has_churned_reengagement,
    has_customer_segments,
    has_partnerships,
    all_tag_categories,
    all_tags,
    -- Cap alert titles to 100 characters
    
    -- Alternative using SUBSTRING (same result)
    -- SUBSTRING(all_alert_titles, 1, 100) as all_alert_titles_capped,
    -- SUBSTRING(all_alert_bodies, 1, 200) as all_alert_bodies_capped,
    unique_canvas_ids,
    unique_campaign_ids,
    first_sent_date,
    last_sent_date
FROM proddb.fionafan.notif_campaign_performance 
where notification_source = 'Braze'
ORDER BY total_notifications DESC;

select deduped_message_id, device_id, count(1) cnt from proddb.fionafan.notif_base_table_w_braze_week 
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 and lifecycle_week >0 
group by all having cnt>1 order by cnt desc limit 10;

select * from proddb.fionafan.notif_base_table_w_braze_week where device_id = '735B2C62-A86D-462D-9FFF-B336D11B8D28' and deduped_message_id = '68793dda17e5807c2d24a95de9e0e9a5'
and consumer_id = '1917314864';


select case when uninstalled_at > sent_at + interval '24h' then 1 else 0 end as uninstalled_after_24h, count(1) cnt  
from proddb.fionafan.notif_base_table_w_braze_week where notification_source = 'Braze' group by all 
limit 10;


select consumer_id, avg(uninstall_within_24h) avg, max(uninstall_within_24h) max 
from proddb.fionafan.notif_base_table_w_braze_week 
where master_campaign_name = 'TRG-NPWS45d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-NEW40OFF' 
group by all 
having avg <0.5 and max = 1
limit 10;

select sent_at, campaign_name, canvas_name, consumer_id, uninstall_within_24h, uninstalled_at
FROM edw.consumer.fact_consumer_notification_engagement n
WHERE 1=1
  AND n.SENT_AT_DATE between '2025-06-30' and '2025-07-11'
  and notification_channel = 'PUSH'
and coalesce(canvas_name, campaign_name) = 'TRG-NPWS45d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-NEW40OFF' 
and consumer_id = '1960062887';


