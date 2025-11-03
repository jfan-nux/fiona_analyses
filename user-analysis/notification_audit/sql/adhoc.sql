create or replace table proddb.fionafan.all_notifs_with_tag_base as (
with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2),
base as (SELECT distinct
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

, tag_base as (
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
, raw_data as (
select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
--coalesce(jl.title, fz.title) as title, 
jl.lifecycle_week,
--fz.pushbody, 
jl.device_id,
jl.received_at,
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.receive_within_1h,
jl.open_within_1h, 
jl.visit_within_1h,
jl.order_within_1h,
jl.device_unsubscribe_within_1h,
jl.unsubscribe_within_1h,
jl.uninstall_within_1h,
jl.open_within_24h, 
jl.visit_within_24h,
jl.order_within_24h,
jl.device_unsubscribe_within_24h,
jl.unsubscribe_within_24h,
jl.uninstall_within_24h,
is_valid_send
from shivanipoddar.growthdeepdive jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'
and is_valid_send = 1

), raw_data_with_tag_base as (
select rd.*, tb.tag_categories, tb.tags, tb.unique_tag_count, tb.has_geographic_targeting, tb.has_cx, tb.has_new_verticals_grocery, tb.has_cart_abandonment, tb.has_mx, tb.has_reminders_notifications, tb.has_multichannel, tb.has_offers_promos, tb.has_technical_automation, tb.has_communication_channels, tb.has_new_customer_experience, tb.has_engagement_recommendations, tb.has_dashpass, tb.has_occasions_seasonal, tb.has_other, tb.has_churned_reengagement, tb.has_customer_segments, tb.has_partnerships
from raw_data rd
left join tag_base tb on rd.master_campaign_id = tb.canvas_id
)

select * from raw_data_with_tag_base);

select notification_source, master_campaign_name, --lifecycle_week,
count(DISTINCT deduped_message_id) AS num_push,
count(DISTINCT CASE WHEN open_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_open, 
count(DISTINCT CASE WHEN visit_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit, 
count(DISTINCT CASE WHEN order_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_order,
count(DISTINCT CASE WHEN unsubscribe_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs,
count(DISTINCT CASE WHEN uninstall_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall,
num_open/nullif(num_push,0) AS send_to_open_1H,
num_visit/nullif(num_push,0) AS send_to_visit_1H,
num_order/nullif(num_push,0) AS send_to_order_1H,
num_unsubs/nullif(num_push,0) AS unsub_1H,
num_uninstall/nullif(num_push,0) AS uninstall_1H,
count(DISTINCT CASE WHEN open_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_open_24, 
count(DISTINCT CASE WHEN visit_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit_24, 
count(DISTINCT CASE WHEN order_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_order_24,
count(DISTINCT CASE WHEN unsubscribe_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs_24,
count(DISTINCT CASE WHEN uninstall_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall_24,
num_open_24/nullif(num_push,0) AS send_to_open_24H,
num_visit_24/nullif(num_push,0) AS send_to_visit_24H,
num_order_24/nullif(num_push,0) AS send_to_order_24H,
num_unsubs_24/nullif(num_push,0) AS unsub_24H,
num_uninstall_24/nullif(num_push,0) AS uninstall_24H
from raw_data
where 1=1
group by 1, 2 --, 3
having num_push > 1000
order by num_push desc
;