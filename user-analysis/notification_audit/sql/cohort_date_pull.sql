CREATE OR REPLACE TABLE proddb.fionafan.all_user_notifications_base AS

SELECT
  n.*,
  c.cohort_type,
  c.exposure_time,
  c.onboarding_day,
  FLOOR(DATEDIFF(day, c.exposure_time, n.SENT_AT_DATE) / 7) + 1 AS lifecycle_week,
  DATEDIFF(day, c.exposure_time, n.SENT_AT_DATE) AS days_since_onboarding
FROM edw.consumer.fact_consumer_notification_engagement n

INNER JOIN proddb.fionafan.all_user_july_cohort c 
  ON n.consumer_id = c.consumer_id
WHERE 1=1
  AND n.SENT_AT_DATE >= '2025-07-01'::date
  AND n.SENT_AT_DATE < '2025-08-01'::date
  AND n.sent_at_date > c.exposure_time
  AND n.sent_at_date <= DATEADD('day', 30, c.exposure_time);


-- Message count per consumer by days since onboarding
SELECT
  cohort_type,
  DATEDIFF(day, exposure_time, SENT_AT_DATE) days_since_onboarding,
  -- COUNT(DISTINCT message_id) as unique_notifications,
  -- count(distinct consumer_id) as unique_consumers,
  count(distinct deduped_message_id)/count(distinct consumer_id)  as message_per_consumer
FROM proddb.fionafan.all_user_notifications_base
-- where notification_source = 'FPN Postal Service' 
where notification_source = 'Braze' 
and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    and is_valid_send = 1 
group by all
order by all;

SELECT
  cohort_type,
  DATEDIFF(day, exposure_time, SENT_AT_DATE) days_since_onboarding,
  -- COUNT(DISTINCT message_id) as unique_notifications,
  -- count(distinct consumer_id) as unique_consumers,
  count(distinct case when opened_at is not null then deduped_message_id end)/count(distinct consumer_id)  as opened_message_per_consumer,
  count(distinct case when uninstalled_at is not null then deduped_message_id end)/count(distinct consumer_id)  as uninstalled_message_per_consumer,
  count(distinct case when uninstalled_at is not null then consumer_id end)/count(distinct consumer_id)  as uninstalled_consumer_per_consumer,
  count(distinct case when unsubscribed_at is not null then deduped_message_id end)/count(distinct consumer_id)  as unsubscribed_message_per_consumer,
  count(distinct case when unsubscribed_at is not null then consumer_id end)/count(distinct consumer_id)  as unsubscribed_consumer_per_consumer
FROM proddb.fionafan.all_user_notifications_base n
where 1=1
-- and notification_source = 'FPN Postal Service' 
and  notification_source = 'Braze' 
and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    and is_valid_send = 1 
group by all
order by all;

select body, count(1) cnt from  proddb.fionafan.all_user_notifications_base
where notification_source = 'FPN Postal Service' 
and notification_channel = 'PUSH'
    and notification_message_type_overall !='TRANSACTIONAL'
    and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    and is_valid_send = 1 group by all order by cnt desc limit 100;

select distinct notification_source from proddb.fionafan.all_user_notifications_base;


-- Aggregated view: Average messages per day across all consumers
SELECT
  cohort_type,
  days_since_onboarding,
  COUNT(DISTINCT consumer_id) as consumers_with_messages,
  SUM(message_count) as total_messages,
  ROUND(AVG(message_count), 2) as avg_messages_per_consumer,
  ROUND(MEDIAN(message_count), 2) as median_messages_per_consumer,
  MIN(message_count) as min_messages,
  MAX(message_count) as max_messages
FROM (
  SELECT
    cohort_type,
    consumer_id,
    days_since_onboarding,
    COUNT(*) as message_count
  FROM proddb.fionafan.all_user_notifications_base
  GROUP BY cohort_type, consumer_id, days_since_onboarding
)
GROUP BY cohort_type, days_since_onboarding
ORDER BY cohort_type, days_since_onboarding;


-- Validation of notification base
SELECT 
  cohort_type,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(*) as total_notifications,
  ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT consumer_id), 2) as avg_notifications_per_consumer
FROM proddb.fionafan.all_user_notifications_base
GROUP BY cohort_type
ORDER BY cohort_type;


-- Step 6: Add Braze campaign data and tag categorization

-- create or replace table proddb.fionafan.all_user_notifications_with_braze_deduped_consumer_id AS(
--     select * from proddb.fionafan.all_user_notifications_with_braze 
-- );
CREATE OR REPLACE TABLE proddb.fionafan.all_user_notifications_with_braze AS

WITH canvas_tags AS (
  WITH base AS (
    SELECT DISTINCT
      *,
      CASE 
        WHEN lower(tag) = 'multichannel' THEN 'Multichannel'
        WHEN lower(tag) = 'mx' THEN 'Mx'
        -- New Cx
        WHEN LOWER(tag) LIKE '%new%cx%' 
          OR LOWER(tag) LIKE '%new%customer%'
          OR LOWER(tag) LIKE '%fmx%'
          OR LOWER(tag) LIKE '%fmu%'
          OR tag IN ('New Cx', 'Unified New Cx Purchaser Welcome')
          THEN 'New Customer Experience'
        WHEN lower(tag) ILIKE '%cx%' OR lower(tag) = 'active' THEN 'Cx'
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
          OR lower(tag) = 'cash app pay'
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
        WHEN lower(tag) IN ('2025','2026') THEN 'Occasions & Seasonal'
        -- Default for unmatched
        ELSE 'Other'
      END AS tag_category
    FROM marketing_fivetran.braze_consumer.canvas_tag
  )
  SELECT 
    canvas_id,
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
  FROM base
  GROUP BY canvas_id
)

SELECT DISTINCT 
  n.*,
  bz.campaign_id as bz_campaign_id,
  bz.campaign_name as bz_campaign_name,
  bz.canvas_step_name as bz_canvas_step_name,
  bz.canvas_id as bz_canvas_id,
  bz.canvas_name as bz_canvas_name,
  COALESCE(bz.canvas_name, bz.campaign_name) as master_campaign_name,
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
  DATEDIFF(hour, n.exposure_time, n.sent_at) AS hours_since_exposure,
  EXTRACT(hour FROM n.sent_at) AS send_hour_of_day
FROM proddb.fionafan.all_user_notifications_base n
LEFT JOIN fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt bz 
  ON bz.dispatch_id = n.deduped_message_id 
  AND bz.external_id = n.consumer_id
LEFT JOIN canvas_tags ct 
  ON ct.canvas_id = coalesce(bz.canvas_id, n.canvas_id, bz.campaign_id, n.campaign_id);
select notification_source, case when n.has_dashpass is null then 0 else 1 end as braze_present, count(1) cnt from proddb.fionafan.all_user_notifications_with_braze n
group by all;


select count(distinct message_id ) from (
select message_id, device_id, count(1) cnt from proddb.fionafan.all_user_notifications_with_braze 
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 and lifecycle_week >0 
group by all having cnt>1 order by cnt desc );


select * from proddb.fionafan.all_user_notifications_with_braze where message_id = '43b57de6-d587-4383-9781-bc97c779ca6e' limit 10;


select notification_source, case when n.bz_campaign_id is null then 0 else 1 end as braze_present, count(1) cnt from proddb.fionafan.all_user_notifications_with_braze n

group by all;

select template_name,listagg(distinct notification_message_type, ', ') within group (order by notification_message_type) as notification_message_types,
listagg(distinct campaign_name, ', ') within group (order by campaign_name) as campaign_names,
 count(1) cnt from proddb.fionafan.all_user_notifications_with_braze 
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL' and notification_message_type!='TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and notification_source = 'FPN Postal Service'
and is_valid_send = 1
group by all having cnt>1000 order by cnt desc limit 1000;

select distinct campaign_name from proddb.fionafan.all_user_notifications_with_braze 
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL' and notification_message_type!='TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and notification_source = 'FPN Postal Service'
and campaign_name ilike '%abandon%'
and is_valid_send = 1 limit 100;


select notification_message_type, count(1) cnt from proddb.fionafan.all_user_notifications_with_braze 
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL' and notification_message_type!='TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1
group by all order by cnt desc limit 1000;
-- having cnt>1000 

WITH source AS (
  SELECT
    COALESCE(campaign_name, canvas_name, bz_campaign_name) AS master_campaign_name,
    notification_message_type,
    deduped_message_id
  FROM proddb.fionafan.all_user_notifications_with_braze
  WHERE 1=1
    AND notification_channel = 'PUSH'
    -- AND notification_source = 'FPN Postal Service'
    AND notification_source = 'Braze'
    AND notification_message_type_overall != 'TRANSACTIONAL'
    AND notification_message_type != 'TRANSACTIONAL'
    AND COALESCE(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
    AND is_valid_send = 1
    -- AND cohort_type = 'post_onboarding'
),

flagged AS (
  SELECT
    master_campaign_name,
    notification_message_type,
    deduped_message_id,

    -- cart tokens (convenience, alcohol, 3p, groceries, grocery)
    CASE WHEN lower(master_campaign_name) LIKE '%convenience%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%alcohol%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%3p%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%groceries%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%grocery%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%nv%' THEN 1 
         ELSE 0 END AS is_nv,

    -- fmx family: fmu / fmx / fm / adpt
    CASE WHEN lower(master_campaign_name) LIKE '%fmu%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%fmx%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%fm%'  THEN 1
         WHEN lower(master_campaign_name) LIKE '%adpt%' THEN 1
         ELSE 0 END AS is_fmx,

    CASE WHEN lower(master_campaign_name) LIKE '%new40off%' THEN 1 ELSE 0 END AS is_npws,
    CASE WHEN lower(master_campaign_name) LIKE '%challenge%' THEN 1 ELSE 0 END AS is_challenge,

    -- dashpass: contains 'dashpass' OR contains 'dp' and NOT 'adpt'
    CASE WHEN lower(master_campaign_name) LIKE '%dashpass%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%dp%' AND lower(master_campaign_name) NOT LIKE '%adpt%' THEN 1
         ELSE 0 END AS is_dashpass,

    CASE WHEN lower(master_campaign_name) LIKE '%abandon%' THEN 1 ELSE 0 END AS is_abandon,

    CASE WHEN lower(master_campaign_name) LIKE '%post_order%' THEN 1
         WHEN lower(master_campaign_name) LIKE '%postcheckout%' THEN 1
         ELSE 0 END AS is_post_order,

    -- fixed typo: reorder
    CASE WHEN lower(master_campaign_name) LIKE '%reorder%' THEN 1 ELSE 0 END AS is_reorder,

    CASE WHEN lower(master_campaign_name) LIKE '%gift%' THEN 1 ELSE 0 END AS is_gift_card_campaign,

    -- message-type based flags
    CASE WHEN lower(notification_message_type) LIKE '%recommendation%' THEN 1 ELSE 0 END AS is_recommendation,
    CASE WHEN lower(notification_message_type) LIKE '%doordash_offer%' THEN 1 ELSE 0 END AS is_doordash_offer,
    CASE WHEN lower(notification_message_type) LIKE '%reminder%' THEN 1 ELSE 0 END AS is_reminder,
    CASE WHEN lower(notification_message_type) LIKE '%store_offer%' THEN 1 ELSE 0 END AS is_store_offer

  FROM source
),

with_is_new AS (
  SELECT
    master_campaign_name,
    notification_message_type,
    deduped_message_id,
    is_nv,
    is_fmx,
    is_npws,
    is_challenge,
    is_dashpass,
    is_abandon,
    is_post_order,
    is_reorder,
    is_gift_card_campaign,
    is_recommendation,
    is_doordash_offer,
    is_reminder,
    is_store_offer,

    -- is_new if contains 'new' OR is_npws OR is_fmx
    CASE WHEN lower(master_campaign_name) LIKE '%new%' THEN 1
         WHEN is_npws = 1 THEN 1
         WHEN is_fmx = 1 THEN 1
         ELSE 0 END AS is_new
  FROM flagged
)

SELECT
  master_campaign_name,
  notification_message_type,

  is_abandon                           AS is_abandon_campaign,
  is_nv,
  is_fmx,
  is_npws,
  is_new,
  is_challenge,
  is_dashpass,
  is_post_order,
  is_reorder,
  is_gift_card_campaign,
  is_recommendation,
  is_doordash_offer,
  is_reminder,
  is_store_offer,

  COUNT(DISTINCT deduped_message_id) AS messages_sent

FROM with_is_new
GROUP BY
  master_campaign_name,
  notification_message_type,
  is_abandon,
  is_nv,
  is_fmx,
  is_npws,
  is_new,
  is_challenge,
  is_dashpass,
  is_post_order,
  is_reorder,
  is_gift_card_campaign,
  is_recommendation,
  is_doordash_offer,
  is_reminder,
  is_store_offer
ORDER BY messages_sent DESC
LIMIT 1000;
select coalesce(campaign_name,canvas_name, bz_campaign_name) as master_campaign_name, notification_message_type

, count(distinct deduped_message_id) messages_sent 
from proddb.fionafan.all_user_notifications_with_braze 
where 1=1
and notification_channel = 'PUSH'
-- and notification_source = 'Braze'
and notification_message_type_overall != 'TRANSACTIONAL' and notification_message_type!='TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1
-- and cohort_type = 'post_onboarding'
group by all order by messages_sent desc
limit 1000;


select notification_message_type, count(1)cnt, listagg(distinct template_name, ', ') within group (order by template_name) as template_names 
from proddb.fionafan.all_user_notifications_with_braze where template_name is null 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL' and notification_message_type!='TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1
and notification_source = 'FPN Postal Service'
group by all
limit 10;