-- Step 5: Add notification data for all cohorts
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
  count(distinct message_id)/count(distinct consumer_id)  as message_per_consumer
FROM proddb.fionafan.all_user_notifications_base
where notification_source = 'FPN Postal Service' 
and notification_channel = 'PUSH'
group by all
order by all;

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
  ON ct.canvas_id = bz.canvas_id;
select * from proddb.fionafan.all_user_notifications_with_braze where deduped_message_id='e73ef50a-1e1e-41ac-b465-4ff620ff20fd' limit 10;

-- Validation of notifications with Braze
SELECT 
  cohort_type,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(*) as total_notifications,
  COUNT(DISTINCT master_campaign_name) as unique_campaigns,
  SUM(CASE WHEN master_campaign_name IS NOT NULL THEN 1 ELSE 0 END) as notifications_with_braze,
  ROUND(100.0 * SUM(CASE WHEN master_campaign_name IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_with_braze
FROM proddb.fionafan.all_user_notifications_with_braze

GROUP BY cohort_type
ORDER BY cohort_type;


-- Notification engagement analysis by cohort and tag category
SELECT 
  cohort_type,
  CASE 
    WHEN has_new_customer_experience = 1 THEN 'New Customer Experience'
    WHEN has_offers_promos = 1 THEN 'Offers & Promos'
    WHEN has_dashpass = 1 THEN 'DashPass'
    WHEN has_churned_reengagement = 1 THEN 'Churned & Reengagement'
    WHEN has_cart_abandonment = 1 THEN 'Cart Abandonment'
    WHEN has_engagement_recommendations = 1 THEN 'Engagement & Recommendations'
    WHEN has_new_verticals_grocery = 1 THEN 'New Verticals & Grocery'
    WHEN has_reminders_notifications = 1 THEN 'Reminders & Notifications'
    ELSE 'Other/Unknown'
  END as primary_tag_category,
  COUNT(DISTINCT consumer_id) as unique_consumers,
  COUNT(*) as total_notifications,
  COUNT(distinct deduped_message_id) as total_messages,
  -- SUM(CASE WHEN notification_action = 'OPEN' THEN 1 ELSE 0 END) as opens,
  -- ROUND(100.0 * SUM(CASE WHEN notification_action = 'OPEN' THEN 1 ELSE 0 END) / COUNT(*), 2) as open_rate,
  AVG(hours_since_exposure) as avg_hours_since_exposure
FROM proddb.fionafan.all_user_notifications_with_braze
WHERE master_campaign_name IS NOT NULL
GROUP BY cohort_type, primary_tag_category
ORDER BY cohort_type, total_notifications DESC;


-- Step 6: Push Opt-In Data for All Cohorts
-- Get push notification preferences at time of exposure and current state

-- Push opt-in status at exposure time (within 1 day after exposure)
CREATE OR REPLACE TABLE proddb.fionafan.all_user_push_optin_at_exposure AS

SELECT 
  c.cohort_type,
  c.consumer_id,
  c.exposure_time,
  c.onboarding_day,
  c.lifestage,
  -- Get push opt-in status closest to exposure time
  FIRST_VALUE(p.is_push_opted_in) OVER (
    PARTITION BY c.consumer_id, c.cohort_type 
    ORDER BY p.event_timestamp
  ) as push_opted_in_at_exposure,
  FIRST_VALUE(p.event_timestamp) OVER (
    PARTITION BY c.consumer_id, c.cohort_type 
    ORDER BY p.event_timestamp
  ) as push_status_timestamp,
  DATEDIFF('hour', c.exposure_time, 
    FIRST_VALUE(p.event_timestamp) OVER (
      PARTITION BY c.consumer_id, c.cohort_type 
      ORDER BY p.event_timestamp
    )
  ) as hours_from_exposure_to_status
FROM proddb.fionafan.all_user_july_cohort c
LEFT JOIN edw.consumer.fact_consumer_notification_preference_scd p

  ON c.consumer_id = p.consumer_id
  AND p.event_timestamp >= c.exposure_time
  AND p.event_timestamp <= DATEADD('day', 1, c.exposure_time)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.consumer_id, c.cohort_type 
  ORDER BY p.event_timestamp
) = 1;


-- Current push opt-in status (most recent as of query run)
CREATE OR REPLACE TABLE proddb.fionafan.all_user_push_optin_current AS

SELECT 
  c.cohort_type,
  c.consumer_id,
  c.exposure_time,
  c.onboarding_day,
  c.lifestage,
  p.is_push_opted_in as push_opted_in_current,
  p.event_timestamp as current_status_timestamp,
  DATEDIFF('day', c.exposure_time, p.event_timestamp) as days_from_exposure_to_current
FROM proddb.fionafan.all_user_july_cohort c
LEFT JOIN edw.consumer.fact_consumer_notification_preference_scd p
  ON c.consumer_id = p.consumer_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.consumer_id, c.cohort_type 
  ORDER BY p.event_timestamp DESC
) = 1;


-- Combined push opt-in table
CREATE OR REPLACE TABLE proddb.fionafan.all_user_push_optin_combined AS

SELECT 
  e.cohort_type,
  e.consumer_id,
  e.exposure_time,
  e.onboarding_day,
  e.lifestage,
  e.push_opted_in_at_exposure,
  e.push_status_timestamp as exposure_status_timestamp,
  e.hours_from_exposure_to_status,
  c.push_opted_in_current,
  c.current_status_timestamp,
  c.days_from_exposure_to_current,
  -- Did user change their push preference?
  CASE 
    WHEN e.push_opted_in_at_exposure IS NULL THEN 'Unknown at Exposure'
    WHEN c.push_opted_in_current IS NULL THEN 'Unknown Current'
    WHEN e.push_opted_in_at_exposure = c.push_opted_in_current THEN 'No Change'
    WHEN e.push_opted_in_at_exposure = TRUE AND c.push_opted_in_current = FALSE THEN 'Opted Out'
    WHEN e.push_opted_in_at_exposure = FALSE AND c.push_opted_in_current = TRUE THEN 'Opted In'
    ELSE 'Other'
  END as push_preference_change
FROM proddb.fionafan.all_user_push_optin_at_exposure e
LEFT JOIN proddb.fionafan.all_user_push_optin_current c
  ON e.cohort_type = c.cohort_type
  AND e.consumer_id = c.consumer_id;


-- Validation: Push opt-in summary by cohort
SELECT 
  cohort_type,
  COUNT(DISTINCT consumer_id) as total_consumers,
  
  -- At exposure
  SUM(CASE WHEN push_opted_in_at_exposure = TRUE THEN 1 ELSE 0 END) as opted_in_at_exposure,
  SUM(CASE WHEN push_opted_in_at_exposure = FALSE THEN 1 ELSE 0 END) as opted_out_at_exposure,
  SUM(CASE WHEN push_opted_in_at_exposure IS NULL THEN 1 ELSE 0 END) as unknown_at_exposure,
  ROUND(100.0 * SUM(CASE WHEN push_opted_in_at_exposure = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_opted_in_at_exposure,
  
  -- Current
  SUM(CASE WHEN push_opted_in_current = TRUE THEN 1 ELSE 0 END) as opted_in_current,
  SUM(CASE WHEN push_opted_in_current = FALSE THEN 1 ELSE 0 END) as opted_out_current,
  SUM(CASE WHEN push_opted_in_current IS NULL THEN 1 ELSE 0 END) as unknown_current,
  ROUND(100.0 * SUM(CASE WHEN push_opted_in_current = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_opted_in_current,
  
  -- Changes
  SUM(CASE WHEN push_preference_change = 'No Change' THEN 1 ELSE 0 END) as no_change,
  SUM(CASE WHEN push_preference_change = 'Opted In' THEN 1 ELSE 0 END) as opted_in_after_exposure,
  SUM(CASE WHEN push_preference_change = 'Opted Out' THEN 1 ELSE 0 END) as opted_out_after_exposure,
  ROUND(100.0 * SUM(CASE WHEN push_preference_change = 'Opted In' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_opted_in_after_exposure
  
FROM proddb.fionafan.all_user_push_optin_combined
GROUP BY cohort_type
ORDER BY cohort_type;


-- Push opt-in rate by lifestage (for active and new cohorts)
SELECT 
  cohort_type,
  lifestage,
  COUNT(DISTINCT consumer_id) as total_consumers,
  SUM(CASE WHEN push_opted_in_at_exposure = TRUE THEN 1 ELSE 0 END) as opted_in_at_exposure,
  ROUND(100.0 * SUM(CASE WHEN push_opted_in_at_exposure = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_opted_in_at_exposure,
  SUM(CASE WHEN push_opted_in_current = TRUE THEN 1 ELSE 0 END) as opted_in_current,
  ROUND(100.0 * SUM(CASE WHEN push_opted_in_current = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_opted_in_current
FROM proddb.fionafan.all_user_push_optin_combined
WHERE lifestage IS NOT NULL
GROUP BY cohort_type, lifestage
ORDER BY cohort_type, lifestage;


-- Consumer-Day Level Push Opt-In Status (Days 1-28 after exposure)
-- Uses SCD table to check if each day falls within scd_start_date and scd_end_date
CREATE OR REPLACE TABLE proddb.fionafan.all_user_push_optin_daily AS

WITH day_sequence AS (
  -- Generate sequence of days 1-28
  SELECT 
    ROW_NUMBER() OVER (ORDER BY SEQ4()) as day_num
  FROM TABLE(GENERATOR(ROWCOUNT => 28))
),

consumer_days AS (
  -- Cross join cohort with days to get consumer-day grain
  SELECT 
    c.cohort_type,
    c.consumer_id,
    c.exposure_time,
    c.onboarding_day,
    c.lifestage,
    d.day_num,
    DATEADD('day', d.day_num, c.exposure_time::date) as calendar_date
  FROM proddb.fionafan.all_user_july_cohort c
  CROSS JOIN day_sequence d
),

push_settings_with_flags AS (
  -- Calculate opt-in flags from SCD3 table
  SELECT 
    consumer_id,
    scd_start_date,
    scd_end_date,
    system_level_status,
    product_updates_news_status,
    prev_product_updates_news_status,
    recommendations_status,
    prev_recommendations_status,
    reminders_status,
    prev_reminders_status,
    doordash_offers_status,
    prev_doordash_offers_status,
    store_offers_status,
    prev_store_offers_status,
    -- Calculate opt-in flags
    CASE WHEN product_updates_news_status = 'on' AND prev_product_updates_news_status = 'off' THEN 1 ELSE 0 END AS product_update_push_opt_in,
    CASE WHEN recommendations_status = 'on' AND prev_recommendations_status = 'off' THEN 1 ELSE 0 END AS recommendations_push_opt_in,
    CASE WHEN reminders_status = 'on' AND prev_reminders_status = 'off' THEN 1 ELSE 0 END AS reminders_push_opt_in,
    CASE WHEN doordash_offers_status = 'on' AND prev_doordash_offers_status = 'off' THEN 1 ELSE 0 END AS dd_offers_push_opt_in,
    CASE WHEN store_offers_status = 'on' AND prev_store_offers_status = 'off' THEN 1 ELSE 0 END AS mx_offers_push_opt_in
  FROM edw.consumer.dimension_consumer_push_settings_scd3
  WHERE scd_start_date >= '2025-06-01' -- Include buffer before July
),

push_settings_enhanced AS (
  SELECT 
    *,
    GREATEST(
      product_update_push_opt_in,
      recommendations_push_opt_in,
      reminders_push_opt_in,
      dd_offers_push_opt_in,
      mx_offers_push_opt_in
    ) AS marketing_push_opt_in,
    CASE 
      WHEN GREATEST(product_update_push_opt_in, recommendations_push_opt_in, reminders_push_opt_in, dd_offers_push_opt_in, mx_offers_push_opt_in) = 1 
        AND system_level_status = 'on' 
      THEN 1 
      ELSE 0 
    END AS push_opt_in
  FROM push_settings_with_flags
)

SELECT 
  cd.cohort_type,
  cd.consumer_id,
  cd.exposure_time,
  cd.onboarding_day,
  cd.lifestage,
  cd.day_num,
  cd.calendar_date,
  -- Push opt-in status for this specific day
  COALESCE(MAX(ps.push_opt_in), 0) as is_push_opted_in,
  COALESCE(MAX(ps.marketing_push_opt_in), 0) as is_marketing_opted_in,
  COALESCE(MAX(ps.product_update_push_opt_in), 0) as is_product_update_opted_in,
  COALESCE(MAX(ps.recommendations_push_opt_in), 0) as is_recommendations_opted_in,
  COALESCE(MAX(ps.reminders_push_opt_in), 0) as is_reminders_opted_in,
  COALESCE(MAX(ps.dd_offers_push_opt_in), 0) as is_dd_offers_opted_in,
  COALESCE(MAX(ps.mx_offers_push_opt_in), 0) as is_mx_offers_opted_in,
  MAX(ps.system_level_status) as system_level_status
FROM consumer_days cd
LEFT JOIN push_settings_enhanced ps
  ON cd.consumer_id = ps.consumer_id
  AND cd.calendar_date >= ps.scd_start_date
  AND cd.calendar_date < COALESCE(ps.scd_end_date, '9999-12-31')
GROUP BY 
  cd.cohort_type,
  cd.consumer_id,
  cd.exposure_time,
  cd.onboarding_day,
  cd.lifestage,
  cd.day_num,
  cd.calendar_date
ORDER BY cd.cohort_type, cd.consumer_id, cd.day_num;


-- Validation: Daily opt-in summary by cohort
SELECT 
  cohort_type,
  day_num,
  COUNT(DISTINCT consumer_id) as total_consumers,
  SUM(is_push_opted_in) as consumers_opted_in,
  ROUND(100.0 * SUM(is_push_opted_in) / COUNT(DISTINCT consumer_id), 2) as pct_opted_in,
  SUM(is_marketing_opted_in) as consumers_marketing_opted_in,
  ROUND(100.0 * SUM(is_marketing_opted_in) / COUNT(DISTINCT consumer_id), 2) as pct_marketing_opted_in
FROM proddb.fionafan.all_user_push_optin_daily
GROUP BY cohort_type, day_num
ORDER BY cohort_type, day_num;


-- Daily opt-in trends: Show change over time
SELECT 
  cohort_type,
  day_num,
  SUM(is_push_opted_in) as opted_in_count,
  SUM(is_push_opted_in) - LAG(SUM(is_push_opted_in)) OVER (PARTITION BY cohort_type ORDER BY day_num) as daily_change,
  ROUND(100.0 * SUM(is_push_opted_in) / SUM(COUNT(DISTINCT consumer_id)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort_opted_in
FROM proddb.fionafan.all_user_push_optin_daily
GROUP BY cohort_type, day_num
ORDER BY cohort_type, day_num;

