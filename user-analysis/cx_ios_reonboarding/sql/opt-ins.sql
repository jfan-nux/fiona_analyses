-- edw.consumer.dimension_consumer_device_push_settings_scd3 


-- Enrich funnel with current opt-in statuses at event date from SCD3 tables
-- Creates: proddb.public.onboarding_funnel_flags_w_optins_curr
-- Joins by consumer_id and as-of date between scd_start_date and scd_end_date (no scd_current_record filter)
CREATE OR REPLACE TABLE proddb.public.onboarding_funnel_flags_w_optins_curr AS
WITH base AS (
  SELECT
    ofc.*,
    /* as-of date for SCD windowing */
    ofc.day AS asof_date
  FROM proddb.public.onboarding_funnel_flags_curr ofc
),
push_scd AS (
  SELECT
    consumer_id,
    scd_start_date,
    scd_end_date,
    system_level_status,
    doordash_offers_status,
    order_updates_status,
    product_updates_news_status,
    recommendations_status,
    reminders_status,
    store_offers_status
  FROM edw.consumer.dimension_consumer_device_push_settings_scd3
),
email_scd AS (
  SELECT
    consumer_id,
    scd_start_date,
    scd_end_date,
    marketing_channel_status,
    customer_research_status,
    news_updates_status,
    notifications_reminders_status,
    recommendations_status,
    special_offers_status,
    store_promotions_status
  FROM edw.consumer.dimension_consumer_email_settings_scd3
),
sms_scd AS (
  SELECT
    consumer_id,
    scd_start_date,
    scd_end_date,
    order_updates_status,
    marketing_status
  FROM edw.consumer.dimension_consumer_sms_settings_scd3
)
SELECT
  b.*,
  -- Push (prefixed)
  ps.system_level_status            AS push_system_level_status,
  ps.doordash_offers_status         AS push_doordash_offers_status,
  ps.order_updates_status           AS push_order_updates_status,
  ps.product_updates_news_status    AS push_product_updates_news_status,
  ps.recommendations_status         AS push_recommendations_status,
  ps.reminders_status               AS push_reminders_status,
  ps.store_offers_status            AS push_store_offers_status,
  -- Email (prefixed)
  es.marketing_channel_status       AS email_marketing_channel_status,
  es.customer_research_status       AS email_customer_research_status,
  es.news_updates_status            AS email_news_updates_status,
  es.notifications_reminders_status AS email_notifications_reminders_status,
  es.recommendations_status         AS email_recommendations_status,
  es.special_offers_status          AS email_special_offers_status,
  es.store_promotions_status        AS email_store_promotions_status,
  -- SMS (prefixed)
  ss.order_updates_status           AS sms_order_updates_status,
  ss.marketing_status               AS sms_marketing_status
FROM base b
LEFT JOIN push_scd ps
  ON ps.consumer_id = b.consumer_id
 AND b.asof_date BETWEEN ps.scd_start_date AND COALESCE(ps.scd_end_date, '9999-12-31')
LEFT JOIN email_scd es
  ON es.consumer_id = b.consumer_id
 AND b.asof_date BETWEEN es.scd_start_date AND COALESCE(es.scd_end_date, '9999-12-31')
LEFT JOIN sms_scd ss
  ON ss.consumer_id = b.consumer_id
 AND b.asof_date BETWEEN ss.scd_start_date AND COALESCE(ss.scd_end_date, '9999-12-31');
select day, count(1) 
from proddb.public.onboarding_funnel_flags_w_optins_curr 
group by all
order by 1;



CREATE OR REPLACE  TABLE proddb.public.onboarding_start_funnel_curr AS
SELECT DISTINCT
  cast(iguazu_timestamp as date) AS day,
  consumer_id,
  DD_DEVICE_ID,
  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID else 'dx_'||DD_DEVICE_ID end), '-') as dd_device_id_filtered,
  dd_platform,
  lower(onboarding_type) as onboarding_type,
  promo_title,
  'start_page' as onboarding_page
FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -14 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
  AND ((lower(onboarding_type) = 'new_user') OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user'))

UNION ALL

SELECT DISTINCT
  cast(iguazu_timestamp as date) AS day,
  consumer_id,
  DD_DEVICE_ID,
  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID else 'dx_'||DD_DEVICE_ID end), '-') as dd_device_id_filtered,
  dd_platform,
  lower(onboarding_type) as onboarding_type,
  NULL as promo_title,
  'start_page' as onboarding_page
FROM iguazu.consumer.M_onboarding_page_view_ice
WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -14 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
  AND lower(onboarding_type) = 'resurrected_user'
  AND lower(page) = 'welcomeback'
  AND lower(dd_platform) = 'ios'
;
grant ALL PRIVILEGES on proddb.public.onboarding_start_funnel_curr  to public;
grant ALL PRIVILEGES on proddb.public.onboarding_start_funnel_curr  to fionafan;

CREATE OR REPLACE  TABLE proddb.public.onboarding_funnel_flags_curr AS
WITH p AS (
  SELECT * FROM proddb.public.onboarding_start_funnel_curr
),
start_clicks AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND ((lower(onboarding_type) = 'new_user') OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user'))
  GROUP BY 1,2
  UNION
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'
    AND click_type = 'primary'
  GROUP BY 1,2
),
notif_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'notification'
  GROUP BY 1,2
),
notif_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'notification'
  GROUP BY 1,2
),
marketing_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2
),
marketing_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2
),
preference_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) like '%preference%'
  GROUP BY 1,2
),
preference_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) like '%preference%'
  GROUP BY 1,2
),
att_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'att'
  GROUP BY 1,2
),
att_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'att'
  GROUP BY 1,2
),
end_page_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
  GROUP BY 1,2
),
end_page_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
  GROUP BY 1,2
),
end_page_promo_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND POSITION('%' IN promo_title) > 0
  GROUP BY 1,2
),
end_page_promo_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 14 AND iguazu_timestamp < current_date + 1
    AND POSITION('%' IN promo_title) > 0
  GROUP BY 1,2
),
eligible AS (
  SELECT DATE(iguazu_sent_at) AS day, consumer_id
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS
  WHERE placement_type = 'PLACEMENT_TYPE_POST_ONBOARDING'
    AND is_eligible = TRUE
    AND iguazu_sent_at >= current_date - 14 AND iguazu_sent_at < current_date + 1
  GROUP BY 1,2
)
SELECT 
  p.day,
  p.consumer_id,
  p.dd_device_id_filtered,
  p.dd_device_id,
  p.dd_platform,
  p.onboarding_type,
  p.promo_title,
  1 as start_page_view_flag,
  IFF(sc.consumer_id IS NOT NULL, 1, 0) as start_page_click_flag,
  IFF(nv.consumer_id IS NOT NULL, 1, 0) as notification_view_flag,
  IFF(nc.consumer_id IS NOT NULL, 1, 0) as notification_click_flag,
  IFF(mv.consumer_id IS NOT NULL, 1, 0) as marketing_sms_view_flag,
  IFF(mc.consumer_id IS NOT NULL, 1, 0) as marketing_sms_click_flag,
  IFF(pv.consumer_id IS NOT NULL, 1, 0) as preference_view_flag,
  IFF(pc.consumer_id IS NOT NULL, 1, 0) as preference_click_flag,
  IFF(av.consumer_id IS NOT NULL, 1, 0) as att_view_flag,
  IFF(ac.consumer_id IS NOT NULL, 1, 0) as att_click_flag,
  IFF(epv.consumer_id IS NOT NULL, 1, 0) as end_page_view_flag,
  IFF(epc.consumer_id IS NOT NULL, 1, 0) as end_page_click_flag,
  IFF(eppv.consumer_id IS NOT NULL, 1, 0) as end_page_promo_view_flag,
  IFF(eppc.consumer_id IS NOT NULL, 1, 0) as end_page_promo_click_flag,
  IFF(el.consumer_id IS NOT NULL, 1, 0) as campaign_eligible_flag
FROM p
LEFT JOIN start_clicks sc ON sc.day = p.day AND sc.consumer_id = p.consumer_id
LEFT JOIN notif_view nv ON nv.day = p.day AND nv.consumer_id = p.consumer_id
LEFT JOIN notif_click nc ON nc.day = p.day AND nc.consumer_id = p.consumer_id
LEFT JOIN marketing_view mv ON mv.day = p.day AND mv.consumer_id = p.consumer_id
LEFT JOIN marketing_click mc ON mc.day = p.day AND mc.consumer_id = p.consumer_id
LEFT JOIN preference_view pv ON pv.day = p.day AND pv.consumer_id = p.consumer_id
LEFT JOIN preference_click pc ON pc.day = p.day AND pc.consumer_id = p.consumer_id
LEFT JOIN att_view av ON av.day = p.day AND av.consumer_id = p.consumer_id
LEFT JOIN att_click ac ON ac.day = p.day AND ac.consumer_id = p.consumer_id
LEFT JOIN end_page_view epv ON epv.day = p.day AND epv.consumer_id = p.consumer_id
LEFT JOIN end_page_click epc ON epc.day = p.day AND epc.consumer_id = p.consumer_id
LEFT JOIN end_page_promo_view eppv ON eppv.day = p.day AND eppv.consumer_id = p.consumer_id
LEFT JOIN end_page_promo_click eppc ON eppc.day = p.day AND eppc.consumer_id = p.consumer_id
LEFT JOIN eligible el ON el.day = p.day AND el.consumer_id = p.consumer_id
;

grant  ALL PRIVILEGES  on proddb.public.onboarding_funnel_flags_curr  to public;
grant  ALL PRIVILEGES  on proddb.public.onboarding_funnel_flags_curr  to fionafan;

WITH start_date_flags AS (SELECT current_date -14 as start_dt),
    end_date_flags AS (SELECT current_date as end_dt),

flag_source AS (
  SELECT *
  FROM proddb.public.onboarding_funnel_flags
  WHERE day BETWEEN (SELECT start_dt FROM start_date_flags) AND (SELECT end_dt FROM end_date_flags)
),

funnel_flags_timeseries AS (
  -- Start Page View
  SELECT 
    'start_page_view' as onboarding_step,
    'start_page' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(start_page_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN start_page_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN start_page_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Start Page Click
  SELECT 
    'start_page_click' as onboarding_step,
    'start_page' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(start_page_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN start_page_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN start_page_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Notification View
  SELECT 
    'notification_view' as onboarding_step,
    'notification' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(notification_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN notification_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN notification_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Notification Click
  SELECT 
    'notification_click' as onboarding_step,
    'notification' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(notification_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN notification_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN notification_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Marketing SMS View
  SELECT 
    'marketing_sms_view' as onboarding_step,
    'marketing_sms' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(marketing_sms_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN marketing_sms_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN marketing_sms_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Marketing SMS Click
  SELECT 
    'marketing_sms_click' as onboarding_step,
    'marketing_sms' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(marketing_sms_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN marketing_sms_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN marketing_sms_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Preference View
  SELECT 
    'preference_view' as onboarding_step,
    'preference' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(preference_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN preference_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN preference_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Preference Click
  SELECT 
    'preference_click' as onboarding_step,
    'preference' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(preference_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN preference_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN preference_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- ATT View
  SELECT 
    'att_view' as onboarding_step,
    'att' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(att_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN att_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN att_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- ATT Click
  SELECT 
    'att_click' as onboarding_step,
    'att' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(att_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN att_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN att_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page View
  SELECT 
    'end_page_view' as onboarding_step,
    'end_page' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(end_page_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN end_page_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN end_page_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page Click
  SELECT 
    'end_page_click' as onboarding_step,
    'end_page' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(end_page_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN end_page_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN end_page_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page Promo View
  SELECT 
    'end_page_promo_view' as onboarding_step,
    'end_page_promo' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(end_page_promo_view_flag) as total_events,
    COUNT(DISTINCT CASE WHEN end_page_promo_view_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN end_page_promo_view_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page Promo Click
  SELECT 
    'end_page_promo_click' as onboarding_step,
    'end_page_promo' as onboarding_page,
    'click' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(end_page_promo_click_flag) as total_events,
    COUNT(DISTINCT CASE WHEN end_page_promo_click_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN end_page_promo_click_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Campaign Eligible
  SELECT 
    'campaign_eligible' as onboarding_step,
    'campaign_eligible' as onboarding_page,
    'view' as onboarding_action,
    day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    SUM(campaign_eligible_flag) as total_events,
    COUNT(DISTINCT CASE WHEN campaign_eligible_flag = 1 THEN consumer_id END) as unique_devices,
    COUNT(DISTINCT CASE WHEN campaign_eligible_flag = 1 THEN consumer_id END) as unique_consumers
  FROM flag_source
  GROUP BY 1,2,3,4,5,6,7
),

pre_result_flags AS (
  SELECT *, CASE 
    WHEN onboarding_page = 'start_page' THEN 1
    WHEN onboarding_page = 'notification' THEN 2
    WHEN onboarding_page = 'marketing_sms' THEN 3
    WHEN onboarding_page = 'preference' THEN 4
    WHEN onboarding_page = 'att' THEN 5
    WHEN onboarding_page = 'end_page' THEN 6
    WHEN onboarding_page = 'end_page_promo' THEN 7
    WHEN onboarding_page = 'campaign_eligible' THEN 8
    ELSE 0
  END AS page_order
  FROM funnel_flags_timeseries
)

SELECT distinct
  page_order||'. '||onboarding_step as onboarding_step,
  page_order||'. '||onboarding_page as onboarding_page,
  onboarding_action,
  day,
  dd_platform,
  promo_title,
  onboarding_type,
  total_events,
  unique_devices,
  unique_consumers
FROM pre_result_flags
ORDER BY day, onboarding_step, dd_platform, promo_title, onboarding_type
;


-- Unpivoted eligibility view: day x dd_platform_onboarding_type with top_of_funnel
CREATE OR REPLACE VIEW proddb.fionafan.onboarding_optins_eligibility_unpivot AS
WITH normalized AS (
  SELECT
    day,
    LOWER(dd_platform) AS dd_platform,
    LOWER(onboarding_type) AS onboarding_type,
    consumer_id,

    /* coalesce event flags */
    COALESCE(start_page_view_flag, 0)                    AS start_page_view_flag,
    COALESCE(start_page_click_flag, 0)                   AS start_page_click_flag,
    COALESCE(notification_view_flag, 0)                  AS notification_view_flag,
    COALESCE(notification_click_flag, 0)                 AS notification_click_flag,
    COALESCE(marketing_sms_view_flag, 0)                 AS marketing_sms_view_flag,
    COALESCE(marketing_sms_click_flag, 0)                AS marketing_sms_click_flag,
    COALESCE(preference_view_flag, 0)                    AS preference_view_flag,
    COALESCE(preference_click_flag, 0)                   AS preference_click_flag,
    COALESCE(att_view_flag, 0)                           AS att_view_flag,
    COALESCE(att_click_flag, 0)                          AS att_click_flag,
    COALESCE(end_page_view_flag, 0)                      AS end_page_view_flag,
    COALESCE(end_page_click_flag, 0)                     AS end_page_click_flag,
    COALESCE(end_page_promo_view_flag, 0)                AS end_page_promo_view_flag,
    COALESCE(end_page_promo_click_flag, 0)               AS end_page_promo_click_flag,
    COALESCE(campaign_eligible_flag, 0)                  AS campaign_eligible_flag,

    /* eligibility 1/0 */
    IFF(LOWER(push_system_level_status) = 'on', 1, 0)            AS push_system_level_eligible,
    IFF(LOWER(push_doordash_offers_status) = 'on', 1, 0)         AS push_doordash_offers_eligible,
    IFF(LOWER(push_order_updates_status) = 'on', 1, 0)           AS push_order_updates_eligible,
    IFF(LOWER(push_product_updates_news_status) = 'on', 1, 0)    AS push_product_updates_news_eligible,
    IFF(LOWER(push_recommendations_status) = 'on', 1, 0)         AS push_recommendations_eligible,
    IFF(LOWER(push_reminders_status) = 'on', 1, 0)               AS push_reminders_eligible,
    IFF(LOWER(push_store_offers_status) = 'on', 1, 0)            AS push_store_offers_eligible,

    IFF(LOWER(email_marketing_channel_status) = 'subscribed', 1, 0)       AS email_marketing_channel_eligible,
    IFF(LOWER(email_customer_research_status) = 'subscribed', 1, 0)       AS email_customer_research_eligible,
    IFF(LOWER(email_news_updates_status) = 'subscribed', 1, 0)            AS email_news_updates_eligible,
    IFF(LOWER(email_notifications_reminders_status) = 'subscribed', 1, 0) AS email_notifications_reminders_eligible,
    IFF(LOWER(email_recommendations_status) = 'subscribed', 1, 0)         AS email_recommendations_eligible,
    IFF(LOWER(email_special_offers_status) = 'subscribed', 1, 0)          AS email_special_offers_eligible,
    IFF(LOWER(email_store_promotions_status) = 'subscribed', 1, 0)        AS email_store_promotions_eligible,

    IFF(LOWER(sms_order_updates_status) = 'on', 1, 0)     AS sms_order_updates_eligible,
    IFF(LOWER(sms_marketing_status) = 'on', 1, 0)         AS sms_marketing_eligible
  FROM proddb.public.onboarding_funnel_flags_w_optins_curr
),
dedup AS (
  SELECT
    day, dd_platform, onboarding_type, consumer_id,
    MAX(start_page_view_flag)                AS start_page_view_flag,
    MAX(start_page_click_flag)               AS start_page_click_flag,
    MAX(notification_view_flag)              AS notification_view_flag,
    MAX(notification_click_flag)             AS notification_click_flag,
    MAX(marketing_sms_view_flag)             AS marketing_sms_view_flag,
    MAX(marketing_sms_click_flag)            AS marketing_sms_click_flag,
    MAX(preference_view_flag)                AS preference_view_flag,
    MAX(preference_click_flag)               AS preference_click_flag,
    MAX(att_view_flag)                       AS att_view_flag,
    MAX(att_click_flag)                      AS att_click_flag,
    MAX(end_page_view_flag)                  AS end_page_view_flag,
    MAX(end_page_click_flag)                 AS end_page_click_flag,
    MAX(end_page_promo_view_flag)            AS end_page_promo_view_flag,
    MAX(end_page_promo_click_flag)           AS end_page_promo_click_flag,
    MAX(campaign_eligible_flag)              AS campaign_eligible_flag,

    MAX(push_system_level_eligible)          AS push_system_level_eligible,
    MAX(push_doordash_offers_eligible)       AS push_doordash_offers_eligible,
    MAX(push_order_updates_eligible)         AS push_order_updates_eligible,
    MAX(push_product_updates_news_eligible)  AS push_product_updates_news_eligible,
    MAX(push_recommendations_eligible)       AS push_recommendations_eligible,
    MAX(push_reminders_eligible)             AS push_reminders_eligible,
    MAX(push_store_offers_eligible)          AS push_store_offers_eligible,

    MAX(email_marketing_channel_eligible)       AS email_marketing_channel_eligible,
    MAX(email_customer_research_eligible)       AS email_customer_research_eligible,
    MAX(email_news_updates_eligible)            AS email_news_updates_eligible,
    MAX(email_notifications_reminders_eligible) AS email_notifications_reminders_eligible,
    MAX(email_recommendations_eligible)         AS email_recommendations_eligible,
    MAX(email_special_offers_eligible)          AS email_special_offers_eligible,
    MAX(email_store_promotions_eligible)        AS email_store_promotions_eligible,

    MAX(sms_order_updates_eligible)          AS sms_order_updates_eligible,
    MAX(sms_marketing_eligible)              AS sms_marketing_eligible
  FROM normalized
  GROUP BY ALL
),
agg AS (
  SELECT
    day,
    dd_platform,
    onboarding_type,
    COUNT(DISTINCT consumer_id) AS unique_consumers,

    SUM(push_system_level_eligible)          AS push_system_level_eligible_users,
    SUM(push_doordash_offers_eligible)       AS push_doordash_offers_eligible_users,
    SUM(push_order_updates_eligible)         AS push_order_updates_eligible_users,
    SUM(push_product_updates_news_eligible)  AS push_product_updates_news_eligible_users,
    SUM(push_recommendations_eligible)       AS push_recommendations_eligible_users,
    SUM(push_reminders_eligible)             AS push_reminders_eligible_users,
    SUM(push_store_offers_eligible)          AS push_store_offers_eligible_users,

    SUM(email_marketing_channel_eligible)       AS email_marketing_channel_eligible_users,
    SUM(email_customer_research_eligible)       AS email_customer_research_eligible_users,
    SUM(email_news_updates_eligible)            AS email_news_updates_eligible_users,
    SUM(email_notifications_reminders_eligible) AS email_notifications_reminders_eligible_users,
    SUM(email_recommendations_eligible)         AS email_recommendations_eligible_users,
    SUM(email_special_offers_eligible)          AS email_special_offers_eligible_users,
    SUM(email_store_promotions_eligible)        AS email_store_promotions_eligible_users,

    SUM(sms_order_updates_eligible)          AS sms_order_updates_eligible_users,
    SUM(sms_marketing_eligible)              AS sms_marketing_eligible_users
  FROM dedup
  GROUP BY ALL
)
SELECT
  a.day,
  a.dd_platform||'_'||a.onboarding_type AS dd_platform_onboarding_type,
  a.unique_consumers                    AS top_of_funnel,
  metric_name,
  eligible_users
FROM agg a
UNPIVOT(
  eligible_users FOR metric_name IN (
    push_system_level_eligible_users,
    push_doordash_offers_eligible_users,
    push_order_updates_eligible_users,
    push_product_updates_news_eligible_users,
    push_recommendations_eligible_users,
    push_reminders_eligible_users,
    push_store_offers_eligible_users,
    email_marketing_channel_eligible_users,
    email_customer_research_eligible_users,
    email_news_updates_eligible_users,
    email_notifications_reminders_eligible_users,
    email_recommendations_eligible_users,
    email_special_offers_eligible_users,
    email_store_promotions_eligible_users,
    sms_order_updates_eligible_users,
    sms_marketing_eligible_users
  )
)
ORDER BY day, dd_platform_onboarding_type, metric_name
;


select day, count(1) 
from proddb.fionafan.onboarding_optins_eligibility_unpivot
group by all
order by 1;

grant all privileges on proddb.fionafan.onboarding_optins_eligibility_unpivot to public;

WITH normalized AS (
  SELECT
    day,
    LOWER(dd_platform) AS dd_platform,
    LOWER(onboarding_type) AS onboarding_type,
    consumer_id,

    -- existing event flags already 0/1; coalesce for safety
    COALESCE(start_page_view_flag, 0)                    AS start_page_view_flag,
    COALESCE(start_page_click_flag, 0)                   AS start_page_click_flag,
    COALESCE(notification_view_flag, 0)                  AS notification_view_flag,
    COALESCE(notification_click_flag, 0)                 AS notification_click_flag,
    COALESCE(marketing_sms_view_flag, 0)                 AS marketing_sms_view_flag,
    COALESCE(marketing_sms_click_flag, 0)                AS marketing_sms_click_flag,
    COALESCE(preference_view_flag, 0)                    AS preference_view_flag,
    COALESCE(preference_click_flag, 0)                   AS preference_click_flag,
    COALESCE(att_view_flag, 0)                           AS att_view_flag,
    COALESCE(att_click_flag, 0)                          AS att_click_flag,
    COALESCE(end_page_view_flag, 0)                      AS end_page_view_flag,
    COALESCE(end_page_click_flag, 0)                     AS end_page_click_flag,
    COALESCE(end_page_promo_view_flag, 0)                AS end_page_promo_view_flag,
    COALESCE(end_page_promo_click_flag, 0)               AS end_page_promo_click_flag,
    COALESCE(campaign_eligible_flag, 0)                  AS campaign_eligible_flag,

    -- eligibility columns mapped to 1/0
    IFF(LOWER(push_system_level_status) = 'on', 1, 0)            AS push_system_level_eligible,
    IFF(LOWER(push_doordash_offers_status) = 'on', 1, 0)         AS push_doordash_offers_eligible,
    IFF(LOWER(push_order_updates_status) = 'on', 1, 0)           AS push_order_updates_eligible,
    IFF(LOWER(push_product_updates_news_status) = 'on', 1, 0)    AS push_product_updates_news_eligible,
    IFF(LOWER(push_recommendations_status) = 'on', 1, 0)         AS push_recommendations_eligible,
    IFF(LOWER(push_reminders_status) = 'on', 1, 0)               AS push_reminders_eligible,
    IFF(LOWER(push_store_offers_status) = 'on', 1, 0)            AS push_store_offers_eligible,

    IFF(LOWER(email_marketing_channel_status) = 'subscribed', 1, 0)       AS email_marketing_channel_eligible,
    IFF(LOWER(email_customer_research_status) = 'subscribed', 1, 0)       AS email_customer_research_eligible,
    IFF(LOWER(email_news_updates_status) = 'subscribed', 1, 0)            AS email_news_updates_eligible,
    IFF(LOWER(email_notifications_reminders_status) = 'subscribed', 1, 0) AS email_notifications_reminders_eligible,
    IFF(LOWER(email_recommendations_status) = 'subscribed', 1, 0)         AS email_recommendations_eligible,
    IFF(LOWER(email_special_offers_status) = 'subscribed', 1, 0)          AS email_special_offers_eligible,
    IFF(LOWER(email_store_promotions_status) = 'subscribed', 1, 0)        AS email_store_promotions_eligible,

    IFF(LOWER(sms_order_updates_status) = 'on', 1, 0)     AS sms_order_updates_eligible,
    IFF(LOWER(sms_marketing_status) = 'on', 1, 0)         AS sms_marketing_eligible
  FROM proddb.public.onboarding_funnel_flags_w_optins_curr

),
-- optional: dedup to one row per consumer per day/platform/type to avoid double-counting
dedup AS (
  SELECT
    day, dd_platform, onboarding_type, consumer_id,
    MAX(start_page_view_flag)                AS start_page_view_flag,
    MAX(start_page_click_flag)               AS start_page_click_flag,
    MAX(notification_view_flag)              AS notification_view_flag,
    MAX(notification_click_flag)             AS notification_click_flag,
    MAX(marketing_sms_view_flag)             AS marketing_sms_view_flag,
    MAX(marketing_sms_click_flag)            AS marketing_sms_click_flag,
    MAX(preference_view_flag)                AS preference_view_flag,
    MAX(preference_click_flag)               AS preference_click_flag,
    MAX(att_view_flag)                       AS att_view_flag,
    MAX(att_click_flag)                      AS att_click_flag,
    MAX(end_page_view_flag)                  AS end_page_view_flag,
    MAX(end_page_click_flag)                 AS end_page_click_flag,
    MAX(end_page_promo_view_flag)            AS end_page_promo_view_flag,
    MAX(end_page_promo_click_flag)           AS end_page_promo_click_flag,
    MAX(campaign_eligible_flag)              AS campaign_eligible_flag,

    MAX(push_system_level_eligible)          AS push_system_level_eligible,
    MAX(push_doordash_offers_eligible)       AS push_doordash_offers_eligible,
    MAX(push_order_updates_eligible)         AS push_order_updates_eligible,
    MAX(push_product_updates_news_eligible)  AS push_product_updates_news_eligible,
    MAX(push_recommendations_eligible)       AS push_recommendations_eligible,
    MAX(push_reminders_eligible)             AS push_reminders_eligible,
    MAX(push_store_offers_eligible)          AS push_store_offers_eligible,

    MAX(email_marketing_channel_eligible)       AS email_marketing_channel_eligible,
    MAX(email_customer_research_eligible)       AS email_customer_research_eligible,
    MAX(email_news_updates_eligible)            AS email_news_updates_eligible,
    MAX(email_notifications_reminders_eligible) AS email_notifications_reminders_eligible,
    MAX(email_recommendations_eligible)         AS email_recommendations_eligible,
    MAX(email_special_offers_eligible)          AS email_special_offers_eligible,
    MAX(email_store_promotions_eligible)        AS email_store_promotions_eligible,

    MAX(sms_order_updates_eligible)          AS sms_order_updates_eligible,
    MAX(sms_marketing_eligible)              AS sms_marketing_eligible
  FROM normalized
  GROUP BY day, dd_platform, onboarding_type, consumer_id
)
SELECT
  day,
  dd_platform,
  onboarding_type,

  COUNT(DISTINCT consumer_id)                                  AS unique_consumers,

  -- event flags as counts of consumers with flag = 1
  SUM(start_page_view_flag)                AS start_page_view_users,
  SUM(start_page_click_flag)               AS start_page_click_users,
  SUM(notification_view_flag)              AS notification_view_users,
  SUM(notification_click_flag)             AS notification_click_users,
  SUM(marketing_sms_view_flag)             AS marketing_sms_view_users,
  SUM(marketing_sms_click_flag)            AS marketing_sms_click_users,
  SUM(preference_view_flag)                AS preference_view_users,
  SUM(preference_click_flag)               AS preference_click_users,
  SUM(att_view_flag)                       AS att_view_users,
  SUM(att_click_flag)                      AS att_click_users,
  SUM(end_page_view_flag)                  AS end_page_view_users,
  SUM(end_page_click_flag)                 AS end_page_click_users,
  SUM(end_page_promo_view_flag)            AS end_page_promo_view_users,
  SUM(end_page_promo_click_flag)           AS end_page_promo_click_users,
  SUM(campaign_eligible_flag)              AS campaign_eligible_users,

  -- eligibility columns converted to 1/0 and aggregated as counts of eligible users
  SUM(push_system_level_eligible)          AS push_system_level_eligible_users,
  SUM(push_doordash_offers_eligible)       AS push_doordash_offers_eligible_users,
  SUM(push_order_updates_eligible)         AS push_order_updates_eligible_users,
  SUM(push_product_updates_news_eligible)  AS push_product_updates_news_eligible_users,
  SUM(push_recommendations_eligible)       AS push_recommendations_eligible_users,
  SUM(push_reminders_eligible)             AS push_reminders_eligible_users,
  SUM(push_store_offers_eligible)          AS push_store_offers_eligible_users,

  SUM(email_marketing_channel_eligible)       AS email_marketing_channel_eligible_users,
  SUM(email_customer_research_eligible)       AS email_customer_research_eligible_users,
  SUM(email_news_updates_eligible)            AS email_news_updates_eligible_users,
  SUM(email_notifications_reminders_eligible) AS email_notifications_reminders_eligible_users,
  SUM(email_recommendations_eligible)         AS email_recommendations_eligible_users,
  SUM(email_special_offers_eligible)          AS email_special_offers_eligible_users,
  SUM(email_store_promotions_eligible)        AS email_store_promotions_eligible_users,

  SUM(sms_order_updates_eligible)          AS sms_order_updates_eligible_users,
  SUM(sms_marketing_eligible)              AS sms_marketing_eligible_users
FROM dedup
GROUP BY day, dd_platform, onboarding_type
ORDER BY day, dd_platform, onboarding_type;