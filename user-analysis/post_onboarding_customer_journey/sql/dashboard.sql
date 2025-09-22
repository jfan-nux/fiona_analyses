CREATE OR REPLACE TEMP TABLE proddb.public.onboarding_start_funnel AS
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

CREATE OR REPLACE TEMP TABLE proddb.public.onboarding_funnel_flags AS
WITH p AS (
  SELECT * FROM proddb.public.onboarding_start_funnel
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
WITH start_date AS (SELECT current_date -14 as start_dt),
     end_date AS (SELECT current_date as end_dt),

onboarding_funnel_timeseries AS (
  -- Start Page View (new_user)
  SELECT 
    'start_page_view' as onboarding_step,
    'start_page' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND ((lower(onboarding_type) = 'new_user') or (lower(dd_platform) = 'android' and lower(onboarding_type) = 'resurrected_user'))
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Start Page Click (new_user)
  SELECT 
    'start_page_click' as onboarding_step,
    'start_page' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND ((lower(onboarding_type) = 'new_user') or (lower(dd_platform) = 'android' and lower(onboarding_type) = 'resurrected_user'))
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Welcome Back View (resurrected_user)
  SELECT 
    'start_page_view' as onboarding_step,
    'start_page' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Welcome Back Click (resurrected_user)
  SELECT 
    'start_page_click' as onboarding_step,
    'start_page' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'
    AND click_type = 'primary'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Notification View
  SELECT 
    'notification_view' as onboarding_step,
    'notification' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
 
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'notification'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Notification Click
  SELECT 
    'notification_click' as onboarding_step,
    'notification' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'notification'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Marketing SMS View
  SELECT 
    'marketing_sms_view' as onboarding_step,
    'marketing_sms' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Marketing SMS Click
  SELECT 
    'marketing_sms_click' as onboarding_step,
    'marketing_sms' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Preference View
  SELECT 
    'preference_view' as onboarding_step,
    'preference' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) like '%preference%'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Preference Click
  SELECT 
    'preference_click' as onboarding_step,
    'preference' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) like '%preference%'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- ATT View
  SELECT 
    'att_view' as onboarding_step,
    'att' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'att'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- ATT Click
  SELECT 
    'att_click' as onboarding_step,
    'att' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'att'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page View
  SELECT 
    'end_page_view' as onboarding_step,
    'end_page' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page Click
  SELECT 
    'end_page_click' as onboarding_step,
    'end_page' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  GROUP BY 1,2,3,4,5,6,7
  
  union all
  
  -- End Page promo View
  SELECT 
    'end_page_promo_view' as onboarding_step,
    'end_page_promo' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  and POSITION('%' IN promo_title)>0
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- End Page promo Click
  SELECT 
    'end_page_promo_click' as onboarding_step,
    'end_page_promo' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    lower(onboarding_type) as onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  and POSITION('%' IN promo_title)>0
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Campaign Eligible from start of funnel
  SELECT 
    'campaign_eligible' as onboarding_step,
    'campaign_eligible' as onboarding_page,
    'view' as onboarding_action,
    p.day AS day,
    p.dd_platform,
    CAST(e.content_id AS VARCHAR) as promo_title,
    lower(p.onboarding_type) as onboarding_type,
    COUNT(DISTINCT p.consumer_id) as total_events,
    COUNT(DISTINCT p.consumer_id) as unique_devices,
    COUNT(DISTINCT p.consumer_id) as unique_consumers
  FROM proddb.fionafan.onboarding_start_funnel p
  LEFT JOIN IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS e
    ON e.consumer_id = p.consumer_id
    AND e.placement_type = 'PLACEMENT_TYPE_POST_ONBOARDING'
    AND e.is_eligible = TRUE
    AND cast(e.iguazu_sent_at as date) = p.day
    AND e.iguazu_sent_at BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  WHERE p.day BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND e.consumer_id IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7

),
pre_result as (
select *,  CASE 
  WHEN ONBOARDING_PAGE = 'start_page' THEN 1
  WHEN ONBOARDING_PAGE = 'notification' THEN 2

  WHEN ONBOARDING_PAGE = 'marketing_sms' THEN 3
  WHEN ONBOARDING_PAGE = 'preference' THEN 4
    WHEN ONBOARDING_PAGE = 'att' THEN 5
  WHEN ONBOARDING_PAGE = 'end_page' THEN 6
  WHEN ONBOARDING_PAGE = 'end_page_promo' THEN 7
  WHEN ONBOARDING_PAGE = 'campaign_eligible' THEN 8
  ELSE 0
END as page_order
from onboarding_funnel_timeseries)

-- Final output - aggregated by day/step/platform/promo/type
SELECT 
  page_order||'. '||onboarding_step as onboarding_step,
  page_order||'. '||onboarding_page as onboarding_page,
  onboarding_action,
  day,
  dd_platform,
  promo_title,
  onboarding_type,
  total_events,
  unique_devices,
  unique_consumers,

  
FROM pre_result
ORDER BY day, onboarding_step, dd_platform, promo_title, onboarding_type
;

select *  from IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS where 
placement_type = 'PLACEMENT_TYPE_POST_ONBOARDING'
and
iguazu_sent_at > current_date - 2
limit 10
;

-- ==========================================
-- Funnel rebuilt using proddb.fionafan.onboarding_funnel_flags
-- ==========================================
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

SELECT 
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


GRANT SELECT ON TABLE proddb.public.onboarding_funnel_flags TO ROLE PUBLIC;

-- create table proddb.public.onboarding_funnel_flags  as (select * from proddb.public.onboarding_funnel_flags);
select * from proddb.public.onboarding_funnel_flags where limit 10;


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

select * from pre_result_flags;