-- Time series union query for onboarding funnel steps
WITH start_date AS (SELECT '2025-08-04' as start_dt),
     end_date AS (SELECT '2025-09-30' as end_dt),

onboarding_funnel_timeseries AS (
  -- Start Page View
  SELECT 
    'start_page_view' as onboarding_step,
    'start_page' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice


  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Start Page Click
  SELECT 
    'start_page_click' as onboarding_step,
    'start_page' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    promo_title,
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Welcome Back View
  SELECT 
    'welcomeback_view' as onboarding_step,
    'welcomeback' as onboarding_page,
    'view' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'welcomeback'
  GROUP BY 1,2,3,4,5,6,7

  UNION ALL

  -- Welcome Back Click
  SELECT 
    'welcomeback_click' as onboarding_step,
    'welcomeback' as onboarding_page,
    'click' as onboarding_action,
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    null as promo_title,
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND lower(page) = 'welcomeback'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice

  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'notification'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'notification'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'marketingSMS'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'marketingSMS'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page ilike '%preference%'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page ilike '%preference%'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'att'
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
    AND page = 'att'
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
    onboarding_type,
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
    onboarding_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-')) as unique_devices,
    COUNT(DISTINCT consumer_id) as unique_consumers
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM start_date) AND (SELECT end_dt FROM end_date)
  GROUP BY 1,2,3,4,5,6,7
)

-- Final output - aggregated by day/step/platform/promo/type
SELECT 
  onboarding_step,
  onboarding_page,
  onboarding_action,
  day,
  dd_platform,
  promo_title,
  onboarding_type,
  total_events,
  unique_devices,
  unique_consumers
FROM onboarding_funnel_timeseries
ORDER BY day, onboarding_step, dd_platform, promo_title, onboarding_type;
