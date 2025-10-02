WITH events AS (
  -- Start Page View (new_user or android resurrected)
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    promo_title,
    'start_page' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND (
      lower(onboarding_type) = 'new_user'
      OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user')
    )

  UNION ALL

  -- Start Page View (ios resurrected welcomeback)
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'start_page' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'

  UNION ALL

  -- Start Page Clicks (new_user or android resurrected)
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    promo_title,
    'start_page' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND (
      lower(onboarding_type) = 'new_user'
      OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user')
    )

  UNION ALL

  -- Start Page Click (ios resurrected welcomeback)
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'start_page' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'
    AND click_type = 'primary'

  UNION ALL

  -- Notification View/Click
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'notification' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'notification'

  UNION ALL

  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'notification' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'notification'

  UNION ALL

  -- Marketing SMS View/Click
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'marketing_sms' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'marketingsms'

  UNION ALL

  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'marketing_sms' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'marketingsms'

  UNION ALL

  -- Preference View/Click
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'preference' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) like '%preference%'

  UNION ALL

  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'preference' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) like '%preference%'

  UNION ALL

  -- ATT View/Click
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'att' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'att'

  UNION ALL

  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    NULL AS promo_title,
    'att' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
    AND lower(page) = 'att'

  UNION ALL

  -- End Page View/Click
  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    promo_title,
    'end_page' AS onboarding_page,
    'view' AS onboarding_action
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE

  UNION ALL

  SELECT DISTINCT
    iguazu_timestamp AS event_ts,
    consumer_id,
    DD_DEVICE_ID AS dd_device_id,
    lower(dd_platform) AS dd_platform,
    lower(onboarding_type) AS onboarding_type,
    promo_title,
    'end_page' AS onboarding_page,
    'click' AS onboarding_action
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp::DATE BETWEEN {{start_date}}::DATE AND {{end_date}}::DATE
)
SELECT
  e.consumer_id,
  e.dd_device_id as device_id,
  e.event_ts,
  e.dd_platform,
  e.onboarding_type,
  case when POSITION('%' IN e.promo_title) > 0 then 'shows promo' else 'no promo' end as promo_flag,
  -- Onboarding Page markers - views/clicks (device_id)
  MAX(CASE WHEN e.onboarding_page = 'start_page' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_start_page_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'start_page' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_start_page_click_device_id,
  MAX(CASE WHEN e.onboarding_page = 'notification' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_notification_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'notification' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_notification_click_device_id,
  MAX(CASE WHEN e.onboarding_page = 'marketing_sms' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_marketing_sms_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'marketing_sms' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_marketing_sms_click_device_id,
  MAX(CASE WHEN e.onboarding_page = 'preference' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_preference_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'preference' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_preference_click_device_id,
  MAX(CASE WHEN e.onboarding_page = 'att' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_att_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'att' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_att_click_device_id,
  MAX(CASE WHEN e.onboarding_page = 'end_page' AND e.onboarding_action = 'view' THEN e.dd_device_id ELSE NULL END) AS page_end_page_view_device_id,
  MAX(CASE WHEN e.onboarding_page = 'end_page' AND e.onboarding_action = 'click' THEN e.dd_device_id ELSE NULL END) AS page_end_page_click_device_id,

  -- Onboarding Page markers - views/clicks (consumer_id)
  MAX(CASE WHEN e.onboarding_page = 'start_page' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_start_page_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'start_page' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_start_page_click_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'notification' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_notification_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'notification' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_notification_click_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'marketing_sms' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_marketing_sms_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'marketing_sms' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_marketing_sms_click_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'preference' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_preference_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'preference' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_preference_click_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'att' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_att_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'att' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_att_click_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'end_page' AND e.onboarding_action = 'view' THEN e.consumer_id ELSE NULL END) AS page_end_page_view_consumer_id,
  MAX(CASE WHEN e.onboarding_page = 'end_page' AND e.onboarding_action = 'click' THEN e.consumer_id ELSE NULL END) AS page_end_page_click_consumer_id,


  -- Platform markers (device_id)
  MAX(CASE WHEN e.dd_platform = 'ios' THEN e.dd_device_id ELSE NULL END) AS platform_ios_device_id,
  MAX(CASE WHEN e.dd_platform = 'android' THEN e.dd_device_id ELSE NULL END) AS platform_android_device_id,

  -- Platform markers (consumer_id)
  MAX(CASE WHEN e.dd_platform = 'ios' THEN e.consumer_id ELSE NULL END) AS platform_ios_consumer_id,
  MAX(CASE WHEN e.dd_platform = 'android' THEN e.consumer_id ELSE NULL END) AS platform_android_consumer_id,

  

  -- Promo markers (device_id)
  MAX(CASE WHEN POSITION('%' IN e.promo_title) > 0 THEN e.dd_device_id ELSE NULL END) AS promo_present_device_id,
  MAX(CASE WHEN (e.promo_title IS NULL OR POSITION('%' IN e.promo_title) = 0) THEN e.dd_device_id ELSE NULL END) AS promo_absent_device_id,

  -- Promo markers (consumer_id)
  MAX(CASE WHEN POSITION('%' IN e.promo_title) > 0 THEN e.consumer_id ELSE NULL END) AS promo_present_consumer_id,
  MAX(CASE WHEN (e.promo_title IS NULL OR POSITION('%' IN e.promo_title) = 0) THEN e.consumer_id ELSE NULL END) AS promo_absent_consumer_id

FROM events e
GROUP BY ALL