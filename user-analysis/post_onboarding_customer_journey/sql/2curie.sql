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
, final AS (
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

)
select * from final;
-- SELECT
--   COUNT(*) AS rows_total,
--   COUNT(DISTINCT consumer_id) AS consumers_total,
--   COUNT(DISTINCT dd_device_id) AS devices_total,
--   COUNT(DISTINCT event_ts) AS event_ts_total,

--   -- Counts for page markers (device_id)
--   COUNT(DISTINCT page_start_page_view_device_id) AS cnt_page_start_page_view_device_id,
--   COUNT(DISTINCT page_start_page_click_device_id) AS cnt_page_start_page_click_device_id,
--   COUNT(DISTINCT page_notification_view_device_id) AS cnt_page_notification_view_device_id,
--   COUNT(DISTINCT page_notification_click_device_id) AS cnt_page_notification_click_device_id,
--   COUNT(DISTINCT page_marketing_sms_view_device_id) AS cnt_page_marketing_sms_view_device_id,
--   COUNT(DISTINCT page_marketing_sms_click_device_id) AS cnt_page_marketing_sms_click_device_id,
--   COUNT(DISTINCT page_preference_view_device_id) AS cnt_page_preference_view_device_id,
--   COUNT(DISTINCT page_preference_click_device_id) AS cnt_page_preference_click_device_id,
--   COUNT(DISTINCT page_att_view_device_id) AS cnt_page_att_view_device_id,
--   COUNT(DISTINCT page_att_click_device_id) AS cnt_page_att_click_device_id,
--   COUNT(DISTINCT page_end_page_view_device_id) AS cnt_page_end_page_view_device_id,
--   COUNT(DISTINCT page_end_page_click_device_id) AS cnt_page_end_page_click_device_id,

--   -- Counts for page markers (consumer_id)
--   COUNT(DISTINCT page_start_page_view_consumer_id) AS cnt_page_start_page_view_consumer_id,
--   COUNT(DISTINCT page_start_page_click_consumer_id) AS cnt_page_start_page_click_consumer_id,
--   COUNT(DISTINCT page_notification_view_consumer_id) AS cnt_page_notification_view_consumer_id,
--   COUNT(DISTINCT page_notification_click_consumer_id) AS cnt_page_notification_click_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_view_consumer_id) AS cnt_page_marketing_sms_view_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_click_consumer_id) AS cnt_page_marketing_sms_click_consumer_id,
--   COUNT(DISTINCT page_preference_view_consumer_id) AS cnt_page_preference_view_consumer_id,
--   COUNT(DISTINCT page_preference_click_consumer_id) AS cnt_page_preference_click_consumer_id,
--   COUNT(DISTINCT page_att_view_consumer_id) AS cnt_page_att_view_consumer_id,
--   COUNT(DISTINCT page_att_click_consumer_id) AS cnt_page_att_click_consumer_id,
--   COUNT(DISTINCT page_end_page_view_consumer_id) AS cnt_page_end_page_view_consumer_id,
--   COUNT(DISTINCT page_end_page_click_consumer_id) AS cnt_page_end_page_click_consumer_id,

--   -- Counts by onboarding_type for page markers (device_id)
--   COUNT(DISTINCT page_start_page_view_new_user_device_id) AS cnt_page_start_page_view_new_user_device_id,
--   COUNT(DISTINCT page_start_page_view_resurrected_user_device_id) AS cnt_page_start_page_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_start_page_click_new_user_device_id) AS cnt_page_start_page_click_new_user_device_id,
--   COUNT(DISTINCT page_start_page_click_resurrected_user_device_id) AS cnt_page_start_page_click_resurrected_user_device_id,
--   COUNT(DISTINCT page_notification_view_new_user_device_id) AS cnt_page_notification_view_new_user_device_id,
--   COUNT(DISTINCT page_notification_view_resurrected_user_device_id) AS cnt_page_notification_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_notification_click_new_user_device_id) AS cnt_page_notification_click_new_user_device_id,
--   COUNT(DISTINCT page_notification_click_resurrected_user_device_id) AS cnt_page_notification_click_resurrected_user_device_id,
--   COUNT(DISTINCT page_marketing_sms_view_new_user_device_id) AS cnt_page_marketing_sms_view_new_user_device_id,
--   COUNT(DISTINCT page_marketing_sms_view_resurrected_user_device_id) AS cnt_page_marketing_sms_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_marketing_sms_click_new_user_device_id) AS cnt_page_marketing_sms_click_new_user_device_id,
--   COUNT(DISTINCT page_marketing_sms_click_resurrected_user_device_id) AS cnt_page_marketing_sms_click_resurrected_user_device_id,
--   COUNT(DISTINCT page_preference_view_new_user_device_id) AS cnt_page_preference_view_new_user_device_id,
--   COUNT(DISTINCT page_preference_view_resurrected_user_device_id) AS cnt_page_preference_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_preference_click_new_user_device_id) AS cnt_page_preference_click_new_user_device_id,
--   COUNT(DISTINCT page_preference_click_resurrected_user_device_id) AS cnt_page_preference_click_resurrected_user_device_id,
--   COUNT(DISTINCT page_att_view_new_user_device_id) AS cnt_page_att_view_new_user_device_id,
--   COUNT(DISTINCT page_att_view_resurrected_user_device_id) AS cnt_page_att_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_att_click_new_user_device_id) AS cnt_page_att_click_new_user_device_id,
--   COUNT(DISTINCT page_att_click_resurrected_user_device_id) AS cnt_page_att_click_resurrected_user_device_id,
--   COUNT(DISTINCT page_end_page_view_new_user_device_id) AS cnt_page_end_page_view_new_user_device_id,
--   COUNT(DISTINCT page_end_page_view_resurrected_user_device_id) AS cnt_page_end_page_view_resurrected_user_device_id,
--   COUNT(DISTINCT page_end_page_click_new_user_device_id) AS cnt_page_end_page_click_new_user_device_id,
--   COUNT(DISTINCT page_end_page_click_resurrected_user_device_id) AS cnt_page_end_page_click_resurrected_user_device_id,

--   -- Counts by onboarding_type for page markers (consumer_id)
--   COUNT(DISTINCT page_start_page_view_new_user_consumer_id) AS cnt_page_start_page_view_new_user_consumer_id,
--   COUNT(DISTINCT page_start_page_view_resurrected_user_consumer_id) AS cnt_page_start_page_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_start_page_click_new_user_consumer_id) AS cnt_page_start_page_click_new_user_consumer_id,
--   COUNT(DISTINCT page_start_page_click_resurrected_user_consumer_id) AS cnt_page_start_page_click_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_notification_view_new_user_consumer_id) AS cnt_page_notification_view_new_user_consumer_id,
--   COUNT(DISTINCT page_notification_view_resurrected_user_consumer_id) AS cnt_page_notification_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_notification_click_new_user_consumer_id) AS cnt_page_notification_click_new_user_consumer_id,
--   COUNT(DISTINCT page_notification_click_resurrected_user_consumer_id) AS cnt_page_notification_click_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_view_new_user_consumer_id) AS cnt_page_marketing_sms_view_new_user_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_view_resurrected_user_consumer_id) AS cnt_page_marketing_sms_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_click_new_user_consumer_id) AS cnt_page_marketing_sms_click_new_user_consumer_id,
--   COUNT(DISTINCT page_marketing_sms_click_resurrected_user_consumer_id) AS cnt_page_marketing_sms_click_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_preference_view_new_user_consumer_id) AS cnt_page_preference_view_new_user_consumer_id,
--   COUNT(DISTINCT page_preference_view_resurrected_user_consumer_id) AS cnt_page_preference_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_preference_click_new_user_consumer_id) AS cnt_page_preference_click_new_user_consumer_id,
--   COUNT(DISTINCT page_preference_click_resurrected_user_consumer_id) AS cnt_page_preference_click_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_att_view_new_user_consumer_id) AS cnt_page_att_view_new_user_consumer_id,
--   COUNT(DISTINCT page_att_view_resurrected_user_consumer_id) AS cnt_page_att_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_att_click_new_user_consumer_id) AS cnt_page_att_click_new_user_consumer_id,
--   COUNT(DISTINCT page_att_click_resurrected_user_consumer_id) AS cnt_page_att_click_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_end_page_view_new_user_consumer_id) AS cnt_page_end_page_view_new_user_consumer_id,
--   COUNT(DISTINCT page_end_page_view_resurrected_user_consumer_id) AS cnt_page_end_page_view_resurrected_user_consumer_id,
--   COUNT(DISTINCT page_end_page_click_new_user_consumer_id) AS cnt_page_end_page_click_new_user_consumer_id,
--   COUNT(DISTINCT page_end_page_click_resurrected_user_consumer_id) AS cnt_page_end_page_click_resurrected_user_consumer_id,

--   -- Platform counts
--   COUNT(DISTINCT platform_ios_device_id) AS cnt_platform_ios_device_id,
--   COUNT(DISTINCT platform_android_device_id) AS cnt_platform_android_device_id,
--   COUNT(DISTINCT platform_ios_consumer_id) AS cnt_platform_ios_consumer_id,
--   COUNT(DISTINCT platform_android_consumer_id) AS cnt_platform_android_consumer_id,

--   -- Onboarding type counts
--   COUNT(DISTINCT type_new_user_device_id) AS cnt_type_new_user_device_id,
--   COUNT(DISTINCT type_resurrected_user_device_id) AS cnt_type_resurrected_user_device_id,
--   COUNT(DISTINCT type_new_user_consumer_id) AS cnt_type_new_user_consumer_id,
--   COUNT(DISTINCT type_resurrected_user_consumer_id) AS cnt_type_resurrected_user_consumer_id,

--   -- Promo counts
--   COUNT(DISTINCT promo_present_device_id) AS cnt_promo_present_device_id,
--   COUNT(DISTINCT promo_absent_device_id) AS cnt_promo_absent_device_id,
--   COUNT(DISTINCT promo_present_consumer_id) AS cnt_promo_present_consumer_id,
--   COUNT(DISTINCT promo_absent_consumer_id) AS cnt_promo_absent_consumer_id,

--   -- Promo counts by onboarding_type
--   COUNT(DISTINCT promo_present_new_user_device_id) AS cnt_promo_present_new_user_device_id,
--   COUNT(DISTINCT promo_absent_new_user_device_id) AS cnt_promo_absent_new_user_device_id,
--   COUNT(DISTINCT promo_present_resurrected_user_device_id) AS cnt_promo_present_resurrected_user_device_id,
--   COUNT(DISTINCT promo_absent_resurrected_user_device_id) AS cnt_promo_absent_resurrected_user_device_id,
--   COUNT(DISTINCT promo_present_new_user_consumer_id) AS cnt_promo_present_new_user_consumer_id,
--   COUNT(DISTINCT promo_absent_new_user_consumer_id) AS cnt_promo_absent_new_user_consumer_id,
--   COUNT(DISTINCT promo_present_resurrected_user_consumer_id) AS cnt_promo_present_resurrected_user_consumer_id,
--   COUNT(DISTINCT promo_absent_resurrected_user_consumer_id) AS cnt_promo_absent_resurrected_user_consumer_id

-- FROM final;

;

-- iguazu.consumer.m_onboarding_start_promo_page_view_ice,iguazu.consumer.M_onboarding_page_view_ice,iguazu.consumer.m_onboarding_start_promo_page_click_ice,iguazu.consumer.M_onboarding_page_click_ice,iguazu.consumer.m_onboarding_end_promo_page_view_ice,iguazu.consumer.m_onboarding_end_promo_page_click_ice


select * from proddb.static.action_dimension;


create or replace table proddb.static.dd_platform_dimension as (
select 'ios' as dd_platform, 'ios' as dim_platform
union
select 'android' as dd_platform, 'android' as dim_platform
);

create or replace table proddb.static.onboarding_type_dimension as (

select 'new_user' as onboarding_type, 'new_user' as dim_onboarding_type
union
select 'resurrected_user' as onboarding_type, 'resurrected_user' as dim_onboarding_type

);


create or replace table proddb.static.promo_type_dimension as (

select 'shows promo' as promo_type, 'shows promo' as dim_promo_type
union
select 'no promo' as promo_type, 'no promo' as dim_promo_type

);

select * from metrics_repo.public.onboarding_flow_view_click limit 10;