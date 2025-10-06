WITH date_spine AS (
  SELECT DATEADD('day', SEQ4(), TO_DATE({{start_date}})) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 366))
  WHERE day BETWEEN TO_DATE({{start_date}}) AND TO_DATE({{end_date}})
),
push_scd AS (
  SELECT DISTINCT
    d.day,
    p.consumer_id,
    CASE
  WHEN LOWER(p.device_id) LIKE 'dx\\_%' ESCAPE '\\'
    THEN p.device_id
  ELSE CONCAT('dx_', p.device_id)
END as device_id,
    p.scd_start_date,
    p.scd_end_date,
    p.system_level_status,
    p.doordash_offers_status,
    p.order_updates_status,
    p.product_updates_news_status,
    p.recommendations_status,
    p.reminders_status,
    p.store_offers_status
  FROM edw.consumer.dimension_consumer_device_push_settings_scd3 p
  JOIN date_spine d
    ON d.day BETWEEN p.scd_start_date AND COALESCE(p.scd_end_date, '9999-12-31')
  where experience = 'doordash' and scd_current_record

),
email_scd AS (
  SELECT DISTINCT
    d.day,
    e.consumer_id,
    e.scd_start_date,
    e.scd_end_date,
    e.marketing_channel_status,
    e.customer_research_status,
    e.news_updates_status,
    e.notifications_reminders_status,
    e.recommendations_status,
    e.special_offers_status,
    e.store_promotions_status
  FROM edw.consumer.dimension_consumer_email_settings_scd3 e
  JOIN date_spine d
    ON d.day BETWEEN e.scd_start_date AND COALESCE(e.scd_end_date, '9999-12-31')
  where experience = 'doordash' and scd_current_record
),
sms_scd AS (
  SELECT DISTINCT
    d.day,
    s.consumer_id,
    s.scd_start_date,
    s.scd_end_date,
    s.order_updates_status,
    s.marketing_status
  FROM edw.consumer.dimension_consumer_sms_settings_scd3 s
  JOIN date_spine d
    ON d.day BETWEEN s.scd_start_date AND COALESCE(s.scd_end_date, '9999-12-31')
  where experience = 'doordash' and scd_current_record
),
consumer_days_raw AS (
  SELECT day, consumer_id FROM push_scd
  UNION ALL
  SELECT day, consumer_id FROM email_scd
  UNION ALL
  SELECT day, consumer_id FROM sms_scd
),
consumer_days AS (
  SELECT DISTINCT day, consumer_id FROM consumer_days_raw
),
final AS (
  SELECT
    cd.day,
    cd.consumer_id,
    ps.device_id,
    -- Push statuses (as-of day)
    ps.system_level_status            AS push_system_level_status,
    ps.doordash_offers_status         AS push_doordash_offers_status,
    ps.order_updates_status           AS push_order_updates_status,
    ps.product_updates_news_status    AS push_product_updates_news_status,
    ps.recommendations_status         AS push_recommendations_status,
    ps.reminders_status               AS push_reminders_status,
    ps.store_offers_status            AS push_store_offers_status,
    -- Email statuses (as-of day)
    es.marketing_channel_status       AS email_marketing_channel_status,
    es.customer_research_status       AS email_customer_research_status,
    es.news_updates_status            AS email_news_updates_status,
    es.notifications_reminders_status AS email_notifications_reminders_status,
    es.recommendations_status         AS email_recommendations_status,
    es.special_offers_status          AS email_special_offers_status,
    es.store_promotions_status        AS email_store_promotions_status,
    -- SMS statuses (as-of day)
    ss.order_updates_status           AS sms_order_updates_status,
    ss.marketing_status               AS sms_marketing_status
  FROM consumer_days cd
  LEFT JOIN push_scd ps
    ON ps.consumer_id = cd.consumer_id
   AND ps.day = cd.day
  LEFT JOIN email_scd es
    ON es.consumer_id = cd.consumer_id
   AND es.day = cd.day
  LEFT JOIN sms_scd ss
    ON ss.consumer_id = cd.consumer_id
   AND ss.day = cd.day
)
SELECT
  f.day as event_ts,
  f.device_id,
  f.consumer_id,
  -- raw channel-specific statuses
  f.push_system_level_status,
  f.push_doordash_offers_status,
  f.push_order_updates_status,
  f.push_product_updates_news_status,
  f.push_recommendations_status,
  f.push_reminders_status,
  f.push_store_offers_status,
  f.email_marketing_channel_status,
  f.email_customer_research_status,
  f.email_news_updates_status,
  f.email_notifications_reminders_status,
  f.email_recommendations_status,
  f.email_special_offers_status,
  f.email_store_promotions_status,
  f.sms_order_updates_status,
  f.sms_marketing_status,
  -- per-metric ids (device)
  CASE WHEN LOWER(f.push_system_level_status) = 'on' THEN f.device_id END AS push_system_level_device_id,
  CASE WHEN LOWER(f.push_doordash_offers_status) = 'on' THEN f.device_id END AS push_doordash_offers_device_id,
  CASE WHEN LOWER(f.push_order_updates_status) = 'on' THEN f.device_id END AS push_order_updates_device_id,
  CASE WHEN LOWER(f.push_product_updates_news_status) = 'on' THEN f.device_id END AS push_product_updates_news_device_id,
  CASE WHEN LOWER(f.push_recommendations_status) = 'on' THEN f.device_id END AS push_recommendations_device_id,
  CASE WHEN LOWER(f.push_reminders_status) = 'on' THEN f.device_id END AS push_reminders_device_id,
  CASE WHEN LOWER(f.push_store_offers_status) = 'on' THEN f.device_id END AS push_store_offers_device_id,
  CASE WHEN LOWER(f.email_marketing_channel_status) = 'subscribed' THEN f.device_id END AS email_marketing_channel_device_id,
  CASE WHEN LOWER(f.email_customer_research_status) = 'subscribed' THEN f.device_id END AS email_customer_research_device_id,
  CASE WHEN LOWER(f.email_news_updates_status) = 'subscribed' THEN f.device_id END AS email_news_updates_device_id,
  CASE WHEN LOWER(f.email_notifications_reminders_status) = 'subscribed' THEN f.device_id END AS email_notifications_reminders_device_id,
  CASE WHEN LOWER(f.email_recommendations_status) = 'subscribed' THEN f.device_id END AS email_recommendations_device_id,
  CASE WHEN LOWER(f.email_special_offers_status) = 'subscribed' THEN f.device_id END AS email_special_offers_device_id,
  CASE WHEN LOWER(f.email_store_promotions_status) = 'subscribed' THEN f.device_id END AS email_store_promotions_device_id,
  CASE WHEN LOWER(f.sms_order_updates_status) = 'on' THEN f.device_id END AS sms_order_updates_device_id,
  CASE WHEN LOWER(f.sms_marketing_status) = 'on' THEN f.device_id END AS sms_marketing_device_id,
  -- per-metric ids (consumer)
  CASE WHEN LOWER(f.push_system_level_status) = 'on' THEN f.consumer_id END AS push_system_level_consumer_id,
  CASE WHEN LOWER(f.push_doordash_offers_status) = 'on' THEN f.consumer_id END AS push_doordash_offers_consumer_id,
  CASE WHEN LOWER(f.push_order_updates_status) = 'on' THEN f.consumer_id END AS push_order_updates_consumer_id,
  CASE WHEN LOWER(f.push_product_updates_news_status) = 'on' THEN f.consumer_id END AS push_product_updates_news_consumer_id,
  CASE WHEN LOWER(f.push_recommendations_status) = 'on' THEN f.consumer_id END AS push_recommendations_consumer_id,
  CASE WHEN LOWER(f.push_reminders_status) = 'on' THEN f.consumer_id END AS push_reminders_consumer_id,
  CASE WHEN LOWER(f.push_store_offers_status) = 'on' THEN f.consumer_id END AS push_store_offers_consumer_id,
  CASE WHEN LOWER(f.email_marketing_channel_status) = 'subscribed' THEN f.consumer_id END AS email_marketing_channel_consumer_id,
  CASE WHEN LOWER(f.email_customer_research_status) = 'subscribed' THEN f.consumer_id END AS email_customer_research_consumer_id,
  CASE WHEN LOWER(f.email_news_updates_status) = 'subscribed' THEN f.consumer_id END AS email_news_updates_consumer_id,
  CASE WHEN LOWER(f.email_notifications_reminders_status) = 'subscribed' THEN f.consumer_id END AS email_notifications_reminders_consumer_id,
  CASE WHEN LOWER(f.email_recommendations_status) = 'subscribed' THEN f.consumer_id END AS email_recommendations_consumer_id,
  CASE WHEN LOWER(f.email_special_offers_status) = 'subscribed' THEN f.consumer_id END AS email_special_offers_consumer_id,
  CASE WHEN LOWER(f.email_store_promotions_status) = 'subscribed' THEN f.consumer_id END AS email_store_promotions_consumer_id,
  CASE WHEN LOWER(f.sms_order_updates_status) = 'on' THEN f.consumer_id END AS sms_order_updates_consumer_id,
  CASE WHEN LOWER(f.sms_marketing_status) = 'on' THEN f.consumer_id END AS sms_marketing_consumer_id,
  -- overall flags per channel
  case when
    LOWER(f.push_system_level_status) = 'on'
    OR LOWER(f.push_doordash_offers_status) = 'on'
    OR LOWER(f.push_order_updates_status) = 'on'
    OR LOWER(f.push_product_updates_news_status) = 'on'
    OR LOWER(f.push_recommendations_status) = 'on'
    OR LOWER(f.push_reminders_status) = 'on'
    OR LOWER(f.push_store_offers_status) = 'on' then consumer_id
  end AS push_optin_overall,
 case when
    LOWER(f.email_marketing_channel_status) = 'subscribed'
    OR LOWER(f.email_customer_research_status) = 'subscribed'
    OR LOWER(f.email_news_updates_status) = 'subscribed'
    OR LOWER(f.email_notifications_reminders_status) = 'subscribed'
    OR LOWER(f.email_recommendations_status) = 'subscribed'
    OR LOWER(f.email_special_offers_status) = 'subscribed'
    OR LOWER(f.email_store_promotions_status) = 'subscribed' then consumer_id
  end AS email_optin_overall,
  case when 
    LOWER(f.sms_order_updates_status) = 'on'
    OR LOWER(f.sms_marketing_status) = 'on' then consumer_id
  end AS sms_optin_overall,
  case when
    LOWER(f.push_system_level_status) = 'on'
    OR LOWER(f.push_doordash_offers_status) = 'on'
    OR LOWER(f.push_order_updates_status) = 'on'
    OR LOWER(f.push_product_updates_news_status) = 'on'
    OR LOWER(f.push_recommendations_status) = 'on'
    OR LOWER(f.push_reminders_status) = 'on'
    OR LOWER(f.push_store_offers_status) = 'on' then device_id
  end AS push_optin_overall_device_id,
 case when
    LOWER(f.email_marketing_channel_status) = 'subscribed'
    OR LOWER(f.email_customer_research_status) = 'subscribed'
    OR LOWER(f.email_news_updates_status) = 'subscribed'
    OR LOWER(f.email_notifications_reminders_status) = 'subscribed'
    OR LOWER(f.email_recommendations_status) = 'subscribed'
    OR LOWER(f.email_special_offers_status) = 'subscribed'
    OR LOWER(f.email_store_promotions_status) = 'subscribed' then device_id
  end AS email_optin_overall_device_id,
  case when 
    LOWER(f.sms_order_updates_status) = 'on'
    OR LOWER(f.sms_marketing_status) = 'on' then device_id
  end AS sms_optin_overall_device_id

FROM final f
ORDER BY day, device_id, consumer_id

