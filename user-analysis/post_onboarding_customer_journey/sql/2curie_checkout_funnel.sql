WITH explore_page AS (
  SELECT DISTINCT
    CAST(DD_DEVICE_ID AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(iguazu_timestamp AS TIMESTAMP) AS event_ts
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD
  WHERE iguazu_timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND DD_DEVICE_ID IS NOT NULL
    AND DD_DEVICE_ID <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND iguazu_timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(iguazu_timestamp), '') IS NOT NULL
    
),
store_page AS (
  SELECT DISTINCT
    CAST(DD_DEVICE_ID AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_RAW.consumer_production.m_store_page_load
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND DD_DEVICE_ID IS NOT NULL
    AND DD_DEVICE_ID <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
cart_page AS (
  SELECT DISTINCT
    CAST(DD_DEVICE_ID AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(iguazu_timestamp AS TIMESTAMP) AS event_ts
  FROM iguazu.consumer.m_order_cart_page_load
  WHERE iguazu_timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND DD_DEVICE_ID IS NOT NULL
    AND DD_DEVICE_ID <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND iguazu_timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(iguazu_timestamp), '') IS NOT NULL
),
checkout_page AS (
  SELECT DISTINCT
    CAST(DD_DEVICE_ID AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_RAW.consumer_production.m_checkout_page_load
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND DD_DEVICE_ID IS NOT NULL
    AND DD_DEVICE_ID <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
item_add AS (
  SELECT DISTINCT
    CAST(dd_device_id AS VARCHAR) AS device_id,
    CAST(a.consumer_id AS VARCHAR) AS consumer_id,
    CAST(iguazu_timestamp AS TIMESTAMP) AS event_ts
  FROM iguazu.consumer.m_item_page_action_add_item a
  WHERE iguazu_timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND dd_device_id IS NOT NULL
    AND dd_device_id <> ''
    AND a.consumer_id IS NOT NULL
    AND a.consumer_id <> ''
    AND iguazu_timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(iguazu_timestamp), '') IS NOT NULL
),
item_quick_add AS (
  SELECT DISTINCT
    CAST(dd_device_id AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_raw.consumer_production.m_action_quick_add_item a
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND dd_device_id IS NOT NULL
    AND dd_device_id <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
item_open AS (
  SELECT DISTINCT
    CAST(context_device_id AS VARCHAR) AS device_id,
    CAST(a.consumer_id AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM iguazu.consumer.m_checkout_page_load a
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND context_device_id IS NOT NULL
    AND context_device_id <> ''
    AND a.consumer_id IS NOT NULL
    AND a.consumer_id <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
mx_info_page AS (
  SELECT DISTINCT
    CAST(dd_device_id AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_raw.consumer_production.m_store_info_page_load a
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND dd_device_id IS NOT NULL
    AND dd_device_id <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
tap_place_order AS (
  SELECT DISTINCT
    CAST(dd_device_id AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_raw.consumer_production.m_checkout_page_action_place_order a
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND dd_device_id IS NOT NULL
    AND dd_device_id <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
 
experiment_conversion AS (
  SELECT DISTINCT
    CAST(dd_device_id AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(timestamp AS TIMESTAMP) AS event_ts
  FROM segment_events_raw.consumer_production.order_cart_submit_received
  WHERE timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND dd_device_id IS NOT NULL
    AND dd_device_id <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(timestamp), '') IS NOT NULL
),
experiment_deliveries AS (
  SELECT DISTINCT
    CAST(a.dd_device_id AS VARCHAR) AS device_id,
    CAST(CONSUMER_ID AS VARCHAR) AS consumer_id,
    CAST(a.timestamp AS TIMESTAMP) AS event_ts,
    MAX(
      CASE WHEN COALESCE(b.is_consumer_pickup, 0) = 1 THEN 1 ELSE 0 END
    ) AS ind_pickup
  FROM segment_events_raw.consumer_production.order_cart_submit_received a
  JOIN proddb.public.dimension_deliveries b
    ON CAST(a.order_cart_id AS VARCHAR) = CAST(b.order_cart_id AS VARCHAR)
  WHERE a.timestamp BETWEEN {{start_date}} AND {{end_date}}
    AND b.is_filtered_core = TRUE
    AND b.source = 'mp'
    AND b.is_caviar = FALSE
    AND a.dd_device_id IS NOT NULL
    AND a.dd_device_id <> ''
    AND CONSUMER_ID IS NOT NULL
    AND CONSUMER_ID <> ''
    AND a.timestamp IS NOT NULL
    AND NULLIF(TO_VARCHAR(a.timestamp), '') IS NOT NULL
  GROUP BY ALL
),
events AS (
  SELECT device_id, consumer_id, event_ts, 'explore' AS funnel_page FROM explore_page
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'store' AS funnel_page FROM store_page
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'cart' AS funnel_page FROM cart_page
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'checkout' AS funnel_page FROM checkout_page
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'item_add' AS funnel_page FROM item_add
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'item_quick_add' AS funnel_page FROM item_quick_add
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'item_open' AS funnel_page FROM item_open
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'mx_info_page' AS funnel_page FROM mx_info_page
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'tap_place_order' AS funnel_page FROM tap_place_order
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'experiment_conversion' AS funnel_page FROM experiment_conversion
  UNION ALL
  SELECT device_id, consumer_id, event_ts, 'experiment_deliveries' AS funnel_page FROM experiment_deliveries
),
final AS (
  SELECT
    device_id,
    consumer_id,
    event_ts,
    MAX(CASE WHEN funnel_page = 'explore' THEN device_id ELSE NULL END) AS page_explore_device_id,
    MAX(CASE WHEN funnel_page = 'explore' THEN consumer_id ELSE NULL END) AS page_explore_consumer_id,
    MAX(CASE WHEN funnel_page = 'store' THEN device_id ELSE NULL END) AS page_store_device_id,
    MAX(CASE WHEN funnel_page = 'store' THEN consumer_id ELSE NULL END) AS page_store_consumer_id,
    MAX(CASE WHEN funnel_page = 'cart' THEN device_id ELSE NULL END) AS page_cart_device_id,
    MAX(CASE WHEN funnel_page = 'cart' THEN consumer_id ELSE NULL END) AS page_cart_consumer_id,
    MAX(CASE WHEN funnel_page = 'checkout' THEN device_id ELSE NULL END) AS page_checkout_device_id,
    MAX(CASE WHEN funnel_page = 'checkout' THEN consumer_id ELSE NULL END) AS page_checkout_consumer_id,
    MAX(CASE WHEN funnel_page = 'item_add' THEN device_id ELSE NULL END) AS page_item_add_device_id,
    MAX(CASE WHEN funnel_page = 'item_add' THEN consumer_id ELSE NULL END) AS page_item_add_consumer_id,
    MAX(CASE WHEN funnel_page = 'item_quick_add' THEN device_id ELSE NULL END) AS page_item_quick_add_device_id,
    MAX(CASE WHEN funnel_page = 'item_quick_add' THEN consumer_id ELSE NULL END) AS page_item_quick_add_consumer_id,
    MAX(CASE WHEN funnel_page = 'item_open' THEN device_id ELSE NULL END) AS page_item_open_device_id,
    MAX(CASE WHEN funnel_page = 'item_open' THEN consumer_id ELSE NULL END) AS page_item_open_consumer_id,
    MAX(CASE WHEN funnel_page = 'mx_info_page' THEN device_id ELSE NULL END) AS page_mx_info_page_device_id,
    MAX(CASE WHEN funnel_page = 'mx_info_page' THEN consumer_id ELSE NULL END) AS page_mx_info_page_consumer_id,
    MAX(CASE WHEN funnel_page = 'tap_place_order' THEN device_id ELSE NULL END) AS page_tap_place_order_device_id,
    MAX(CASE WHEN funnel_page = 'tap_place_order' THEN consumer_id ELSE NULL END) AS page_tap_place_order_consumer_id,
    MAX(CASE WHEN funnel_page = 'experiment_conversion' THEN device_id ELSE NULL END) AS page_experiment_conversion_device_id,
    MAX(CASE WHEN funnel_page = 'experiment_conversion' THEN consumer_id ELSE NULL END) AS page_experiment_conversion_consumer_id,
    MAX(CASE WHEN funnel_page = 'experiment_deliveries' THEN device_id ELSE NULL END) AS page_experiment_deliveries_device_id,
    MAX(CASE WHEN funnel_page = 'experiment_deliveries' THEN consumer_id ELSE NULL END) AS page_experiment_deliveries_consumer_id
  FROM events
  GROUP BY ALL
)

select
  f.device_id,
  f.consumer_id,
  f.event_ts,
  f.page_explore_device_id,
  f.page_explore_consumer_id,
  f.page_store_device_id,
  f.page_store_consumer_id,
  f.page_cart_device_id,
  f.page_cart_consumer_id,
  f.page_checkout_device_id,
  f.page_checkout_consumer_id,
  f.page_item_add_device_id,
  f.page_item_add_consumer_id,
  f.page_item_quick_add_device_id,
  f.page_item_quick_add_consumer_id,
  f.page_item_open_device_id,
  f.page_item_open_consumer_id,
  f.page_mx_info_page_device_id,
  f.page_mx_info_page_consumer_id,
  f.page_tap_place_order_device_id,
  f.page_tap_place_order_consumer_id,
  f.page_experiment_conversion_device_id,
  f.page_experiment_conversion_consumer_id,
  f.page_experiment_deliveries_device_id,
  f.page_experiment_deliveries_consumer_id,
  null as page_checkout_page_failure_device_id,
  null as page_checkout_page_failure_consumer_id
from final f



--IGUAZU.SERVER_EVENTS_PRODUCTION.M_STORE_CONTENT_PAGE_LOAD,segment_events_RAW.consumer_production.m_store_page_load,iguazu.consumer.m_order_cart_page_load,segment_events_RAW.consumer_production.m_checkout_page_load,iguazu.consumer.m_item_page_action_add_item,segment_events_raw.consumer_production.m_action_quick_add_item,iguazu.consumer.m_checkout_page_load,segment_events_raw.consumer_production.m_store_info_page_load,segment_events_raw.consumer_production.m_checkout_page_action_place_order,segment_events_raw.consumer_production.m_checkout_page_system_checkout_failure,iguazu.consumer.system_checkout_failure,segment_events_raw.consumer_production.order_cart_submit_received,segment_events_raw.consumer_production.order_cart_submit_received