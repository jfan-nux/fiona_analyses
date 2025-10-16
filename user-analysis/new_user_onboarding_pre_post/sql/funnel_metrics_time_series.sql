

CREATE OR REPLACE  TABLE proddb.fionafan.onboarding_start_funnel AS
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
WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -90 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
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
WHERE iguazu_timestamp BETWEEN (SELECT start_dt FROM (SELECT current_date -90 as start_dt)) AND (SELECT end_dt FROM (SELECT current_date as end_dt))
  AND lower(onboarding_type) = 'resurrected_user'
  AND lower(page) = 'welcomeback'
  AND lower(dd_platform) = 'ios'
;




CREATE OR REPLACE  TABLE proddb.fionafan.onboarding_funnel_flags AS
WITH p AS (
  SELECT * FROM proddb.fionafan.onboarding_start_funnel
),
start_clicks AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND ((lower(onboarding_type) = 'new_user') OR (lower(dd_platform) = 'android' AND lower(onboarding_type) = 'resurrected_user'))
  GROUP BY 1,2
  UNION
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(onboarding_type) = 'resurrected_user'
    AND lower(page) = 'welcomeback'
    AND lower(dd_platform) = 'ios'
    AND click_type = 'primary'
  GROUP BY 1,2
),
notif_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'notification'
  GROUP BY 1,2
),
notif_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'notification'
  GROUP BY 1,2
),
marketing_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2
),
marketing_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'marketingsms'
  GROUP BY 1,2
),
preference_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) like '%preference%'
  GROUP BY 1,2
),
preference_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) like '%preference%'
  GROUP BY 1,2
),
att_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'att'
  GROUP BY 1,2
),
att_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.M_onboarding_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND lower(page) = 'att'
  GROUP BY 1,2
),
end_page_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
  GROUP BY 1,2
),
end_page_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
  GROUP BY 1,2
),
end_page_promo_view AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND POSITION('%' IN promo_title) > 0
  GROUP BY 1,2
),
end_page_promo_click AS (
  SELECT cast(iguazu_timestamp as date) AS day, consumer_id
  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
  WHERE iguazu_timestamp >= current_date - 90 AND iguazu_timestamp < current_date + 1
    AND POSITION('%' IN promo_title) > 0
  GROUP BY 1,2
),
eligible AS (
  SELECT DATE(iguazu_sent_at) AS day, consumer_id
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS
  WHERE placement_type = 'PLACEMENT_TYPE_POST_ONBOARDING'
    AND is_eligible = TRUE
    AND iguazu_sent_at >= current_date - 90 AND iguazu_sent_at < current_date + 1
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

select day, sum(end_page_promo_view_flag),count(1), avg(end_page_promo_view_flag) from proddb.fionafan.onboarding_funnel_flags where dd_platform='ios' group by all;
-- where consumer_id = '1964455115' and day = '2025-08-12';


-- Onboarding funnel metrics with multi-day conversion windows
-- Base: proddb.fionafan.onboarding_funnel_flags (consumer_id, device_id, day level)
-- Includes: same-day, 7-day, 14-day, and 28-day conversion funnels
create or replace table proddb.fionafan.onboarding_funnel_metrics_w_funnel as (

WITH base_onboarding AS (
    -- Start with onboarding funnel flags table
    SELECT 
        day,
        consumer_id,
        dd_device_id_filtered,
        dd_device_id,
        dd_platform,
        onboarding_type,
        promo_title,
        start_page_view_flag,
        start_page_click_flag,
        notification_view_flag,
        notification_click_flag,
        marketing_sms_view_flag,
        marketing_sms_click_flag,
        preference_view_flag,
        preference_click_flag,
        att_view_flag,
        att_click_flag,
        end_page_view_flag,
        end_page_click_flag,
        end_page_promo_view_flag,
        end_page_promo_click_flag,
        campaign_eligible_flag
    FROM proddb.fionafan.onboarding_funnel_flags
),

same_day_conversions AS (
    -- Same-day conversion metrics from fact_unique_visitors_full_pt
    SELECT 
        DATE(f.event_date) AS event_date,
        replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-') AS dd_device_id_filtered,
        MAX(COALESCE(f.unique_store_page_visitor, 0)) AS d0_store_page_visitor,
        MAX(COALESCE(f.unique_store_content_page_visitor, 0)) AS d0_store_content_visitor,
        MAX(COALESCE(f.unique_order_cart_page_visitor, 0)) AS d0_cart_visitor,
        MAX(COALESCE(f.unique_checkout_page_visitor, 0)) AS d0_checkout_visitor,
        MAX(COALESCE(f.unique_purchaser, 0)) AS d0_purchaser
    FROM proddb.public.fact_unique_visitors_full_pt f
    where f.event_date >= current_date - 90 and f.event_date < current_date + 1
    GROUP BY event_date, dd_device_id_filtered
),

forward_7d_conversions AS (
    -- 7-day forward-looking conversion window
    SELECT 
        b.day,
        b.dd_device_id_filtered,
        MAX(COALESCE(f.unique_store_page_visitor, 0)) AS d7_store_page_visitor,
        MAX(COALESCE(f.unique_store_content_page_visitor, 0)) AS d7_store_content_visitor,
        MAX(COALESCE(f.unique_order_cart_page_visitor, 0)) AS d7_cart_visitor,
        MAX(COALESCE(f.unique_checkout_page_visitor, 0)) AS d7_checkout_visitor,
        MAX(COALESCE(f.unique_purchaser, 0)) AS d7_purchaser
    FROM base_onboarding b
    LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
        ON replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-') = b.dd_device_id_filtered
        AND DATE(f.event_date) >= b.day
        AND DATE(f.event_date) < DATEADD(day, 7, b.day)
    where f.event_date >= current_date - 90 and f.event_date < current_date + 1
    GROUP BY b.day, b.dd_device_id_filtered
),

forward_14d_conversions AS (
    -- 14-day forward-looking conversion window
    SELECT 
        b.day,
        b.dd_device_id_filtered,
        MAX(COALESCE(f.unique_store_page_visitor, 0)) AS d14_store_page_visitor,
        MAX(COALESCE(f.unique_store_content_page_visitor, 0)) AS d14_store_content_visitor,
        MAX(COALESCE(f.unique_order_cart_page_visitor, 0)) AS d14_cart_visitor,
        MAX(COALESCE(f.unique_checkout_page_visitor, 0)) AS d14_checkout_visitor,
        MAX(COALESCE(f.unique_purchaser, 0)) AS d14_purchaser
    FROM base_onboarding b
    LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
        ON replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-') = b.dd_device_id_filtered
        AND DATE(f.event_date) >= b.day
        AND DATE(f.event_date) < DATEADD(day, 14, b.day)
    where f.event_date >= current_date - 90 and f.event_date < current_date + 1
    GROUP BY b.day, b.dd_device_id_filtered
),

forward_28d_conversions AS (
    -- 28-day forward-looking conversion window
    SELECT 
        b.day,
        b.dd_device_id_filtered,
        MAX(COALESCE(f.unique_store_page_visitor, 0)) AS d28_store_page_visitor,
        MAX(COALESCE(f.unique_store_content_page_visitor, 0)) AS d28_store_content_visitor,
        MAX(COALESCE(f.unique_order_cart_page_visitor, 0)) AS d28_cart_visitor,
        MAX(COALESCE(f.unique_checkout_page_visitor, 0)) AS d28_checkout_visitor,
        MAX(COALESCE(f.unique_purchaser, 0)) AS d28_purchaser
    FROM base_onboarding b
    LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
        ON replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-') = b.dd_device_id_filtered
        AND DATE(f.event_date) >= b.day
        AND DATE(f.event_date) < DATEADD(day, 28, b.day)
    where f.event_date >= current_date - 90 and f.event_date < current_date + 1
    GROUP BY b.day, b.dd_device_id_filtered
)

SELECT 
    -- Base onboarding fields
    b.day,
    b.consumer_id,
    b.dd_device_id_filtered,
    b.dd_device_id,
    b.dd_platform,
    b.onboarding_type,
    b.promo_title,
    
    -- Onboarding funnel flags
    b.start_page_view_flag,
    b.start_page_click_flag,
    b.notification_view_flag,
    b.notification_click_flag,
    b.marketing_sms_view_flag,
    b.marketing_sms_click_flag,
    b.preference_view_flag,
    b.preference_click_flag,
    b.att_view_flag,
    b.att_click_flag,
    b.end_page_view_flag,
    b.end_page_click_flag,
    b.end_page_promo_view_flag,
    b.end_page_promo_click_flag,
    b.campaign_eligible_flag,
    
    -- Same-day (D0) conversion metrics
    COALESCE(d0.d0_store_page_visitor, 0) AS d0_store_page_visitor,
    COALESCE(d0.d0_store_content_visitor, 0) AS d0_store_content_visitor,
    COALESCE(d0.d0_cart_visitor, 0) AS d0_cart_visitor,
    COALESCE(d0.d0_checkout_visitor, 0) AS d0_checkout_visitor,
    COALESCE(d0.d0_purchaser, 0) AS d0_purchaser,
    
    -- 7-day conversion metrics
    COALESCE(d7.d7_store_page_visitor, 0) AS d7_store_page_visitor,
    COALESCE(d7.d7_store_content_visitor, 0) AS d7_store_content_visitor,
    COALESCE(d7.d7_cart_visitor, 0) AS d7_cart_visitor,
    COALESCE(d7.d7_checkout_visitor, 0) AS d7_checkout_visitor,
    COALESCE(d7.d7_purchaser, 0) AS d7_purchaser,
    
    -- 14-day conversion metrics
    COALESCE(d14.d14_store_page_visitor, 0) AS d14_store_page_visitor,
    COALESCE(d14.d14_store_content_visitor, 0) AS d14_store_content_visitor,
    COALESCE(d14.d14_cart_visitor, 0) AS d14_cart_visitor,
    COALESCE(d14.d14_checkout_visitor, 0) AS d14_checkout_visitor,
    COALESCE(d14.d14_purchaser, 0) AS d14_purchaser,
    
    -- 28-day conversion metrics
    COALESCE(d28.d28_store_page_visitor, 0) AS d28_store_page_visitor,
    COALESCE(d28.d28_store_content_visitor, 0) AS d28_store_content_visitor,
    COALESCE(d28.d28_cart_visitor, 0) AS d28_cart_visitor,
    COALESCE(d28.d28_checkout_visitor, 0) AS d28_checkout_visitor,
    COALESCE(d28.d28_purchaser, 0) AS d28_purchaser
    
FROM base_onboarding b
LEFT JOIN same_day_conversions d0
    ON b.day = d0.event_date
    AND b.dd_device_id_filtered = d0.dd_device_id_filtered
LEFT JOIN forward_7d_conversions d7
    ON b.day = d7.day
    AND b.dd_device_id_filtered = d7.dd_device_id_filtered
LEFT JOIN forward_14d_conversions d14
    ON b.day = d14.day
    AND b.dd_device_id_filtered = d14.dd_device_id_filtered
LEFT JOIN forward_28d_conversions d28
    ON b.day = d28.day
    AND b.dd_device_id_filtered = d28.dd_device_id_filtered
ORDER BY b.day, b.consumer_id);



-- Query to add order metrics to the onboarding funnel metrics table
-- Uses proddb.fionafan.onboarding_funnel_metrics_w_funnel as base and LEFT JOINs order data
create or replace table proddb.fionafan.onboarding_funnel_metrics_w_orders as (

WITH same_day_orders AS (
    -- Same-day (D1) order metrics
    SELECT 
        f.day,
        f.consumer_id,
        f.dd_device_id_filtered,
        COUNT(DISTINCT d.delivery_id) AS d1_order_rate,
        IFF(COUNT(DISTINCT d.delivery_id) > 1, 1, 0) AS d1_mau,
        COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) AS d1_new_user_order_rate,
        IFF(COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) > 1, 1, 0) AS d1_new_user_mau
    FROM proddb.fionafan.onboarding_funnel_metrics_w_funnel f
    LEFT JOIN proddb.public.dimension_deliveries d
        ON d.creator_id = f.consumer_id
        AND replace(lower(CASE WHEN d.dd_device_id like 'dx_%' then d.dd_device_id else 'dx_'||d.dd_device_id end), '-') = f.dd_device_id_filtered
        AND DATE(d.created_at) >= f.day
        AND DATE(d.created_at) <= DATEADD(day, 1, f.day)
        AND d.is_filtered_core = true
        AND d.cancelled_at IS NULL
        AND d.is_consumer_pickup = false
    GROUP BY f.day, f.consumer_id, f.dd_device_id_filtered
),

forward_7d_orders AS (
    -- 7-day forward-looking order metrics
    SELECT 
        f.day,
        f.consumer_id,
        f.dd_device_id_filtered,
        COUNT(DISTINCT d.delivery_id) AS d7_order_rate,
        IFF(COUNT(DISTINCT d.delivery_id) > 1, 1, 0) AS d7_mau,
        COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) AS d7_new_user_order_rate,
        IFF(COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) > 1, 1, 0) AS d7_new_user_mau
    FROM proddb.fionafan.onboarding_funnel_metrics_w_funnel f
    LEFT JOIN proddb.public.dimension_deliveries d
        ON d.creator_id = f.consumer_id
        AND replace(lower(CASE WHEN d.dd_device_id like 'dx_%' then d.dd_device_id else 'dx_'||d.dd_device_id end), '-') = f.dd_device_id_filtered
        AND DATE(d.created_at) >= f.day
        AND DATE(d.created_at) < DATEADD(day, 7, f.day)
        AND d.is_filtered_core = true
        AND d.cancelled_at IS NULL
        AND d.is_consumer_pickup = false
    GROUP BY f.day, f.consumer_id, f.dd_device_id_filtered
),

forward_14d_orders AS (
    -- 14-day forward-looking order metrics
    SELECT 
        f.day,
        f.consumer_id,
        f.dd_device_id_filtered,
        COUNT(DISTINCT d.delivery_id) AS d14_order_rate,
        IFF(COUNT(DISTINCT d.delivery_id) > 1, 1, 0) AS d14_mau,
        COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) AS d14_new_user_order_rate,
        IFF(COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) > 1, 1, 0) AS d14_new_user_mau
    FROM proddb.fionafan.onboarding_funnel_metrics_w_funnel f
    LEFT JOIN proddb.public.dimension_deliveries d
        ON d.creator_id = f.consumer_id
        AND replace(lower(CASE WHEN d.dd_device_id like 'dx_%' then d.dd_device_id else 'dx_'||d.dd_device_id end), '-') = f.dd_device_id_filtered
        AND DATE(d.created_at) >= f.day
        AND DATE(d.created_at) < DATEADD(day, 14, f.day)
        AND d.is_filtered_core = true
        AND d.cancelled_at IS NULL
        AND d.is_consumer_pickup = false
    GROUP BY f.day, f.consumer_id, f.dd_device_id_filtered
),

forward_28d_orders AS (
    -- 28-day forward-looking order metrics
    SELECT 
        f.day,
        f.consumer_id,
        f.dd_device_id_filtered,
        COUNT(DISTINCT d.delivery_id) AS d28_order_rate,
        IFF(COUNT(DISTINCT d.delivery_id) > 1, 1, 0) AS d28_mau,
        COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) AS d28_new_user_order_rate,
        IFF(COUNT(DISTINCT CASE WHEN d.is_first_ordercart_dd = true THEN d.delivery_id END) > 1, 1, 0) AS d28_new_user_mau
    FROM proddb.fionafan.onboarding_funnel_metrics_w_funnel f
    LEFT JOIN proddb.public.dimension_deliveries d
        ON d.creator_id = f.consumer_id
        AND replace(lower(CASE WHEN d.dd_device_id like 'dx_%' then d.dd_device_id else 'dx_'||d.dd_device_id end), '-') = f.dd_device_id_filtered
        AND DATE(d.created_at) >= f.day
        AND DATE(d.created_at) < DATEADD(day, 28, f.day)
        AND d.is_filtered_core = true
        AND d.cancelled_at IS NULL
        AND d.is_consumer_pickup = false
    GROUP BY f.day, f.consumer_id, f.dd_device_id_filtered
)

SELECT 
    -- All columns from base onboarding funnel metrics table
    f.*,
    
    -- Same-day (D1) order metrics
    COALESCE(o1.d1_order_rate, 0) AS d1_order_rate,
    COALESCE(o1.d1_mau, 0) AS d1_mau,
    COALESCE(o1.d1_new_user_order_rate, 0) AS d1_new_user_order_rate,
    COALESCE(o1.d1_new_user_mau, 0) AS d1_new_user_mau,
    
    -- 7-day order metrics
    COALESCE(o7.d7_order_rate, 0) AS d7_order_rate,
    COALESCE(o7.d7_mau, 0) AS d7_mau,
    COALESCE(o7.d7_new_user_order_rate, 0) AS d7_new_user_order_rate,
    COALESCE(o7.d7_new_user_mau, 0) AS d7_new_user_mau,
    
    -- 14-day order metrics
    COALESCE(o14.d14_order_rate, 0) AS d14_order_rate,
    COALESCE(o14.d14_mau, 0) AS d14_mau,
    COALESCE(o14.d14_new_user_order_rate, 0) AS d14_new_user_order_rate,
    COALESCE(o14.d14_new_user_mau, 0) AS d14_new_user_mau,
    
    -- 28-day order metrics
    COALESCE(o28.d28_order_rate, 0) AS d28_order_rate,
    COALESCE(o28.d28_mau, 0) AS d28_mau,
    COALESCE(o28.d28_new_user_order_rate, 0) AS d28_new_user_order_rate,
    COALESCE(o28.d28_new_user_mau, 0) AS d28_new_user_mau
    
FROM proddb.fionafan.onboarding_funnel_metrics_w_funnel f
LEFT JOIN same_day_orders o1
    ON f.day = o1.day
    AND f.consumer_id = o1.consumer_id
    AND f.dd_device_id_filtered = o1.dd_device_id_filtered
LEFT JOIN forward_7d_orders o7
    ON f.day = o7.day
    AND f.consumer_id = o7.consumer_id
    AND f.dd_device_id_filtered = o7.dd_device_id_filtered
LEFT JOIN forward_14d_orders o14
    ON f.day = o14.day
    AND f.consumer_id = o14.consumer_id
    AND f.dd_device_id_filtered = o14.dd_device_id_filtered
LEFT JOIN forward_28d_orders o28
    ON f.day = o28.day
    AND f.consumer_id = o28.consumer_id
    AND f.dd_device_id_filtered = o28.dd_device_id_filtered
ORDER BY f.day, f.consumer_id);


-- Aggregated funnel and order metrics by day and platform
SELECT 
    day,
    dd_platform,
    
    -- Sample size
    COUNT(DISTINCT consumer_id) AS unique_users,
    COUNT(DISTINCT dd_device_id_filtered) AS unique_devices,
    
    -- Onboarding funnel flags (% of users)
    AVG(start_page_view_flag) * 100 AS start_page_view_pct,
    AVG(start_page_click_flag) * 100 AS start_page_click_pct,
    AVG(notification_view_flag) * 100 AS notification_view_pct,
    AVG(notification_click_flag) * 100 AS notification_click_pct,
    AVG(marketing_sms_view_flag) * 100 AS marketing_sms_view_pct,
    AVG(marketing_sms_click_flag) * 100 AS marketing_sms_click_pct,
    AVG(preference_view_flag) * 100 AS preference_view_pct,
    AVG(preference_click_flag) * 100 AS preference_click_pct,
    AVG(att_view_flag) * 100 AS att_view_pct,
    AVG(att_click_flag) * 100 AS att_click_pct,
    AVG(end_page_view_flag) * 100 AS end_page_view_pct,
    AVG(end_page_click_flag) * 100 AS end_page_click_pct,
    AVG(end_page_promo_view_flag) * 100 AS end_page_promo_view_pct,
    AVG(end_page_promo_click_flag) * 100 AS end_page_promo_click_pct,
    AVG(campaign_eligible_flag) * 100 AS campaign_eligible_pct,
    
    -- Same-day (D0) conversion metrics
    AVG(d0_store_page_visitor) * 100 AS d0_store_page_visitor_pct,
    AVG(d0_store_content_visitor) * 100 AS d0_store_content_visitor_pct,
    AVG(d0_cart_visitor) * 100 AS d0_cart_visitor_pct,
    AVG(d0_checkout_visitor) * 100 AS d0_checkout_visitor_pct,
    AVG(d0_purchaser) * 100 AS d0_purchaser_pct,
    
    -- 7-day conversion metrics
    AVG(d7_store_page_visitor) * 100 AS d7_store_page_visitor_pct,
    AVG(d7_store_content_visitor) * 100 AS d7_store_content_visitor_pct,
    AVG(d7_cart_visitor) * 100 AS d7_cart_visitor_pct,
    AVG(d7_checkout_visitor) * 100 AS d7_checkout_visitor_pct,
    AVG(d7_purchaser) * 100 AS d7_purchaser_pct,
    
    -- 14-day conversion metrics
    AVG(d14_store_page_visitor) * 100 AS d14_store_page_visitor_pct,
    AVG(d14_store_content_visitor) * 100 AS d14_store_content_visitor_pct,
    AVG(d14_cart_visitor) * 100 AS d14_cart_visitor_pct,
    AVG(d14_checkout_visitor) * 100 AS d14_checkout_visitor_pct,
    AVG(d14_purchaser) * 100 AS d14_purchaser_pct,
    
    -- 28-day conversion metrics
    AVG(d28_store_page_visitor) * 100 AS d28_store_page_visitor_pct,
    AVG(d28_store_content_visitor) * 100 AS d28_store_content_visitor_pct,
    AVG(d28_cart_visitor) * 100 AS d28_cart_visitor_pct,
    AVG(d28_checkout_visitor) * 100 AS d28_checkout_visitor_pct,
    AVG(d28_purchaser) * 100 AS d28_purchaser_pct,
    
    -- Same-day (D1) order metrics
    AVG(d1_order_rate) AS d1_avg_orders_per_user,
    SUM(d1_order_rate) AS d1_total_orders,
    AVG(d1_mau) * 100 AS d1_mau_pct,
    AVG(d1_new_user_order_rate) AS d1_avg_new_user_orders_per_user,
    SUM(d1_new_user_order_rate) AS d1_total_new_user_orders,
    AVG(d1_new_user_mau) * 100 AS d1_new_user_mau_pct,
    
    -- 7-day order metrics
    AVG(d7_order_rate) AS d7_avg_orders_per_user,
    SUM(d7_order_rate) AS d7_total_orders,
    AVG(d7_mau) * 100 AS d7_mau_pct,
    AVG(d7_new_user_order_rate) AS d7_avg_new_user_orders_per_user,
    SUM(d7_new_user_order_rate) AS d7_total_new_user_orders,
    AVG(d7_new_user_mau) * 100 AS d7_new_user_mau_pct,
    
    -- 14-day order metrics
    AVG(d14_order_rate) AS d14_avg_orders_per_user,
    SUM(d14_order_rate) AS d14_total_orders,
    AVG(d14_mau) * 100 AS d14_mau_pct,
    AVG(d14_new_user_order_rate) AS d14_avg_new_user_orders_per_user,
    SUM(d14_new_user_order_rate) AS d14_total_new_user_orders,
    AVG(d14_new_user_mau) * 100 AS d14_new_user_mau_pct,
    
    -- 28-day order metrics
    AVG(d28_order_rate) AS d28_avg_orders_per_user,
    SUM(d28_order_rate) AS d28_total_orders,
    AVG(d28_mau) * 100 AS d28_mau_pct,
    AVG(d28_new_user_order_rate) AS d28_avg_new_user_orders_per_user,
    SUM(d28_new_user_order_rate) AS d28_total_new_user_orders,
    AVG(d28_new_user_mau) * 100 AS d28_new_user_mau_pct

FROM proddb.fionafan.onboarding_funnel_metrics_w_orders
where dd_platform='ios'
GROUP BY day, dd_platform
ORDER BY day, dd_platform;
