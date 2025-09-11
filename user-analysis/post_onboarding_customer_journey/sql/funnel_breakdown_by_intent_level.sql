-- Step 1: Create funnel table, 31min 
-- CREATE OR REPLACE TABLE proddb.fionafan.onboarding_funnel_base AS (
-- WITH exposure AS (
--   SELECT ee.tag,
--          ee.result,
--          ee.bucket_key,
--          replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
--                   else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered,
--          segment,
--          MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day,
--          MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
--   FROM proddb.public.fact_dedup_experiment_exposure ee
--   WHERE experiment_name = 'cx_mobile_onboarding_preferences'
--     AND experiment_version::INT = 1
--     AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-08-04' AND '2025-09-30'
--   GROUP BY 1,2,3,4,5
-- ),

-- start_page_view AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
-- ),

-- start_page_click AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.m_onboarding_start_promo_page_click_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
-- ),

-- notification_view AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_view_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'notification'
-- ),

-- notification_click AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_click_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'notification'
-- ),

-- marketing_sms_view AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_view_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'marketingSMS'
-- ),

-- marketing_sms_click AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_click_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'marketingSMS'
-- ),

-- att_view AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_view_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'att'
-- ),

-- att_click AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.M_onboarding_page_click_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
--     AND page = 'att'
-- ),

-- end_page_view AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
-- ),

-- end_page_click AS (
--   SELECT DISTINCT 
--          replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
--                       else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered,
--          cast(iguazu_timestamp as date) AS day,
--          consumer_id
--   FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice
--   WHERE iguazu_timestamp BETWEEN '2025-08-04' AND '2025-09-30'
-- )

-- SELECT DISTINCT 
--        ee.tag,
--        ee.dd_device_ID_filtered,
--        ee.day,
--        COALESCE(a.consumer_id, b.consumer_id, c.consumer_id, d.consumer_id, 
--                sv.consumer_id, sc.consumer_id, e.consumer_id, f.consumer_id,
--                g.consumer_id, h.consumer_id) as consumer_id,
--        MAX(CASE WHEN a.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS start_page_view,
--        MAX(CASE WHEN b.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS start_page_click,
--        MAX(CASE WHEN c.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS notification_view,
--        MAX(CASE WHEN d.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS notification_click,
--        MAX(CASE WHEN sv.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS marketing_sms_view,
--        MAX(CASE WHEN sc.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS marketing_sms_click,
--        MAX(CASE WHEN e.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS att_view,
--        MAX(CASE WHEN f.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS att_click,
--        MAX(CASE WHEN g.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS end_page_view,
--        MAX(CASE WHEN h.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS end_page_click
-- FROM exposure ee
-- LEFT JOIN start_page_view a
--   ON ee.dd_device_ID_filtered = a.dd_device_ID_filtered
--   AND ee.day <= a.day
-- LEFT JOIN start_page_click b
--   ON ee.dd_device_ID_filtered = b.dd_device_ID_filtered
--   AND ee.day <= b.day
-- LEFT JOIN notification_view c
--   ON ee.dd_device_ID_filtered = c.dd_device_ID_filtered
--   AND ee.day <= c.day
-- LEFT JOIN notification_click d
--   ON ee.dd_device_ID_filtered = d.dd_device_ID_filtered
--   AND ee.day <= d.day
-- LEFT JOIN marketing_sms_view sv
--   ON ee.dd_device_ID_filtered = sv.dd_device_ID_filtered
--   AND ee.day <= sv.day
-- LEFT JOIN marketing_sms_click sc
--   ON ee.dd_device_ID_filtered = sc.dd_device_ID_filtered
--   AND ee.day <= sc.day
-- LEFT JOIN att_view e
--   ON ee.dd_device_ID_filtered = e.dd_device_ID_filtered
--   AND ee.day <= e.day
-- LEFT JOIN att_click f
--   ON ee.dd_device_ID_filtered = f.dd_device_ID_filtered
--   AND ee.day <= f.day
-- LEFT JOIN end_page_view g
--   ON ee.dd_device_ID_filtered = g.dd_device_ID_filtered
--   AND ee.day <= g.day
-- LEFT JOIN end_page_click h
--   ON ee.dd_device_ID_filtered = h.dd_device_ID_filtered
--   AND ee.day <= h.day
-- GROUP BY 1,2,3,4

-- );

-- Step 2: Use the funnel table for intent analysis
WITH intent_levels AS (
  SELECT consumer_id,
         CASE 
           WHEN entity_cnt = 1 THEN 'low intent'
           WHEN entity_cnt = 20 THEN 'low intent'
           ELSE 'high intent'
         END as intent_level
  FROM proddb.fionafan.preference_entity_cnt_distribution
  WHERE page = 'onboarding_preference_page'
    AND entity_cnt IS NOT NULL
),

funnel_res AS (
  SELECT f.tag,
         CASE 
           WHEN f.consumer_id IS NULL THEN 'no preference'  -- No consumer_id available
           WHEN il.intent_level IS NULL THEN 'no preference'  -- No preferences recorded
           ELSE il.intent_level 
         END as intent_level,
         COUNT(DISTINCT f.dd_device_ID_filtered) as exposure,
         SUM(f.start_page_view) start_page_view,
         SUM(f.start_page_view) / COUNT(DISTINCT f.dd_device_ID_filtered) AS start_page_view_rate,
         SUM(f.start_page_click) AS start_page_click,
         SUM(f.start_page_click) / NULLIF(SUM(f.start_page_view),0) AS start_page_click_rate,
         SUM(f.notification_view) AS notification_view,
         SUM(f.notification_view) / NULLIF(SUM(f.start_page_click),0) AS notification_view_rate,
         SUM(f.notification_click) AS notification_click,
         SUM(f.notification_click) / NULLIF(SUM(f.notification_view),0) AS notification_click_rate,
         SUM(f.marketing_sms_view) AS marketing_sms_view,
         SUM(f.marketing_sms_view) / NULLIF(SUM(f.notification_click),0) AS marketing_sms_view_rate,
         SUM(f.marketing_sms_click) AS marketing_sms_click,
         SUM(f.marketing_sms_click) / NULLIF(SUM(f.marketing_sms_view),0) AS marketing_sms_click_rate,
         SUM(f.att_view) AS att_view,
         SUM(f.att_view) / NULLIF(SUM(f.marketing_sms_click),0) AS att_view_rate,
         SUM(f.att_click) AS att_click,
         SUM(f.att_click) / NULLIF(SUM(f.att_view),0) AS att_click_rate,
         SUM(f.end_page_view) AS end_page_view,
         SUM(f.end_page_view) / NULLIF(SUM(f.att_click),0) AS end_page_view_rate,
         SUM(f.end_page_click) AS end_page_click,
         SUM(f.end_page_click) / NULLIF(SUM(f.end_page_view),0) AS end_page_click_rate,
         SUM(f.att_click) / SUM(f.start_page_view) as onboarding_completion
  FROM proddb.fionafan.onboarding_funnel_base f
  LEFT JOIN intent_levels il
    ON f.consumer_id = il.consumer_id
  GROUP BY 1, 2
),

res AS (
  SELECT f.*
  FROM funnel_res f
  ORDER BY 1, 2
)

SELECT r1.tag,
       r1.intent_level,
       r1.exposure,
       r1.start_page_view,
       r1.start_page_view_rate,
       r1.start_page_view_rate / NULLIF(r2.start_page_view_rate,0) - 1 AS Lift_start_page_view_rate,
       r1.start_page_click,
       r1.start_page_click_rate,
       r1.start_page_click_rate / NULLIF(r2.start_page_click_rate,0) - 1 AS Lift_start_page_click_rate,
       r1.notification_view,
       r1.notification_view_rate,
       r1.notification_view_rate / NULLIF(r2.notification_view_rate,0) - 1 AS Lift_notification_view_rate,
       r1.notification_click,
       r1.notification_click_rate,
       r1.notification_click_rate / NULLIF(r2.notification_click_rate,0) - 1 AS Lift_notification_click_rate,
       r1.marketing_sms_view,
       r1.marketing_sms_view_rate,
       r1.marketing_sms_view_rate / NULLIF(r2.marketing_sms_view_rate,0) - 1 AS Lift_marketing_sms_view_rate,
       r1.marketing_sms_click,
       r1.marketing_sms_click_rate,
       r1.marketing_sms_click_rate / NULLIF(r2.marketing_sms_click_rate,0) - 1 AS Lift_marketing_sms_click_rate,
       r1.att_view,
       r1.att_view_rate,
       r1.att_view_rate / NULLIF(r2.att_view_rate,0) - 1 AS Lift_att_view_rate,
       r1.att_click,
       r1.att_click_rate,
       r1.att_click_rate / NULLIF(r2.att_click_rate,0) - 1 AS Lift_att_click_rate,
       r1.end_page_view,
       r1.end_page_view_rate,
       r1.end_page_view_rate / NULLIF(r2.end_page_view_rate,0) - 1 AS Lift_end_page_view_rate,
       r1.end_page_click,
       r1.end_page_click_rate,
       r1.end_page_click_rate / NULLIF(r2.end_page_click_rate,0) - 1 AS Lift_end_page_click_rate,
       r1.onboarding_completion,
       r1.onboarding_completion / NULLIF(r2.onboarding_completion,0) - 1 AS Lift_onboarding_completion,
       
       -- Control group statistics for each intent level
       r2.exposure AS control_exposure,
       r2.start_page_view AS control_start_page_view,
       r2.start_page_view_rate AS control_start_page_view_rate,
       r2.start_page_click AS control_start_page_click,
       r2.start_page_click_rate AS control_start_page_click_rate,
       r2.notification_view AS control_notification_view,
       r2.notification_view_rate AS control_notification_view_rate,
       r2.notification_click AS control_notification_click,
       r2.notification_click_rate AS control_notification_click_rate,
       r2.marketing_sms_view AS control_marketing_sms_view,
       r2.marketing_sms_view_rate AS control_marketing_sms_view_rate,
       r2.marketing_sms_click AS control_marketing_sms_click,
       r2.marketing_sms_click_rate AS control_marketing_sms_click_rate,
       r2.att_view AS control_att_view,
       r2.att_view_rate AS control_att_view_rate,
       r2.att_click AS control_att_click,
       r2.att_click_rate AS control_att_click_rate,
       r2.end_page_view AS control_end_page_view,
       r2.end_page_view_rate AS control_end_page_view_rate,
       r2.end_page_click AS control_end_page_click,
       r2.end_page_click_rate AS control_end_page_click_rate,
       r2.onboarding_completion AS control_onboarding_completion

FROM res r1
LEFT JOIN res r2
  ON r1.tag != r2.tag
  AND r2.tag = 'control'
  AND r1.intent_level = r2.intent_level  -- Match on same intent level for lift calculations
ORDER BY r1.intent_level, r1.tag;
