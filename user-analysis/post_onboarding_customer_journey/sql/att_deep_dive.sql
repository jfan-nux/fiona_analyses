create or replace temp view proddb.fionafan.notification_one_day_view as (
SELECT 
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    DD_DEVICE_ID,
    consumer_id,
    convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp) as pst_exposure_time,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-') as dd_filtered_device_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE date_trunc('day',convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp)) = '2025-08-26'
  and lower(dd_platform) = 'ios'
    AND page = 'notification'
);


create or replace temp view proddb.fionafan.att_one_day_view as (
SELECT 
    cast(iguazu_timestamp as date) AS day,
    dd_platform,
    DD_DEVICE_ID,
    consumer_id,
    convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp) as pst_exposure_time,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                  else 'dx_'||DD_DEVICE_ID end), '-') as dd_filtered_device_id
  FROM iguazu.consumer.M_onboarding_page_view_ice
  WHERE date_trunc('day',convert_timezone(iguazu_context_timezone,'America/Los_Angeles',iguazu_timestamp)) = '2025-08-26'
    AND page = 'att'
    and lower(dd_platform) = 'ios'
);



-- Simple funnel analysis with ATT tracking events
WITH att_tracking_events AS (
  SELECT 
    replace(lower(CASE WHEN DD_device_id like 'dx_%' then DD_device_id
                      else 'dx_'||DD_device_id end), '-') as dd_filtered_device_id,
    min(convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) as min_att_event_time,
    'declined' as att_decision
  FROM segment_events_raw.consumer_production.m_att_system_tracking_declined 
  WHERE date_trunc('day',convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) >= '2025-08-26'::date-30
  GROUP BY 1
  
  UNION ALL
  
  SELECT 
    replace(lower(CASE WHEN DD_device_id like 'dx_%' then DD_device_id
                      else 'dx_'||DD_device_id end), '-') as dd_filtered_device_id,
    min(convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) as min_att_event_time,
    'authorized' as att_decision
  FROM segment_events_raw.consumer_production.m_att_system_tracking_authorized 

  WHERE date_trunc('day',convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) >= '2025-08-26'::date-30
  GROUP BY 1
  UNION ALL
  

  SELECT 
    replace(lower(CASE WHEN DD_device_id like 'dx_%' then DD_device_id
                      else 'dx_'||DD_device_id end), '-') as dd_filtered_device_id,
    min(convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) as min_att_event_time,
    'authorized' as att_decision
  FROM segment_events_raw.consumer_production.m_att_description_view_appear 
  WHERE date_trunc('day',convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) >= '2025-08-26'::date-30
  GROUP BY 1

),

att_tracking_summary AS (
  SELECT 
    dd_filtered_device_id,
    min(min_att_event_time) as earliest_att_event_time,
    listagg(att_decision, ', ') WITHIN GROUP (ORDER BY min_att_event_time) as att_decisions
  FROM att_tracking_events
  GROUP BY 1
),

funnel AS (
  SELECT 
    COALESCE(n.dd_filtered_device_id, a.dd_filtered_device_id) as device_id,
    CASE WHEN n.dd_filtered_device_id IS NOT NULL THEN 1 ELSE 0 END as in_notification,
    CASE WHEN a.dd_filtered_device_id IS NOT NULL THEN 1 ELSE 0 END as in_att,
    n.pst_exposure_time as notification_exposure_time,
    a.pst_exposure_time as att_page_exposure_time
  FROM (SELECT DISTINCT dd_filtered_device_id, min(pst_exposure_time) as pst_exposure_time 
        FROM proddb.fionafan.notification_one_day_view GROUP BY 1) n
  LEFT JOIN (SELECT DISTINCT dd_filtered_device_id, min(pst_exposure_time) as pst_exposure_time 
                   FROM proddb.fionafan.att_one_day_view GROUP BY 1) a
    ON n.dd_filtered_device_id = a.dd_filtered_device_id
),

funnel_with_att AS (
  SELECT 
    f.*,
    att.earliest_att_event_time,
    att.att_decisions,
    CASE WHEN att.earliest_att_event_time IS NOT NULL THEN 1 ELSE 0 END as has_att_decision
  FROM funnel f
  LEFT JOIN att_tracking_summary att ON f.device_id = att.dd_filtered_device_id 
  and att.earliest_att_event_time<=f.notification_exposure_time
)

SELECT 
  COUNT(CASE WHEN in_notification = 1 THEN 1 END) as devices_saw_notification,
  COUNT(CASE WHEN in_notification = 1 AND in_att = 1 THEN 1 END) as devices_saw_att_page,
  COUNT(CASE WHEN in_notification = 1 AND (in_att = 1 OR has_att_decision = 1) THEN 1 END) as devices_att_page_or_decision,
  ROUND(
    COUNT(CASE WHEN in_notification = 1 AND in_att = 1 THEN 1 END)::FLOAT 
    / NULLIF(COUNT(CASE WHEN in_notification = 1 THEN 1 END), 0) * 100, 
    2
  ) as conversion_rate_pct,
  ROUND(
    100 - (COUNT(CASE WHEN in_notification = 1 AND in_att = 1 THEN 1 END)::FLOAT 
           / NULLIF(COUNT(CASE WHEN in_notification = 1 THEN 1 END), 0) * 100), 
    2
  ) as dropoff_rate_pct,
  ROUND(
    COUNT(CASE WHEN in_notification = 1 AND (in_att = 1 OR has_att_decision = 1) THEN 1 END)::FLOAT 
    / NULLIF(COUNT(CASE WHEN in_notification = 1 THEN 1 END), 0) * 100, 
    2
  ) as pct_att_page_or_decision
FROM funnel_with_att;




SET exp_name = 'cx_mobile_onboarding_preferences';
SET start_date = '2025-08-04';
SET end_date = CURRENT_DATE;
SET version = 1;

WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
                , segment
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = $exp_name
AND experiment_version::INT = $version
AND segment = 'iOS'
-- AND segment = 'ios - US'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY 1,2,3,4,5
)
, preference_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE iguazu_timestamp  BETWEEN $start_date AND $end_date
and page ilike '%preference%'
)
, att_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE iguazu_timestamp  BETWEEN $start_date AND $end_date
and page = 'att'
)


, end_page_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , cast(iguazu_timestamp as date) AS day
      , consumer_id
from iguazu.consumer.m_onboarding_end_promo_page_view_ice
WHERE iguazu_timestamp BETWEEN $start_date AND $end_date
)


-- Restrict funnel to only CTEs defined in this block (preference_view, att_view, end_page_view)
, funnel AS (
SELECT DISTINCT ee.tag
                , ee.dd_device_ID_filtered
                , ee.day
                , MAX(CASE WHEN pv.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS preference_view
                , MAX(CASE WHEN av.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS att_view
                , MAX(CASE WHEN ev.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS end_page_view
FROM exposure ee
LEFT JOIN preference_view pv
    ON ee.dd_device_ID_filtered = pv.dd_device_ID_filtered
    AND ee.day <= pv.day
LEFT JOIN att_view av
    ON ee.dd_device_ID_filtered = av.dd_device_ID_filtered
    AND ee.day <= av.day
LEFT JOIN end_page_view ev
    ON ee.dd_device_ID_filtered = ev.dd_device_ID_filtered
    AND ee.day <= ev.day
GROUP BY 1,2,3
)

SELECT tag
        , COUNT(DISTINCT dd_device_ID_filtered) AS exposure
        , SUM(preference_view) AS preference_view
        , SUM(preference_view) / NULLIF(COUNT(DISTINCT dd_device_ID_filtered), 0) AS preference_view_rate
        , SUM(att_view) AS att_view
        , SUM(att_view) / NULLIF(SUM(preference_view), 0) AS att_view_rate
        , SUM(end_page_view)  AS end_page_view
        , SUM(end_page_view) / NULLIF(SUM(att_view), 0) AS end_page_view_rate
FROM funnel
GROUP BY 1
ORDER BY 1
;

select distinct page from iguazu.consumer.M_onboarding_page_view_ice where iguazu_timestamp >= '2025-08-04';