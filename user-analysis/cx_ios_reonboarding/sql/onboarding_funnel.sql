SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-08-11';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

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
AND segment = $segment
AND convert_timezone('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY 1,2,3,4,5
)

, welcome_back_view AS (
SELECT 
    DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'welcomeBack'
)

, welcome_back_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
-- from datalake.iguazu_consumer.M_onboarding_page_click_ice
from iguazu.consumer.M_onboarding_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'welcomeBack'
)

, notification_view AS (
SELECT 
    DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'notification'
)

, notification_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
-- from datalake.iguazu_consumer.M_onboarding_page_click_ice
from iguazu.consumer.M_onboarding_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'notification'
)

, marketing_sms_view AS (
SELECT 
    DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and page = 'marketingSMS'
)

, marketing_sms_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
-- from datalake.iguazu_consumer.M_onboarding_page_click_ice
from iguazu.consumer.M_onboarding_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and page = 'marketingSMS'
)

, preference_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'preference'
)

, preference_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'preference'
)

, att_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'att'
)

, att_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from  iguazu.consumer.M_onboarding_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and page = 'att'
)

, end_page_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from iguazu.consumer.m_onboarding_end_promo_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
)

, promo_view AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from iguazu.consumer.m_onboarding_end_promo_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
and promo_title != 'Welcome back'
)

, end_page_click AS (
SELECT DISTINCT  replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                        else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_ID_filtered
      , convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date AS day
      , consumer_id
from iguazu.consumer.m_onboarding_end_promo_page_click_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) BETWEEN $start_date AND $end_date
and onboarding_type = 'resurrected_user'
)

, funnel AS (
SELECT DISTINCT ee.tag
                , ee.dd_device_ID_filtered
                , ee.day
                , MAX(CASE WHEN a.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS welcome_back_view
                , MAX(CASE WHEN b.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS welcome_back_click
                , MAX(CASE WHEN c.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS notification_view
                , MAX(CASE WHEN d.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS notification_click 
                , MAX(CASE WHEN sv.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS marketing_sms_view 
                , MAX(CASE WHEN sc.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS marketing_sms_click 
                , MAX(CASE WHEN pv.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS preference_view
                , MAX(CASE WHEN pc.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS preference_click
                , MAX(CASE WHEN e.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS att_view
                , MAX(CASE WHEN f.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS att_click    
                , MAX(CASE WHEN g.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS end_page_view
                , MAX(CASE WHEN promo.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS promo_view
                , MAX(CASE WHEN h.dd_device_ID_filtered IS NOT NULL THEN 1 ELSE 0 END) AS end_page_click                
FROM exposure ee
LEFT JOIN welcome_back_view a
    ON ee.dd_device_ID_filtered = a.dd_device_ID_filtered
    AND ee.day <= a.day
LEFT JOIN welcome_back_click b
    ON ee.dd_device_ID_filtered = b.dd_device_ID_filtered
    AND ee.day <= b.day
LEFT JOIN notification_view c
    ON ee.dd_device_ID_filtered = c.dd_device_ID_filtered
    AND ee.day <= c.day
LEFT JOIN notification_click d
    ON ee.dd_device_ID_filtered = d.dd_device_ID_filtered
    AND ee.day <= d.day
LEFT JOIN marketing_sms_view sv
    ON ee.dd_device_ID_filtered = sv.dd_device_ID_filtered
    AND ee.day <= sv.day
LEFT JOIN marketing_sms_click sc
    ON ee.dd_device_ID_filtered = sc.dd_device_ID_filtered
    AND ee.day <= sc.day
LEFT JOIN preference_view pv
    ON ee.dd_device_ID_filtered = pv.dd_device_ID_filtered
    AND ee.day <= pv.day
LEFT JOIN preference_click pc
    ON ee.dd_device_ID_filtered = pc.dd_device_ID_filtered
    AND ee.day <= pc.day
LEFT JOIN att_view e
    ON ee.dd_device_ID_filtered = e.dd_device_ID_filtered
    AND ee.day <= e.day
LEFT JOIN att_click f
    ON ee.dd_device_ID_filtered = f.dd_device_ID_filtered
    AND ee.day <= f.day
LEFT JOIN end_page_view g
    ON ee.dd_device_ID_filtered = g.dd_device_ID_filtered
    AND ee.day <= g.day
LEFT JOIN promo_view promo
    ON ee.dd_device_ID_filtered = promo.dd_device_ID_filtered
    AND ee.day <= promo.day
LEFT JOIN end_page_click h
    ON ee.dd_device_ID_filtered = h.dd_device_ID_filtered
    AND ee.day <= h.day    
GROUP BY 1,2,3
)

SELECT tag
        , count(distinct dd_device_ID_filtered) as exposure
        , SUM(welcome_back_view) welcome_back_view
        , SUM(welcome_back_view) / COUNT(DISTINCT dd_device_ID_filtered) AS welcome_back_view_rate
        , SUM(welcome_back_click) AS welcome_back_click
        , SUM(welcome_back_click) / nullif(SUM(welcome_back_view),0) AS welcome_back_click_rate
        , SUM(notification_view) AS notification_view
        , SUM(notification_view) / nullif(SUM(welcome_back_click),0) AS notification_view_rate
        , SUM(notification_click) AS notification_click
        , SUM(notification_click) / nullif(SUM(notification_view),0) AS notification_click_rate 
        , SUM(marketing_sms_view) AS marketing_sms_view
        , SUM(marketing_sms_view) / nullif(SUM(notification_click),0) AS marketing_sms_view_rate
        , SUM(marketing_sms_click) AS marketing_sms_click
        , SUM(marketing_sms_click) / nullif(SUM(marketing_sms_view),0) AS marketing_sms_click_rate
        , SUM(preference_view) AS preference_view
        , SUM(preference_view) / nullif(SUM(marketing_sms_click),0) AS preference_view_rate
        , SUM(preference_click) AS preference_click
        , SUM(preference_click) / nullif(SUM(preference_view),0) AS preference_click_rate
        , SUM(att_view) AS att_view
        , SUM(att_view) / nullif(SUM(preference_click),0) AS att_view_rate
        , SUM(att_click) AS att_click
        , SUM(att_click) / nullif(SUM(att_view),0) AS att_click_rate   
        , SUM(end_page_view)  AS end_page_view
        , SUM(end_page_view) / nullif(SUM(att_click),0) AS end_page_view_rate
        , SUM(promo_view)  AS promo_view
        , SUM(promo_view) / nullif(SUM(end_page_view),0) AS promo_view_rate
        , SUM(end_page_click)  AS end_page_click
        , SUM(end_page_click) / nullif(SUM(end_page_view),0) AS end_page_click_rate  
        , SUM(end_page_click) / NULLIF(SUM(welcome_back_view), 0) as onboarding_completion
FROM funnel 
GROUP BY 1
ORDER BY 1 desc
;


