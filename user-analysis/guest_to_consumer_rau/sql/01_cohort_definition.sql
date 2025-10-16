/*
Purpose: Define August 2025 cohorts - Guest-to-Consumer vs Onboarding Signups
Creates: proddb.fionafan.guest_vs_onboarding_july_cohorts
*/

CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_cohorts AS

-- Guest-to-Consumer iOS
SELECT DISTINCT
    DD_DEVICE_ID,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                      else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_id_filtered,
    'ios' as platform,
    'guest_to_consumer' as cohort_type,
    consumer_id,
    min(TIMESTAMP::date) as cohort_date,
    min(TIMESTAMP) as cohort_timestamp,
FROM SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.M_REGISTRATION_SUCCESS
WHERE TIMESTAMP BETWEEN '2025-08-01' AND '2025-08-31'
    AND CONTEXT_DEVICE_TYPE = 'ios'
    AND SOURCE != 'guest'
    AND IS_GUEST = 'true'
group by all
UNION ALL

-- Guest-to-Consumer Android
SELECT DISTINCT
 DD_DEVICE_ID,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                      else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_id_filtered,
    'android' as platform,
    'guest_to_consumer' as cohort_type,
    consumer_id::VARCHAR as consumer_id,
    min(IGUAZU_TIMESTAMP::date) as cohort_date,
    min(IGUAZU_TIMESTAMP) as cohort_timestamp
FROM IGUAZU.CONSUMER.M_SOCIAL_SIGNUP_ICE
WHERE IGUAZU_TIMESTAMP BETWEEN '2025-08-01' AND '2025-08-31'
    AND DD_PLATFORM = 'Android'
    AND TARGET_APP = 'CONSUMER'
    AND EVENT = 'signup_complete'
    AND IS_GUEST_CONSUMER = true
group by all

UNION ALL

-- Onboarding Users (iOS + Android)
SELECT DISTINCT
 DD_DEVICE_ID,
    replace(lower(CASE WHEN DD_DEVICE_ID like 'dx_%' then DD_DEVICE_ID
                      else 'dx_'||DD_DEVICE_ID end), '-') AS dd_device_id_filtered,
    lower(dd_platform) as platform,
    'onboarding' as cohort_type,
    consumer_id::VARCHAR as consumer_id,
        min(IGUAZU_TIMESTAMP::date) AS cohort_date,
    min(IGUAZU_TIMESTAMP) as cohort_timestamp
FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
WHERE IGUAZU_TIMESTAMP BETWEEN '2025-08-01' AND '2025-08-31'
group by all;


-- Verify counts
SELECT 
    platform,
    cohort_type,
    COUNT(DISTINCT dd_device_id_filtered) as unique_users,
    MIN(cohort_date) as first_date,
    MAX(cohort_date) as last_date
FROM proddb.fionafan.guest_vs_onboarding_august_cohorts
GROUP BY ALL
ORDER BY platform, cohort_type;

select distinct IS_GUEST_CONSUMER, max(IGUAZU_TIMESTAMP), count(1) from IGUAZU.CONSUMER.M_SOCIAL_SIGNUP_ICE 
where IGUAZU_TIMESTAMP between '2025-08-01'::date and '2025-08-31'::date    
group by all  ;

select date_trunc('day', IGUAZU_TIMESTAMP) as date, count(1) from IGUAZU.CONSUMER.M_SOCIAL_SIGNUP_ICE group by all;