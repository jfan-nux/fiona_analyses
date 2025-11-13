/*
Break down onboarding_start_funnel_curr cohort by promo eligibility
Joins with cx_ios_reonboarding_promo_user_level to determine eligibility
*/

-- Overview of promo eligibility breakdown
WITH funnel_with_promo AS (
    SELECT 
        f.day,
        f.consumer_id,
        f.dd_device_id,
        f.dd_device_id_filtered,
        f.dd_platform,
        f.onboarding_type,
        f.promo_title,
        f.onboarding_page,
        COALESCE(p.is_promo_eligible, 0) AS is_promo_eligible,
        p.tag AS experiment_tag,
        p.num_programs_eligible,
        p.saw_promo_page,
        p.did_reonboarding_flow,
        p.has_redeemed
    FROM proddb.public.onboarding_start_funnel_curr f
    LEFT JOIN proddb.fionafan.cx_ios_reonboarding_promo_user_level p
        ON f.dd_device_id = p.dd_device_id
    where onboarding_type = 'resurrected_user'
)
SELECT 
    -- day,
    -- onboarding_type,
    is_promo_eligible,
    experiment_tag,
    COUNT(DISTINCT dd_device_id) AS unique_devices,
    COUNT(DISTINCT consumer_id) AS unique_consumers,
    COUNT(*) AS total_events,
    -- Promo interaction metrics
    SUM(saw_promo_page) AS users_saw_promo,
    SUM(did_reonboarding_flow) AS users_did_reonboarding,
    SUM(has_redeemed) AS users_redeemed,
    -- Percentages
    SUM(saw_promo_page) / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS pct_saw_promo,
    SUM(did_reonboarding_flow) / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS pct_did_reonboarding,
    SUM(has_redeemed) / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS pct_redeemed
FROM funnel_with_promo
GROUP BY ALL
order by all;


-- Summary by promo eligibility
SELECT 
    CASE 
        WHEN is_promo_eligible = 1 THEN 'Promo Eligible'
        WHEN is_promo_eligible = 0 THEN 'Not Promo Eligible'
        ELSE 'Unknown'
    END AS promo_eligibility_status,
    onboarding_type,
    experiment_tag,
    COUNT(DISTINCT dd_device_id) AS unique_devices,
    COUNT(DISTINCT consumer_id) AS unique_consumers,
    -- Promo metrics
    SUM(saw_promo_page) AS total_saw_promo,
    SUM(did_reonboarding_flow) AS total_did_reonboarding,
    SUM(has_redeemed) AS total_redeemed,
    -- Rates
    SUM(saw_promo_page)::FLOAT / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS promo_view_rate,
    SUM(did_reonboarding_flow)::FLOAT / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS reonboarding_rate,
    SUM(has_redeemed)::FLOAT / NULLIF(COUNT(DISTINCT dd_device_id), 0) AS redemption_rate
FROM (
    SELECT 
        f.dd_device_id,
        f.consumer_id,
        f.onboarding_type,
        COALESCE(p.is_promo_eligible, 0) AS is_promo_eligible,
        p.tag AS experiment_tag,
        p.saw_promo_page,
        p.did_reonboarding_flow,
        p.has_redeemed
    FROM proddb.public.onboarding_start_funnel_curr f
    LEFT JOIN proddb.fionafan.cx_ios_reonboarding_promo_user_level p
        ON f.dd_device_id = p.dd_device_id
)
GROUP BY ALL
ORDER BY promo_eligibility_status, onboarding_type, experiment_tag;


-- Detailed view with promo titles
SELECT 
    f.promo_title,
    CASE 
        WHEN p.is_promo_eligible = 1 THEN 'Promo Eligible'
        WHEN p.is_promo_eligible = 0 THEN 'Not Promo Eligible'
        ELSE 'Unknown'
    END AS promo_eligibility_status,
    f.onboarding_type,
    p.tag AS experiment_tag,
    COUNT(DISTINCT f.dd_device_id) AS unique_devices,
    COUNT(DISTINCT f.consumer_id) AS unique_consumers,
    SUM(p.saw_promo_page) AS saw_promo_count,
    SUM(p.did_reonboarding_flow) AS reonboarding_count,
    SUM(p.has_redeemed) AS redemption_count
FROM proddb.public.onboarding_start_funnel_curr f
LEFT JOIN proddb.fionafan.cx_ios_reonboarding_promo_user_level p
    ON f.dd_device_id = p.dd_device_id
GROUP BY ALL
ORDER BY unique_devices DESC;


-- ============================================================================
-- NOTIFICATION PAGE VIEW ANALYSIS BY PROMO ELIGIBILITY (TREATMENT ONLY)
-- Date Range: 09/08/2025 - 11/03/2025
-- ============================================================================

WITH notif_view AS (
    SELECT 
        CAST(iguazu_timestamp AS DATE) AS day, 
        consumer_id,
        COUNT(*) AS notification_page_views
    FROM iguazu.consumer.M_onboarding_page_view_ice
    WHERE iguazu_timestamp >= '2025-09-08' 
        AND iguazu_timestamp < '2025-11-04'  -- Exclusive upper bound
        AND LOWER(page) = 'notification'
    GROUP BY 1, 2
),
promo_treatment_users AS (
    SELECT 
        user_id,
        dd_device_id,
        tag,
        is_promo_eligible,
        exposure_time,
        saw_promo_page,
        did_reonboarding_flow,
        has_redeemed
    FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
    WHERE tag = 'treatment'
        AND exposure_time >= '2025-09-08'
        AND exposure_time < '2025-11-04'
)
SELECT 
    CASE 
        WHEN p.is_promo_eligible = 1 THEN 'Promo Eligible'
        WHEN p.is_promo_eligible = 0 THEN 'Not Promo Eligible'
    END AS promo_eligibility_status,
    COUNT(DISTINCT p.dd_device_id) AS total_treatment_users,
    COUNT(DISTINCT p.user_id) AS total_treatment_consumers,
    COUNT(DISTINCT n.consumer_id) AS users_saw_notification,
    SUM(n.notification_page_views) AS total_notification_views,
    -- Rates
    ROUND(COUNT(DISTINCT n.consumer_id)::FLOAT / 
          NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS notification_view_rate,
    ROUND(AVG(n.notification_page_views), 2) AS avg_views_per_viewer,
    -- Other engagement metrics
    SUM(p.saw_promo_page) AS users_saw_promo_page,
    sum(p.saw_promo_page) / nullif(count(distinct p.user_id), 0) as pct_saw_promo_page,
    -- SUM(p.did_reonboarding_flow) AS users_did_reonboarding,
    SUM(p.has_redeemed) AS users_redeemed
FROM promo_treatment_users p
LEFT JOIN notif_view n
    ON p.user_id = n.consumer_id
GROUP BY p.is_promo_eligible
ORDER BY p.is_promo_eligible DESC;


-- Daily breakdown of notification views (Treatment only)
WITH notif_view AS (
    SELECT 
        CAST(iguazu_timestamp AS DATE) AS day, 
        consumer_id
    FROM iguazu.consumer.M_onboarding_page_view_ice
    WHERE iguazu_timestamp >= '2025-09-08' 
        AND iguazu_timestamp < '2025-11-04'
        AND LOWER(page) = 'notification'
    GROUP BY 1, 2
),
promo_treatment_users AS (
    SELECT 
        user_id,
        dd_device_id,
        tag,
        is_promo_eligible,
        CAST(exposure_time AS DATE) AS exposure_day
    FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
    WHERE tag = 'treatment'
        AND exposure_time >= '2025-09-08'
        AND exposure_time < '2025-11-04'
)
SELECT 
    n.day,
    CASE 
        WHEN p.is_promo_eligible = 1 THEN 'Promo Eligible'
        WHEN p.is_promo_eligible = 0 THEN 'Not Promo Eligible'
    END AS promo_eligibility_status,
    COUNT(DISTINCT n.consumer_id) AS users_saw_notification,
    COUNT(DISTINCT p.user_id) AS total_treatment_users_exposed
FROM notif_view n
LEFT JOIN promo_treatment_users p
    ON n.consumer_id = p.user_id
    AND n.day >= p.exposure_day  -- Only count views after exposure
WHERE p.user_id IS NOT NULL
GROUP BY n.day, p.is_promo_eligible
ORDER BY n.day DESC, p.is_promo_eligible DESC;

