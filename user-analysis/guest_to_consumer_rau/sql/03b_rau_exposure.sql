/*
Purpose: Track RAU (Retention Action Unit / Engagement Milestone) exposure
Creates: proddb.fionafan.guest_vs_onboarding_august_rau
*/

CREATE OR REPLACE TABLE proddb.fionafan.guest_vs_onboarding_august_rau AS
SELECT 
    c.dd_device_id_filtered,
    c.consumer_id,
    c.cohort_date,
    c.cohort_type,
    c.platform,
    
    -- RAU exposure flags (1 if saw RAU, 0 if not)
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 1 THEN 1 ELSE 0 END) as d1_saw_rau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 7 THEN 1 ELSE 0 END) as d7_saw_rau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 14 THEN 1 ELSE 0 END) as d14_saw_rau,
    MAX(CASE WHEN DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 28 THEN 1 ELSE 0 END) as d28_saw_rau,
    
    -- First RAU view timing
    MIN(DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date)) as days_to_first_rau,
    
    -- Count unique milestones seen in first 28 days
    COUNT(DISTINCT CASE WHEN DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 28 THEN m.milestone_type END) as unique_milestones_seen_d28

FROM proddb.fionafan.guest_vs_onboarding_august_cohorts c
LEFT JOIN IGUAZU.CONSUMER.M_ENGAGEMENT_MILESTONE_VIEW_ICE m
    ON c.dd_device_id_filtered = replace(lower(CASE WHEN m.dd_device_id like 'dx_%' then m.dd_device_id
                                                    else 'dx_'||m.dd_device_id end), '-')
    AND convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date >= c.cohort_date
    AND DATEDIFF(day, c.cohort_date, convert_timezone('UTC','America/Los_Angeles', m.iguazu_timestamp)::date) <= 28
GROUP BY 
    c.dd_device_id_filtered,
    c.consumer_id,
    c.cohort_date,
    c.cohort_type,
    c.platform;

-- Verify RAU exposure rates
SELECT 
    cohort_type,
    platform,
    COUNT(*) as user_count,
    AVG(d1_saw_rau) as d1_rau_exposure_rate,
    AVG(d7_saw_rau) as d7_rau_exposure_rate,
    AVG(d14_saw_rau) as d14_rau_exposure_rate,
    AVG(d28_saw_rau) as d28_rau_exposure_rate,
    AVG(unique_milestones_seen_d28) as avg_milestones_d28
FROM proddb.fionafan.guest_vs_onboarding_august_rau
GROUP BY ALL
ORDER BY cohort_type, platform;

