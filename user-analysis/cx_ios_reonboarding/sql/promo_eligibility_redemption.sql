-- User Level Table with Promo Eligibility and Redemption Flags  
-- Base: Left join from experiment exposures
-- Granularity: user_id

CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_promo_user_level AS (

WITH campaign_ids AS (
  -- All campaign IDs from the reonboarding campaigns
  SELECT '6fb7311c-bd92-4be8-9f39-b6d47a3ebf4f' AS id UNION ALL
  SELECT '1249c207-e19e-483b-a1f4-8c0e84735496' UNION ALL
  SELECT '972621ec-85b8-459e-9af5-8b2e05f5acf0' UNION ALL
  SELECT '93704378-a991-4910-8a2d-f7617b701969' UNION ALL
  SELECT 'fe49c7ee-0b98-43d1-8767-a8a59ed24c01' UNION ALL
  SELECT '6a80aef8-4264-4941-ac2f-a4250367e44e' UNION ALL
  SELECT 'a633de5c-44ea-4b13-bc65-e5e5767193de' UNION ALL
  SELECT '1a380b31-4699-4461-b3b3-cdeb15ba6e8b' UNION ALL
  SELECT '8129beb8-c49f-451c-b35c-cb5fede5efd2' UNION ALL
  SELECT 'dc43bf44-637a-43d3-b47d-b2949ffd2399' UNION ALL
  SELECT '6cf4f0d5-9aa8-4d85-ae3c-12b126d475bb' UNION ALL
  SELECT '93979c47-e1e8-4696-a350-501703d6948f' UNION ALL
  SELECT 'e3faf09b-cf59-4771-a515-b18a8e61b6fb' UNION ALL
  SELECT 'd6bdc3d5-ece6-4a61-aaca-e858e811960a' UNION ALL
  SELECT 'b1058c7b-5d82-47f6-9a85-dd47bb9640f2' UNION ALL
  SELECT 'c94b881b-e186-4f8f-b66e-3c26263871c1' UNION ALL
  SELECT '41c827fe-69b9-4065-a690-94dc0af2791c' UNION ALL
  SELECT '19a1184e-5dcc-4ea7-b3de-eab5d3ac6a17' UNION ALL
  SELECT 'd6a236c2-3e41-43c9-97ea-efeb8350b4a9' UNION ALL
  SELECT '70e828df-1a62-4764-bbcf-b4c23a229fd7' UNION ALL
  SELECT '4eda12a8-6869-413d-bf6b-38b252c94a33' UNION ALL
  SELECT '7e6d2cb4-22c1-4f10-a558-b119f29d0c87' UNION ALL
  SELECT 'e328fbf9-4e1b-4fdb-9d76-3d8daaa79e62' UNION ALL
  SELECT 'afaad922-a28c-4712-9aeb-d4d9d646dcc1' UNION ALL
  SELECT '922fd2cd-3a62-4f33-9216-7b00dbe24568' UNION ALL
  SELECT '95f24f7c-f2f0-485e-ab76-5937f9e51697' UNION ALL
  SELECT 'e5525135-de61-4d64-afa2-7ba415158f95' UNION ALL
  SELECT '68557a8c-2f1a-4f16-a7e1-09fcf21e3254' UNION ALL
  SELECT '08e4b4cc-64c3-442a-80bc-f6a48400897c' UNION ALL
  SELECT 'e8b840f3-1778-493f-84cb-d2333216d694' UNION ALL
  SELECT 'cbf2f472-434c-46d2-8812-3320907e25d1/info' UNION ALL
  SELECT 'df2d3da8-c90f-465a-b032-ebebd8bf11fc' UNION ALL
  SELECT '6f5af820-e8fa-4646-a3e7-dfe116dcb9ff' UNION ALL
  SELECT 'c19a1b53-a6d4-4ddd-9a7b-64177c7ac7aa' UNION ALL
  SELECT 'e76df8ed-f329-4061-b58b-acddcf372afd' UNION ALL
  SELECT 'beada144-87be-439e-a2e5-f70e778472c0' UNION ALL
  SELECT 'b71aa829-6234-4d6f-adda-e98ab9d876f8' UNION ALL
  SELECT '8d0540a6-86b7-4ccc-923e-866c23139271/' UNION ALL
  SELECT '98a6aa76-001f-4f91-afd7-4e17158dbbfb' UNION ALL
  SELECT '22e99af-6b54-4a6b-b94e-ae368e738f3b' UNION ALL
  SELECT 'f0938385-96ca-4f43-9dbd-e4a52a9e6f27' UNION ALL
  SELECT '1dafdb62-fc55-41b4-93b7-5416c04de749'
),

clean_campaign_ids AS (
  SELECT DISTINCT REGEXP_REPLACE(id, '(/info|/)$', '') AS campaign_id
  FROM campaign_ids
),

-- Get promo eligibility (users who were targeted in engagement programs)
promo_eligible AS (
    SELECT DISTINCT
        ep.CONSUMER_ID,
        1 AS is_promo_eligible,
        COUNT(DISTINCT ep.PROGRAM_NAME) AS num_programs_eligible
    FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp
    INNER JOIN SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep 
        ON REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '')::varchar = ep.CONSUMER_ID::varchar
        AND exp.exposure_time>ep.timestamp
    WHERE ep.TIMESTAMP >= '2025-09-01'
        AND ep.PROGRAM_NAME IN (
            'ep_consumer_dormant_late_bloomers_us_v1', 
            'ep_consumer_dormant_winback_us_v1', 
            'ep_consumer_dewo_phase2_us_v1', 
            'ep_consumer_dewo_phase3_us_v1', 
            'ep_consumer_dewo_phase1_retarget_us_v1', 
            'ep_consumer_ml_churn_prevention_us_v1_p1_active', 
            'ep_consumer_ml_churn_prevention_us_v1_p1_dormant', 
            'ep_consumer_ml_churn_prevention_us_v1_p2_active_active', 
            'ep_consumer_ml_churn_prevention_us_v1_p2_active_dormant', 
            'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_active', 
            'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_dormant', 
            'ep_consumer_dormant_churned_browsers_us_v1', 
            'ep_consumer_enhanced_rxauto_90d_us_v1', 
            'ep_consumer_enhanced_rxauto_120d_test_us_v1', 
            'ep_consumer_enhanced_rxauto_150day_test_us_v1', 
            'ep_consumer_enhanced_rxauto_180day_test_us_v1', 
            'ep_consumer_churned_btm_pickup_exclude_test_us_v1', 
            'ep_consumer_churned_latebloomers_auto_ctc_test_us', 
            'ep_consumer_rx_reachability_auto_us', 
            'ep_consumer_repeatchurned_us', 
            'ep_consumer_very_churned_med_vp_us_v1', 
            'ep_consumer_super_churned_low_vp_us_v1', 
            'ep_consumer_super_churned_med_vp_us_v1', 
            'ep_consumer_churned_low_vp_us_v1', 
            'ep_consumer_churned_med_vp_us_v1'
        )
        AND ep.PROGRAM_EXPERIMENT_VARIANT IS NOT NULL
        AND LOWER(ep.PROGRAM_EXPERIMENT_VARIANT) NOT LIKE '%control%'
    GROUP BY ep.CONSUMER_ID
),

-- Get users who actually saw the promo page
saw_promo AS (
    SELECT DISTINCT
        consumer_id,
        1 AS saw_promo_page,
        COUNT(DISTINCT iguazu_id) AS promo_page_views
    FROM iguazu.consumer.M_onboarding_end_promo_page_view_ice
    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-09-08'
        AND lower(onboarding_type) = 'resurrected_user'
        and  (position('%', promo_title) > 0 or position('$', promo_title)>0)
    GROUP BY consumer_id
),
did_reonboarding AS (
    SELECT DISTINCT
        consumer_id,
        1 AS did_reonboarding_flow,
        COUNT(DISTINCT iguazu_id) AS reonboarding_clicks
    FROM iguazu.consumer.M_onboarding_page_click_ice
    WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-09-08'
        AND lower(onboarding_type) = 'resurrected_user'
        AND page = 'welcomeBack'
        AND click_type = 'primary'
    GROUP BY consumer_id
),
-- Get redemptions (using existing campaign user level table)
redemptions AS (
    SELECT
        user_id AS CONSUMER_ID,
        1 AS has_redeemed,
        SUM(redemptions_since_2025_09_01) AS redemption_count,
        COUNT(DISTINCT campaign_id) AS unique_campaigns_redeemed,
        SUM(total_discount_usd) AS total_discount_usd,
        MAX(last_redeemed_at) AS last_redemption_date
    FROM proddb.fionafan.cx_ios_reonboarding_campaign_user_level
    WHERE redemptions_since_2025_09_01 > 0
    GROUP BY user_id
)

-- Main query: Left join everything to exposures
SELECT
    exp.consumer_id AS user_id,
    exp.bucket_key AS dd_device_id,
    exp.exposure_time,
    exp.tag,
    exp.result,
    
    -- Promo eligibility flag (1/0) - targeted by engagement program
    COALESCE(pe.is_promo_eligible, 0) AS is_promo_eligible,
    COALESCE(pe.num_programs_eligible, 0) AS num_programs_eligible,
    
    -- Saw promo page flag (1/0) - actually viewed promo
    COALESCE(sp.saw_promo_page, 0) AS saw_promo_page,
    COALESCE(sp.promo_page_views, 0) AS promo_page_views,
    
    -- Did reonboarding flow flag (1/0) - clicked through welcome back
    COALESCE(dr.did_reonboarding_flow, 0) AS did_reonboarding_flow,
    COALESCE(dr.reonboarding_clicks, 0) AS reonboarding_clicks,
    
    -- Redemption flag (1/0)
    COALESCE(red.has_redeemed, 0) AS has_redeemed,
    COALESCE(red.redemption_count, 0) AS redemption_count,
    COALESCE(red.unique_campaigns_redeemed, 0) AS unique_campaigns_redeemed,
    COALESCE(red.total_discount_usd, 0) AS total_discount_usd,
    red.last_redemption_date

FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures exp

-- Left join to get promo eligibility (strip quotes from consumer_id)
LEFT JOIN promo_eligible pe
    ON REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '')::varchar = pe.consumer_id::varchar

-- Left join to get users who saw promo page (strip quotes from consumer_id)
LEFT JOIN saw_promo sp
    ON REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '')::varchar = sp.consumer_id::varchar

-- Left join to get users who did reonboarding flow (strip quotes from consumer_id)
LEFT JOIN did_reonboarding dr
    ON REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '')::varchar = dr.consumer_id::varchar

-- Left join to get redemptions (strip quotes from consumer_id)
LEFT JOIN redemptions red
    ON REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '')::varchar = red.consumer_id::varchar

-- Deduplicate: one row per user
QUALIFY ROW_NUMBER() OVER (PARTITION BY REPLACE(REPLACE(exp.consumer_id, '"', ''), '\\', '') ORDER BY exp.exposure_time) = 1
);



select *
from  iguazu.consumer.M_onboarding_page_view_ice
WHERE convert_timezone('UTC','America/Los_Angeles', iguazu_timestamp) >= '2025-09-08'
and lower(onboarding_type) = 'resurrected_user'
and page = 'welcomeBack'
and dd_device_id ilike'%F4C064ED%';

select * 
FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice

    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-09-08'
        AND lower(onboarding_type) = 'resurrected_user'
        -- and  (position('%', promo_title) > 0 or position('$', promo_title)>0)
        and dd_device_id ilike'%F4C064ED%';



grant select on proddb.fionafan.cx_ios_reonboarding_promo_user_level  to public;
select * from proddb.fionafan.cx_ios_reonboarding_promo_user_level  where user_id = '323879498';



-- select num_programs_eligible, count(1) cnt from proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' group by all;
select count(1) as cnt, avg(is_promo_eligible) as avg_is_promo_eligible, avg(saw_promo_page) as avg_saw_promo_page, avg(has_redeemed) as avg_has_redeemed 
from proddb.fionafan.cx_ios_reonboarding_promo_user_level a
-- inner join METRICS_REPO.PUBLIC.enable_post_onboarding_in_consumer_targeting_exposures b
-- on a.user_id = b.bucket_key and experiment_group <>'control'
where tag = 'treatment' and exposure_time>='2025-09-25';

select * from proddb.fionafan.cx_ios_reonboarding_promo_user_level 
where tag = 'treatment' and exposure_time>='2025-09-25' 
and is_promo_eligible = 1 and saw_promo_page = 0 and did_reonboarding_flow = 1
limit 10;

-- Pivot view: Did Reonboarding vs. Eligible for Promo
SELECT
    did_reonboarding_flow,
    is_promo_eligible,
    COUNT(*) AS user_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
WHERE tag = 'treatment' AND exposure_time >= '2025-09-25'
GROUP BY did_reonboarding_flow, is_promo_eligible
ORDER BY did_reonboarding_flow DESC, is_promo_eligible DESC;
select * from
SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep
    WHERE ep.TIMESTAMP >= '2025-09-01'
    and consumer_id = '323879498'
        AND ep.PROGRAM_NAME IN (
            'ep_consumer_dormant_late_bloomers_us_v1',
            'ep_consumer_dormant_winback_us_v1',
            'ep_consumer_dewo_phase2_us_v1',
            'ep_consumer_dewo_phase3_us_v1',
            'ep_consumer_dewo_phase1_retarget_us_v1',
            'ep_consumer_ml_churn_prevention_us_v1_p1_active',
            'ep_consumer_ml_churn_prevention_us_v1_p1_dormant',
            'ep_consumer_ml_churn_prevention_us_v1_p2_active_active',
            'ep_consumer_ml_churn_prevention_us_v1_p2_active_dormant',
            'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_active',
            'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_dormant',
            'ep_consumer_dormant_churned_browsers_us_v1',
            'ep_consumer_enhanced_rxauto_90d_us_v1',
            'ep_consumer_enhanced_rxauto_120d_test_us_v1',
            'ep_consumer_enhanced_rxauto_150day_test_us_v1',
            'ep_consumer_enhanced_rxauto_180day_test_us_v1',
            'ep_consumer_churned_btm_pickup_exclude_test_us_v1',
            'ep_consumer_churned_latebloomers_auto_ctc_test_us',
            'ep_consumer_rx_reachability_auto_us',
            'ep_consumer_repeatchurned_us',
            'ep_consumer_very_churned_med_vp_us_v1',
            'ep_consumer_super_churned_low_vp_us_v1',
            'ep_consumer_super_churned_med_vp_us_v1',
            'ep_consumer_churned_low_vp_us_v1',
            'ep_consumer_churned_med_vp_us_v1'
        )
        AND ep.PROGRAM_EXPERIMENT_VARIANT IS NOT NULL
        AND LOWER(ep.PROGRAM_EXPERIMENT_VARIANT) NOT LIKE '%control%';


    SELECT DISTINCT
        promo_title
    FROM iguazu.consumer.M_onboarding_end_promo_page_view_ice
    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-09-08'
        AND lower(onboarding_type) = 'resurrected_user'
        -- and  (position('%', promo_title) > 0 or position('$', promo_title)>0)
        and lower(promo_title) not like '%welcome%';
-- Summary stats
SELECT 
    'Total Users' AS metric,
    COUNT(*) AS count,
    NULL AS pct
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' and exposure_time>='2025-09-25'

UNION ALL

SELECT 
    'Users Eligible for Promo (in engagement program)' AS metric,
    SUM(is_promo_eligible) AS count,
    AVG(is_promo_eligible) * 100.0 AS pct
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' and exposure_time>='2025-09-25'

UNION ALL

SELECT 
    'Users Who Saw Promo Page' AS metric,
    SUM(saw_promo_page) AS count,
    AVG(saw_promo_page) * 100.0 AS pct
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' and exposure_time>='2025-09-25'

UNION ALL

SELECT 
    'Users Who Did Reonboarding Flow' AS metric,
    SUM(did_reonboarding_flow) AS count,
    AVG(did_reonboarding_flow) * 100.0 AS pct
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' and exposure_time>='2025-09-25'

UNION ALL

SELECT 
    'Users Who Redeemed Promo' AS metric,
    SUM(has_redeemed) AS count,
    AVG(has_redeemed) * 100.0 AS pct
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level where tag = 'treatment' and exposure_time>='2025-09-25';

-- Breakdown by treatment/control
SELECT
    result,
    COUNT(*) AS total_users,
    SUM(is_promo_eligible) AS users_promo_eligible,
    AVG(is_promo_eligible) * 100.0 AS pct_promo_eligible,
    SUM(saw_promo_page) AS users_saw_promo,
    AVG(saw_promo_page) * 100.0 AS pct_saw_promo,
    SUM(did_reonboarding_flow) AS users_did_reonboarding,
    AVG(did_reonboarding_flow) * 100.0 AS pct_did_reonboarding,
    SUM(has_redeemed) AS users_redeemed,
    AVG(has_redeemed) * 100.0 AS pct_redeemed
FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
GROUP BY result
ORDER BY result;

select * from proddb.fionafan.cx_ios_reonboarding_promo_user_level 
where tag = 'treatment' and exposure_time>='2025-09-25' 
and is_promo_eligible = 1 and saw_promo_page = 0
limit 10;