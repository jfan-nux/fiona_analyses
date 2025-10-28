-- Promo Funnel Analysis by Campaign with Lift Calculation
-- Shows: Eligibility rate, Saw promo rate, Redemption rate
-- Calculates lift for each step by campaign

CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_promo_campaign_funnel AS (WITH raw_ids AS (
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

clean_ids AS (
  SELECT DISTINCT REGEXP_REPLACE(id, '(/info|/)$', '') AS campaign_id
  FROM raw_ids
),

campaign_to_tag_mapping AS (
  SELECT '6fb7311c-bd92-4be8-9f39-b6d47a3ebf4f' AS campaign_id, 'can-js-npws-promo-test-h2-25-trt-40off1' AS tag UNION ALL
  SELECT '1249c207-e19e-483b-a1f4-8c0e84735496', 'AUS-npws-promo-treatment' UNION ALL
  SELECT '972621ec-85b8-459e-9af5-8b2e05f5acf0', 'ep_aus_F28D_treatment2' UNION ALL
  SELECT '93704378-a991-4910-8a2d-f7617b701969', 'can-js-npws-promo-test-h2-25-trt-40off1' UNION ALL
  SELECT 'fe49c7ee-0b98-43d1-8767-a8a59ed24c01', 'can-js-npws-promo-v4-0-orders' UNION ALL
  SELECT '6a80aef8-4264-4941-ac2f-a4250367e44e', 'can-js-npws-promo-test-h2-25-trt-40off3' UNION ALL
  SELECT 'a633de5c-44ea-4b13-bc65-e5e5767193de', 'can-js-npws-promo-test-h2-25-trt-50off1' UNION ALL
  SELECT '1a380b31-4699-4461-b3b3-cdeb15ba6e8b', 'NZ_NewCx_30' UNION ALL
  SELECT '8129beb8-c49f-451c-b35c-cb5fede5efd2', 'NZ_NewCx_50' UNION ALL
  SELECT 'dc43bf44-637a-43d3-b47d-b2949ffd2399', 'npws_45d_t1' UNION ALL
  SELECT '6cf4f0d5-9aa8-4d85-ae3c-12b126d475bb', 'npws_30d_t2' UNION ALL
  SELECT '93979c47-e1e8-4696-a350-501703d6948f', 'npws_14d_t3' UNION ALL
  SELECT 'e3faf09b-cf59-4771-a515-b18a8e61b6fb', 'npws_no_siw_t1' UNION ALL
  SELECT 'd6bdc3d5-ece6-4a61-aaca-e858e811960a', 'npws_siw_t2' UNION ALL
  SELECT 'b1058c7b-5d82-47f6-9a85-dd47bb9640f2', 'npws-fobt-interim-40off1' UNION ALL
  SELECT 'c94b881b-e186-4f8f-b66e-3c26263871c1', 'ep_au_rx_resurrection_automation_90d_treatment' UNION ALL
  SELECT '41c827fe-69b9-4065-a690-94dc0af2791c', 'ep_au_rx_resurrection_automation_120d_treatment' UNION ALL
  SELECT '19a1184e-5dcc-4ea7-b3de-eab5d3ac6a17', 'ep_au_rx_resurrection_automation_150d_treatment' UNION ALL
  SELECT 'd6a236c2-3e41-43c9-97ea-efeb8350b4a9', 'ep_nz_resurrection_automation_180d' UNION ALL
  SELECT '70e828df-1a62-4764-bbcf-b4c23a229fd7', 'ep_nz_resurrection_automation_150d_treatment' UNION ALL
  SELECT '4eda12a8-6869-413d-bf6b-38b252c94a33', 'ep_consumer_very_churned_low_vp_us_v1_t1' UNION ALL
  SELECT '7e6d2cb4-22c1-4f10-a558-b119f29d0c87', 'ep_consumer_very_churned_med_vp_us_v1_t1' UNION ALL
  SELECT 'e328fbf9-4e1b-4fdb-9d76-3d8daaa79e62', 'ep_consumer_super_churned_low_vp_us_v1_t1' UNION ALL
  SELECT 'afaad922-a28c-4712-9aeb-d4d9d646dcc1', 'ep_consumer_super_churned_low_vp_us_v1_t1' UNION ALL
  SELECT '922fd2cd-3a62-4f33-9216-7b00dbe24568', 'ep_consumer_churned_low_vp_us_v1_t1' UNION ALL
  SELECT '95f24f7c-f2f0-485e-ab76-5937f9e51697', 'ep_consumer_churned_med_vp_us_v1_t1' UNION ALL
  SELECT 'e5525135-de61-4d64-afa2-7ba415158f95', 'ep_consumer_dormant_late_bloomers_us_v1_t1' UNION ALL
  SELECT '68557a8c-2f1a-4f16-a7e1-09fcf21e3254', 'ep_consumer_dormant_winback_us_v1_t1' UNION ALL
  SELECT '08e4b4cc-64c3-442a-80bc-f6a48400897c', 'ep_consumer_dewo_phase2_us_v1_t1' UNION ALL
  SELECT 'e8b840f3-1778-493f-84cb-d2333216d694', 'ep_consumer_dewo_phase1_retarget_us_v1_t1' UNION ALL
  SELECT 'cbf2f472-434c-46d2-8812-3320907e25d1', 'ep_consumer_ml_churn_prevention_us_v2_p1_active_t1' UNION ALL
  SELECT 'df2d3da8-c90f-465a-b032-ebebd8bf11fc', 'ep_consumer_ml_churn_prevention_us_v2_p1_dormant_t1' UNION ALL
  SELECT '6f5af820-e8fa-4646-a3e7-dfe116dcb9ff', 'ep_consumer_ml_churn_prevention_us_v2_p2_active_active_t1' UNION ALL
  SELECT 'c19a1b53-a6d4-4ddd-9a7b-64177c7ac7aa', 'ep_consumer_ml_churn_prevention_us_v2_p2_active_dormant_t1' UNION ALL
  SELECT 'e76df8ed-f329-4061-b58b-acddcf372afd', 'ep_consumer_ml_churn_prevention_us_v2_p2_dormant_dormant_t1' UNION ALL
  SELECT 'beada144-87be-439e-a2e5-f70e778472c0', 'ep_consumer_enhanced_rxauto_90d_us_v1_t1' UNION ALL
  SELECT 'b71aa829-6234-4d6f-adda-e98ab9d876f8', 'ep_consumer_enhanced_rxauto_120d_test_us_v1_t2' UNION ALL
  SELECT '8d0540a6-86b7-4ccc-923e-866c23139271', 'ep_consumer_enhanced_rxauto_150day_test_us_v1_t1' UNION ALL
  SELECT '98a6aa76-001f-4f91-afd7-4e17158dbbfb', 'ep_consumer_enhanced_rxauto_180day_test_us_v1_t1' UNION ALL
  SELECT '22e99af-6b54-4a6b-b94e-ae368e738f3b', 'ep_consumer_churned_btm_pickup_exclude_test_us_v1_t2' UNION ALL
  SELECT 'f0938385-96ca-4f43-9dbd-e4a52a9e6f27', 'ep_consumer_churned_latebloomers_auto_ctc_test_us_v1_t1' UNION ALL
  SELECT '1dafdb62-fc55-41b4-93b7-5416c04de749', 'ep_consumer_rx_reachability_auto_us_t1'
),

clean_ids AS (
  SELECT DISTINCT REGEXP_REPLACE(id, '(/info|/)$', '') AS campaign_id
  FROM raw_ids
),

-- Get eligibility by campaign
new_again_cx AS (
    SELECT DISTINCT v.CONSUMER_ID
    FROM EDW.CONSUMER.COMBINED_GROWTH_ACCOUNTING_PT_BASE v
    INNER JOIN edw.growth.fact_consumer_app_open_events i 
        ON i.CONSUMER_ID = v.CONSUMER_ID
    WHERE v.CALENDAR_DATE >= '2025-09-01'
        AND v.VISIT_STATUS_90D_DEFINITION = 'resurrected_today'
),

eligible_by_campaign AS (
    SELECT DISTINCT
        ep.CONSUMER_ID,
        map.campaign_id,
        1 AS is_eligible
    FROM new_again_cx n
    INNER JOIN SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep 
        ON ep.CONSUMER_ID = n.CONSUMER_ID
    INNER JOIN campaign_to_tag_mapping map
        ON ep.PROGRAM_EXPERIMENT_VARIANT = map.tag
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
),

-- Get users who saw promo page (any promo)
-- Note: iguazu table doesn't have campaign_id, so we can't break down by campaign here
-- We'll use "saw any promo" as a proxy
saw_any_promo AS (
    SELECT DISTINCT
        consumer_id,
        1 AS saw_promo
    FROM iguazu.consumer.M_onboarding_end_promo_page_view_ice
    WHERE CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)::date >= '2025-01-01'
        AND lower(onboarding_type) = 'resurrected_user'
        AND promo_title IS NOT NULL
        AND promo_title != ''
),

-- Get redemptions by campaign
redemptions_by_campaign AS (
  SELECT
    r.CONSUMER_ID,
    r.CAMPAIGN_ID,
    MAX(r.CAMPAIGN_NAME) AS campaign_name,
    COUNT(*) AS redemptions_count,
    MAX(r.DELIVERY_ACTUAL_TIME) AS last_redeemed_at,
    SUM(COALESCE(r.DISCOUNT_SUBSIDY_USD, 0)) AS total_discount_usd
  FROM edw.ads.fact_promo_order_redemption r
  JOIN clean_ids c
    ON r.CAMPAIGN_ID = c.campaign_id
  WHERE
    r.DELIVERY_ACTIVE_DATE >= DATE '2025-09-01'
    AND COALESCE(r.BILLABLE_EVENT_STATUS, 'EVENT_STATUS_CREATE') <> 'EVENT_STATUS_CANCEL'
    AND COALESCE(r.IS_INCLUDED_BILLABLE_EVENT, TRUE)
  GROUP BY r.CONSUMER_ID, r.CAMPAIGN_ID
),

-- Get all users who had ANY interaction with each campaign
user_campaign_interactions AS (
    SELECT DISTINCT consumer_id, campaign_id FROM eligible_by_campaign
    UNION  
    SELECT DISTINCT consumer_id, campaign_id FROM redemptions_by_campaign
)

-- Build campaign-level funnel by joining to master features
SELECT
    uci.campaign_id,
    MAX(red.campaign_name) AS campaign_name,
    f.tag,
    f.result,
    COUNT(DISTINCT f.user_id) AS total_users,
    
    -- Eligibility
    SUM(COALESCE(elig.is_eligible, 0)) AS users_eligible,
    
    -- Saw promo (any promo, not campaign-specific)
    SUM(COALESCE(saw.saw_promo, 0)) AS users_saw_promo,
    
    -- Redeemed
    SUM(CASE WHEN red.redemptions_count > 0 THEN 1 ELSE 0 END) AS users_redeemed

FROM user_campaign_interactions uci

-- Join to master features to get tag/result
INNER JOIN proddb.fionafan.cx_ios_reonboarding_master_features_user_level f
    ON uci.consumer_id::varchar = f.user_id::varchar

LEFT JOIN eligible_by_campaign elig
    ON uci.consumer_id = elig.consumer_id
    AND uci.campaign_id = elig.campaign_id

LEFT JOIN saw_any_promo saw
    ON uci.consumer_id = saw.consumer_id

LEFT JOIN redemptions_by_campaign red
    ON uci.consumer_id = red.consumer_id
    AND uci.campaign_id = red.campaign_id

GROUP BY uci.campaign_id, f.tag, f.result
);

-- Calculate funnel rates and lift by campaign
WITH funnel_rates AS (
    SELECT
        campaign_id,
        campaign_name,
        result,
        total_users,
        users_eligible,
        users_saw_promo,
        users_redeemed,
        
        -- Rates
        ROUND(users_eligible * 100.0 / NULLIF(total_users, 0), 2) AS eligible_rate,
        ROUND(users_saw_promo * 100.0 / NULLIF(users_eligible, 0), 2) AS saw_rate_of_eligible,
        ROUND(users_redeemed * 100.0 / NULLIF(users_saw_promo, 0), 2) AS redeemed_rate_of_saw
    FROM proddb.fionafan.cx_ios_reonboarding_promo_campaign_funnel
),

lift_calc AS (
    SELECT
        campaign_id,
        MAX(campaign_name) AS campaign_name,
        
        -- Control metrics
        MAX(CASE WHEN result = 'false' THEN total_users END) AS control_total,
        MAX(CASE WHEN result = 'false' THEN eligible_rate END) AS control_eligible_rate,
        MAX(CASE WHEN result = 'false' THEN saw_rate_of_eligible END) AS control_saw_rate,
        MAX(CASE WHEN result = 'false' THEN redeemed_rate_of_saw END) AS control_redeem_rate,
        
        -- Treatment metrics
        MAX(CASE WHEN result = 'true' THEN total_users END) AS treatment_total,
        MAX(CASE WHEN result = 'true' THEN eligible_rate END) AS treatment_eligible_rate,
        MAX(CASE WHEN result = 'true' THEN saw_rate_of_eligible END) AS treatment_saw_rate,
        MAX(CASE WHEN result = 'true' THEN redeemed_rate_of_saw END) AS treatment_redeem_rate
    FROM funnel_rates
    GROUP BY campaign_id
)

SELECT
    campaign_id,
    campaign_name,
    control_total,
    treatment_total,
    
    -- Eligible rate and lift
    control_eligible_rate AS control_eligible_pct,
    treatment_eligible_rate AS treatment_eligible_pct,
    ROUND(treatment_eligible_rate - control_eligible_rate, 2) AS eligible_lift,
    
    -- Saw promo rate (of eligible) and lift
    control_saw_rate AS control_saw_pct,
    treatment_saw_rate AS treatment_saw_pct,
    ROUND(treatment_saw_rate - control_saw_rate, 2) AS saw_lift,
    
    -- Redemption rate (of saw promo) and lift
    control_redeem_rate AS control_redeem_pct,
    treatment_redeem_rate AS treatment_redeem_pct,
    ROUND(treatment_redeem_rate - control_redeem_rate, 2) AS redeem_lift

FROM lift_calc
ORDER BY treatment_total DESC;

