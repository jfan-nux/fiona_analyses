-- Analysis of campaign redemptions by engagement program
-- Maintains user_id - campaign_id granularity from cx_ios_reonboarding_campaign_user_level
-- Joins engagement program data using campaign_id to tag mapping

SET start_date = CURRENT_DATE - 29;
SET end_date = CURRENT_DATE;

WITH campaign_to_tag_mapping AS (
  -- Mapping of campaign_id to PROGRAM_EXPERIMENT_VARIANT (tag)
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
engagement_program_filtered AS (
  -- Get engagement program events for the relevant time period and programs
  SELECT
    ep.CONSUMER_ID,
    ep.PROGRAM_NAME,
    ep.PROGRAM_EXPERIMENT_VARIANT,
    ep.TIMESTAMP AS program_timestamp
  FROM SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep
  WHERE ep.TIMESTAMP BETWEEN $start_date AND $end_date
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
)
SELECT
  -- All columns from campaign user level table
  cu.*,
  
  -- Engagement program information (using mapping)
  map.tag AS mapped_program_variant,
  ep.PROGRAM_NAME,
  ep.PROGRAM_EXPERIMENT_VARIANT,
  ep.program_timestamp,
  
  -- Flag if program matches
  CASE 
    WHEN ep.PROGRAM_NAME IS NOT NULL THEN 1 
    ELSE 0 
  END AS has_engagement_program_match

FROM proddb.fionafan.cx_ios_reonboarding_campaign_user_level cu

-- Join to mapping to get the expected program variant for this campaign
LEFT JOIN campaign_to_tag_mapping map
  ON cu.campaign_id = map.campaign_id

-- Join to engagement programs using consumer_id and matching on the mapped tag
LEFT JOIN engagement_program_filtered ep
  ON ep.CONSUMER_ID::varchar = cu.user_id::varchar
  AND ep.PROGRAM_EXPERIMENT_VARIANT = map.tag

ORDER BY cu.user_id, cu.campaign_id;
