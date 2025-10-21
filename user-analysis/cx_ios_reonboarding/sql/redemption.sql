create or replace table proddb.fionafan.cx_ios_reonboarding_campaign_user_level as (
WITH raw_ids AS (
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
user_redemptions AS (
  SELECT
    r.CONSUMER_ID AS user_id,
    r.CAMPAIGN_ID,
    MAX(r.CAMPAIGN_NAME) AS campaign_name,
    COUNT(*) AS redemptions_since_2025_09_01,
    MAX(r.DELIVERY_ACTUAL_TIME) AS last_redeemed_at,
    -- Monetary fields: convert minor units to dollars where applicable
    SUM(COALESCE(r.DISCOUNT_SUBSIDY_USD, 0)) AS total_discount_usd,
    SUM(COALESCE(r.FEE_AMOUNT_USD, 0)) / 100.0 AS total_fee_usd,
    SUM(COALESCE(r.FEE_TAX_AMOUNT_USD, 0)) / 100.0 AS total_fee_tax_usd,
    CASE WHEN COUNT(*) > 0 THEN 'REDEEMED' ELSE 'NO_REDEMPTIONS_SINCE_2025_09_01' END AS redemption_status
  FROM edw.ads.fact_promo_order_redemption r
  JOIN clean_ids c
    ON r.CAMPAIGN_ID = c.campaign_id
  WHERE
    r.DELIVERY_ACTIVE_DATE >= DATE '2025-09-01'
    AND COALESCE(r.BILLABLE_EVENT_STATUS, 'EVENT_STATUS_CREATE') <> 'EVENT_STATUS_CANCEL'
    AND COALESCE(r.IS_INCLUDED_BILLABLE_EVENT, TRUE)
  GROUP BY
    r.CONSUMER_ID,
    r.CAMPAIGN_ID
)
SELECT
  ur.user_id,
  ur.campaign_id,
  ur.campaign_name,
  ur.redemptions_since_2025_09_01,
  ur.last_redeemed_at,
  ur.total_discount_usd,
  ur.total_fee_usd,
  ur.total_fee_tax_usd,
  ur.redemption_status,
  -- Features from master table (excluding user_id to avoid duplicate)
  f.exposure_time,
  f.exposure_day,
  f.tag,
  f.result,
  f.scd_consumer_id,
  f.scd_start_date,
  f.scd_end_date,
  f.signup_date,
  f.first_order_date,
  f.last_order_date,
  f.lifestage,
  f.lifestage_bucket,
  f.experience,
  f.business_vertical_line,
  f.country_id,
  f.region_name,
  f.submarket_id,
  f.tenure_days_at_exposure,
  f.days_since_first_order,
  f.days_since_last_order,
  f.store_content_visitor_count,
  f.has_store_content_visit,
  f.days_before_exposure_store_content,
  f.store_page_visitor_count,
  f.has_store_page_visit,
  f.days_before_exposure_store_page,
  f.order_cart_visitor_count,
  f.has_order_cart_visit,
  f.days_before_exposure_order_cart,
  f.checkout_visitor_count,
  f.has_checkout_visit,
  f.days_before_exposure_checkout,
  f.purchaser_count,
  f.has_purchase,
  f.days_before_exposure_purchase,
  f.platform,
  f.app_version,
  f.urban_type,
  f.is_dashpass,
  f.first_event_date,
  f.last_event_date,
  f.num_event_days,
  f.merchant_total_orders_ytd,
  f.unique_merchants_ytd,
  f.merchant_total_spend_ytd,
  f.unique_verticals_ytd,
  f.num_favorite_merchants,
  f.top_merchant_id,
  f.top_merchant_name,
  f.top_merchant_vertical,
  f.top_merchant_orders,
  f.top_merchant_pct_orders,
  f.max_merchant_order_concentration,
  f.avg_merchant_order_share,
  f.merchant_diversity,
  f.user_ordering_type,
  f.frequency_total_orders_ytd,
  f.distinct_order_days,
  f.unique_stores,
  f.frequency_total_spend_ytd,
  f.ytd_period_days,
  f.ytd_period_weeks,
  f.avg_orders_per_week,
  f.avg_orders_per_month,
  f.avg_days_between_orders,
  f.avg_days_between_orders_actual,
  f.median_days_between_orders,
  f.min_gap_between_orders,
  f.max_gap_between_orders,
  f.stddev_gap_between_orders,
  f.longest_streak_weeks,
  f.orders_in_longest_streak,
  f.number_of_streaks,
  f.orders_0_7d,
  f.orders_8_14d,
  f.orders_15_30d,
  f.orders_31_60d,
  f.orders_61_90d,
  f.orders_91_180d,
  f.orders_180plus_d,
  f.active_weeks,
  f.pct_weeks_active,
  f.frequency_bucket,
  f.regularity_pattern,
  f.orders_early_period,
  f.orders_late_period,
  f.activity_trend,
  f.pre_exposure_engagement_level,
  f.days_since_last_funnel_action,
  f.is_consistent_orderer,
  f.did_reonboarding,
  f.push_system_optin,
  f.push_doordash_offers_optin,
  f.push_order_updates_optin,
  f.sms_marketing_optin,
  f.sms_order_updates_optin,
  f.total_orders_post_exposure,
  f.new_cx_orders_post_exposure,
  f.has_order_post_exposure,
  f.total_gov_post_exposure,
  f.total_vp_post_exposure,
  f.avg_order_value,
  f.first_order_date_post_exposure,
  f.last_order_date_post_exposure,
  f.days_to_first_order
FROM user_redemptions ur
INNER JOIN proddb.fionafan.cx_ios_reonboarding_master_features_user_level f
  ON f.user_id::varchar = ur.user_id::varchar
ORDER BY ur.redemptions_since_2025_09_01 DESC, ur.user_id, ur.campaign_id
);
select campaign_name, count(1) cnt
, sum(case when tag ='treatment' then total_orders_post_exposure else 0 end) as treatment_order
, sum(case when tag = 'control' then total_orders_post_exposure else 0 end) as control_order
, (sum(case when tag ='treatment' then total_orders_post_exposure else 0 end)-sum(case when tag = 'control' then total_orders_post_exposure else 0 end) )/count(1) as order_lift

from proddb.fionafan.cx_ios_reonboarding_campaign_user_level group by all;

