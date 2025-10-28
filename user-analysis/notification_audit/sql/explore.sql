select * from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt limit 10;


select DISTINCT TABLE_NAME from Tyleranderson.sf_columns where lower(column_name) ilike '%canvas_id%' and table_catalog not like '%CAVIAR%' 
and table_schema not like '%CAVIAR%'
limit 1000;

select * from marketing_fivetran.braze_consumer.canvas_tag  limit 10;


select * from proddb.public.dimension_consumer limit 10;

select distinct notification_source, count(1) cnt from edw.consumer.fact_consumer_notification_engagement 
where sent_at_date>= '2025-08-31' group by all order by cnt desc;

drop table if exists proddb.fionafan.notif_campaign_performance_factors;
select * from proddb.fionafan.notif_campaign_performance_factors;

select count(1) from  proddb.fionafan.notif_base_table_w_braze_week;

-- grant all privileges on proddb.fionafan.all_user_notifications_base   to public;

select case when bz_consumer_id is null then 0 else 1 end as bz_present, count(1) cnt from proddb.fionafan.notif_base_table_w_braze_week 

where notification_source ='Braze' 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 -- mostly valid for fpn sends
group by all;




select  cohort_type,
    count(distinct deduped_message_id||consumer_id||device_id) as total_notifications,
    count(distinct consumer_id||device_id) as unique_consumer_devices,
    count(distinct deduped_message_id||consumer_id||device_id)/NULLIF(count(distinct consumer_id||device_id), 0) as avg_pushes_per_customer,
    
    -- Core engagement metrics (message-level denominator)
    count(distinct case when opened_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_rate,
    count(distinct case when open_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_within_24h_rate,
    count(distinct case when open_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as open_within_4h_rate,
    count(distinct case when first_session_id_after_send is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as first_session_rate,
    
    -- Unsubscribe metrics (message-level denominator)
    count(distinct case when unsubscribed_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as unsubscribed_rate,
    count(distinct case when unsubscribe_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as unsubscribed_within_24h_rate,
    count(distinct case when unsubscribe_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as unsubscribed_within_4h_rate,
    sum(case when unsubscribed_at is not null then DATEDIFF(minute, sent_at, unsubscribed_at) else 0 end)/NULLIF(count(case when unsubscribed_at is not null then 1 end), 0) as avg_time_to_unsubscribe_minutes,
    
    -- Uninstall metrics (message-level denominator)

    count(distinct case when uninstalled_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_rate,
    count(distinct case when uninstall_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_within_24h_rate,
    count(distinct case when uninstall_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_within_4h_rate,
    sum(case when uninstalled_at is not null then DATEDIFF(minute, sent_at, uninstalled_at) else 0 end)/NULLIF(count(case when uninstalled_at is not null then 1 end), 0) as avg_time_to_uninstall_minutes,
    -- Uninstall after open metrics (message-level denominator)
    count(distinct case when opened_at is not null and uninstalled_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_after_open_rate,
    count(distinct case when open_within_4h=1 and uninstall_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_after_open_within_4h_rate,
    count(distinct case when open_within_24h=1 and uninstall_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_after_open_within_24h_rate,
    -- Uninstall among non-purchasers metrics (message-level denominator)
    count(distinct case when ordered_at is null and uninstalled_at is not null then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_non_purchasers_rate,
    count(distinct case when order_within_4h=0 and uninstall_within_4h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_non_purchasers_within_4h_rate,
    count(distinct case when order_within_24h=0 and uninstall_within_24h=1 then deduped_message_id end)/NULLIF(count(distinct deduped_message_id), 0) as uninstall_non_purchasers_within_24h_rate,
    -- Open timing metrics
    sum(case when opened_at is not null then DATEDIFF(minute, sent_at, opened_at) else 0 end)/NULLIF(count(case when opened_at is not null then 1 end), 0) as avg_time_to_open_minutes
    , count(distinct case when ordered_at is not null then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) avg_ordered

    , count(distinct case when order_within_1h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_1h
, count(distinct case when order_within_4h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_4h
, count(distinct case when order_within_24h=1 then deduped_message_id end)/nullif(count(distinct deduped_message_id),0) order_within_24h,


from proddb.fionafan.all_user_notifications_base 
where campaign_name = 'reserved_for_notification_v2_do_not_use' 
and notification_source ='FPN Postal Service' 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 -- mostly valid for fpn sends
group by all
limit 100;
select distinct campaign_id from proddb.fionafan.all_user_notifications_base 

where campaign_name = 'reserved_for_notification_v2_do_not_use' 
and notification_source ='FPN Postal Service' 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 -- mostly valid for fpn sends
;

-- Analysis with propensity scores broken down by quartile and cohort

CREATE OR REPLACE TABLE proddb.fionafan.all_july_cohort_with_scores AS (

WITH daily_propensity_scores AS (
    -- Step 1: Get propensity scores and calculate quartiles for each day
    SELECT 
        consumer_id,
        active_date,
        CAF_CS_PROPENSITY_SCORE_7D_GROCERY,
        NTILE(4) OVER (PARTITION BY active_date ORDER BY CAF_CS_PROPENSITY_SCORE_7D_GROCERY) AS score_quartile
    FROM proddb.ml.fact_cx_cross_vertical_propensity_scores_v1
    WHERE active_date >= '2025-06-20' and active_date<='2025-08-31'
),

cohort_with_scores AS (
    -- Step 2: Inner join cohort table to daily quartiles
    SELECT 
        c.consumer_id,
        c.cohort_type,
        c.onboarding_day,
        dps.active_date,
        dps.CAF_CS_PROPENSITY_SCORE_7D_GROCERY,
        dps.score_quartile
    FROM proddb.fionafan.all_user_july_cohort c
    INNER JOIN daily_propensity_scores dps
        ON c.consumer_id = dps.consumer_id
)
SELECT *
FROM cohort_with_scores
);
-- Check table structure
SELECT * FROM proddb.fionafan.all_july_cohort_with_scores LIMIT 5;

SELECT 
    cohort_type, 
    score_quartile,
    DATEDIFF(day, onboarding_day, active_date) AS days_since_onboarding,
    COUNT(1) AS cnt,
    ROUND(100.0 * COUNT(1) / SUM(COUNT(1)) OVER (PARTITION BY cohort_type, DATEDIFF(day, onboarding_day, active_date)), 2) AS pct_within_cohort_day
FROM proddb.fionafan.all_july_cohort_with_scores 
WHERE active_date >= '2025-06-20'
GROUP BY cohort_type, score_quartile, days_since_onboarding
HAVING days_since_onboarding BETWEEN 0 AND 28
ORDER BY cohort_type, days_since_onboarding, score_quartile;

select distinct consumer_id, DATEADD(day, -7, sent_at::date) 
FROM proddb.fionafan.all_user_notifications_base_with_scores 
;

CREATE OR REPLACE TABLE proddb.fionafan.all_user_notifications_base_with_scores AS (
SELECT 
    n.*,
    cws.CAF_CS_PROPENSITY_SCORE_7D_GROCERY,
    cws.score_quartile,
    cws.active_date AS propensity_score_date,
    cws.cohort_type AS cohort_type_from_july
FROM proddb.fionafan.all_user_notifications_base n
LEFT JOIN proddb.fionafan.all_july_cohort_with_scores cws
    ON n.consumer_id = cws.consumer_id
    AND cws.active_date = DATEADD(day, -7, n.sent_at::date)
WHERE n.campaign_name = 'reserved_for_notification_v2_do_not_use' 
    AND n.notification_source = 'FPN Postal Service' 
    AND n.notification_channel = 'PUSH'
    AND n.notification_message_type_overall != 'TRANSACTIONAL'
    AND COALESCE(n.canvas_name, n.campaign_name) != '[Martech] FPN Silent Push'
    AND n.is_valid_send = 1
);


SELECT 
    cohort_type,
    score_quartile,
    ROUND(AVG(CAF_CS_PROPENSITY_SCORE_7D_GROCERY), 4) AS avg_propensity_score,
    ROUND(MIN(CAF_CS_PROPENSITY_SCORE_7D_GROCERY), 4) AS min_propensity_score,
    ROUND(MAX(CAF_CS_PROPENSITY_SCORE_7D_GROCERY), 4) AS max_propensity_score,
    
    COUNT(DISTINCT deduped_message_id||consumer_id||device_id) AS total_notifications,
    COUNT(DISTINCT consumer_id||device_id) AS unique_consumer_devices,
    COUNT(DISTINCT deduped_message_id||consumer_id||device_id)/NULLIF(COUNT(DISTINCT consumer_id||device_id), 0) AS avg_pushes_per_customer,
    
    -- Core engagement metrics (message-level denominator)
    COUNT(DISTINCT CASE WHEN open_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS open_within_1h_rate,
    COUNT(DISTINCT CASE WHEN first_session_id_after_send IS NOT NULL THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS first_session_rate,
    
    -- Unsubscribe metrics (message-level denominator)
    COUNT(DISTINCT CASE WHEN unsubscribe_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS unsubscribed_within_1h_rate,
  
    -- Uninstall metrics (message-level denominator)
    COUNT(DISTINCT CASE WHEN uninstall_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_within_1h_rate,

    -- Uninstall after open metrics (message-level denominator)
    COUNT(DISTINCT CASE WHEN open_within_1h=1 AND uninstall_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_after_open_within_1h_rate,
    COUNT(DISTINCT CASE WHEN open_within_4h=1 AND uninstall_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_after_open_within_4h_rate,
    COUNT(DISTINCT CASE WHEN open_within_24h=1 AND uninstall_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_after_open_within_24h_rate,
    
    -- Uninstall among non-purchasers metrics (message-level denominator)
    COUNT(DISTINCT CASE WHEN ordered_at IS NULL AND uninstalled_at IS NOT NULL THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_non_purchasers_rate,
    COUNT(DISTINCT CASE WHEN order_within_1h=0 AND uninstall_within_4h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_non_purchasers_within_1h_rate,
    COUNT(DISTINCT CASE WHEN order_within_4h=0 AND uninstall_within_4h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_non_purchasers_within_4h_rate,
    COUNT(DISTINCT CASE WHEN order_within_24h=0 AND uninstall_within_24h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS uninstall_non_purchasers_within_24h_rate,
    
    -- Open timing metrics
    SUM(CASE WHEN opened_at IS NOT NULL THEN DATEDIFF(minute, sent_at, opened_at) ELSE 0 END)/NULLIF(COUNT(CASE WHEN opened_at IS NOT NULL THEN 1 END), 0) AS avg_time_to_open_minutes,
    
    -- Order metrics
    COUNT(DISTINCT CASE WHEN ordered_at IS NOT NULL THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS avg_ordered,
    COUNT(DISTINCT CASE WHEN order_within_1h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS order_within_1h,
    COUNT(DISTINCT CASE WHEN order_within_4h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS order_within_4h,
    COUNT(DISTINCT CASE WHEN order_within_24h=1 THEN deduped_message_id END)/NULLIF(COUNT(DISTINCT deduped_message_id), 0) AS order_within_24h

FROM proddb.fionafan.all_user_notifications_base_with_scores 
GROUP BY cohort_type, score_quartile
ORDER BY cohort_type, score_quartile;
select * from proddb.fionafan.all_user_notifications_base_with_scores order by 1,2;

  SELECT
*
  FROM edw.ads.dim_campaign_tag_history c
  WHERE c.campaign_id IN (
    '8cce6ead-8635-4a25-9d8c-7cccbabd28f1',
    '3e288e4a-8617-44e0-82da-a734ffba5add',
    'f4ee3d88-2f2f-4d60-a7c3-a733d4109f19',
    '3f1d79ea-5b96-43a7-94d3-03d98512694b',
    'f8af63ff-f998-468b-8c8a-6b91ca02af22',
    'c96a185f-7f4c-4874-8ee3-dfef69dae8a3',
    '4911e8be-da79-4565-abce-6f0064f353c8',
    'dca08b20-a3d6-450c-bff5-8f469343945f',
    'e4d49c3c-2a9b-4b63-8078-b1deb4d69d26',
    'f6bfe710-4994-4296-a634-30308968421c',
    '284d1ee7-1e8c-4fa6-94dd-d993a88f28ad',
    '3c7fd171-5ee2-4b2c-83d5-1a97861bb279',
    'a4d7e8c0-72f3-485f-b6d1-965ce549fd51',
    '9349a273-6a4f-42c2-aa10-bcbd6e90a5b2',
    'a4c26ed9-ccb6-4dc1-9efb-2fa1059e3408',
    '4f65eab7-9eb6-453e-8ece-8e517921ae99',
    '4fa628f5-0cca-4cbc-a301-4f27a4ba6f5c',
    'd809f07b-c42f-459e-b3f0-3669222daf71',
    '025c2b99-b587-4989-9fba-4082fdbcd99d',
    'd40d0267-755a-46dd-8245-9d1ce1492e5d',
    'f1c344a7-b43a-42ba-bdad-978769ea1f27',
    'ef34ee87-db61-49f3-8f55-0c7692ddc631',
    'c84efe87-88c1-4c06-88d6-5bd82f9815d9',
    '7534814a-4ee0-4fa6-b132-3d6e7d5a844b',
    '3df63be6-367a-46a6-a3aa-d74e5b7dec0f',
    'd6f5d8c7-3e4b-40a2-a4eb-35b41b3524ce',
    'bd18bef8-a113-4e06-a206-bbf9ab0c6edd',
    'dfde6610-4623-45ae-a931-76ec3f87ca9b',
    'f4981c9e-eae3-4c8f-8cd0-241fe0b2e290',
    '45b72ecc-b3f8-4f75-9dfa-b27860e13227',
    '4ccc6fb7-29f4-4a9d-b45c-4a7ad3486fae',
    '632bc250-fbb2-40dd-b3c0-d25bad9f68fd',
    '6916f9bc-fccd-4a91-b849-56dff955ca9b',
    '937a9d8b-942c-4b0d-ba6c-092bd179d943',
    '90ce79f5-ee70-454d-b671-9ee904c20db9',
    '51b262d0-c0ab-4985-afb8-9eddcaf47ca1',
    '8cca3ef2-90c4-4784-b2a5-98eec908fcc8',
    'deadc750-d3f3-4b0d-8c8a-6b91ca02af22',
    '152fbda0-cde8-4e41-8636-5b50f69d353d',
    '01b36d9b-7db0-4d64-80c1-3c5828f518c1',
    'd8e075ea-5884-45df-b4f2-ab61e2a0c456',
    '982cac6f-a0f7-4fc3-9e7a-37e606beb945',
    '6e8674d5-4ba4-4142-aa4b-4ff5cc90c86c',
    '0c5c2bcf-290a-43de-9098-993228e6f396',
    '116c527f-4960-4130-ac11-4055a67280c6',
    'e8c89863-a582-432b-b140-797221173ac2',
    'fed45387-4c6d-4ece-9852-d13892b1f61f'
  );
WITH recent_campaigns AS (
  SELECT
    c.id AS campaign_id,
    c.name AS campaign_name,
    c.updated_at,
    c.created_at
  FROM campaign_service.public.campaign c
  WHERE 1=1
AND lower(id) in ('8cce6ead-8635-4a25-9d8c-7cccbabd28f1',
'3e288e4a-8617-44e0-82da-a734ffba5add',
'f4ee3d88-2f2f-4d60-a7c3-a733d4109f19',
'3f1d79ea-5b96-43a7-94d3-03d98512694b',
'f8af63ff-f998-468b-8a9f-e0f9c0fd4827',
'c96a185f-7f4c-4874-8ee3-dfef69dae8a3',
'4911e8be-da79-4565-abce-6f0064f353c8',
'dca08b20-a3d6-450c-bff5-8f469343945f',
'e4d49c3c-2a9b-4b63-8078-b1deb4d69d26',
'f6bfe710-4994-4296-a634-30308968421c',
'284d1ee7-1e8c-4fa6-94dd-d993a88f28ad',
'3c7fd171-5ee2-4b2c-83d5-1a97861bb279',
'a4d7e8c0-72f3-485f-b6d1-965ce549fd51',
'9349a273-6a4f-42c2-aa10-bcbd6e90a5b2',
'a4c26ed9-ccb6-4dc1-9efb-2fa1059e3408',
'4f65eab7-9eb6-453e-8ece-8e517921ae99',
'4fa628f5-0cca-4cbc-a301-4f27a4ba6f5c',
'd809f07b-c42f-459e-b3f0-3669222daf71',
'025c2b99-b587-4989-9fba-4082fdbcd99d',
'd40d0267-755a-46dd-8245-9d1ce1492e5d',
'f1c344a7-b43a-42ba-bdad-978769ea1f27',
'ef34ee87-db61-49f3-8f55-0c7692ddc631',
'c84efe87-88c1-4c06-88d6-5bd82f9815d9',
'7534814a-4ee0-4fa6-b132-3d6e7d5a844b',
'3df63be6-367a-46a6-a3aa-d74e5b7dec0f',
'd6f5d8c7-3e4b-40a2-a4eb-35b41b3524ce',
'bd18bef8-a113-4e06-a206-bbf9ab0c6edd',
'dfde6610-4623-45ae-a931-76ec3f87ca9b',
'f4981c9e-eae3-4c8f-8cd0-241fe0b2e290',
'45b72ecc-b3f8-4f75-9dfa-b27860e13227',
'4ccc6fb7-29f4-4a9d-b45c-4a7ad3486fae',
'632bc250-fbb2-40dd-b3c0-d25bad9f68fd',
'6916f9bc-fccd-4a91-b849-56dff955ca9b',
'937a9d8b-942c-4b0d-ba6c-092bd179d943',
'90ce79f5-ee70-454d-b671-9ee904c20db9',
'51b262d0-c0ab-4985-afb8-9eddcaf47ca1',
'8cca3ef2-90c4-4784-b2a5-98eec908fcc8',
'deadc750-d3f3-4b0d-8c8a-6b91ca02af22',
'152fbda0-cde8-4e41-8636-5b50f69d353d',
'01b36d9b-7db0-4d64-80c1-3c5828f518c1',
'd8e075ea-5884-45df-b4f2-ab61e2a0c456',
'982cac6f-a0f7-4fc3-9e7a-37e606beb945',
'6e8674d5-4ba4-4142-aa4b-4ff5cc90c86c',
'0c5c2bcf-290a-43de-9098-993228e6f396',
'116c527f-4960-4130-ac11-4055a67280c6',
'e8c89863-a582-432b-b140-797221173ac2',
'fed45387-4c6d-4ece-9852-d13892b1f61f')
),
ep_tags AS (
  SELECT
    ct.campaign_id,
    ct.tag_key,
    TRIM(ct.tag_value) AS tag_value,
    ct.transaction_ts
  FROM campaign_service.public.campaign_tag ct

  WHERE ct.transaction_ts >= DATEADD(day, -7, CURRENT_DATE)
    AND ct.is_deleted = FALSE
    AND ct.tag_key ILIKE '%engagement%program%'  -- catches engagement_program, engagementProgramTag, etc.
)
SELECT
  rc.campaign_id,
  rc.campaign_name,
  et.tag_key AS ep_tag_key,
  et.tag_value AS ep_tag_value,            -- expected to be the EP tag string
  dep.program_name AS ep_program_name,     -- validated against EP dimension
  dep.active AS ep_active,
  dep.tag_entity_type
FROM recent_campaigns rc
LEFT JOIN ep_tags et
  ON et.campaign_id = rc.campaign_id
LEFT JOIN edw.growth.dimension_engagement_program dep

  ON dep.program_name = et.tag_value
ORDER BY rc.campaign_name, et.tag_key;