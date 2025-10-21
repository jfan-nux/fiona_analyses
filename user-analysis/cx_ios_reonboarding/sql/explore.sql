select * from proddb.public.REONBOARDING_PRIOR_ENGAGEMENT_STATUS 

where event_recency_bucket = '91-180 days ago' and tag = 'treatment'
limit 10;


select * from tyleranderson.events_all 
where event_date between '2025-08-10'::date-7 and '2025-08-10' 
and user_id = '652477316'
order by user_id, timestamp asc;


SELECT
  consumer_id,
  max(CASE WHEN promo_title IS NOT NULL AND promo_title <> '' THEN 1 ELSE 0 END) AS had_promo
FROM
  datalake.iguazu_consumer.m_onboarding_end_promo_page_view
WHERE 
  iguazu_timestamp > NOW() - INTERVAL '7' DAY
AND
  onboarding_type = 'resurrected_user'
AND
  dd_platform = 'ios'
and consumer_id in (select consumer_id from proddb.public.REONBOARDING_PRIOR_ENGAGEMENT_STATUS where event_recency_bucket = '91-180 days ago' and tag = 'treatment');

with base as (
  select
    a.program_name,
    b.event_recency_bucket,
    count(distinct a.consumer_id) as cnt
  from SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM a
  inner join (
    select user_id, event_recency_bucket
    from proddb.public.REONBOARDING_PRIOR_ENGAGEMENT_STATUS
    where tag = 'treatment'
  ) b
    on a.consumer_id = b.user_id
    and a.PROGRAM_NAME IN (
'ep_consumer_churned_latebloomers_auto_ctc_test_us', 
'ep_consumer_churned_low_vp_us_v1', 
'ep_consumer_churned_low_vp_us_v1', 
'ep_consumer_churned_med_vp_us_v1', 
'ep_consumer_dewo_phase1_retarget_us_v1', 
'ep_consumer_dewo_phase2_us_v1', 
'ep_consumer_dewo_phase3_us_v1', 
'ep_consumer_dormant_churned_browsers_us_v1', 
'ep_consumer_dormant_late_bloomers_us_v1', 
'ep_consumer_dormant_late_bloomers_us_v1', 
'ep_consumer_dormant_late_bloomers_us_v1', 
'ep_consumer_dormant_winback_us_v1', 
'ep_consumer_enhanced_rxauto_120d_test_us_v1', 
'ep_consumer_enhanced_rxauto_150day_test_us_v1', 
'ep_consumer_enhanced_rxauto_180day_test_us_v1', 
'ep_consumer_enhanced_rxauto_90d_us_v1', 
'ep_consumer_ml_churn_prevention_us_v1_p1_active', 
'ep_consumer_ml_churn_prevention_us_v1_p1_dormant', 
'ep_consumer_ml_churn_prevention_us_v1_p2_active_active', 
'ep_consumer_ml_churn_prevention_us_v1_p2_active_dormant', 
'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_active', 
'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_dormant', 
'ep_consumer_repeatchurned_us', 
'ep_consumer_repeatchurned_us', 
'ep_consumer_repeatchurned_us', 
'ep_consumer_repeatchurned_us', 
'ep_consumer_rx_reachability_auto_us', 
'ep_consumer_super_churned_low_vp_us_v1', 
'ep_consumer_super_churned_med_vp_us_v1', 
'ep_consumer_very_churned_med_vp_us_v1'
)
  where sent_at >= '2025-08-11'
  group by all
),
program_counts as (
  select
    event_recency_bucket,
    program_name,
    sum(cnt) as program_cnt
  from base
  group by all
),
bucket_totals as (
  select
    event_recency_bucket,
    sum(program_cnt) as bucket_cnt
  from program_counts
  group by all
)
select
  p.event_recency_bucket,
  p.program_name,
  p.program_cnt,
  b.bucket_cnt
from program_counts p
join bucket_totals b
  on b.event_recency_bucket = p.event_recency_bucket
order by b.bucket_cnt desc, p.program_cnt desc, p.program_name;


select max(day) from proddb.public.onboarding_funnel_flags_curr;

select count(1) from (
select consumer_id, active_date, count(1) cnt 
from proddb.ml.fact_cx_cross_vertical_propensity_scores_v1 group by all having cnt>1 );


select * from proddb.ml.fact_cx_cross_vertical_propensity_scores_v1 where consumer_id = '606970768' and active_date = '2025-04-28' limit 10;


create or replace table proddb.fionafan.cx_ios_reonboarding_experiment_exposures as (
SELECT  ee.tag
               , ee.result
               , ee.bucket_key 
               , replace(lower(CASE WHEN bucket_key like 'dx_%' then bucket_key
                    else 'dx_'||bucket_key end), '-') AS dd_device_ID_filtered
              , CUSTOM_ATTRIBUTES:userId as consumer_id
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'cx_ios_reonboarding'
AND experiment_version::INT = 1
AND segment = 'Users'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-09-08' AND current_date
GROUP BY all
);


CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_all_sessions AS

WITH cohort AS (
  SELECT DISTINCT
    consumer_id,
    bucket_key as dd_device_id,
    replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id else 'dx_'||dd_device_id end), '-') as dd_device_id_filtered,
    exposure_time,
    day AS exposure_date,
    tag as treatment_group
  FROM proddb.fionafan.cx_ios_reonboarding_experiment_exposures
),

all_sessions AS (
  SELECT 
    c.exposure_date,
    c.exposure_time,
    c.consumer_id,
    c.dd_device_id,
    c.dd_device_id_filtered,
    c.treatment_group,
    s.DD_DEVICE_ID as session_device_id,
    replace(lower(CASE WHEN s.DD_DEVICE_ID like 'dx_%' then s.DD_DEVICE_ID else 'dx_'||s.DD_DEVICE_ID end), '-') as session_device_id_filtered,
    s.dd_platform,
    s.dd_session_id,
    min(s.iguazu_timestamp) as session_timestamp,
    min(cast(s.iguazu_timestamp as date)) as session_date,
    datediff('day', c.exposure_time, min(s.iguazu_timestamp)) as days_since_exposure
  FROM cohort c
  INNER JOIN iguazu.server_events_production.m_store_content_page_load s
    ON c.dd_device_id_filtered = replace(lower(CASE WHEN s.DD_DEVICE_ID like 'dx_%' then s.DD_DEVICE_ID else 'dx_'||s.DD_DEVICE_ID end), '-')
    AND s.iguazu_timestamp >= c.exposure_time
  GROUP BY all
)

SELECT DISTINCT
  exposure_date,
  exposure_time,
  consumer_id,
  dd_device_id,
  dd_device_id_filtered,
  treatment_group,
  session_device_id,
  session_device_id_filtered,
  dd_platform,
  dd_session_id,
  session_date,
  session_timestamp,
  days_since_exposure
FROM all_sessions
ORDER BY exposure_date, consumer_id, session_timestamp;



CREATE OR REPLACE TABLE proddb.fionafan.cx_ios_reonboarding_sessions_enriched AS

WITH session_sequences AS (
  SELECT 
    *,
    -- Nth session since exposure at consumer+device level
    ROW_NUMBER() OVER (
      PARTITION BY consumer_id, dd_device_id_filtered 
      ORDER BY session_timestamp
    ) as nth_session_device,
    -- Nth session since exposure at consumer level (across all devices)
    ROW_NUMBER() OVER (
      PARTITION BY consumer_id
      ORDER BY session_timestamp
    ) as nth_session_consumer
  FROM proddb.fionafan.cx_ios_reonboarding_all_sessions
),

consumer_exposure AS (
  -- Get earliest exposure info per consumer
  SELECT 
    consumer_id,
    MIN(exposure_date) as exposure_date,
    MIN(exposure_time) as exposure_time,
    MIN(dd_platform) as dd_platform,
    MIN(treatment_group) as treatment_group
  FROM proddb.fionafan.cx_ios_reonboarding_all_sessions
  GROUP BY consumer_id
),

latest_sessions AS (
  SELECT
    consumer_id,
    MAX(CASE WHEN rn_week_1 = 1 THEN dd_session_id END) as latest_session_day_0_7,
    MAX(CASE WHEN rn_week_2 = 1 THEN dd_session_id END) as latest_session_day_8_14,
    MAX(CASE WHEN rn_week_3 = 1 THEN dd_session_id END) as latest_session_day_15_21,
    MAX(CASE WHEN rn_week_4 = 1 THEN dd_session_id END) as latest_session_day_22_28
  FROM (
    SELECT
      consumer_id,
      dd_session_id,
      days_since_exposure,
      session_timestamp,
      CASE 
        WHEN days_since_exposure BETWEEN 0 AND 7 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_exposure BETWEEN 0 AND 7 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_1,
      CASE 
        WHEN days_since_exposure BETWEEN 8 AND 14 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_exposure BETWEEN 8 AND 14 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_2,
      CASE 
        WHEN days_since_exposure BETWEEN 15 AND 21 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_exposure BETWEEN 15 AND 21 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_3,
      CASE 
        WHEN days_since_exposure BETWEEN 22 AND 28 
        THEN ROW_NUMBER() OVER (PARTITION BY consumer_id, CASE WHEN days_since_exposure BETWEEN 22 AND 28 THEN 1 END ORDER BY session_timestamp DESC)
      END as rn_week_4
    FROM session_sequences
  ) ranked
  GROUP BY consumer_id
),

consumer_sessions AS (
  SELECT
    consumer_id,
    -- Total session counts
    COUNT(DISTINCT dd_session_id) as total_sessions,
    COUNT(DISTINCT dd_device_id_filtered) as total_devices,
    
    -- Sample session IDs (first, second, third overall)
    MAX(CASE WHEN nth_session_consumer = 1 THEN dd_session_id END) as first_session_id,
    MAX(CASE WHEN nth_session_consumer = 2 THEN dd_session_id END) as second_session_id,
    MAX(CASE WHEN nth_session_consumer = 3 THEN dd_session_id END) as third_session_id,
    
    -- Session counts by week buckets
    COUNT(DISTINCT CASE WHEN days_since_exposure BETWEEN 0 AND 7 THEN dd_session_id END) as sessions_day_0_7,
    COUNT(DISTINCT CASE WHEN days_since_exposure BETWEEN 8 AND 14 THEN dd_session_id END) as sessions_day_8_14,
    COUNT(DISTINCT CASE WHEN days_since_exposure BETWEEN 15 AND 21 THEN dd_session_id END) as sessions_day_15_21,
    COUNT(DISTINCT CASE WHEN days_since_exposure BETWEEN 22 AND 28 THEN dd_session_id END) as sessions_day_22_28
    
  FROM session_sequences
  GROUP BY consumer_id
)

SELECT 
  e.consumer_id,
  e.exposure_date,
  e.exposure_time,
  e.dd_platform,
  e.treatment_group,
  s.total_sessions,
  s.total_devices,
  s.first_session_id,
  s.second_session_id,
  s.third_session_id,
  l.latest_session_day_0_7,
  l.latest_session_day_8_14,
  l.latest_session_day_15_21,
  l.latest_session_day_22_28,
  s.sessions_day_0_7,
  s.sessions_day_8_14,
  s.sessions_day_15_21,
  s.sessions_day_22_28
FROM consumer_exposure e
LEFT JOIN consumer_sessions s
  ON e.consumer_id = s.consumer_id
LEFT JOIN latest_sessions l
  ON e.consumer_id = l.consumer_id
ORDER BY e.exposure_date, e.consumer_id;

-- Validation query
SELECT 
  treatment_group,
  COUNT(*) as total_consumers,
  AVG(total_sessions) as avg_sessions_per_consumer,
  AVG(total_devices) as avg_devices_per_consumer,
  COUNT(CASE WHEN first_session_id IS NOT NULL THEN 1 END) as consumers_with_first_session,
  COUNT(CASE WHEN second_session_id IS NOT NULL THEN 1 END) as consumers_with_second_session,
  COUNT(CASE WHEN third_session_id IS NOT NULL THEN 1 END) as consumers_with_third_session,
  AVG(sessions_day_0_7) as avg_sessions_week_1,
  AVG(sessions_day_8_14) as avg_sessions_week_2,
  AVG(sessions_day_15_21) as avg_sessions_week_3,
  AVG(sessions_day_22_28) as avg_sessions_week_4
FROM proddb.fionafan.cx_ios_reonboarding_sessions_enriched
GROUP BY treatment_group;


-- Average and Median Tenure and Days Since Last Purchase
-- By lifestage (excluding New Cx and Active)
SELECT
    lifestage,
    -- result,
    round(COUNT(DISTINCT user_id)/sum(count(distinct user_id)) over () * 100.0, 2) AS user_count_pct,
    
    -- Tenure metrics
    ROUND(AVG(tenure_days_at_exposure), 1) AS avg_tenure_days,
    ROUND(MEDIAN(tenure_days_at_exposure), 1) AS median_tenure_days,
    
    -- Days since last purchase metrics
    ROUND(AVG(days_since_last_order), 1) AS avg_days_since_last_order,
    ROUND(MEDIAN(days_since_last_order), 1) AS median_days_since_last_order
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
WHERE lifestage IS NOT NULL
    AND lifestage NOT IN ('New Cx', 'Active', 'Resurrected')
GROUP BY all
ORDER BY all;

-- Overall summary (excluding New Cx and Active)
SELECT
    'Overall' AS lifestage,
    result,
    COUNT(DISTINCT user_id) AS user_count,
    
    -- Tenure metrics
    ROUND(AVG(tenure_days_at_exposure), 1) AS avg_tenure_days,
    ROUND(MEDIAN(tenure_days_at_exposure), 1) AS median_tenure_days,
    
    -- Days since last purchase metrics
    ROUND(AVG(days_since_last_order), 1) AS avg_days_since_last_order,
    ROUND(MEDIAN(days_since_last_order), 1) AS median_days_since_last_order
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
WHERE lifestage IS NOT NULL
    AND lifestage NOT IN ('New Cx', 'Active')
GROUP BY result
ORDER BY result;


-- Order rate by order history with lift calculation
WITH base AS (
    SELECT 
        CASE WHEN frequency_total_orders_ytd > 0 THEN 1 ELSE 0 END AS has_order_history,
        tag,
        COUNT(1) AS cnt,
        AVG(has_order_post_exposure) AS avg_order_rate
    FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level 
    GROUP BY ALL
),
pivot_data AS (
    SELECT
        has_order_history,
        MAX(CASE WHEN tag = 'control' THEN cnt END) AS control_cnt,
        MAX(CASE WHEN tag = 'control' THEN avg_order_rate END) AS control_rate,
        MAX(CASE WHEN tag = 'treatment' THEN cnt END) AS treatment_cnt,
        MAX(CASE WHEN tag = 'treatment' THEN avg_order_rate END) AS treatment_rate
    FROM base
    GROUP BY has_order_history
)
SELECT
    CASE WHEN has_order_history = 1 THEN 'Has Order History' ELSE 'No Order History' END AS segment,
    control_cnt,
    ROUND(control_rate * 100, 2) AS control_pct,
    treatment_cnt,
    ROUND(treatment_rate * 100, 2) AS treatment_pct,
    ROUND((treatment_rate - control_rate) * 100, 2) AS absolute_lift_pp,
    ROUND((treatment_rate / NULLIF(control_rate, 0) - 1) * 100, 2) AS relative_lift_pct
FROM pivot_data
ORDER BY has_order_history DESC;


-- Order rate lift by funnel recency (all stages side-by-side)
WITH bucketed_data AS (
    SELECT 
        user_id,
        tag,
        has_order_post_exposure,
        -- Bucket all funnel stages with same bins
        CASE 
            WHEN days_before_exposure_store_content IS NULL THEN 'No visit'
            WHEN days_before_exposure_store_content <= 90 THEN '0-90 days'
            WHEN days_before_exposure_store_content <= 120 THEN '90-120 days'
            WHEN days_before_exposure_store_content <= 150 THEN '120-150 days'
            WHEN days_before_exposure_store_content <= 180 THEN '150-180 days'
            WHEN days_before_exposure_store_content <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS store_content_bucket,
        CASE 
            WHEN days_before_exposure_store_page IS NULL THEN 'No visit'
            WHEN days_before_exposure_store_page <= 90 THEN '0-90 days'
            WHEN days_before_exposure_store_page <= 120 THEN '90-120 days'
            WHEN days_before_exposure_store_page <= 150 THEN '120-150 days'
            WHEN days_before_exposure_store_page <= 180 THEN '150-180 days'
            WHEN days_before_exposure_store_page <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS store_page_bucket,
        CASE 
            WHEN days_before_exposure_order_cart IS NULL THEN 'No visit'
            WHEN days_before_exposure_order_cart <= 90 THEN '0-90 days'
            WHEN days_before_exposure_order_cart <= 120 THEN '90-120 days'
            WHEN days_before_exposure_order_cart <= 150 THEN '120-150 days'
            WHEN days_before_exposure_order_cart <= 180 THEN '150-180 days'
            WHEN days_before_exposure_order_cart <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS order_cart_bucket,
        CASE 
            WHEN days_before_exposure_checkout IS NULL THEN 'No visit'
            WHEN days_before_exposure_checkout <= 90 THEN '0-90 days'
            WHEN days_before_exposure_checkout <= 120 THEN '90-120 days'
            WHEN days_before_exposure_checkout <= 150 THEN '120-150 days'
            WHEN days_before_exposure_checkout <= 180 THEN '150-180 days'
            WHEN days_before_exposure_checkout <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS checkout_bucket,
        CASE 
            WHEN days_before_exposure_purchase IS NULL THEN 'No visit'
            WHEN days_before_exposure_purchase <= 90 THEN '0-90 days'
            WHEN days_before_exposure_purchase <= 120 THEN '90-120 days'
            WHEN days_before_exposure_purchase <= 150 THEN '120-150 days'
            WHEN days_before_exposure_purchase <= 180 THEN '150-180 days'
            WHEN days_before_exposure_purchase <= 365 THEN '180-365 days'
            ELSE '365+ days'
        END AS purchase_bucket
    FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
),

-- Calculate lift for store_content
store_content_lift AS (
    SELECT 
        store_content_bucket AS recency_bucket,
        ROUND((AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) - 
               AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END)) * 100, 2) AS store_content_lift
    FROM bucketed_data
    GROUP BY store_content_bucket
),

-- Calculate lift for store_page
store_page_lift AS (
    SELECT 
        store_page_bucket AS recency_bucket,
        ROUND((AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) - 
               AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END)) * 100, 2) AS store_page_lift
    FROM bucketed_data
    GROUP BY store_page_bucket
),

-- Calculate lift for order_cart
order_cart_lift AS (
    SELECT 
        order_cart_bucket AS recency_bucket,
        ROUND((AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) - 
               AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END)) * 100, 2) AS order_cart_lift
    FROM bucketed_data
    GROUP BY order_cart_bucket
),

-- Calculate lift for checkout
checkout_lift AS (
    SELECT 
        checkout_bucket AS recency_bucket,
        ROUND((AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) - 
               AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END)) * 100, 2) AS checkout_lift
    FROM bucketed_data
    GROUP BY checkout_bucket
),

-- Calculate lift for purchase
purchase_lift AS (
    SELECT 
        purchase_bucket AS recency_bucket,
        ROUND((AVG(CASE WHEN tag = 'treatment' THEN has_order_post_exposure END) - 
               AVG(CASE WHEN tag = 'control' THEN has_order_post_exposure END)) * 100, 2) AS purchase_lift
    FROM bucketed_data
    GROUP BY purchase_bucket
)

-- Final output: Join all lifts together
SELECT
    COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket, 
             ch.recency_bucket, pu.recency_bucket) AS recency_bucket,
    sc.store_content_lift,
    sp.store_page_lift,
    oc.order_cart_lift,
    ch.checkout_lift,
    pu.purchase_lift
FROM store_content_lift sc
FULL OUTER JOIN store_page_lift sp ON sc.recency_bucket = sp.recency_bucket
FULL OUTER JOIN order_cart_lift oc ON COALESCE(sc.recency_bucket, sp.recency_bucket) = oc.recency_bucket
FULL OUTER JOIN checkout_lift ch ON COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket) = ch.recency_bucket
FULL OUTER JOIN purchase_lift pu ON COALESCE(sc.recency_bucket, sp.recency_bucket, oc.recency_bucket, ch.recency_bucket) = pu.recency_bucket
ORDER BY 
    CASE 
        WHEN recency_bucket = '0-90 days' THEN 1
        WHEN recency_bucket = '90-120 days' THEN 2
        WHEN recency_bucket = '120-150 days' THEN 3
        WHEN recency_bucket = '150-180 days' THEN 4
        WHEN recency_bucket = '180-365 days' THEN 5
        WHEN recency_bucket = '365+ days' THEN 6
        WHEN recency_bucket = 'No visit' THEN 7
    END;


