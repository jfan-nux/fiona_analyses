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
