set start_date = current_date()-28;
set end_date = current_date();

with new_again_cx as (
select distinct v.CONSUMER_ID

from edw.consumer.combined_growth_accounting_pt_base v 

inner JOIN edw.growth.fact_consumer_app_open_events a ON a.CONSUMER_ID = v.CONSUMER_ID


where 1=1
and v.CALENDAR_DATE between $start_date and $end_date
and v.VISIT_STATUS_90D_DEFINITION = 'resurrected_today'
and a.EVENT_TIMESTAMP between $start_date and $end_date

group by 1
),

promo_eligible_cx as (
select distinct ep.consumer_id
, ep.program_name
, ep.PROGRAM_EXPERIMENT_VARIANT

FROM new_again_cx n
INNER JOIN SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep ON ep.CONSUMER_ID = n.CONSUMER_ID

  
WHERE ep.TIMESTAMP between $start_date and $end_date
AND ep.PROGRAM_NAME IN (
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
AND ep.PROGRAM_EXPERIMENT_VARIANT IS NOT NULL
and lower(ep.PROGRAM_EXPERIMENT_VARIANT) not like '%control%'
group by 1,2,3
)



select p.program_name
, count(p.consumer_id)

from promo_eligible_cx p

group by 1
order by 2 desc;

select * from edw.growth.fact_consumer_app_open_events where 1=1
and consumer_id in ('504375583','508874255','1353964748','498587320','1152811383','141964823','884693816','1879161170') 
and event_timestamp between '2025-08-10'::date-7 and '2025-08-10'
 limit 10;

 select * from tyleranderson.events_all 
where event_date between '2025-08-10'::date-7 and '2025-08-10' 
and user_id in ('504375583','508874255','1353964748','498587320','1152811383','141964823','884693816','1879161170') 
order by user_id, timestamp asc;


select *
from iguazu.consumer.m_card_view
where timestamp between '2025-08-10'::date-7 and '2025-08-10'
and consumer_id  ='1353964748'
limit 10;



SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-08-11';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

create or replace table proddb.fionafan.reonboarding_exposure as (
SELECT  
        ee.tag,
        ee.result,
        ee.bucket_key,
        REPLACE(LOWER(CASE WHEN bucket_key LIKE 'dx_%' THEN bucket_key
                     ELSE 'dx_'||bucket_key END), '-') AS dd_device_id_filtered,
        segment,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS exposure_day,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS exposure_time,
        max(ee.custom_attributes:userId) as user_id
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
    AND experiment_version::INT = $version
    AND segment = $segment
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY 1,2,3,4,5

);
CREATE OR REPLACE TABLE proddb.fionafan.onboarding_promo_eligible_users AS (
WITH exposure AS (
    select * from proddb.fionafan.reonboarding_exposure
)

select e.tag, e.user_id, e.dd_device_ID_filtered, e.exposure_time, ep.program_name, min(ep.timestamp) as min_timestamp, max(ep.timestamp) as max_timestamp
from exposure e
INNER JOIN SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep ON ep.CONSUMER_ID = e.user_id
WHERE ep.TIMESTAMP between $start_date-90 and $end_date
AND ep.PROGRAM_NAME IN (
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
AND ep.PROGRAM_EXPERIMENT_VARIANT IS NOT NULL
and lower(ep.PROGRAM_EXPERIMENT_VARIANT) not like '%control%'
group by all
);

select count(distinct user_id) from proddb.fionafan.onboarding_promo_eligible_users limit 10;
select count(1) from proddb.fionafan.reonboarding_exposure where tag='treatment' limit 10;


select a.user_id, 
from proddb.fionafan.reonboarding_exposure a
left join proddb.fionafan.onboarding_promo_eligible_users b
on a.user_id = b.user_id
and a.tag='treatment'
;

select user_id, count(1) cnt from proddb.fionafan.onboarding_promo_eligible_users group by all having  cnt>1 limit 10;

select * from proddb.fionafan.onboarding_promo_eligible_users where user_id = '1417449834' limit 10;


select 
select count(distinct user_id) from proddb.fionafan.onboarding_promo_eligible_users where tag = 'treatment' and exposure_time <=max_timestamp limit 10;

SELECT
  consumer_id,
  max(CASE WHEN promo_title IS NOT NULL AND promo_title <> '' THEN 1 ELSE 0 END) AS had_promo
FROM
  iguazu.consumer.m_onboarding_end_promo_page_view_ice
WHERE 
  iguazu_timestamp > NOW() - INTERVAL '7' DAY
AND
  onboarding_type = 'resurrected_user'
AND
  dd_platform = 'ios';


-- People in promo end-promo view (last 7d, iOS resurrected) but NOT in eligible_users
with base as (
  select distinct to_varchar(consumer_id) as user_id
  from iguazu.consumer.m_onboarding_end_promo_page_view_ice
  where iguazu_timestamp > dateadd(day, -7, current_timestamp())
    and onboarding_type = 'resurrected_user'
    and dd_platform = 'ios' and promo_title = 'Welcome back'
),
eligible as (
  select distinct user_id, max(program_name) as program_name
  from proddb.fionafan.onboarding_promo_eligible_users
  where tag = 'treatment'
    and exposure_time <= max_timestamp
group by all
),
error_user as (
select b.user_id, e.program_name
from base b
left join eligible e
  on e.user_id = b.user_id
where e.user_id is not null
),
joined_with_bucket as (
  select
    event_recency_bucket,
    count(1) as cnt,
    min(user_id) as sample_user_id,
    listagg(iff(rn <= 10, user_id, null), ',') within group (order by user_id) as top10_user_ids
  from (
    select
      b.event_recency_bucket,
      a.user_id,
      row_number() over (partition by b.event_recency_bucket order by a.user_id) as rn
    from error_user a
    left join proddb.public.REONBOARDING_PRIOR_ENGAGEMENT_STATUS b
      on a.user_id = b.user_id
  ) ranked
  group by all
)

select 'eligible_for_promo' as type, count(1) as cnt, null as sample_user_id, null as top10_user_ids from eligible
union all
select 'error_user' as type, count(1) as cnt, null as sample_user_id, null as top10_user_ids from error_user
union all
select 'error_user_' || event_recency_bucket as type, sum(cnt) as cnt, min(sample_user_id) as sample_user_id, max(top10_user_ids) as top10_user_ids
from joined_with_bucket
group by event_recency_bucket;



select * from proddb.fionafan.onboarding_promo_eligible_users where user_id = '807907832' limit 10;



select had_promo, count(1) cnt from (
SELECT 
  consumer_id, 
  max(case when REGEXP_LIKE(promo_title, '%') then 1 else 0 END ) as had_promo, 
FROM 
  iguazu.consumer.m_onboarding_end_promo_page_view_ice a
  inner join (select distinct user_id from proddb.public.REONBOARDING_PRIOR_ENGAGEMENT_STATUS where event_recency_bucket = 'No logged events since 2025-06-12' and tag = 'treatment') b on a.CONSUMER_ID = b.user_id

WHERE 
  iguazu_timestamp > current_date - INTERVAL '7 DAYs'
AND
  onboarding_type = 'resurrected_user'
AND
  dd_platform = 'ios'
group by all having had_promo = 0) group by all;