set start_date = current_date()-28;
set end_date = current_date();

with new_again_cx as (
select distinct v.CONSUMER_ID
--, v.calendar_date

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
, ep.program_experiment_name
, ep.sent_at as eligibility_date

FROM new_again_cx n
INNER JOIN SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep ON ep.CONSUMER_ID = n.CONSUMER_ID

  
WHERE ep.TIMESTAMP between $start_date and $end_date
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
    'ep_consumer_very_churned_low_vp_us_v1', 
    'ep_consumer_very_churned_med_vp_us_v1', 
    'ep_consumer_super_churned_low_vp_us_v1', 
    'ep_consumer_super_churned_med_vp_us_v1', 
    'ep_consumer_churned_low_vp_us_v1', 
    'ep_consumer_churned_med_vp_us_v1'
)
AND ep.PROGRAM_EXPERIMENT_VARIANT IS NOT NULL
and lower(ep.PROGRAM_EXPERIMENT_VARIANT) not like '%control%'
group by 1,2,3,4,5
)

select program_name, PROGRAM_EXPERIMENT_VARIANT, program_experiment_name, count(1) cnt 
from promo_eligible_cx group by all;