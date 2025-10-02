





-- select user_id, count(1) cnt from proddb.fionafan.experiment_preference_events group by all having cnt>1 order by cnt desc;
select * from proddb.fionafan.experiment_preference_events where user_id = '1125900323560425' order by event_timestamp asc limit 10;




select count(1), sum(case when entity_ids is not null then 1 else 0 end) from proddb.fionafan.experiment_preference_events ;

-- select date_trunc('day', iguazu_timestamp) as date, count(1) cnt 
-- FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice group by all order by 1;


-- Test the new table with action ranking
select event, discovery_surface, discovery_feature, detail, count(1) cnt
from proddb.fionafan.experiment_preference_events 
where event_timestamp >= effective_exposure_time and event_timestamp < effective_exposure_time + interval '24 hour'
and action_rank<=10
and entity_ids is not null
group by all
;


-- Check the new table - following the experiment analysis pattern
SELECT 
  tag, is_first_ordercart,
  COUNT(DISTINCT dd_device_ID_filtered) AS unique_users,
  COUNT(DISTINCT delivery_id) AS total_orders,
  count(1) cnt,
  AVG(total_item_count) AS avg_item_count
FROM proddb.fionafan.experiment_preference_orders
GROUP BY all
ORDER BY all;


with event_type_totals as (
  select event_type, count(1) as cnt_total
  from proddb.tyleranderson.events_all 
  where date_trunc('day',timestamp)='2025-08-01' 
  group by event_type
),
detailed_counts as (
  select event_type, case when event_type = 'error' then 'error' else event end as event
  ,event_rank, case when event_type = 'error' then 'error' else discovery_feature end as  discovery_feature, discovery_surface, count(1) cnt
  from proddb.tyleranderson.events_all 
  where date_trunc('day',timestamp)='2025-08-01' 
  group by event_type, case when event_type = 'error' then 'error' else event end, event_rank
  , case when event_type = 'error' then 'error' else discovery_feature end, discovery_surface
)
select d.event_type, d.event, d.event_rank, d.discovery_feature, d.discovery_surface, d.cnt, t.cnt_total
from detailed_counts d
join event_type_totals t on d.event_type = t.event_type
order by t.cnt_total desc, d.cnt desc;

select distinct event_type, count(1) cnt from proddb.tyleranderson.events_all where date_trunc('day',timestamp)='2025-08-01' group by all;

select event_type, event, event_rank,discovery_feature,discovery_surface,  count(1) cnt from proddb.tyleranderson.events_all 

where event_type = 'funnel' and date_trunc('day',timestamp)='2025-08-01'
group by all
order by event_rank asc, cnt desc;

-- does the viewed content contain at least one expressed preference? at what order of viewed content?
-- first session length, and rate of place order at first session




select *  FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice where consumer_id = '1449581118'  limit 10;

select date_trunc('day', iguazu_timestamp) as date, dd_platform, promo_title, onboarding_type, count(1) cnt 
FROM iguazu.consumer.m_onboarding_end_promo_page_click_ice group by all order by 1;



select distinct page FROM iguazu.consumer.M_onboarding_page_click_ice;



select experiment_name, min(exposure_time) as min_exposure_time, max(exposure_time) as max_exposure_time from proddb.public.fact_dedup_experiment_exposure
where exposure_time >='2025-03-15' and exposure_time < '2025-05-16' and experiment_name = 'cx_mobile_show_onboarding_screen_marketing_sms'
group by all ;

select * from proddb.fionafan.document_index_community limit 10;

select active_date, count(1) cnt from proddb.ml.cx_sensitivity_v3 where active_date>='2025-09-07' group by all;

select * from proddb.tyleranderson.events_all where event_date = '2025-09-07' and user_id = '1125900281188900' limit 10;


select tag, case when b.dd_device_id_filtered is not null then 'view' else 'no_view' end as had_view, count(1) cnt 
from proddb.fionafan.preference_experiment_data a
left join (select distinct dd_device_id_filtered from proddb.fionafan.preference_experiment_m_card_view ) b
on a.dd_device_id_filtered = b.dd_device_id_filtered
group by all;

limit 10;

select * from limit 10;


 SELECT DATE_TRUNC('minute', CONVERT_TIMEZONE('UTC','America/Los_Angeles', iguazu_timestamp)) AS minute_pst, count(distinct consumer_id)
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice

  WHERE iguazu_timestamp >= current_date - 1 AND iguazu_timestamp < current_date + 1
  GROUP BY 1
  order by 1;

  

  SELECT DISTINCT CAST(target_id AS STRING) AS target_id
    FROM audience_service.public.cassandra_tags_by_target
    WHERE tag_name = 'nux_reonboarding_ms1_eligible'



    SELECT DATE(iguazu_sent_at) AS day, consumer_id
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS

  WHERE placement_type = 'PLACEMENT_TYPE_POST_ONBOARDING'
    AND is_eligible = TRUE
    AND iguazu_sent_at >= current_date - 14 AND iguazu_sent_at < current_date + 1;
  
  select placement_type, count(1) cnt
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.CAMPAIGN_ELIGIBLE_EVENTS

  WHERE 1=1
    AND is_eligible = TRUE
    AND iguazu_sent_at >= current_date - 5 AND iguazu_sent_at < current_date + 1
    group by all;


    WITH nux AS (
  SELECT
    CAST(target_id AS STRING) AS target_id,
    LOWER(tag_name) AS tag_name
  FROM audience_service.public.cassandra_tags_by_target
  WHERE tag_name ILIKE 'nux_%'
),
per_user AS (
  SELECT
    target_id,
    COUNT(DISTINCT tag_name) AS num_nux_tags,
    ARRAY_AGG(DISTINCT tag_name) AS tag_list,
    LISTAGG(DISTINCT tag_name, ', ') WITHIN GROUP (ORDER BY tag_name) AS tag_list_csv
  FROM nux
  GROUP BY target_id
)
SELECT
  target_id,
  num_nux_tags,
  tag_list,
  tag_list_csv
FROM per_user
WHERE num_nux_tags > 1
ORDER BY num_nux_tags DESC, target_id;

select distinct tag_name, count(1) cnt FROM audience_service.public.cassandra_tags_by_target

  WHERE imported_at>='2025-09-01' and tag_name ILIKE 'nux_%'
group by all
order by cnt desc;



select count(1) from fact_campaign_merchant_criteria_audience_target
where campaign_id = '972621ec-85b8-459e-9af5-8b2e05f5acf0';

select *
from audience_service.public.cassandra_tags_by_target where tag_name = 'ep_aus_F28D_treatment2' limit 10;



SELECT
  c.campaign_id,
  f.audience_tag_name AS tag,
  COUNT(DISTINCT f.target_id) AS unique_targets
FROM campaigns c
LEFT JOIN fact_campaign_merchant_criteria_audience_target f
  ON f.campaign_id = c.campaign_id
GROUP BY 1,2
ORDER BY 1,2;

-- Overlap of specified audience tags with NUX reonboarding eligible set
create or replace table proddb.fionafan.post_onboarding_end_promo_audience_tags as (
WITH tags(tag_name) AS (
  SELECT column1 FROM VALUES
    ('can-js-npws-promo-test-h2-25-trt-40off1'),
    ('AUS-npws-promo-treatment'),
    ('ep_aus_F28D_treatment2'),
    ('can-js-npws-promo-test-h2-25-trt-40off1'),
    ('can-js-npws-promo-v4-0-orders'),
    ('can-js-npws-promo-test-h2-25-trt-40off3'),
    ('can-js-npws-promo-test-h2-25-trt-50off1'),
    ('NZ_NewCx_30'),
    ('NZ_NewCx_50'),
    ('npws_45d_t1'),
    ('npws_30d_t2'),
    ('npws_14d_t3'),
    ('npws_no_siw_t1'),
    ('npws_siw_t2'),
    ('npws-fobt-interim-40off1'),
    ('ep_au_rx_resurrection_automation_90d_treatment'),
    ('ep_au_rx_resurrection_automation_120d_treatment'),
    ('ep_au_rx_resurrection_automation_150d_treatment'),
    ('ep_nz_resurrection_automation_180d'),
    ('ep_nz_resurrection_automation_150d_treatment'),
    ('ep_consumer_very_churned_low_vp_us_v1_t1'),
    ('ep_consumer_very_churned_med_vp_us_v1_t1'),
    ('ep_consumer_super_churned_low_vp_us_v1_t1'),
    ('ep_consumer_super_churned_low_vp_us_v1_t1'),
    ('ep_consumer_churned_low_vp_us_v1_t1'),
    ('ep_consumer_churned_med_vp_us_v1_t1'),
    ('ep_consumer_dormant_late_bloomers_us_v1_t1'),
    ('ep_consumer_dormant_winback_us_v1_t1'),
    ('ep_consumer_dewo_phase2_us_v1_t1'),
    ('ep_consumer_dewo_phase1_retarget_us_v1_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p1_active_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p1_dormant_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_active_active_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_active_dormant_t1'),
    ('ep_consumer_ml_churn_prevention_us_v2_p2_dormant_dormant_t1'),
    ('ep_consumer_enhanced_rxauto_90d_us_v1_t1'),
    ('ep_consumer_enhanced_rxauto_120d_test_us_v1_t2'),
    ('ep_consumer_enhanced_rxauto_150day_test_us_v1_t1'),
    ('ep_consumer_enhanced_rxauto_180day_test_us_v1_t1'),
    ('ep_consumer_churned_btm_pickup_exclude_test_us_v1_t2'),
    ('ep_consumer_churned_latebloomers_auto_ctc_test_us_v1_t1'),
    ('ep_consumer_rx_reachability_auto_us_t1')
),
audience_targets AS (
  SELECT LOWER(tag_name) AS tag_name, CAST(target_id AS STRING) AS target_id
  FROM audience_service.public.cassandra_tags_by_target

  WHERE LOWER(tag_name) IN (SELECT LOWER(tag_name) FROM tags)
)
select * from audience_targets);
select tag_name, count(1) cnt from proddb.fionafan.post_onboarding_end_promo_audience_tags group by all;
with nux_targets AS (
  SELECT DISTINCT CAST(target_id AS STRING) AS target_id
  FROM audience_service.public.cassandra_tags_by_target
  WHERE LOWER(tag_name) = 'nux_reonboarding_ms1_eligible'
)
SELECT
  a.tag_name,
  COUNT(1) AS tag_targets,
  COUNT(CASE WHEN n.target_id IS NOT NULL THEN 1 END) AS overlap_targets,
  ROUND(COUNT( CASE WHEN n.target_id IS NOT NULL THEN 1 END) * 100.0 / NULLIF(COUNT(1), 0), 2) AS overlap_pct
FROM proddb.fionafan.post_onboarding_end_promo_audience_tags a
right JOIN nux_targets n
  ON n.target_id = a.target_id
GROUP BY a.tag_name
ORDER BY a.tag_name;

-- Overlap of all non-NUX audience tags with resurrected_user, show_promo = 'no' cohort (last 1 day)
WITH cohort AS (
  SELECT DISTINCT CAST(consumer_id AS STRING) AS target_id,promo_title
  FROM iguazu.consumer.m_onboarding_end_promo_page_view_ice
  WHERE iguazu_timestamp >= current_date-2  AND iguazu_timestamp < current_date + 1
    AND LOWER(onboarding_type) = 'resurrected_user'
    AND NOT (POSITION('%' IN promo_title) > 0)
),
audience_targets AS (
select * from proddb.fionafan.post_onboarding_end_promo_audience_tags

)
-- SELECT
--   case when (POSITION('%' IN c.promo_title) > 0)is null then 'promo' else 'no promo' end as promo_flag,
--   a.tag_name,
--   -- c.promo_title,
--   COUNT(DISTINCT c.target_id) AS tag_targets
-- FROM audience_targets a
-- right JOIN cohort c
--   ON c.target_id = a.target_id
-- GROUP BY all;
select c.*, a.tag_name
FROM audience_targets a
right JOIN cohort c
  ON c.target_id = a.target_id
where a.tag_name  is  null;


