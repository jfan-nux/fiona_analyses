





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