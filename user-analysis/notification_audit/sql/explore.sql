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



select case when bz_consumer_id is null then 0 else 1 end as bz_present, count(1) cnt from proddb.fionafan.notif_base_table_w_braze_week 

where notification_source ='Braze' 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 -- mostly valid for fpn sends
group by all;




proddb.ml.fact_cx_cross_vertical_propensity_scores_v1
;
