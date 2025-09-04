select * from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt limit 10;


select DISTINCT TABLE_NAME from Tyleranderson.sf_columns where lower(column_name) ilike '%canvas_id%' and table_catalog not like '%CAVIAR%' 
and table_schema not like '%CAVIAR%'
limit 1000;

select * from marketing_fivetran.braze_consumer.canvas_tag  limit 10;


select * from proddb.public.dimension_consumer limit 10;

select distinct notification_source, count(1) cnt from edw.consumer.fact_consumer_notification_engagement 
where sent_at_date>= '2025-08-31' group by all order by cnt desc;


