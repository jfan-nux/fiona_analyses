create or replace table shivanipoddar.tableY as (
SELECT
  n.*,
  c.LIFESTAGE,
  FLOOR(DATEDIFF(day, c.first_order_date, n.SENT_AT_DATE) / 7) + 1 AS LIFECYCLE_WEEK
FROM edw.consumer.fact_consumer_notification_engagement n
JOIN edw.growth.consumer_growth_accounting_scd3 c
  ON n.CONSUMER_ID = c.CONSUMER_ID
  AND n.SENT_AT_DATE >= c.SCD_START_DATE
  AND (n.SENT_AT_DATE < c.SCD_END_DATE OR c.SCD_END_DATE IS NULL)
JOIN proddb.public.dimension_consumer dc
  ON n.CONSUMER_ID = dc.ID
WHERE 1=1
  AND c.LIFESTAGE ILIKE 'New Cx'
  AND n.SENT_AT_DATE >= '2025-07-01'
  AND n.SENT_AT_DATE < '2025-08-13'
  AND c.first_order_date >= '2025-07-01'
  AND c.first_order_date < '2025-07-15'
  AND dc.DEFAULT_COUNTRY = 'United States'
  );
  
---- copy, but with table Y

with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2),

raw_data as (
select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
--coalesce(jl.title, fz.title) as title, 
jl.lifecycle_week,
--fz.pushbody, 
jl.device_id,
jl.received_at,
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.receive_within_1h,
jl.open_within_1h, 
jl.visit_within_1h,
jl.order_within_1h,
jl.device_unsubscribe_within_1h,
jl.unsubscribe_within_1h,
jl.uninstall_within_1h,
is_valid_send
from shivanipoddar.tableY jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'
--and is_valid_send = 1

)

select notification_source, master_campaign_name, --, title, pushbody, lifecycle_week,
count(DISTINCT deduped_message_id) AS num_push,
count(DISTINCT CASE WHEN open_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_open, 
num_open/nullif(num_push,0) AS send_to_open_1H,
count(DISTINCT CASE WHEN visit_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit, 
num_visit/nullif(num_push,0) AS send_to_visit_1H,
count(DISTINCT CASE WHEN order_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_order,
num_order/nullif(num_push,0) AS send_to_order_1H,
count(DISTINCT CASE WHEN device_unsubscribe_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_device_unsubs,
num_device_unsubs/nullif(num_push,0) AS device_unsub_1H,
count(DISTINCT CASE WHEN unsubscribe_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs,
num_unsubs/nullif(num_push,0) AS unsub_1H,
count(DISTINCT CASE WHEN uninstall_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall,
num_uninstall/nullif(num_push,0) AS uninstall_1H
from raw_data
where 1=1
and notification_source = 'FPN Postal Service'
and is_valid_send = 1
--and master_campaign_name ilike '%TRG-NewNV_ALDIAdoptionQ4-P-US-NV-P%'
group by 1, 2 --, 3 --, 4, 5
--having num_push > 10000
order by num_push desc
;

---- how many pushes do we send, on average every week of the life stage?
with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2),

raw_data as (
select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
coalesce(jl.title, fz.title) as title, 
jl.lifecycle_week,
fz.pushbody, 
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.open_within_24h, 
jl.visit_within_24h,
jl.order_within_24h,
jl.device_unsubscribe_within_24h,
jl.unsubscribe_within_24h,
jl.uninstall_within_24h
from shivanipoddar.tableY jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'

),

dataq as (
select consumer_id, lifecycle_week, count(DISTINCT deduped_message_id) AS num_push
from raw_data
group by 1, 2
)

select lifecycle_week, avg(num_push), variance(num_push), stddev(num_push)
from dataq
group by 1 
order by 1 asc;


----- how many notifs, on average, before first order?

with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2),


temp_table as (
select distinct 
jl.consumer_id,
jl.notification_source, 
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
coalesce(jl.title, fz.title) as title, 
fz.pushbody, 
date_trunc('hour', jl.sent_at) as sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at
from shivanipoddar.tableY jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'

--and consumer_id = 1125900237214048
),

tt_2 as (
select temp_table.*, RANK() over (partition by consumer_id order by date_trunc('hour', sent_at) asc) as notif_sequence
from temp_table
--order by consumer_id, notif_sequence desc
--qualify notif_sequence in (1)
--and notification_source = 'FPN Postal Service'
qualify ordered_at is not null
),

tt_3 as (
select consumer_id, min(notif_sequence) as first_order_by_push
from tt_2
group by 1

)

select avg(first_order_by_push)
from tt_3 ;


------------ mark notification journeys to unsubs and orders


with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2),


temp_table as (
select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
coalesce(jl.title, fz.title) as title, 
jl.lifecycle_week,
fz.pushbody, 
jl.device_id,
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.open_within_24h, 
jl.visit_within_24h,
jl.order_within_24h,
jl.device_unsubscribe_within_24h,
jl.unsubscribe_within_24h,
jl.uninstall_within_24h
from shivanipoddar.tableY jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'
--and consumer_id = 1125900237214048
),

tt_2 as (
select temp_table.*, RANK() over (partition by consumer_id order by date_trunc('hour', sent_at) asc) as notif_sequence
from temp_table
--order by consumer_id, notif_sequence desc
--qualify notif_sequence in (1)
--and notification_source = 'FPN Postal Service'
--and ordered_at is not null
)

select master_campaign_name, title, pushbody,
count(DISTINCT deduped_message_id) AS num_push,
count(DISTINCT CASE WHEN open_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_open, 
num_open/nullif(num_push,0) AS send_to_open_24H,
count(DISTINCT CASE WHEN visit_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit, 
num_visit/nullif(num_push,0) AS send_to_visit_24H,
count(DISTINCT CASE WHEN order_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_order,
num_order/nullif(num_push,0) AS send_to_order_24H,
count(DISTINCT CASE WHEN device_unsubscribe_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_device_unsubs,
num_device_unsubs/nullif(num_push,0) AS device_unsub_24H,
count(DISTINCT CASE WHEN unsubscribe_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs,
num_unsubs/nullif(num_push,0) AS unsub_24H,
count(DISTINCT CASE WHEN uninstall_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall,
num_uninstall/nullif(num_push,0) AS uninstall_24H
from tt_2
where notif_sequence = 1
group by 1, 2, 3
having num_push > 5000
order by 4 desc
;


-------- Copy analysis

with braze_copy as (
select distinct canvas_id, canvas_name, any_value(alert_title) as title, any_value(alert_body) as pushbody
from fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt
where sent_at_timestamp_iso_utc >= current_date-90
and alert_title is not null
group by 1, 2)


select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
coalesce(jl.CANVAS_ID, jl.campaign_id) as master_campaign_id,
coalesce(jl.canvas_name, jl.campaign_name) as master_campaign_name,
jl.canvas_step_name, 
coalesce(jl.title, fz.title) as title, 
jl.lifecycle_week,
fz.pushbody, 
jl.device_id,
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.open_within_24h, 
jl.visit_within_24h,
jl.order_within_24h,
jl.device_unsubscribe_within_24h,
jl.unsubscribe_within_24h,
jl.uninstall_within_24h
from shivanipoddar.tableY jl
left join braze_copy fz on fz.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and master_campaign_name != '[Martech] FPN Silent Push'
and master_campaign_name = 'TRG-NPWS30d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-YOUGOT40'
--and master_campaign_name ilike ('%FMX_GIMME30%') 
;
--and consumer_id = 1125900237214048



with temptable as (
select distinct 
jl.consumer_id,
jl.notification_source, 
deduped_message_id,
dd_event_id,
jl.canvas_id,
jl.canvas_name,
jl.canvas_step_name, 
bt.alert_title, 
bt.alert_body,
jl.lifecycle_week,
jl.device_id,
jl.sent_at,
jl.unsubscribed_at,
jl.ordered_at,
jl.opened_at,
jl.click_through_at,
jl.open_within_24h, 
jl.visit_within_24h,
jl.order_within_24h,
jl.device_unsubscribe_within_24h,
jl.unsubscribe_within_24h,
jl.uninstall_within_24h,
case when bt.alert_body ilike '%miss%' THEN 'Don''t miss out on 30% off'
when bt.alert_body ilike '%PLUS%' THEN 'PLUS 30% off for your entire first month'
else 'Use code GIMME30 and save up to $7' END as bucket,
date_trunc(day, bt.sent_at_timestamp_iso_utc),
date_trunc(day, jl.sent_at),
bt.external_id,
jl.consumer_id,
bt.canvas_name,
jl.canvas_name
from shivanipoddar.tableY jl
left join fusion_dev.test_rahul_narakula.braze_sent_messages_push_bt bt 
on date_trunc(day, bt.sent_at_timestamp_iso_utc) = date_trunc(day, jl.sent_at)
and bt.external_id = jl.consumer_id
and bt.canvas_name = jl.canvas_name
where 1=1
and notification_channel = 'PUSH'
and jl.canvas_name = 'TRG-NA-FMX_GIMME30_W1-SO-US-Ret-P-Y-NA-EN_US' 
--and consumer_id = 1125900237214048
)

select bucket, 
count(DISTINCT deduped_message_id) AS num_push,
count(DISTINCT CASE WHEN open_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_open, 
num_open/nullif(num_push,0) AS send_to_open_24H,
count(DISTINCT CASE WHEN visit_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit, 
num_visit/nullif(num_push,0) AS send_to_visit_24H,
count(DISTINCT CASE WHEN order_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_order,
num_order/nullif(num_push,0) AS send_to_order_24H,
count(DISTINCT CASE WHEN device_unsubscribe_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_device_unsubs,
num_device_unsubs/nullif(num_push,0) AS device_unsub_24H,
count(DISTINCT CASE WHEN unsubscribe_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs,
num_unsubs/nullif(num_push,0) AS unsub_24H,
count(DISTINCT CASE WHEN uninstall_within_24h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall,
num_uninstall/nullif(num_push,0) AS uninstall_24H
from temptable
where 1=1
--and master_campaign_name ilike '%TRG-NewNV_ALDIAdoptionQ4-P-US-NV-P%'
group by 1
--having num_push > 10000
--order by num_push desc

;

select distinct title, body,
count(DISTINCT deduped_message_id) AS num_push,
--count(DISTINCT CASE WHEN receive_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_push,
count(DISTINCT CASE WHEN open_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_open, 
num_open/nullif(num_push,0) AS send_to_open_1H,
count(DISTINCT CASE WHEN visit_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_visit, 
num_visit/nullif(num_push,0) AS send_to_visit_1H,
count(DISTINCT CASE WHEN order_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_order,
num_order/nullif(num_push,0) AS send_to_order_1H,
count(DISTINCT CASE WHEN device_unsubscribe_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_device_unsubs,
num_device_unsubs/nullif(num_push,0) AS device_unsub_1H,
count(DISTINCT CASE WHEN unsubscribe_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_unsubs,
num_unsubs/nullif(num_push,0) AS unsub_1H,
count(DISTINCT CASE WHEN uninstall_within_1h = 1 THEN deduped_message_id ELSE NULL END) AS num_uninstall,
num_uninstall/nullif(num_push,0) AS uninstall_1H
from shivanipoddar.tableY
--edw.consumer.fact_consumer_notification_engagement
where notification_source = 'FPN Postal Service'
and campaign_name ilike ('%universe%')
--and campaign_name ilike ('ep_dd_batch_personalized_send_day_notification_store_promo_launch', 'ep_dd_batch_consumer_everlift_notification_store_promo_launch')
--AND SENT_AT_DATE >= '2025-07-01'
--AND SENT_AT_DATE < '2025-08-13'
group by 1, 2
order by 3 desc;
--where canvas_name = 'TRG-NPWS45d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-NEW40OFF' ;


-- edw.consumer.fact_consumer_notification_engagement
select distinct title, body from edw.consumer.fact_consumer_notification_engagement 
where 1=1
--and notification_source = 'FPN Postal Service' 
and campaign_name = 'TRG-NPWS30d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-YOUGOT40' 
--and body is not null 
--limit 50
;

select notification_source, is_valid_send, count(*)
from shivanipoddar.tableY
group by 1,2;
