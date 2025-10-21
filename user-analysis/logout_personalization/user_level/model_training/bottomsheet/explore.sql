
select 'overall' as treatment, avg(DEVICE_ID_PLACED_AN_ORDER_REAL_TIME::int) 
, avg(has_associated_consumer_id::int), count(1) as total_records
from proddb.markwu.logged_out_personalization_training_v2
union
select DEVICE_ID_SHOW_APP_DOWNLOAD_BOTTOM_SHEET_REAL_TIME as treatment, avg(DEVICE_ID_PLACED_AN_ORDER_REAL_TIME::int) 
, avg(has_associated_consumer_id::int), count(1) as total_records
from proddb.markwu.logged_out_personalization_training_v2
group by all

;
select 
select top 10 * from proddb.markwu.logged_out_personalization_training_v2;

select 'overall' as treatment, avg(DEVICE_ID_PLACED_AN_ORDER_REAL_TIME::int) 
, avg(has_associated_consumer_id::int), count(1) as total_records
from proddb.markwu.logged_out_personalization_web_device_id_v1
union
select DEVICE_ID_SHOW_GOOGLE_ONE_TAP_MODAL_REAL_TIME as treatment, avg(DEVICE_ID_PLACED_AN_ORDER_REAL_TIME::int) 
, avg(has_associated_consumer_id::int), count(1) as total_records
from proddb.markwu.logged_out_personalization_web_device_id_v1
group by all
;

select 0.071376/0.071711-1 as uplift_pct;


select count(1) from proddb.fionafan.logged_out_personalization_training_v4;

select count(1) from proddb.fionafan.raw_unique_web_device_id_add_recent_28_days_web_info_comprehensive_v5;