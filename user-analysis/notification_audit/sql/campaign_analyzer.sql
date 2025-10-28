select count(1) from edw.consumer.campaign_analyzer_exposures where exposure_date= '2025-09-01' 
and BUSINESS_CAMPAIGN_NAME ='reserved_for_notification_v2_do_not_use';

select * from edw.consumer.campaign_analyzer_exposures where consumer_id = '248545980' and exposure_date between '2025-07-21' and '2025-07-22' limit 100;
select top 100 *
from proddb.fionafan.all_user_notifications_base 
where campaign_name = 'reserved_for_notification_v2_do_not_use' 
and notification_source ='FPN Postal Service' 
and notification_channel = 'PUSH'
and notification_message_type_overall != 'TRANSACTIONAL'
and coalesce(canvas_name, campaign_name) != '[Martech] FPN Silent Push'
and is_valid_send = 1 -- mostly valid for fpn sends;