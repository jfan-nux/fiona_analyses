select * from segment_events_raw.consumer_production.order_cart_submit_received
where timestamp>='2025-08-20' and consumer_id is not null limit 10;


