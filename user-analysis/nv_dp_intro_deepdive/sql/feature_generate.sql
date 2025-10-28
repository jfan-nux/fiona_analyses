-- Session-level NV vs non-NV impression metrics using only the base events table as source
create or replace table proddb.fionafan.nv_dp_new_user_session_impressions as (


with base as (
  select * from proddb.fionafan.nv_dp_new_user_all_events_30d_w_session_num
)
, events_with_nv as (
  select
    e.consumer_id,
    e.dd_device_id,
    e.join_time,
    e.session_num,
    e.sequential_session_num,
    e.event_timestamp,
    e.event_type,
    e.store_id,
    case when e.store_id is not null and (
           ds.NV_ORG is not null or ds.NV_VERTICAL_NAME is not null or
           ds.NV_BUSINESS_LINE is not null or ds.NV_BUSINESS_SUB_TYPE is not null
         ) then 1 else 0 end as is_nv_store
  from base e
  left join edw.merchant.dimension_store ds
    on e.store_id = ds.store_id
)
select
  dd_device_id,
  consumer_id,
  join_time,
  session_num,
  sequential_session_num,
  min(event_timestamp) as session_start_time,
  min(case when event_type = 'store_impression' then event_timestamp end) as first_impression_time,
  min(case when event_type = 'store_impression' and is_nv_store = 1 then event_timestamp end) as first_nv_impression_time,
  min(case when event_type = 'store_impression' and is_nv_store = 0 and store_id is not null then event_timestamp end) as first_non_nv_impression_time,
  max(case when event_type = 'store_impression' and is_nv_store = 1 then 1 else 0 end) as had_nv_impression,
  max(case when event_type = 'store_impression' and is_nv_store = 0 and store_id is not null then 1 else 0 end) as had_non_nv_impression,
  sum(case when event_type = 'store_impression' and is_nv_store = 1 then 1 else 0 end) as nv_impression_count,
  sum(case when event_type = 'store_impression' and is_nv_store = 0 and store_id is not null then 1 else 0 end) as non_nv_impression_count
from events_with_nv
group by all
);



-- Daily notification features using notifications table as base
create or replace table proddb.fionafan.nv_dp_notifications_daily_features as (

with daily as (
  select
    consumer_id,
    replace(lower(CASE WHEN device_id like 'dx_%' then device_id else 'dx_'||device_id end), '-') as dd_device_id_filtered,
    cast(sent_at as date) as day,
    count(*) as notifications_received,
    case when count(*) > 0 then 1 else 0 end as received_any_notification,
    sum(case when coalesce(has_new_verticals_grocery,0) = 1 then 1 else 0 end) as nv_notifications_received,
    case when sum(case when coalesce(has_new_verticals_grocery,0) = 1 then 1 else 0 end) > 0 then 1 else 0 end as received_any_nv_notification,
    -- DashPass from same weekly table
    sum(case when coalesce(has_dashpass,0) = 1 then 1 else 0 end) as dp_notifications_received,
    case when sum(case when coalesce(has_dashpass,0) = 1 then 1 else 0 end) > 0 then 1 else 0 end as received_any_dp_notification,
    min(send_hour_of_day) as first_send_hour_of_day,
    max(send_hour_of_day) as last_send_hour_of_day,
    array_to_string(array_sort(array_agg(distinct send_hour_of_day)), ',') as send_hours_of_day,
    -- DP hours derived from has_dashpass rows
    min(case when coalesce(has_dashpass,0)=1 then send_hour_of_day end) as dp_first_send_hour_of_day,
    max(case when coalesce(has_dashpass,0)=1 then send_hour_of_day end) as dp_last_send_hour_of_day,
    array_to_string(array_sort(array_agg(distinct case when coalesce(has_dashpass,0)=1 then send_hour_of_day end)), ',') as dp_send_hours_of_day
  from proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week
  where sent_at is not null
  group by consumer_id, dd_device_id_filtered, day
)
select * from daily
);


-- Device-level impression features aggregated from session-level table (aligned to earliest join per device)
create or replace table proddb.fionafan.nv_dp_impressions_device_features as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, session_imps as (
  select s.*
  from proddb.fionafan.nv_dp_new_user_session_impressions s
  join base_devices b
    on s.dd_device_id = b.device_id
   and s.consumer_id = b.consumer_id
   and s.join_time  = b.join_time
)
, agg as (
  select 
    b.device_id,
    b.consumer_id,
    b.join_time,
    max(case when s.sequential_session_num = 1 then s.had_nv_impression else 0 end) as had_nv_imp_first_session,
    max(case when s.sequential_session_num = 1 then s.had_non_nv_impression else 0 end) as had_non_nv_imp_first_session,
    max(case when s.sequential_session_num = 2 then s.had_nv_impression else 0 end) as had_nv_imp_second_session,
    max(case when s.sequential_session_num = 2 then s.had_non_nv_impression else 0 end) as had_non_nv_imp_second_session,
    min(s.first_nv_impression_time) as first_nv_impression_time_overall,
    min(s.first_non_nv_impression_time) as first_non_nv_impression_time_overall
  from base_devices b
  left join session_imps s
    on s.dd_device_id = b.device_id and s.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id, b.join_time
)
select 
  device_id,
  consumer_id,
  join_time,
  had_nv_imp_first_session,
  had_non_nv_imp_first_session,
  had_nv_imp_second_session,
  had_non_nv_imp_second_session,
  case when first_nv_impression_time_overall     <= dateadd('hour', 1,  join_time) then 1 else 0 end as had_nv_imp_1h,
  case when first_non_nv_impression_time_overall <= dateadd('hour', 1,  join_time) then 1 else 0 end as had_non_nv_imp_1h,
  case when first_nv_impression_time_overall     <= dateadd('hour', 4,  join_time) then 1 else 0 end as had_nv_imp_4h,
  case when first_non_nv_impression_time_overall <= dateadd('hour', 4,  join_time) then 1 else 0 end as had_non_nv_imp_4h,
  case when first_nv_impression_time_overall     <= dateadd('hour', 12, join_time) then 1 else 0 end as had_nv_imp_12h,
  case when first_non_nv_impression_time_overall <= dateadd('hour', 12, join_time) then 1 else 0 end as had_non_nv_imp_12h,
  case when first_nv_impression_time_overall     <= dateadd('hour', 24, join_time) then 1 else 0 end as had_nv_imp_24h,
  case when first_non_nv_impression_time_overall <= dateadd('hour', 24, join_time) then 1 else 0 end as had_non_nv_imp_24h,
  case when first_nv_impression_time_overall     <= dateadd('day',  7,  join_time) then 1 else 0 end as had_nv_imp_7d,
  case when first_non_nv_impression_time_overall <= dateadd('day',  7,  join_time) then 1 else 0 end as had_non_nv_imp_7d,
  case when first_nv_impression_time_overall     <= dateadd('day',  30, join_time) then 1 else 0 end as had_nv_imp_30d,
  case when first_non_nv_impression_time_overall <= dateadd('day',  30, join_time) then 1 else 0 end as had_non_nv_imp_30d,
  datediff('minute', join_time, first_nv_impression_time_overall)     as minutes_to_first_nv_impression,
  datediff('minute', join_time, first_non_nv_impression_time_overall) as minutes_to_first_non_nv_impression
from agg
);


-- Device-level notification features aggregated from daily table
create or replace table proddb.fionafan.nv_dp_notifications_device_features as (


with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, notif as (
  select 
    n.consumer_id,
    n.day,
    n.received_any_notification,
    n.nv_notifications_received,
    n.received_any_nv_notification,
    n.received_any_dp_notification
  from proddb.fionafan.nv_dp_notifications_daily_features n
)
select 
  b.device_id,
  b.consumer_id,
  max(case when d.day <= date_trunc('day', dateadd('day', 30, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif_30d,
  max(case when d.day <= date_trunc('day', dateadd('day', 60, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif_60d,
  -- DashPass notification flags from same table
  max(coalesce(d.received_any_dp_notification, 0)) as dp_has_notif_any_day,
  max(case when d.day <= date_trunc('day', dateadd('day', 30, b.join_time)) then coalesce(d.received_any_dp_notification,0) else 0 end) as dp_has_notif_30d,
  max(case when d.day <= date_trunc('day', dateadd('day', 60, b.join_time)) then coalesce(d.received_any_dp_notification,0) else 0 end) as dp_has_notif_60d
from base_devices b
left join notif d
  on d.consumer_id = b.consumer_id
group by b.device_id, b.consumer_id
);


-- Device-level order features aggregated from orders table
create or replace table proddb.fionafan.nv_dp_orders_device_features as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, orders_after_join as (
  select 
    o.dd_device_id_filtered as device_id,
    o.consumer_id,
    o.order_timestamp,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, ranked as (
  select 
    b.device_id,
    b.consumer_id,
    b.join_time,
    o.order_timestamp,
    o.is_nv_order,
    row_number() over (partition by b.device_id order by o.order_timestamp) as order_rank
  from base_devices b
  left join orders_after_join o
    on o.device_id = b.device_id and o.order_timestamp > b.join_time
)
select 
  device_id,
  consumer_id,
  max(case when order_rank = 1 and is_nv_order = 1 then 1 else 0 end) as nv_first_order,
  max(case when order_rank = 2 and is_nv_order = 1 then 1 else 0 end) as nv_second_order,
  max(case when is_nv_order = 1 and order_timestamp <= dateadd('hour', 24, join_time) then 1 else 0 end) as nv_order_24h,
  max(case when is_nv_order = 1 and order_timestamp <= dateadd('day', 7, join_time) then 1 else 0 end)  as nv_order_7d,
  max(case when is_nv_order = 1 and order_timestamp <= dateadd('day', 30, join_time) then 1 else 0 end) as nv_order_30d
from ranked
group by device_id, consumer_id
);


-- Master feature table at device level
create or replace table proddb.fionafan.nv_dp_master_features as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
select 
  b.device_id,
  b.consumer_id,
  -- Impression features
  i.had_nv_imp_first_session,
  i.had_non_nv_imp_first_session,
  i.had_nv_imp_second_session,
  i.had_non_nv_imp_second_session,
  i.had_nv_imp_1h,
  i.had_non_nv_imp_1h,
  i.had_nv_imp_4h,
  i.had_non_nv_imp_4h,
  i.had_nv_imp_12h,
  i.had_non_nv_imp_12h,
  i.had_nv_imp_24h,
  i.had_non_nv_imp_24h,
  i.had_nv_imp_7d,
  i.had_non_nv_imp_7d,
  i.had_nv_imp_30d,
  i.had_non_nv_imp_30d,
  i.minutes_to_first_nv_impression,
  i.minutes_to_first_non_nv_impression,
  -- Notification features
  n.has_nv_notif_30d,
  n.has_nv_notif_60d,
  -- Order features
  o.nv_first_order,
  o.nv_second_order,
  o.nv_order_24h,
  o.nv_order_7d,
  o.nv_order_30d
from base_devices b
left join proddb.fionafan.nv_dp_impressions_device_features i
  on i.device_id = b.device_id
left join proddb.fionafan.nv_dp_notifications_device_features n
  on n.device_id = b.device_id
left join proddb.fionafan.nv_dp_orders_device_features o
  on o.device_id = b.device_id 
);


-- DashPass daily notification features
create or replace table proddb.fionafan.dp_notifications_daily_features as (

with base as (
  select 
    consumer_id,
    replace(lower(CASE WHEN device_id like 'dx_%' then device_id else 'dx_'||device_id end), '-') as device_id,
    cast(sent_at_date as date) as day,
    sent_at_date,
    extract(hour from cast(sent_at_date as timestamp)) as send_hour_of_day,
    coalesce(has_dashpass, 0) as is_dp_notification
  from proddb.fionafan.dp_new_user_notifications_60d_w_braze
)
select
  consumer_id,
  device_id,
  day,
  count(*) as notifications_received,
  case when count(*) > 0 then 1 else 0 end as received_any_notification,
  sum(case when is_dp_notification = 1 then 1 else 0 end) as dp_notifications_received,
  case when sum(case when is_dp_notification = 1 then 1 else 0 end) > 0 then 1 else 0 end as received_any_dp_notification,
  min(send_hour_of_day) as first_send_hour_of_day,
  max(send_hour_of_day) as last_send_hour_of_day,
  array_to_string(array_sort(array_agg(distinct send_hour_of_day)), ',') as send_hours_of_day
from base
group by consumer_id, device_id, day
);


-- DashPass device-level order features (DP at order time)
create or replace table proddb.fionafan.dp_orders_device_features as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, orders_after_join as (
  select 
    o.dd_device_id_filtered as device_id,
    o.consumer_id,
    o.order_timestamp,
    coalesce(o.is_subscribed_consumer, 0) as is_dp_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, ranked as (
  select 
    b.device_id,
    b.consumer_id,
    b.join_time,
    o.order_timestamp,
    o.is_dp_order,
    row_number() over (partition by b.device_id order by o.order_timestamp) as order_rank
  from base_devices b
  left join orders_after_join o
    on o.device_id = b.device_id and o.consumer_id = b.consumer_id and o.order_timestamp > b.join_time
)
select 
  device_id,
  consumer_id,
  max(case when order_rank = 1 and is_dp_order = 1 then 1 else 0 end) as dp_first_order,
  max(case when order_rank = 2 and is_dp_order = 1 then 1 else 0 end) as dp_second_order,
  max(case when is_dp_order = 1 and order_timestamp <= dateadd('hour', 24, join_time) then 1 else 0 end) as dp_order_24h,
  max(case when is_dp_order = 1 and order_timestamp <= dateadd('day', 7, join_time) then 1 else 0 end)  as dp_order_7d,
  max(case when is_dp_order = 1 and order_timestamp <= dateadd('day', 30, join_time) then 1 else 0 end) as dp_order_30d
from ranked
group by device_id, consumer_id
);


-- DashPass master features at device level
create or replace table proddb.fionafan.dp_master_features as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
select 
  b.device_id,
  b.consumer_id,
  n.received_any_dp_notification as has_dp_notif_any_day,
  max(case when d.day <= date_trunc('day', dateadd('day', 30, b.join_time)) then n.received_any_dp_notification else 0 end) as has_dp_notif_30d,
  max(n.received_any_dp_notification) as has_dp_notif_60d,
  o.dp_first_order,
  o.dp_second_order,
  o.dp_order_24h,
  o.dp_order_7d,
  o.dp_order_30d
from base_devices b
left join proddb.fionafan.dp_notifications_daily_features n
  on n.device_id = b.device_id 
left join proddb.fionafan.dp_notifications_daily_features d
  on d.device_id = b.device_id
left join proddb.fionafan.dp_orders_device_features o
  on o.device_id = b.device_id
group by b.device_id, b.consumer_id, n.received_any_dp_notification, o.dp_first_order, o.dp_second_order, o.dp_order_24h, o.dp_order_7d, o.dp_order_30d
);




-- Device-level target labels (orders within windows, first order/new cx, month 1-4 retention)
create or replace table proddb.fionafan.nv_dp_targets as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, orders as (
  select 
    o.dd_device_id_filtered as device_id,
    o.consumer_id,
    o.delivery_id,
    o.order_timestamp,
    coalesce(o.is_first_ordercart_dd, 0) as is_first_ordercart_dd,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order,
    coalesce(o.is_subscribed_consumer, 0) as is_dp_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, orders_joined as (
  select 
    b.device_id,
    b.consumer_id,
    b.join_time,
    o.delivery_id,
    o.order_timestamp,
    o.is_first_ordercart_dd,
    o.is_nv_order,
    o.is_dp_order
  from base_devices b
  left join orders o
    on o.device_id = b.device_id and o.consumer_id = b.consumer_id
)
, first_order as (
  select 
    device_id,
    consumer_id,
    min(order_timestamp) as first_order_ts,
    max(case when is_first_ordercart_dd = 1 then 1 else 0 end) as is_new_cx_first_order
  from orders_joined
  group by device_id, consumer_id
)
, orders_ranked as (
  select 
    o.device_id,
    o.consumer_id,
    o.order_timestamp,
    o.is_nv_order,
    o.is_dp_order,
    row_number() over (partition by o.device_id order by o.order_timestamp) as order_rank
  from orders_joined o
)
, first_second_flags as (
  select 
    device_id,
    consumer_id,
    max(case when order_rank = 1 and is_nv_order = 1 then 1 else 0 end) as nv_first_order,
    max(case when order_rank = 2 and is_nv_order = 1 then 1 else 0 end) as nv_second_order,
    max(case when order_rank = 1 and is_dp_order = 1 then 1 else 0 end) as dp_first_order,
    max(case when order_rank = 2 and is_dp_order = 1 then 1 else 0 end) as dp_second_order
  from orders_ranked
  group by device_id, consumer_id
)
, retention as (
  select 
    b.device_id,
    b.consumer_id,
    max(case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) then 1 else 0 end) as ordered_month1,
    max(case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) then 1 else 0 end) as ordered_month2,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) then 1 else 0 end) as ordered_month3,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day',120, b.join_time) then 1 else 0 end) as ordered_month4
  from base_devices b
  left join orders_joined o
    on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id
)
, retention_nv_dp as (
  select 
    b.device_id,
    b.consumer_id,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90,  b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as nv_order_month3,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as nv_order_month4,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90,  b.join_time) and o.is_dp_order = 1 then 1 else 0 end) as dp_order_month3,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_dp_order = 1 then 1 else 0 end) as dp_order_month4
  from base_devices b
  left join orders_joined o
    on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id
)
select 
  b.device_id,
  b.consumer_id,
  -- Order within 24h of join
  max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) then 1 else 0 end) as ordered_24h,
  -- First order (new cx) flag
  case when f.is_new_cx_first_order = 1 then 1 else 0 end as first_order_new_cx,
  -- Monthly retention targets
  r.ordered_month1,
  r.ordered_month2,
  r.ordered_month3,
  r.ordered_month4,
  max(coalesce(fs.nv_first_order, 0))  as nv_first_order,
  max(coalesce(fs.nv_second_order, 0)) as nv_second_order,
  max(coalesce(fs.dp_first_order, 0))  as dp_first_order,
  max(coalesce(fs.dp_second_order, 0)) as dp_second_order,
  max(coalesce(rnd.nv_order_month3, 0)) as nv_order_month3,
  max(coalesce(rnd.nv_order_month4, 0)) as nv_order_month4,
  max(coalesce(rnd.dp_order_month3, 0)) as dp_order_month3,
  max(coalesce(rnd.dp_order_month4, 0)) as dp_order_month4
from base_devices b
left join orders_joined o
  on o.device_id = b.device_id 
left join first_order f
  on f.device_id = b.device_id 
left join retention r
  on r.device_id = b.device_id
left join first_second_flags fs
  on fs.device_id = b.device_id and fs.consumer_id = b.consumer_id
left join retention_nv_dp rnd
  on rnd.device_id = b.device_id and rnd.consumer_id = b.consumer_id
group by b.device_id, b.consumer_id, first_order_new_cx, r.ordered_month1, r.ordered_month2, r.ordered_month3, r.ordered_month4
);




-- Final assembled features + targets at device level (left-joined to base)
create or replace table proddb.fionafan.nv_dp_features_targets as (

with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time, day as join_day
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
select 
  b.device_id,
  b.consumer_id,
  b.join_time,
  b.join_day,

  -- NV impression features (impute flags to 0)
  coalesce(m.had_nv_imp_first_session, 0)      as had_nv_imp_first_session,
  coalesce(m.had_non_nv_imp_first_session, 0)  as had_non_nv_imp_first_session,
  coalesce(m.had_nv_imp_second_session, 0)     as had_nv_imp_second_session,
  coalesce(m.had_non_nv_imp_second_session, 0) as had_non_nv_imp_second_session,
  coalesce(m.had_nv_imp_1h, 0)                 as had_nv_imp_1h,
  coalesce(m.had_non_nv_imp_1h, 0)             as had_non_nv_imp_1h,
  coalesce(m.had_nv_imp_4h, 0)                 as had_nv_imp_4h,
  coalesce(m.had_non_nv_imp_4h, 0)             as had_non_nv_imp_4h,
  coalesce(m.had_nv_imp_12h, 0)                as had_nv_imp_12h,
  coalesce(m.had_non_nv_imp_12h, 0)            as had_non_nv_imp_12h,
  coalesce(m.had_nv_imp_24h, 0)                as had_nv_imp_24h,
  coalesce(m.had_non_nv_imp_24h, 0)            as had_non_nv_imp_24h,
  coalesce(m.had_nv_imp_7d, 0)                 as had_nv_imp_7d,
  coalesce(m.had_non_nv_imp_7d, 0)             as had_non_nv_imp_7d,
  coalesce(m.had_nv_imp_30d, 0)                as had_nv_imp_30d,
  coalesce(m.had_non_nv_imp_30d, 0)            as had_non_nv_imp_30d,
  -- Keep minutes as NULL if no impression yet
  m.minutes_to_first_nv_impression,
  m.minutes_to_first_non_nv_impression,

  -- NV notification features
  coalesce(m.has_nv_notif_30d, 0) as has_nv_notif_30d,
  coalesce(m.has_nv_notif_60d, 0) as has_nv_notif_60d,

  -- NV order features
  coalesce(m.nv_first_order, 0) as nv_first_order,
  coalesce(m.nv_second_order, 0) as nv_second_order,
  coalesce(m.nv_order_24h, 0) as nv_order_24h,
  coalesce(m.nv_order_7d, 0) as nv_order_7d,
  coalesce(m.nv_order_30d, 0) as nv_order_30d,

  -- DashPass features
  coalesce(dp.has_dp_notif_any_day, 0) as dp_has_notif_any_day,
  coalesce(dp.has_dp_notif_30d, 0)     as dp_has_notif_30d,
  coalesce(dp.has_dp_notif_60d, 0)     as dp_has_notif_60d,
  coalesce(dp.dp_first_order, 0)       as dp_first_order,
  coalesce(dp.dp_second_order, 0)      as dp_second_order,
  coalesce(dp.dp_order_24h, 0)         as dp_order_24h,
  coalesce(dp.dp_order_7d, 0)          as dp_order_7d,
  coalesce(dp.dp_order_30d, 0)         as dp_order_30d,

  -- Targets (impute to 0)
  coalesce(t.ordered_24h, 0)     as target_ordered_24h,
  coalesce(t.first_order_new_cx, 0) as target_first_order_new_cx,
  coalesce(t.ordered_month1, 0)  as target_ordered_month1,
  coalesce(t.ordered_month2, 0)  as target_ordered_month2,
  coalesce(t.ordered_month3, 0)  as target_ordered_month3,
  coalesce(t.ordered_month4, 0)  as target_ordered_month4

from base_devices b
left join proddb.fionafan.nv_dp_master_features m
  on m.device_id = b.device_id and m.consumer_id = b.consumer_id
left join proddb.fionafan.dp_master_features dp
  on dp.device_id = b.device_id and dp.consumer_id = b.consumer_id
left join proddb.fionafan.nv_dp_targets t
  on t.device_id = b.device_id and t.consumer_id = b.consumer_id
);

-- Means summary for all features and targets
select 
  count(*) as n,
  avg(had_nv_imp_first_session)      as mean_had_nv_imp_first_session,
  avg(had_non_nv_imp_first_session)  as mean_had_non_nv_imp_first_session,
  avg(had_nv_imp_second_session)     as mean_had_nv_imp_second_session,
  avg(had_non_nv_imp_second_session) as mean_had_non_nv_imp_second_session,
  avg(had_nv_imp_1h)  as mean_had_nv_imp_1h,
  avg(had_non_nv_imp_1h) as mean_had_non_nv_imp_1h,
  avg(had_nv_imp_4h)  as mean_had_nv_imp_4h,
  avg(had_non_nv_imp_4h) as mean_had_non_nv_imp_4h,
  avg(had_nv_imp_12h) as mean_had_nv_imp_12h,
  avg(had_non_nv_imp_12h) as mean_had_non_nv_imp_12h,
  avg(had_nv_imp_24h) as mean_had_nv_imp_24h,
  avg(had_non_nv_imp_24h) as mean_had_non_nv_imp_24h,
  avg(had_nv_imp_7d)  as mean_had_nv_imp_7d,
  avg(had_non_nv_imp_7d) as mean_had_non_nv_imp_7d,
  avg(had_nv_imp_30d) as mean_had_nv_imp_30d,
  avg(had_non_nv_imp_30d) as mean_had_non_nv_imp_30d,
  avg(minutes_to_first_nv_impression)     as mean_minutes_to_first_nv_impression,
  avg(minutes_to_first_non_nv_impression) as mean_minutes_to_first_non_nv_impression,
  avg(has_nv_notif_30d) as mean_has_nv_notif_30d,
  avg(has_nv_notif_60d) as mean_has_nv_notif_60d,
  avg(nv_first_order) as mean_nv_first_order,
  avg(nv_second_order) as mean_nv_second_order,
  avg(nv_order_24h) as mean_nv_order_24h,
  avg(nv_order_7d)  as mean_nv_order_7d,
  avg(nv_order_30d) as mean_nv_order_30d,
  avg(dp_has_notif_any_day) as mean_dp_has_notif_any_day,
  avg(dp_has_notif_30d)     as mean_dp_has_notif_30d,
  avg(dp_has_notif_60d)     as mean_dp_has_notif_60d,
  avg(dp_first_order) as mean_dp_first_order,
  avg(dp_second_order) as mean_dp_second_order,
  avg(dp_order_24h) as mean_dp_order_24h,
  avg(dp_order_7d)  as mean_dp_order_7d,
  avg(dp_order_30d) as mean_dp_order_30d,
  avg(target_ordered_24h)      as mean_target_ordered_24h,
  avg(target_first_order_new_cx) as mean_target_first_order_new_cx,
  avg(target_ordered_month1) as mean_target_ordered_month1,
  avg(target_ordered_month2) as mean_target_ordered_month2,
  avg(target_ordered_month3) as mean_target_ordered_month3,
  avg(target_ordered_month4) as mean_target_ordered_month4
from proddb.fionafan.nv_dp_features_targets;



select dp_first_order, count(1) cnt 
, avg(target_ordered_24h) target_ordered_24h
, avg(target_first_order_new_cx) target_first_order_new_cx
, avg(target_ordered_month1) target_ordered_month1
, avg(target_ordered_month2) target_ordered_month2
, avg(target_ordered_month3) target_ordered_month3
, avg(target_ordered_month4) target_ordered_month4
from proddb.fionafan.nv_dp_features_targets group by all order by all desc; 

select
  a.dp_first_order,
  count(*) as n,
  
  -- Covariates: NV impressions
  avg(a.had_nv_imp_1h) as avg_had_nv_imp_1h,
  avg(a.had_nv_imp_4h) as avg_had_nv_imp_4h,
  avg(a.had_nv_imp_12h) as avg_had_nv_imp_12h,
  avg(a.had_nv_imp_24h) as avg_had_nv_imp_24h,
  avg(a.had_nv_imp_7d) as avg_had_nv_imp_7d,
  avg(a.had_nv_imp_30d) as avg_had_nv_imp_30d,
  avg(a.had_nv_imp_first_session) as avg_had_nv_imp_first_session,
  avg(a.had_nv_imp_second_session) as avg_had_nv_imp_second_session,
  
  -- Covariates: NV orders
  avg(a.nv_first_order) as avg_nv_first_order,
  avg(a.nv_second_order) as avg_nv_second_order,
  
  -- Covariates: NV notifications
  avg(a.has_nv_notif_30d) as avg_has_nv_notif_30d,
  avg(a.has_nv_notif_60d) as avg_has_nv_notif_60d,
  
  -- Covariates: Time to impressions
  avg(a.minutes_to_first_non_nv_impression) as avg_minutes_to_first_non_nv_impression,
  avg(a.minutes_to_first_nv_impression) as avg_minutes_to_first_nv_impression,
  
  -- Covariates: Non-NV impressions
  avg(a.had_non_nv_imp_1h) as avg_had_non_nv_imp_1h,
  avg(a.had_non_nv_imp_4h) as avg_had_non_nv_imp_4h,
  avg(a.had_non_nv_imp_12h) as avg_had_non_nv_imp_12h,
  avg(a.had_non_nv_imp_24h) as avg_had_non_nv_imp_24h,
  avg(a.had_non_nv_imp_7d) as avg_had_non_nv_imp_7d,
  avg(a.had_non_nv_imp_30d) as avg_had_non_nv_imp_30d,
  avg(a.had_non_nv_imp_first_session) as avg_had_non_nv_imp_first_session,
  avg(a.had_non_nv_imp_second_session) as avg_had_non_nv_imp_second_session,
  
  -- Covariates: DP notifications
  avg(a.dp_has_notif_30d) as avg_dp_has_notif_30d,
  avg(a.dp_has_notif_any_day) as avg_dp_has_notif_any_day,
  
  -- Target variables
  avg(a.target_ordered_24h) as avg_target_ordered_24h,
  avg(a.target_first_order_new_cx) as avg_target_first_order_new_cx,
  avg(a.target_ordered_month1) as avg_target_ordered_month1,
  avg(a.target_ordered_month2) as avg_target_ordered_month2,
  avg(a.target_ordered_month3) as avg_target_ordered_month3,
  avg(a.target_ordered_month4) as avg_target_ordered_month4
  
from proddb.fionafan.nv_dp_features_targets a
inner join proddb.fionafan.psm_matched_data_dp_first_order b on a.device_id = b.device_id and a.consumer_id = b.consumer_id
group by all;

select  dp_first_order, avg(had_nv_imp_30d) as avg_had_nv_imp_30d,
  avg(has_nv_notif_30d) as avg_has_nv_notif_30d,
  avg(had_nv_imp_1h) as avg_had_nv_imp_1h from proddb.fionafan.psm_matched_data_dp_first_order group by all;