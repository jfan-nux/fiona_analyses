-- Notification performance using daily features table directly
-- Builds order_rate (distinct deliveries / devices) and retention (consumers with any order / devices)
-- Splits by order_type = overall | nv_order | non_nv_order; includes NV vs non-NV notification ratios

create or replace table proddb.fionafan.nv_dp_notif_performance_from_daily as (
with base_users as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, daily as (
  select 
    n.consumer_id,
    n.dd_device_id_filtered as device_id,
    n.day,
    coalesce(n.received_any_nv_notification, 0) as received_any_nv_notification
  from proddb.fionafan.nv_dp_notifications_daily_features n
)
, notif_flags as (
  -- Build per-window NV notification flags at device level from daily features
  select 
    '24h' as window_name,
    b.device_id,
    max(case when d.day <= date_trunc('day', dateadd('day', 1, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif
  from base_users b
  left join daily d on d.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '7d' as window_name,
    b.device_id,
    max(case when d.day <= date_trunc('day', dateadd('day', 7, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif
  from base_users b
  left join daily d on d.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '30d' as window_name,
    b.device_id,
    max(case when d.day <= date_trunc('day', dateadd('day', 30, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif
  from base_users b
  left join daily d on d.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '60d' as window_name,
    b.device_id,
    max(case when d.day <= date_trunc('day', dateadd('day', 60, b.join_time)) then d.received_any_nv_notification else 0 end) as has_nv_notif
  from base_users b
  left join daily d on d.consumer_id = b.consumer_id
  group by b.device_id
)
, orders_base as (
  select 
    o.dd_device_id_filtered as device_id,
    o.consumer_id,
    o.delivery_id,
    o.order_timestamp,
    coalesce(o.is_first_ordercart_dd, 0) as is_first_ordercart_dd,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, orders_per_window_device as (
  -- Aggregate distinct deliveries and retained flags per device per window
  select 
    '24h' as window_name,
    b.device_id,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) then o.delivery_id end) as deliveries_overall,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as deliveries_nv,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as deliveries_non_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) then 1 else 0 end) as retained_overall,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as retained_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('hour', 24, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as retained_non_nv
  from base_users b
  left join orders_base o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '7d' as window_name,
    b.device_id,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) then o.delivery_id end) as deliveries_overall,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as deliveries_nv,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as deliveries_non_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) then 1 else 0 end) as retained_overall,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as retained_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 7, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as retained_non_nv
  from base_users b
  left join orders_base o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '30d' as window_name,
    b.device_id,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) then o.delivery_id end) as deliveries_overall,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as deliveries_nv,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as deliveries_non_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) then 1 else 0 end) as retained_overall,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as retained_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as retained_non_nv
  from base_users b
  left join orders_base o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id

  union all

  select 
    '60d' as window_name,
    b.device_id,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) then o.delivery_id end) as deliveries_overall,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as deliveries_nv,
    count(distinct case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as deliveries_non_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) then 1 else 0 end) as retained_overall,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as retained_nv,
    max(case when o.order_timestamp > b.join_time and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as retained_non_nv
  from base_users b
  left join orders_base o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id
)
, joined as (
  select 
    f.window_name,
    f.device_id,
    coalesce(f.has_nv_notif, 0) as has_nv_notif,
    coalesce(o.deliveries_overall, 0) as deliveries_overall,
    coalesce(o.deliveries_nv, 0) as deliveries_nv,
    coalesce(o.deliveries_non_nv, 0) as deliveries_non_nv,
    coalesce(o.retained_overall, 0) as retained_overall,
    coalesce(o.retained_nv, 0) as retained_nv,
    coalesce(o.retained_non_nv, 0) as retained_non_nv
  from notif_flags f
  left join orders_per_window_device o
    on o.device_id = f.device_id and o.window_name = f.window_name
)
, aggregated as (
  -- Aggregate to window_name x has_nv_notif and compute per order_type
  select 
    window_name,
    has_nv_notif,
    'overall' as order_type,
    count(distinct device_id) as devices,
    sum(retained_overall) as consumers_retained,
    sum(deliveries_overall) as distinct_deliveries,
    sum(deliveries_overall) / nullif(count(distinct device_id), 0) as order_rate,
    sum(retained_overall)  / nullif(count(distinct device_id), 0) as retention
  from joined
  group by window_name, has_nv_notif

  union all

  select 
    window_name,
    has_nv_notif,
    'nv_order' as order_type,
    count(distinct device_id) as devices,
    sum(retained_nv) as consumers_retained,
    sum(deliveries_nv) as distinct_deliveries,
    sum(deliveries_nv) / nullif(count(distinct device_id), 0) as order_rate,
    sum(retained_nv)  / nullif(count(distinct device_id), 0) as retention
  from joined
  group by window_name, has_nv_notif

  union all

  select 
    window_name,
    has_nv_notif,
    'non_nv_order' as order_type,
    count(distinct device_id) as devices,
    sum(retained_non_nv) as consumers_retained,
    sum(deliveries_non_nv) as distinct_deliveries,
    sum(deliveries_non_nv) / nullif(count(distinct device_id), 0) as order_rate,
    sum(retained_non_nv)  / nullif(count(distinct device_id), 0) as retention
  from joined
  group by window_name, has_nv_notif
)
select 
  a.*,
  -- Ratios: NV-notified vs Non-NV-notified within window_name x order_type
  max(case when a.has_nv_notif = 1 then a.order_rate end) over (partition by a.window_name, a.order_type)
    / nullif(max(case when a.has_nv_notif = 0 then a.order_rate end) over (partition by a.window_name, a.order_type), 0) as ratio_order_rate_nv_over_non_nv,
  max(case when a.has_nv_notif = 0 then a.order_rate end) over (partition by a.window_name, a.order_type)
    / nullif(max(case when a.has_nv_notif = 1 then a.order_rate end) over (partition by a.window_name, a.order_type), 0) as ratio_order_rate_non_nv_over_nv,
  max(case when a.has_nv_notif = 1 then a.retention end) over (partition by a.window_name, a.order_type)
    / nullif(max(case when a.has_nv_notif = 0 then a.retention end) over (partition by a.window_name, a.order_type), 0) as ratio_retention_nv_over_non_nv,
  max(case when a.has_nv_notif = 0 then a.retention end) over (partition by a.window_name, a.order_type)
    / nullif(max(case when a.has_nv_notif = 1 then a.retention end) over (partition by a.window_name, a.order_type), 0) as ratio_retention_non_nv_over_nv
from aggregated a
);

-- Preview
select * from proddb.fionafan.nv_dp_notif_performance_from_daily order by window_name, order_type, has_nv_notif desc;


