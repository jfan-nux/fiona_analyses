-- nv_dp_intro_deepdive: Answers to readme.md questions using tables from explore.sql
-- Cohort: Users who onboarded in April 2025 (as defined in proddb.fionafan.nv_dp_new_user_table)

-- =============================================================
-- Q1. NV impressions vs no NV impressions → order rate / new cx rate
--    Windows: first session, second session, 24h, 1 week, 30 days
-- =============================================================
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, events_w_session as (
  select 
    ews.*,
    -- Always normalize device id before any joins
    replace(lower(CASE WHEN ews.dd_device_id like 'dx_%' then ews.dd_device_id
                       else 'dx_'||ews.dd_device_id end), '-') as dd_device_id_filtered_ws,
    -- NV definition at the event row via store join
    case when ews.store_id is not null and (
           ds.NV_ORG is not null or ds.NV_VERTICAL_NAME is not null or 
           ds.NV_BUSINESS_LINE is not null or ds.NV_BUSINESS_SUB_TYPE is not null
         ) then 1 else 0 end as is_nv_store
  from proddb.fionafan.nv_dp_new_user_all_events_30d_w_session_num ews
  left join edw.merchant.dimension_store ds
    on ews.store_id = ds.store_id
    where session_num <=3
)
, nv_impression_flags as (
  -- Compute per-user flags for each window
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    -- First session NV impression
    max(case when ews.sequential_session_num = 1 
                  and ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1 then 1 else 0 end) as had_nv_imp_first_session,
    -- Second session NV impression
    max(case when ews.sequential_session_num = 2 
                  and ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1 then 1 else 0 end) as had_nv_imp_second_session,
    -- Time windows relative to join
    max(case when ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1
                  and ews.event_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as had_nv_imp_24h,
    max(case when ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1
                  and ews.event_timestamp <= dateadd('day', 7, u.join_time) then 1 else 0 end) as had_nv_imp_7d,
    max(case when ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1
                  and ews.event_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as had_nv_imp_30d,
    -- Minutes to first NV impression by session
    min(case when ews.sequential_session_num = 1 
                  and ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1
             then datediff('minute', u.join_time, ews.event_timestamp) end) as minutes_to_nv_imp_first_session,
    min(case when ews.sequential_session_num = 2 
                  and ews.event_type = 'store_impression'
                  and ews.is_nv_store = 1
             then datediff('minute', u.join_time, ews.event_timestamp) end) as minutes_to_nv_imp_second_session
  from base_users u
  left join events_w_session ews
    on ews.dd_device_id_filtered_ws = u.dd_device_id_filtered
  group by all
)
, session_starts as (
  select 
    dd_device_id_filtered_ws as dd_device_id_filtered,
    min(case when sequential_session_num = 1 then event_timestamp end) as first_session_start,
    min(case when sequential_session_num = 2 then event_timestamp end) as second_session_start
  from events_w_session
  group by 1
)
, nv_imp_times as (
  select 
    dd_device_id_filtered_ws as dd_device_id_filtered,
    min(case when sequential_session_num = 1 and event_type = 'store_impression' and is_nv_store = 1 then event_timestamp end) as first_nv_imp_time,
    min(case when sequential_session_num = 2 and event_type = 'store_impression' and is_nv_store = 1 then event_timestamp end) as second_nv_imp_time
  from events_w_session
  group by 1
)
, order_thresholds as (
  select 
    u.dd_device_id_filtered,
    u.join_time,
    coalesce(n.first_nv_imp_time, s.first_session_start)  as first_order_threshold,
    coalesce(n.second_nv_imp_time, s.second_session_start) as second_order_threshold
  from base_users u
  left join session_starts s on s.dd_device_id_filtered = u.dd_device_id_filtered
  left join nv_imp_times n   on n.dd_device_id_filtered = u.dd_device_id_filtered
)
, orders_base as (
  select 
    o.dd_device_id_filtered,
    o.consumer_id,
    o.order_timestamp,
    o.delivery_id,
    o.is_first_ordercart_dd as is_first_ordercart_dd,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, metrics_by_window as (
  -- Aggregate order/new cx per user for each window
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'first_session' as window_name,
    coalesce(f.had_nv_imp_first_session,0) as had_nv,
    max(case when o.delivery_id is not null then 1 else 0 end) as any_order,
    max(case when o.delivery_id is not null and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.delivery_id is not null and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.delivery_id is not null and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_thresholds t on t.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
   and o.order_timestamp > t.first_order_threshold
   and o.order_timestamp <= dateadd('day', 30, u.join_time)
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'second_session' as window_name,
    coalesce(f.had_nv_imp_second_session,0) as had_nv,
    max(case when o.delivery_id is not null then 1 else 0 end) as any_order,
    max(case when o.delivery_id is not null and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.delivery_id is not null and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.delivery_id is not null and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_thresholds t on t.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
   and o.order_timestamp > t.second_order_threshold
   and o.order_timestamp <= dateadd('day', 30, u.join_time)
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '24h' as window_name,
    coalesce(f.had_nv_imp_24h,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '7d' as window_name,
    coalesce(f.had_nv_imp_7d,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '30d' as window_name,
    coalesce(f.had_nv_imp_30d,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
select 
  window_name,
  had_nv,
  count(distinct dd_device_id_filtered) as devices,
  sum(any_order) as orders,
  sum(any_order_nv) as orders_nv,
  sum(any_order_non_nv) as orders_non_nv,
  sum(any_new_cx) as new_cx,
  sum(any_order) / nullif(count(distinct dd_device_id_filtered),0) as order_rate,
  sum(any_order_nv) / nullif(count(distinct dd_device_id_filtered),0) as order_rate_nv,
  sum(any_order_non_nv) / nullif(count(distinct dd_device_id_filtered),0) as order_rate_non_nv,
  sum(any_new_cx) / nullif(count(distinct dd_device_id_filtered),0) as new_cx_rate
from metrics_by_window
group by all
order by 1, 2;


-- =============================================================
-- Q2. NV notifications vs no NV notifications → order rate / new cx rate
--    Windows: 24h, 1 week, 30 days
-- =============================================================
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, notif as (
  select n.*, u.join_time
  from proddb.fionafan.nv_dp_new_user_notifications_30d_w_braze_week n
  join base_users u
    on n.consumer_id = u.consumer_id
)
, notif_flags as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    max(case when has_new_verticals_grocery = 1 and sent_at_date <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as has_nv_notif_24h,
    max(case when has_new_verticals_grocery = 1 and sent_at_date <= dateadd('day', 7,  u.join_time) then 1 else 0 end) as has_nv_notif_7d,
    max(case when has_new_verticals_grocery = 1 and sent_at_date <= dateadd('day', 30, u.join_time) then 1 else 0 end) as has_nv_notif_30d
  from base_users u
  left join notif n
    on n.consumer_id = u.consumer_id
  group by all
)
, orders_base as (
  select dd_device_id_filtered, consumer_id, order_timestamp, delivery_id, is_first_ordercart_dd,
         case when coalesce(nv_org, nv_vertical_name, nv_business_line, nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders

)
, notif_metrics as (
  select 
    '24h' as window_name,
    coalesce(f.has_nv_notif_24h,0) as has_nv_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    '7d' as window_name,
    coalesce(f.has_nv_notif_7d,0) as has_nv_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    '30d' as window_name,
    coalesce(f.has_nv_notif_30d,0) as has_nv_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
  from base_users u
  left join notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
select 
  window_name,
  has_nv_notif,
  count(distinct dd_device_id_filtered) as devices,
  sum(any_order) as orders,
  sum(any_order_nv) as orders_nv,
  sum(any_order_non_nv) as orders_non_nv,
  sum(any_new_cx) as new_cx,
  sum(any_order) / nullif(count(distinct dd_device_id_filtered),0) as order_rate,
  sum(any_order_nv) / nullif(count(distinct dd_device_id_filtered),0) as order_rate_nv,
  sum(any_order_non_nv) / nullif(count(distinct dd_device_id_filtered),0) as order_rate_non_nv,
  sum(any_new_cx) / nullif(count(distinct dd_device_id_filtered),0) as new_cx_rate
from notif_metrics
group by all
order by 1, 2;


-- =============================================================
-- Q3. Groceries in first/second/24h/1 week order → retention/order/MAU
--    Measure outcomes at months 1, 2, 3, 4 after join
-- =============================================================
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, orders_ranked as (
  select 
    o.*,
    row_number() over (partition by o.dd_device_id_filtered order by o.order_timestamp) as order_rank,
    case when lower(coalesce(o.nv_business_line, o.nv_vertical_name)) like '%grocery%' then 1 else 0 end as is_grocery_order,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, cohort_flags as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    max(case when r.order_rank = 1 and r.is_grocery_order = 1 then 1 else 0 end) as grocery_first_order,
    max(case when r.order_rank = 2 and r.is_grocery_order = 1 then 1 else 0 end) as grocery_second_order,
    max(case when r.is_grocery_order = 1 and r.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as grocery_24h,
    max(case when r.is_grocery_order = 1 and r.order_timestamp <= dateadd('day', 7,  u.join_time) then 1 else 0 end) as grocery_7d
  from base_users u
  left join orders_ranked r
    on r.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
, retention_windows as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month1' as month_bucket,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then o.delivery_id end) as orders
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month2' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) then o.delivery_id end) as orders
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month3' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) then o.delivery_id end) as orders
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month4' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) then o.delivery_id end) as orders
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
select 
  cohort,
  month_bucket,
  count(distinct rw.dd_device_id_filtered) as devices,
  -- Order rate = fraction with any order in that month
  sum(any_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate,
  -- NV vs non-NV order rates
  sum(any_order_nv) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate_nv,
  sum(any_order_non_nv) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate_non_nv,
  -- MAU rate proxy = users with any order in the month (same as order_rate here)
  sum(any_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as mau_rate,
  -- Orders count in the month
  sum(orders) as orders
from (
  select 'grocery_first_order' as cohort, * from cohort_flags
  union all
  select 'grocery_second_order' as cohort, * from cohort_flags
  union all
  select 'grocery_24h' as cohort, * from cohort_flags
  union all
  select 'grocery_7d' as cohort, * from cohort_flags
) cf
join retention_windows rw
  on rw.dd_device_id_filtered = cf.dd_device_id_filtered
where (
  (cohort = 'grocery_first_order' and cf.grocery_first_order = 1) or
  (cohort = 'grocery_second_order' and cf.grocery_second_order = 1) or
  (cohort = 'grocery_24h' and cf.grocery_24h = 1) or
  (cohort = 'grocery_7d' and cf.grocery_7d = 1)
)
group by all
order by 1, 2;


