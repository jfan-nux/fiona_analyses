-- nv_dp_intro_deepdive: Answers to readme.md questions using tables from explore.sql
-- Cohort: Users who onboarded in April 2025 (as defined in proddb.fionafan.nv_dp_new_user_table)

-- =============================================================
-- Q1. NV impressions vs no NV impressions → order rate / new cx rate
--    Windows: first session, second session, 24h, 1 week, 30 days
-- =============================================================
select sequential_session_num, count(distinct dd_device_id) 
from proddb.fionafan.nv_dp_new_user_all_events_30d_w_session_num group by 1;
--1h51min
-- Materialize NV impression flags
create or replace table proddb.fionafan.nv_impression_flags as (
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, session_impressions as (
  select 
    replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
                       else 'dx_'||dd_device_id end), '-') as dd_device_id_filtered_si,
    consumer_id,
    join_time,
    session_num,
    session_start_time,
    first_impression_time,
    first_nv_impression_time,
    first_non_nv_impression_time,
    had_nv_impression,
    had_non_nv_impression,
    nv_impression_count,
    non_nv_impression_count
  from proddb.fionafan.nv_dp_new_user_session_impressions
)
select 
  u.consumer_id,
  u.dd_device_id_filtered,
  u.join_time,
  max(case when si.first_nv_impression_time is not null and si.first_nv_impression_time <= dateadd('hour', 1, u.join_time) then 1 else 0 end) as had_nv_imp_1h,
  max(case when si.first_nv_impression_time is not null and si.first_nv_impression_time <= dateadd('hour', 4, u.join_time) then 1 else 0 end) as had_nv_imp_4h,
  max(case when si.session_num = 1 and coalesce(si.had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_imp_first_session,
  max(case when si.session_num = 2 and coalesce(si.had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_imp_second_session,
  max(case when si.first_nv_impression_time is not null and si.first_nv_impression_time <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as had_nv_imp_24h,
  max(case when si.first_nv_impression_time is not null and si.first_nv_impression_time <= dateadd('day', 7, u.join_time) then 1 else 0 end) as had_nv_imp_7d,
  max(case when si.first_nv_impression_time is not null and si.first_nv_impression_time <= dateadd('day', 30, u.join_time) then 1 else 0 end) as had_nv_imp_30d
from base_users u
left join session_impressions si
  on si.dd_device_id_filtered_si = u.dd_device_id_filtered
group by all
);

select had_nv_imp_second_session, count(1) from proddb.fionafan.nv_impression_flags group by 1;
create or replace table proddb.fionafan.nv_dp_new_user_session_performance as (
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, session_impressions as (
  select 
    replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id
                       else 'dx_'||dd_device_id end), '-') as dd_device_id_filtered_si,
    consumer_id,
    join_time,
    session_num,
    session_start_time,
    first_impression_time,
    first_nv_impression_time,
    first_non_nv_impression_time,
    had_nv_impression,
    had_non_nv_impression,
    nv_impression_count,
    non_nv_impression_count
  from proddb.fionafan.nv_dp_new_user_session_impressions
)
, nv_impression_flags as (
  select 
    f.consumer_id,
    f.dd_device_id_filtered,
    f.join_time,
    f.had_nv_imp_1h,
    f.had_nv_imp_4h,
    f.had_nv_imp_first_session,
    f.had_nv_imp_second_session,
    f.had_nv_imp_24h,
    f.had_nv_imp_7d,
    f.had_nv_imp_30d
  from proddb.fionafan.nv_impression_flags f
)
, session_starts as (
  select 
    dd_device_id_filtered_si as dd_device_id_filtered,
    min(case when session_num = 1 then session_start_time end) as first_session_start,
    min(case when session_num = 2 then session_start_time end) as second_session_start
  from session_impressions
  group by 1
)
, nv_imp_times as (
  select 
    dd_device_id_filtered_si as dd_device_id_filtered,
    min(case when session_num = 1 then first_nv_impression_time end) as first_nv_imp_time,
    min(case when session_num = 2 then first_nv_impression_time end) as second_nv_imp_time
  from session_impressions
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
, order_flags as (
  select 
    u.dd_device_id_filtered,
    count(distinct case when o.order_timestamp <= dateadd('hour', 1,  u.join_time) then o.delivery_id end) as order_count_1h,
    count(distinct case when o.order_timestamp <= dateadd('hour', 4,  u.join_time) then o.delivery_id end) as order_count_4h,
    count(distinct case when o.order_timestamp <= dateadd('hour', 12, u.join_time) then o.delivery_id end) as order_count_12h,
    count(distinct case when o.order_timestamp <= dateadd('hour', 24, u.join_time) then o.delivery_id end) as order_count_24h,
    count(distinct case when o.order_timestamp <= dateadd('day', 7,  u.join_time) then o.delivery_id end) as order_count_7d,
    count(distinct case when o.order_timestamp <= dateadd('day', 30, u.join_time) then o.delivery_id end) as order_count_30d,
    -- NV vs Non-NV splits (counts)
    count(distinct case when o.order_timestamp <= dateadd('hour', 1,  u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_1h_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 1,  u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_1h_non_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 4,  u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_4h_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 4,  u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_4h_non_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 12, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_12h_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 12, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_12h_non_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_24h_nv,
    count(distinct case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_24h_non_nv,
    count(distinct case when o.order_timestamp <= dateadd('day', 7,  u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_7d_nv,
    count(distinct case when o.order_timestamp <= dateadd('day', 7,  u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_7d_non_nv,
    count(distinct case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as order_count_30d_nv,
    count(distinct case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as order_count_30d_non_nv
  from base_users u
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.dd_device_id_filtered
)
, metrics_by_window as (
  -- Aggregate order/new cx per user for each window, plus base flags
  -- First session
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'first_session' as window_name,
    coalesce(f.had_nv_imp_first_session,0) as had_nv,
    -- in-window outcomes
    max(case when o.delivery_id is not null then 1 else 0 end) as any_order,
    max(case when o.delivery_id is not null and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.delivery_id is not null and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.delivery_id is not null and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    -- base flag: must have session 1
    case when s.first_session_start is not null then 1 else 0 end as in_base,
    -- global order window flags (relative to join)
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_thresholds t on t.dd_device_id_filtered = u.dd_device_id_filtered
  left join session_starts s on s.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
   and o.order_timestamp > t.first_order_threshold
   and o.order_timestamp <= dateadd('day', 30, u.join_time)
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- Second session
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'second_session' as window_name,
    coalesce(f.had_nv_imp_second_session,0) as had_nv,
    max(case when o.delivery_id is not null then 1 else 0 end) as any_order,
    max(case when o.delivery_id is not null and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.delivery_id is not null and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.delivery_id is not null and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    case when s.second_session_start is not null then 1 else 0 end as in_base,
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_thresholds t on t.dd_device_id_filtered = u.dd_device_id_filtered
  left join session_starts s on s.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
   and o.order_timestamp > t.second_order_threshold
   and o.order_timestamp <= dateadd('day', 30, u.join_time)
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- 1h window (base = all cohort)
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '1h' as window_name,
    coalesce(f.had_nv_imp_1h,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('hour', 1, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('hour', 1, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('hour', 1, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('hour', 1, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    1 as in_base,
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- 4h window (base = all cohort)
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '4h' as window_name,
    coalesce(f.had_nv_imp_4h,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('hour', 4, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('hour', 4, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('hour', 4, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('hour', 4, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    1 as in_base,
    coalesce(ofl.order_count_4h,0) as order_count_4h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_4h_nv,0) as order_count_4h_nv,
    coalesce(ofl.order_count_4h_non_nv,0) as order_count_4h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- 24h window (base = all cohort)
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '24h' as window_name,
    coalesce(f.had_nv_imp_24h,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    1 as in_base,
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- 7d window (base = all cohort)
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '7d' as window_name,
    coalesce(f.had_nv_imp_7d,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    1 as in_base,
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name

  union all

  -- 30d window (base = all cohort)
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    '30d' as window_name,
    coalesce(f.had_nv_imp_30d,0) as had_nv,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    1 as in_base,
    coalesce(ofl.order_count_1h,0) as order_count_1h,
    coalesce(ofl.order_count_12h,0) as order_count_12h,
    coalesce(ofl.order_count_24h,0) as order_count_24h,
    coalesce(ofl.order_count_7d,0) as order_count_7d,
    coalesce(ofl.order_count_30d,0) as order_count_30d,
    coalesce(ofl.order_count_1h_nv,0) as order_count_1h_nv,
    coalesce(ofl.order_count_1h_non_nv,0) as order_count_1h_non_nv,
    coalesce(ofl.order_count_12h_nv,0) as order_count_12h_nv,
    coalesce(ofl.order_count_12h_non_nv,0) as order_count_12h_non_nv,
    coalesce(ofl.order_count_24h_nv,0) as order_count_24h_nv,
    coalesce(ofl.order_count_24h_non_nv,0) as order_count_24h_non_nv,
    coalesce(ofl.order_count_7d_nv,0) as order_count_7d_nv,
    coalesce(ofl.order_count_7d_non_nv,0) as order_count_7d_non_nv,
    coalesce(ofl.order_count_30d_nv,0) as order_count_30d_nv,
    coalesce(ofl.order_count_30d_non_nv,0) as order_count_30d_non_nv
  from base_users u
  left join nv_impression_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join order_flags ofl on ofl.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o
    on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by u.consumer_id, u.dd_device_id_filtered, had_nv, in_base,
           order_count_1h, order_count_4h, order_count_12h, order_count_24h, order_count_7d, order_count_30d,
           order_count_1h_nv, order_count_1h_non_nv, order_count_4h_nv, order_count_4h_non_nv, order_count_12h_nv, order_count_12h_non_nv,
           order_count_24h_nv, order_count_24h_non_nv, order_count_7d_nv, order_count_7d_non_nv,
           order_count_30d_nv, order_count_30d_non_nv, window_name
)
select 
  window_name,
  had_nv,
  count(distinct case when in_base = 1 then dd_device_id_filtered end) as devices,
  sum(any_order) as orders,
  sum(any_order_nv) as orders_nv,
  sum(any_order_non_nv) as orders_non_nv,
  sum(any_new_cx) as new_cx,
  -- primary order rates with correct bases
  sum(any_order) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate,
  sum(any_order_nv) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_nv,
  sum(any_order_non_nv) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_non_nv,
  sum(any_new_cx) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as new_cx_rate,
  -- remove 1h/12h columns from output; keep 24h/7d/30d only
  sum(order_count_24h) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_24h,
  sum(order_count_7d)  / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_7d,
  sum(order_count_30d) / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_30d,
  sum(order_count_24h_nv)      / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_24h_nv,
  sum(order_count_24h_non_nv)  / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_24h_non_nv,
  sum(order_count_7d_nv)       / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_7d_nv,
  sum(order_count_7d_non_nv)   / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_7d_non_nv,
  sum(order_count_30d_nv)      / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_30d_nv,
  sum(order_count_30d_non_nv)  / nullif(count(distinct case when in_base = 1 then dd_device_id_filtered end),0) as order_rate_30d_non_nv
from metrics_by_window
group by window_name, had_nv
order by 1, 2
);
select * from proddb.fionafan.nv_dp_new_user_session_performance order by 1,2;


-- =============================================================
-- Q2. NV notifications vs no NV notifications → order rate / new cx rate
--    Windows: 24h, 1 week, 30 days
-- =============================================================
create or replace table proddb.fionafan.nv_dp_new_user_notif_performance as (
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, notif as (
  select n.*, u.join_time
  from proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week n
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

  union all

  select 
    '60d' as window_name,
    coalesce(f.has_nv_notif_30d,0) as has_nv_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 60, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_first_ordercart_dd = 1 then 1 else 0 end) as any_new_cx,
    max(case when o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv
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
order by 1, 2
);
select * from proddb.fionafan.nv_dp_new_user_notif_performance order by 1,2;


-- =============================================================
-- Q3. Groceries in first/second/24h/1 week order → retention/order/MAU
--    Measure outcomes at months 1, 2, 3, 4 after join
-- =============================================================

select delivery_id, count(1) cnt from proddb.fionafan.nv_dp_new_user_orders group by 1 having cnt > 1 order by cnt desc;


create or replace table proddb.fionafan.nv_dp_new_user_orders_performance as (

with base_users as (
  -- Deduplicate to one row per device using earliest join_time
  select consumer_id, dd_device_id_filtered, join_time, day
  from (
    select t.*,
           row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, orders_ranked as (
  select 
    o.*,
    row_number() over (partition by o.dd_device_id_filtered order by o.order_timestamp) as order_rank,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, cohort_flags as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    -- NV order breakdowns
    max(case when r.order_rank = 1 and r.is_nv_order = 1 then 1 else 0 end) as nv_first_order,
    max(case when r.order_rank = 2 and r.is_nv_order = 1 then 1 else 0 end) as nv_second_order,
    max(case when r.is_nv_order = 1 and r.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as nv_24h,
    max(case when r.is_nv_order = 1 and r.order_timestamp <= dateadd('day', 7,  u.join_time) then 1 else 0 end) as nv_7d,
    -- Non-NV order breakdowns
    max(case when r.order_rank = 1 and r.is_nv_order = 0 then 1 else 0 end) as non_nv_first_order,
    max(case when r.order_rank = 2 and r.is_nv_order = 0 then 1 else 0 end) as non_nv_second_order,
    max(case when r.is_nv_order = 0 and r.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as non_nv_24h,
    max(case when r.is_nv_order = 0 and r.order_timestamp <= dateadd('day', 7,  u.join_time) then 1 else 0 end) as non_nv_7d
  from base_users u
  left join orders_ranked r
    on r.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
, cohort_any as (
  select 
    dd_device_id_filtered,
    case when coalesce(nv_first_order,0)=1 
           or coalesce(nv_second_order,0)=1 
           or coalesce(nv_24h,0)=1 
           or coalesce(nv_7d,0)=1 then 1 else 0 end as nv_any,
    case when coalesce(non_nv_first_order,0)=1 
           or coalesce(non_nv_second_order,0)=1 
           or coalesce(non_nv_24h,0)=1 
           or coalesce(non_nv_7d,0)=1 then 1 else 0 end as non_nv_any
  from cohort_flags
)
, retention_windows as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month1' as month_bucket,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered and o.consumer_id = u.consumer_id
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month2' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered and o.consumer_id = u.consumer_id
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month3' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered and o.consumer_id = u.consumer_id
  group by all

  union all

  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month4' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered and o.consumer_id = u.consumer_id      
  group by all
)
-- Two cohort views: NV-any and Non-NV-any; devices can appear in both if they qualify
select 
  c.cohort,
  rw.month_bucket,
  count(distinct rw.dd_device_id_filtered) as devices,
  -- Order rate as distinct deliveries per distinct devices
  sum(rw.orders) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate,
  -- NV vs non-NV order rates
  sum(rw.orders_nv) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate_nv,
  sum(rw.orders_non_nv) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate_non_nv,
  -- Early-window breakdowns (rates)
  sum(cf.nv_first_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as nv_first_order_rate,
  sum(cf.nv_second_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as nv_second_order_rate,
  sum(cf.nv_24h) / nullif(count(distinct rw.dd_device_id_filtered),0) as nv_24h_rate,
  sum(cf.nv_7d) / nullif(count(distinct rw.dd_device_id_filtered),0) as nv_7d_rate,
  sum(cf.non_nv_first_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as non_nv_first_order_rate,
  sum(cf.non_nv_second_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as non_nv_second_order_rate,
  sum(cf.non_nv_24h) / nullif(count(distinct rw.dd_device_id_filtered),0) as non_nv_24h_rate,
  sum(cf.non_nv_7d) / nullif(count(distinct rw.dd_device_id_filtered),0) as non_nv_7d_rate,
  -- Orders count in the month
  sum(rw.orders) as orders
from retention_windows rw
join cohort_any ca
  on ca.dd_device_id_filtered = rw.dd_device_id_filtered
join cohort_flags cf
  on cf.dd_device_id_filtered = rw.dd_device_id_filtered
join (
  select 'nv_first_order' as cohort, dd_device_id_filtered from cohort_flags where nv_first_order = 1
  union all
  select 'nv_second_order' as cohort, dd_device_id_filtered from cohort_flags where nv_second_order = 1
  union all
  select 'nv_24h' as cohort, dd_device_id_filtered from cohort_flags where nv_24h = 1
  union all
  select 'nv_7d' as cohort, dd_device_id_filtered from cohort_flags where nv_7d = 1
  union all
  select 'non_nv_any' as cohort, dd_device_id_filtered from cohort_any where nv_any = 0
) c
  on c.dd_device_id_filtered = rw.dd_device_id_filtered
group by c.cohort, rw.month_bucket
order by 1, 2
);
select * from proddb.fionafan.nv_dp_new_user_orders_performance order by 1,2;


