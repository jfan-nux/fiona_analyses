-- =============================================================
-- DashPass Deep Dive
-- Q2-analog: DashPass notifications vs no DashPass notifications → order rate
-- Q3-analog: Placed an order with DashPass (subscriber at order) → order rate
--
-- Inputs referenced:
--  - Notifications: edw.consumer.fact_consumer_notification_engagement + Braze metadata mapping
--  - Subscription status (daily): edw.consumer.fact_consumer_subscription__daily
--  - Orders and subscription at order time: dimension_deliveries.is_subscribed_consumer
--  - New-user base and orders borrowed from nv_dp_intro_deepdive (join windows/dates)
--
-- Outputs created under proddb.fionafan:
--  - proddb.fionafan.dp_new_user_notif_performance
--  - proddb.fionafan.dp_new_user_orders_performance
-- =============================================================

-- Date window aligned with NV analysis windows (hard-coded)

-- Ensure base tables from NV analysis exist (new user table and orders)
-- Using: proddb.fionafan.nv_dp_new_user_table, proddb.fionafan.nv_dp_new_user_orders


-- =============================================================
-- Note: No separate DP notifications table needed. We will use has_dashpass from
-- proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week downstream.


-- =============================================================
-- Q2 analog: DashPass notifications vs no-notifications → order rate (24h/7d/30d/60d)
-- =============================================================
create or replace table proddb.fionafan.dp_new_user_notif_performance as (
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from proddb.fionafan.nv_dp_new_user_table
)
, dp_notif_flags as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    max(case when coalesce(w.has_dashpass,0)=1 and w.sent_at <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as has_dp_notif_24h,
    max(case when coalesce(w.has_dashpass,0)=1 and w.sent_at <= dateadd('day', 7,  u.join_time) then 1 else 0 end) as has_dp_notif_7d,
    max(case when coalesce(w.has_dashpass,0)=1 and w.sent_at <= dateadd('day', 30, u.join_time) then 1 else 0 end) as has_dp_notif_30d,
    max(case when coalesce(w.has_dashpass,0)=1 and w.sent_at <= dateadd('day', 60, u.join_time) then 1 else 0 end) as has_dp_notif_60d
  from base_users u
  left join proddb.fionafan.nv_dp_new_user_notifications_60d_w_braze_week w
    on w.consumer_id = u.consumer_id
  group by all
)
, orders_base as (
  select 
    dd_device_id_filtered, consumer_id, order_timestamp, delivery_id, is_first_ordercart_dd,
    is_subscribed_consumer
  from proddb.fionafan.nv_dp_new_user_orders
)
, notif_metrics as (
  select 
    '24h' as window_name,
    coalesce(f.has_dp_notif_24h,0) as has_dp_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('hour', 24, u.join_time) then 1 else 0 end) as any_order
  from base_users u
  left join dp_notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    '7d' as window_name,
    coalesce(f.has_dp_notif_7d,0) as has_dp_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 7, u.join_time) then 1 else 0 end) as any_order
  from base_users u
  left join dp_notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    '30d' as window_name,
    coalesce(f.has_dp_notif_30d,0) as has_dp_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order
  from base_users u
  left join dp_notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all

  union all

  select 
    '60d' as window_name,
    coalesce(f.has_dp_notif_60d,0) as has_dp_notif,
    u.dd_device_id_filtered,
    max(case when o.order_timestamp <= dateadd('day', 60, u.join_time) then 1 else 0 end) as any_order
  from base_users u
  left join dp_notif_flags f on f.dd_device_id_filtered = u.dd_device_id_filtered
  left join orders_base o on o.dd_device_id_filtered = u.dd_device_id_filtered
  group by all
)
select 
  window_name,
  has_dp_notif,
  count(distinct dd_device_id_filtered) as devices,
  sum(any_order) as orders,
  sum(any_order) / nullif(count(distinct dd_device_id_filtered),0) as order_rate
from notif_metrics
group by all
order by 1, 2
);
select * from proddb.fionafan.dp_new_user_notif_performance order by 1,2;

-- =============================================================
-- Q3 analog: Cohorts based on DashPass usage at/after join → subsequent monthly order rates
-- Two cohorts considered:
--   (A) dp_first_order: first order after join has is_subscribed_consumer = 1
--   (B) dp_any_30d: any order within 30d where is_subscribed_consumer = 1
-- We then compute order rates by months 1-4 following join
-- =============================================================
create or replace table proddb.fionafan.dp_new_user_orders_performance as (
with base_users as (
  select consumer_id, dd_device_id_filtered, join_time, day
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, orders_ranked as (
  select 
    o.*,
    row_number() over (partition by o.dd_device_id_filtered order by o.order_timestamp) as order_rank
  from proddb.fionafan.nv_dp_new_user_orders o
)
, first_subscription as (
  select 
    u.consumer_id,
    min(s.dte) as first_sub_date
  from base_users u
  join edw.consumer.fact_consumer_subscription__daily s
    on s.consumer_id = u.consumer_id
  where s.dte >= date_trunc('day', u.join_time)
    and (coalesce(s.is_in_paid_balance, false) = true or coalesce(s.is_in_trial_balance, false) = true)
  group by all
)
, cohort_flags as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    u.join_time,
    max(case when r.order_rank = 1 and r.is_subscribed_consumer = 1 then 1 else 0 end) as dp_first_order,
    max(case when r.order_rank = 2 and r.is_subscribed_consumer = 1 then 1 else 0 end) as dp_second_order,
    max(case when fs.first_sub_date is not null and fs.first_sub_date <= dateadd('day', 1,  date_trunc('day', u.join_time)) then 1 else 0 end) as dp_signup_24h,
    max(case when fs.first_sub_date is not null and fs.first_sub_date <= dateadd('day', 7,  date_trunc('day', u.join_time)) then 1 else 0 end) as dp_signup_7d,
    max(case when fs.first_sub_date is not null and fs.first_sub_date <= dateadd('day', 30, date_trunc('day', u.join_time)) then 1 else 0 end) as dp_signup_30d
  from base_users u
  left join orders_ranked r
    on r.dd_device_id_filtered = u.dd_device_id_filtered
  left join first_subscription fs
    on fs.consumer_id = u.consumer_id
  group by all
)
, retention_windows as (
  select 
    u.consumer_id,
    u.dd_device_id_filtered,
    'month1' as month_bucket,
    max(case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then 1 else 0 end) as any_order,
    count(distinct case when o.order_timestamp >  u.join_time and o.order_timestamp <= dateadd('day', 30, u.join_time) then o.delivery_id end) as orders
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
    count(distinct case when o.order_timestamp >  dateadd('day', 30, u.join_time) and o.order_timestamp <= dateadd('day', 60, u.join_time) then o.delivery_id end) as orders
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
    count(distinct case when o.order_timestamp >  dateadd('day', 60, u.join_time) and o.order_timestamp <= dateadd('day', 90, u.join_time) then o.delivery_id end) as orders
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
    count(distinct case when o.order_timestamp >  dateadd('day', 90, u.join_time) and o.order_timestamp <= dateadd('day', 120, u.join_time) then o.delivery_id end) as orders
  from base_users u
  left join orders_ranked o
    on o.dd_device_id_filtered = u.dd_device_id_filtered and o.consumer_id = u.consumer_id
  group by all
)
select 
  c.cohort,
  rw.month_bucket,
  count(distinct rw.dd_device_id_filtered) as devices,
  sum(rw.orders) / nullif(count(distinct rw.dd_device_id_filtered),0) as order_rate,
  sum(rw.any_order) / nullif(count(distinct rw.dd_device_id_filtered),0) as mau_rate,
  sum(rw.orders) as orders
from retention_windows rw
join (
  select 'dp_first_order' as cohort, dd_device_id_filtered from cohort_flags where dp_first_order = 1
  union all
  select 'dp_second_order'  as cohort, dd_device_id_filtered from cohort_flags where dp_second_order = 1
  union all
  select 'dp_signup_24h'  as cohort, dd_device_id_filtered from cohort_flags where dp_signup_24h = 1
  union all
  select 'dp_signup_7d'   as cohort, dd_device_id_filtered from cohort_flags where dp_signup_7d = 1
  union all
  select 'dp_signup_30d'  as cohort, dd_device_id_filtered from cohort_flags where dp_signup_30d = 1
) c
  on c.dd_device_id_filtered = rw.dd_device_id_filtered
group by c.cohort, rw.month_bucket
order by 1, 2
);


-- Quick selects
select * from proddb.fionafan.dp_new_user_notif_performance order by 1,2;
select * from proddb.fionafan.dp_new_user_orders_performance order by 1,2;


