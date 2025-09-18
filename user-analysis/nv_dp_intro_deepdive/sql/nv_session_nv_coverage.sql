-- Materialize per-session NV impression coverage among new users
-- Denominator: all consumers in proddb.fionafan.nv_dp_new_user_table
-- Numerator per sequential_session_num: distinct consumers with had_nv_impression = 1 in that session

create or replace table proddb.fionafan.nv_dp_new_user_session_nv_coverage as (

with base as (
  select distinct consumer_id
  from proddb.fionafan.nv_dp_new_user_table
)
, session_nv as (
  select 
    consumer_id,
    sequential_session_num,
    max(coalesce(had_nv_impression, 0)) as had_nv_impression
  from proddb.fionafan.nv_dp_new_user_session_impressions

  group by all
)
, totals as (
  select count(distinct consumer_id) as consumers_total from base
)
select 
  s.sequential_session_num as sequential_session_num,
  count(distinct case when s.had_nv_impression = 1 then s.consumer_id end) as consumers_with_nv_impression,
  count(distinct case when s.had_nv_impression = 0 then s.consumer_id end) as consumers_with_non_nv_impression,
  count(distinct s.consumer_id) as consumers_with_session,
  max(t.consumers_total) as consumers_total,
  count(distinct case when s.had_nv_impression = 1 then s.consumer_id end) / nullif(max(t.consumers_total), 0) as nv_share,
  count(distinct case when s.had_nv_impression = 0 then s.consumer_id end) / nullif(max(t.consumers_total), 0) as non_nv_share,
  count(distinct case when s.had_nv_impression = 1 then s.consumer_id end) / nullif(count(distinct s.consumer_id), 0) as nv_share_among_session
from base b
left join session_nv s
  on s.consumer_id = b.consumer_id
cross join totals t
group by s.sequential_session_num
order by s.sequential_session_num
);

-- Preview
select * from proddb.fionafan.nv_dp_new_user_session_nv_coverage order by sequential_session_num;




-- Order rate (distinct delivery_id / consumers) and retention (distinct consumers / consumers)
-- after session start for 1st, 2nd, 3rd sessions, split by NV impression and order_type
create or replace table proddb.fionafan.nv_dp_new_user_session_order_rates as (
with session_starts as (
  select 
    consumer_id,
    join_time,
    min(first_nv_impression_time) as session_start_timestamp,
    sequential_session_num as session_num
  from proddb.fionafan.nv_dp_new_user_all_events_30d_w_session_num
  where sequential_session_num in (1,2,3)
  group by all
)
, session_nv as (
  select 
    consumer_id,
    session_num,
    max(coalesce(had_nv_impression, 0)) as had_nv_impression,
    max(case when first_nv_impression_time <= dateadd('hour', 1, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_1h,
    max(case when first_nv_impression_time <= dateadd('hour', 4, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_4h,
    max(case when first_nv_impression_time <= dateadd('hour', 12, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_12h,
    max(case when first_nv_impression_time <= dateadd('hour', 24, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_24h,
    max(case when first_nv_impression_time <= dateadd('day', 7, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_7d,
    max(case when first_nv_impression_time <= dateadd('day', 14, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_14d,
    max(case when first_nv_impression_time <= dateadd('day', 30, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_30d
  from proddb.fionafan.nv_dp_new_user_session_impressions
  where session_num in (1,2,3)
  group by all
)
, session_base as (
  select 
    s.consumer_id,
    s.session_num,
    s.session_start_timestamp,
    coalesce(n.had_nv_impression, 0) as had_nv_impression,
    coalesce(n.had_nv_impression_1h, 0) as had_nv_impression_1h,
    coalesce(n.had_nv_impression_4h, 0) as had_nv_impression_4h,
    coalesce(n.had_nv_impression_12h, 0) as had_nv_impression_12h,
    coalesce(n.had_nv_impression_24h, 0) as had_nv_impression_24h,
    coalesce(n.had_nv_impression_7d, 0) as had_nv_impression_7d,
    coalesce(n.had_nv_impression_14d, 0) as had_nv_impression_14d,
    coalesce(n.had_nv_impression_30d, 0) as had_nv_impression_30d
  from session_starts s
  left join session_nv n
    on n.consumer_id = s.consumer_id and n.session_num = s.session_num
)
, orders_join as (
  select 
    b.consumer_id,
    b.session_num,
    b.had_nv_impression,
    b.had_nv_impression_1h,
    b.had_nv_impression_4h,
    b.had_nv_impression_12h,
    b.had_nv_impression_24h,
    b.had_nv_impression_7d,
    b.had_nv_impression_14d,
    b.had_nv_impression_30d,
    b.session_start_timestamp,
    o.delivery_id,
    o.order_timestamp,
    case when (o.nv_business_line is not null or o.nv_business_sub_type is not null) then 1 else 0 end as is_nv_order
  from session_base b
  left join proddb.fionafan.nv_dp_new_user_orders o
    on o.consumer_id = b.consumer_id
    and o.order_timestamp >= b.session_start_timestamp
    and o.order_timestamp < dateadd('day', 30, b.session_start_timestamp)
)
, orders_agg_per_consumer as (
  select 
    consumer_id,
    session_num,
    had_nv_impression,
    had_nv_impression_1h,
    had_nv_impression_4h,
    had_nv_impression_12h,
    had_nv_impression_24h,
    had_nv_impression_7d,
    had_nv_impression_14d,
    had_nv_impression_30d,
    -- overall
    count(distinct case when order_timestamp < dateadd('hour', 1, session_start_timestamp) then delivery_id end) as deliveries_1h_overall,
    count(distinct case when order_timestamp < dateadd('hour', 4, session_start_timestamp) then delivery_id end) as deliveries_4h_overall,
    count(distinct case when order_timestamp < dateadd('hour', 12, session_start_timestamp) then delivery_id end) as deliveries_12h_overall,
    count(distinct case when order_timestamp < dateadd('hour', 24, session_start_timestamp) then delivery_id end) as deliveries_24h_overall,
    count(distinct case when order_timestamp < dateadd('day', 7, session_start_timestamp) then delivery_id end) as deliveries_7d_overall,
    count(distinct case when order_timestamp < dateadd('day', 14, session_start_timestamp) then delivery_id end) as deliveries_14d_overall,
    count(distinct case when order_timestamp < dateadd('day', 30, session_start_timestamp) then delivery_id end) as deliveries_30d_overall,
    -- nv only
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('hour', 1, session_start_timestamp) then delivery_id end) as deliveries_1h_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('hour', 4, session_start_timestamp) then delivery_id end) as deliveries_4h_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('hour', 12, session_start_timestamp) then delivery_id end) as deliveries_12h_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('hour', 24, session_start_timestamp) then delivery_id end) as deliveries_24h_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('day', 7, session_start_timestamp) then delivery_id end) as deliveries_7d_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('day', 14, session_start_timestamp) then delivery_id end) as deliveries_14d_nv,
    count(distinct case when is_nv_order = 1 and order_timestamp < dateadd('day', 30, session_start_timestamp) then delivery_id end) as deliveries_30d_nv,
    -- non-nv only
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('hour', 1, session_start_timestamp) then delivery_id end) as deliveries_1h_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('hour', 4, session_start_timestamp) then delivery_id end) as deliveries_4h_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('hour', 12, session_start_timestamp) then delivery_id end) as deliveries_12h_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('hour', 24, session_start_timestamp) then delivery_id end) as deliveries_24h_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('day', 7, session_start_timestamp) then delivery_id end) as deliveries_7d_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('day', 14, session_start_timestamp) then delivery_id end) as deliveries_14d_non_nv,
    count(distinct case when is_nv_order = 0 and order_timestamp < dateadd('day', 30, session_start_timestamp) then delivery_id end) as deliveries_30d_non_nv
  from orders_join
  group by all
)
, denom as (
  select session_num, had_nv_impression, count(distinct consumer_id) as consumers_started_session
  from session_base
  group by all
)
, unioned as (
  -- overall
  select 
    'overall' as order_type,
    a.session_num,
    a.had_nv_impression,
    max(d.consumers_started_session) as consumers_started_session,
    sum(case when a.deliveries_1h_overall > 0 then 1 else 0 end) as consumers_retained_1h,
    sum(case when a.deliveries_4h_overall > 0 then 1 else 0 end) as consumers_retained_4h,
    sum(case when a.deliveries_24h_overall > 0 then 1 else 0 end) as consumers_retained_24h,
    sum(a.deliveries_1h_overall) as distinct_deliveries_1h,
    sum(a.deliveries_4h_overall) as distinct_deliveries_4h,
    sum(a.deliveries_24h_overall) as distinct_deliveries_24h,
    sum(a.deliveries_1h_overall) / nullif(max(d.consumers_started_session),0) as order_rate_1h,
    sum(a.deliveries_4h_overall) / nullif(max(d.consumers_started_session),0) as order_rate_4h,
    sum(a.deliveries_24h_overall) / nullif(max(d.consumers_started_session),0) as order_rate_24h,
    sum(case when a.deliveries_1h_overall > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_1h,
    sum(case when a.deliveries_4h_overall > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_4h,
    sum(case when a.deliveries_24h_overall > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_24h
  from orders_agg_per_consumer a
  join denom d using (session_num, had_nv_impression)
  group by a.session_num, a.had_nv_impression

  union all

  -- nv orders
  select 
    'nv_order' as order_type,
    a.session_num,
    a.had_nv_impression,
    max(d.consumers_started_session) as consumers_started_session,
    sum(case when a.deliveries_1h_nv > 0 then 1 else 0 end) as consumers_retained_1h,
    sum(case when a.deliveries_4h_nv > 0 then 1 else 0 end) as consumers_retained_4h,
    sum(case when a.deliveries_24h_nv > 0 then 1 else 0 end) as consumers_retained_24h,
    sum(a.deliveries_1h_nv) as distinct_deliveries_1h,
    sum(a.deliveries_4h_nv) as distinct_deliveries_4h,
    sum(a.deliveries_24h_nv) as distinct_deliveries_24h,
    sum(a.deliveries_1h_nv) / nullif(max(d.consumers_started_session),0) as order_rate_1h,
    sum(a.deliveries_4h_nv) / nullif(max(d.consumers_started_session),0) as order_rate_4h,
    sum(a.deliveries_24h_nv) / nullif(max(d.consumers_started_session),0) as order_rate_24h,
    sum(case when a.deliveries_1h_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_1h,
    sum(case when a.deliveries_4h_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_4h,
    sum(case when a.deliveries_24h_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_24h
  from orders_agg_per_consumer a
  join denom d using (session_num, had_nv_impression)
  group by a.session_num, a.had_nv_impression

  union all

  -- non-nv orders
  select 
    'non_nv_order' as order_type,
    a.session_num,
    a.had_nv_impression,
    max(d.consumers_started_session) as consumers_started_session,
    sum(case when a.deliveries_1h_non_nv > 0 then 1 else 0 end) as consumers_retained_1h,
    sum(case when a.deliveries_4h_non_nv > 0 then 1 else 0 end) as consumers_retained_4h,
    sum(case when a.deliveries_24h_non_nv > 0 then 1 else 0 end) as consumers_retained_24h,
    sum(a.deliveries_1h_non_nv) as distinct_deliveries_1h,
    sum(a.deliveries_4h_non_nv) as distinct_deliveries_4h,
    sum(a.deliveries_24h_non_nv) as distinct_deliveries_24h,
    sum(a.deliveries_1h_non_nv) / nullif(max(d.consumers_started_session),0) as order_rate_1h,
    sum(a.deliveries_4h_non_nv) / nullif(max(d.consumers_started_session),0) as order_rate_4h,
    sum(a.deliveries_24h_non_nv) / nullif(max(d.consumers_started_session),0) as order_rate_24h,
    sum(case when a.deliveries_1h_non_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_1h,
    sum(case when a.deliveries_4h_non_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_4h,
    sum(case when a.deliveries_24h_non_nv > 0 then 1 else 0 end) / nullif(max(d.consumers_started_session),0) as consumer_ordered_24h
  from orders_agg_per_consumer a
  join denom d using (session_num, had_nv_impression)
  group by a.session_num, a.had_nv_impression
)
select 
  u.*,
  -- Ratios NV=1 vs NV=0 within order_type + session_num
  max(case when u.had_nv_impression = 1 then order_rate_1h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then order_rate_1h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_order_rate_1h_nv_over_non_nv,
  max(case when u.had_nv_impression = 1 then order_rate_4h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then order_rate_4h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_order_rate_4h_nv_over_non_nv,
  max(case when u.had_nv_impression = 1 then order_rate_24h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then order_rate_24h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_order_rate_24h_nv_over_non_nv,
  max(case when u.had_nv_impression = 1 then consumer_ordered_1h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then consumer_ordered_1h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_consumer_ordered_1h_nv_over_non_nv,
  max(case when u.had_nv_impression = 1 then consumer_ordered_4h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then consumer_ordered_4h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_consumer_ordered_4h_nv_over_non_nv,
  max(case when u.had_nv_impression = 1 then consumer_ordered_24h end) over (partition by u.order_type, u.session_num)
    / nullif(max(case when u.had_nv_impression = 0 then consumer_ordered_24h end) over (partition by u.order_type, u.session_num), 0)
    as ratio_consumer_ordered_24h_nv_over_non_nv
from unioned u
);

-- Preview
select * from proddb.fionafan.nv_dp_new_user_session_order_rates order by  order_type,  session_num, had_nv_impression desc;


-- Order rate (distinct delivery_id / consumers) and retention (distinct consumers / consumers)
-- after join across horizons (7d, 14d, 21d, 28d, 60d, 90d, 12d), split by NV impression windows (1h, 4h, 12h, 24h, 7d, 14d, 30d) and order_type
create or replace table proddb.fionafan.nv_dp_new_user_join_order_rates as (

-- Create separate rows for each impression window
select 
  'overall' as order_type,
  nv_window,
  had_nv_impression,
  consumers_joined,
  distinct_deliveries_7d,
  distinct_deliveries_14d,
  distinct_deliveries_21d,
  distinct_deliveries_28d,
  distinct_deliveries_60d,
  distinct_deliveries_90d,
  distinct_deliveries_12d,
  consumers_retained_7d,
  consumers_retained_14d,
  consumers_retained_21d,
  consumers_retained_28d,
  consumers_retained_60d,
  consumers_retained_90d,
  consumers_retained_12d,
  order_rate_7d,
  order_rate_14d,
  order_rate_21d,
  order_rate_28d,
  order_rate_60d,
  order_rate_90d,
  order_rate_12d,
  consumer_ordered_7d,
  consumer_ordered_14d,
  consumer_ordered_21d,
  consumer_ordered_28d,
  consumer_ordered_60d,
  consumer_ordered_90d,
  consumer_ordered_12d,
  -- Ratios
  max(case when had_nv_impression = 1 then order_rate_7d  end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_7d  end) over (partition by nv_window), 0) as ratio_order_rate_7d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_14d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_14d end) over (partition by nv_window), 0) as ratio_order_rate_14d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_21d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_21d end) over (partition by nv_window), 0) as ratio_order_rate_21d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_28d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_28d end) over (partition by nv_window), 0) as ratio_order_rate_28d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_60d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_60d end) over (partition by nv_window), 0) as ratio_order_rate_60d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_90d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_90d end) over (partition by nv_window), 0) as ratio_order_rate_90d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then order_rate_12d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then order_rate_12d end) over (partition by nv_window), 0) as ratio_order_rate_12d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_7d  end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_7d  end) over (partition by nv_window), 0) as ratio_consumer_ordered_7d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_14d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_14d end) over (partition by nv_window), 0) as ratio_consumer_ordered_14d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_21d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_21d end) over (partition by nv_window), 0) as ratio_consumer_ordered_21d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_28d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_28d end) over (partition by nv_window), 0) as ratio_consumer_ordered_28d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_60d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_60d end) over (partition by nv_window), 0) as ratio_consumer_ordered_60d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_90d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_90d end) over (partition by nv_window), 0) as ratio_consumer_ordered_90d_nv_over_non_nv,
  max(case when had_nv_impression = 1 then consumer_ordered_12d end) over (partition by nv_window)
    / nullif(max(case when had_nv_impression = 0 then consumer_ordered_12d end) over (partition by nv_window), 0) as ratio_consumer_ordered_12d_nv_over_non_nv
from (
  with base as (
    select consumer_id, join_time
    from proddb.fionafan.nv_dp_new_user_table
  )
  , nv_flags as (
    select 
      consumer_id,
      max(case when first_nv_impression_time <= dateadd('hour', 1, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_1h,
      max(case when first_nv_impression_time <= dateadd('hour', 4, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_4h,
      max(case when first_nv_impression_time <= dateadd('hour', 12, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_12h,
      max(case when first_nv_impression_time <= dateadd('hour', 24, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_24h,
      max(case when first_nv_impression_time <= dateadd('day', 7, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_7d,
      max(case when first_nv_impression_time <= dateadd('day', 14, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_14d,
      max(case when first_nv_impression_time <= dateadd('day', 30, join_time) and coalesce(had_nv_impression,0) = 1 then 1 else 0 end) as had_nv_impression_30d
    from proddb.fionafan.nv_dp_new_user_session_impressions s

    join base b using (consumer_id)
    group by consumer_id
  )
  , orders as (
    select 
      o.consumer_id,
      o.order_timestamp,
      o.delivery_id,
      case when (o.nv_business_line is not null or o.nv_business_sub_type is not null) then 1 else 0 end as is_nv_order
    from proddb.fionafan.nv_dp_new_user_orders o
  )
  , joined as (
    select 
      b.consumer_id,
      n.had_nv_impression_1h,
      n.had_nv_impression_4h,
      n.had_nv_impression_12h,
      n.had_nv_impression_24h,
      n.had_nv_impression_7d,
      n.had_nv_impression_14d,
      n.had_nv_impression_30d,
      b.join_time,
      o.delivery_id,
      o.order_timestamp,
      o.is_nv_order
    from base b
    left join nv_flags n using (consumer_id)
    left join orders o
      on o.consumer_id = b.consumer_id
      and o.order_timestamp >= b.join_time
      and o.order_timestamp < dateadd('day', 90, b.join_time)
  )
  , agg_per_consumer as (
    select 
      consumer_id,
      had_nv_impression_1h,
      had_nv_impression_4h,
      had_nv_impression_12h,
      had_nv_impression_24h,
      had_nv_impression_7d,
      had_nv_impression_14d,
      had_nv_impression_30d,
      -- overall deliveries
      count(distinct case when order_timestamp < dateadd('day', 7,  join_time) then delivery_id end)  as deliveries_7d_overall,
      count(distinct case when order_timestamp < dateadd('day', 14, join_time) then delivery_id end)  as deliveries_14d_overall,
      count(distinct case when order_timestamp < dateadd('day', 21, join_time) then delivery_id end)  as deliveries_21d_overall,
      count(distinct case when order_timestamp < dateadd('day', 28, join_time) then delivery_id end)  as deliveries_28d_overall,
      count(distinct case when order_timestamp < dateadd('day', 60, join_time) then delivery_id end)  as deliveries_60d_overall,
      count(distinct case when order_timestamp < dateadd('day', 90, join_time) then delivery_id end)  as deliveries_90d_overall,
      count(distinct case when order_timestamp < dateadd('day', 12, join_time) then delivery_id end)  as deliveries_12d_overall
    from joined
    group by all
  )
  -- Unpivot impression windows and aggregate metrics
  select 
    '1h' as nv_window, 
    coalesce(had_nv_impression_1h, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_1h, 0)
  
  union all
  
  select 
    '4h' as nv_window, 
    coalesce(had_nv_impression_4h, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_4h, 0)
  
  union all
  
  select 
    '12h' as nv_window, 
    coalesce(had_nv_impression_12h, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_12h, 0)
  
  union all
  
  select 
    '24h' as nv_window, 
    coalesce(had_nv_impression_24h, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_24h, 0)
  
  union all
  
  select 
    '7d' as nv_window, 
    coalesce(had_nv_impression_7d, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_7d, 0)
  
  union all
  
  select 
    '14d' as nv_window, 
    coalesce(had_nv_impression_14d, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_14d, 0)
  
  union all
  
  select 
    '30d' as nv_window, 
    coalesce(had_nv_impression_30d, 0) as had_nv_impression,
    count(distinct consumer_id) as consumers_joined,
    sum(deliveries_7d_overall) as distinct_deliveries_7d,
    sum(deliveries_14d_overall) as distinct_deliveries_14d,
    sum(deliveries_21d_overall) as distinct_deliveries_21d,
    sum(deliveries_28d_overall) as distinct_deliveries_28d,
    sum(deliveries_60d_overall) as distinct_deliveries_60d,
    sum(deliveries_90d_overall) as distinct_deliveries_90d,
    sum(deliveries_12d_overall) as distinct_deliveries_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) as consumers_retained_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) as consumers_retained_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) as consumers_retained_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) as consumers_retained_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) as consumers_retained_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) as consumers_retained_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) as consumers_retained_12d,
    sum(deliveries_7d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_7d,
    sum(deliveries_14d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_14d,
    sum(deliveries_21d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_21d,
    sum(deliveries_28d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_28d,
    sum(deliveries_60d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_60d,
    sum(deliveries_90d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_90d,
    sum(deliveries_12d_overall) / nullif(count(distinct consumer_id), 0) as order_rate_12d,
    sum(case when deliveries_7d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_7d,
    sum(case when deliveries_14d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_14d,
    sum(case when deliveries_21d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_21d,
    sum(case when deliveries_28d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_28d,
    sum(case when deliveries_60d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_60d,
    sum(case when deliveries_90d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_90d,
    sum(case when deliveries_12d_overall > 0 then 1 else 0 end) / nullif(count(distinct consumer_id), 0) as consumer_ordered_12d
  from agg_per_consumer 
  group by coalesce(had_nv_impression_30d, 0)
) unpivoted
);


CREATE OR REPLACE TABLE proddb.fionafan.nv_impact_combined_analysis AS
  WITH session_metrics AS (
    SELECT
      'session_' || session_num::string as impact_type,
      order_type,
      had_nv_impression,
      session_num,
      NULL as nv_window,
      consumers_started_session as consumers_base,
      -- Normalize metrics to be comparable
      order_rate_1h,
      order_rate_4h,
      order_rate_12h,
      order_rate_24h,
      order_rate_7d,
      order_rate_14d,
      order_rate_30d,
      order_rate_24h as order_rate_short_term,
      consumer_ordered_1h,
      consumer_ordered_4h,
      consumer_ordered_12h,
      consumer_ordered_24h,
      consumer_ordered_7d,
      consumer_ordered_14d,
      consumer_ordered_30d,
      consumer_ordered_24h as retention_short_term,
      ratio_order_rate_1h_nv_over_non_nv,
      ratio_order_rate_4h_nv_over_non_nv,
      ratio_order_rate_12h_nv_over_non_nv,
      ratio_order_rate_24h_nv_over_non_nv,
      ratio_order_rate_7d_nv_over_non_nv,
      ratio_order_rate_14d_nv_over_non_nv,
      ratio_order_rate_30d_nv_over_non_nv,
      ratio_order_rate_24h_nv_over_non_nv as impact_multiplier_short,
      ratio_consumer_ordered_1h_nv_over_non_nv,
      ratio_consumer_ordered_4h_nv_over_non_nv,
      ratio_consumer_ordered_12h_nv_over_non_nv,
      ratio_consumer_ordered_24h_nv_over_non_nv,
      ratio_consumer_ordered_7d_nv_over_non_nv,
      ratio_consumer_ordered_14d_nv_over_non_nv,
      ratio_consumer_ordered_30d_nv_over_non_nv,
      ratio_consumer_ordered_24h_nv_over_non_nv as retention_multiplier_short,
      order_rate_30d as order_rate_long_term,
      consumer_ordered_30d as retention_long_term,
      ratio_order_rate_30d_nv_over_non_nv as impact_multiplier_long,
      ratio_consumer_ordered_30d_nv_over_non_nv as retention_multiplier_long
    FROM proddb.fionafan.nv_dp_new_user_session_order_rates
    WHERE session_num <= 3
  ),

  impression_window_metrics AS (
    SELECT
      'impression_window_' || nv_window as impact_type,
      order_type,
      had_nv_impression,
      NULL as session_num,
      nv_window,
      consumers_joined as consumers_base,
      -- Map to session structure for comparison
      NULL as order_rate_1h,
      NULL as order_rate_4h,
      NULL as order_rate_12h,
      NULL as order_rate_24h,
      order_rate_7d,
      order_rate_14d,
      NULL as order_rate_30d,
      order_rate_7d as order_rate_short_term,  -- Use 7d as "short-term" 
      NULL as consumer_ordered_1h,
      NULL as consumer_ordered_4h,
      NULL as consumer_ordered_12h,
      NULL as consumer_ordered_24h,
      consumer_ordered_7d,
      consumer_ordered_14d,
      NULL as consumer_ordered_30d,
      consumer_ordered_7d as retention_short_term,
      NULL as ratio_order_rate_1h_nv_over_non_nv,
      NULL as ratio_order_rate_4h_nv_over_non_nv,
      NULL as ratio_order_rate_12h_nv_over_non_nv,
      NULL as ratio_order_rate_24h_nv_over_non_nv,
      ratio_order_rate_7d_nv_over_non_nv,
      ratio_order_rate_14d_nv_over_non_nv,
      NULL as ratio_order_rate_30d_nv_over_non_nv,
      ratio_order_rate_7d_nv_over_non_nv as impact_multiplier_short,
      NULL as ratio_consumer_ordered_1h_nv_over_non_nv,
      NULL as ratio_consumer_ordered_4h_nv_over_non_nv,
      NULL as ratio_consumer_ordered_12h_nv_over_non_nv,
      NULL as ratio_consumer_ordered_24h_nv_over_non_nv,
      ratio_consumer_ordered_7d_nv_over_non_nv,
      ratio_consumer_ordered_14d_nv_over_non_nv,
      NULL as ratio_consumer_ordered_30d_nv_over_non_nv,
      ratio_consumer_ordered_7d_nv_over_non_nv as retention_multiplier_short,
      order_rate_90d as order_rate_long_term,
      consumer_ordered_90d as retention_long_term,
      ratio_order_rate_90d_nv_over_non_nv as impact_multiplier_long,
      ratio_consumer_ordered_90d_nv_over_non_nv as retention_multiplier_long
    FROM proddb.fionafan.nv_dp_new_user_join_order_rates
  )

  SELECT * FROM session_metrics
  UNION ALL
  SELECT * FROM impression_window_metrics
  ORDER BY impact_type, order_type, nv_window, session_num, had_nv_impression DESC;


-- Preview
select * from proddb.fionafan.nv_dp_new_user_session_order_rates order by 1, 2 desc;
select * from proddb.fionafan.nv_dp_new_user_join_order_rates order by 1, 2 ;


select * from proddb.fionafan.nv_impact_combined_analysis order by 1, 2 desc;
