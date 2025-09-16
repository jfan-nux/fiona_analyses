-- Q3 using device-level orders features directly
-- Cohorts: nv_first_order, nv_second_order, nv_order_24h, nv_order_7d
-- Outcomes: monthly order_rate (distinct deliveries / devices) overall|nv_order|non_nv_order, and retention (distinct consumers / devices)
-- Includes ratios of cohort=1 vs cohort=0 for order_rate and retention per month and order_type

create or replace table proddb.fionafan.nv_dp_orders_performance_from_device as (
with base_devices as (
  select dd_device_id_filtered as device_id, consumer_id, join_time
  from (
    select t.*, row_number() over (partition by dd_device_id_filtered order by join_time) as rn
    from proddb.fionafan.nv_dp_new_user_table t
  )
  where rn = 1
)
, cohorts as (
  select 
    d.device_id,
    d.consumer_id,
    'nv_first_order'  as cohort_name,
    coalesce(o.nv_first_order, 0)  as cohort_flag
  from base_devices d
  left join proddb.fionafan.nv_dp_orders_device_features o
    on o.device_id = d.device_id and o.consumer_id = d.consumer_id

  union all

  select 
    d.device_id,
    d.consumer_id,
    'nv_second_order' as cohort_name,
    coalesce(o.nv_second_order, 0) as cohort_flag
  from base_devices d
  left join proddb.fionafan.nv_dp_orders_device_features o
    on o.device_id = d.device_id and o.consumer_id = d.consumer_id

  union all

  select 
    d.device_id,
    d.consumer_id,
    'nv_order_24h'   as cohort_name,
    coalesce(o.nv_order_24h, 0)   as cohort_flag
  from base_devices d
  left join proddb.fionafan.nv_dp_orders_device_features o
    on o.device_id = d.device_id and o.consumer_id = d.consumer_id

  union all

  select 
    d.device_id,
    d.consumer_id,
    'nv_order_7d'    as cohort_name,
    coalesce(o.nv_order_7d, 0)    as cohort_flag
  from base_devices d
  left join proddb.fionafan.nv_dp_orders_device_features o
    on o.device_id = d.device_id and o.consumer_id = d.consumer_id
)
, orders as (
  select 
    o.dd_device_id_filtered as device_id,
    o.consumer_id,
    o.delivery_id,
    o.order_timestamp,
    case when coalesce(o.nv_org, o.nv_vertical_name, o.nv_business_line, o.nv_business_sub_type) is not null then 1 else 0 end as is_nv_order
  from proddb.fionafan.nv_dp_new_user_orders o
)
, monthly as (
  select 
    b.device_id,
    b.consumer_id,
    'month1' as month_bucket,
    max(case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  b.join_time and o.order_timestamp <= dateadd('day', 30, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_devices b
  left join orders o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id

  union all

  select 
    b.device_id,
    b.consumer_id,
    'month2' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 30, b.join_time) and o.order_timestamp <= dateadd('day', 60, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_devices b
  left join orders o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id

  union all

  select 
    b.device_id,
    b.consumer_id,
    'month3' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 60, b.join_time) and o.order_timestamp <= dateadd('day', 90, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_devices b
  left join orders o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id

  union all

  select 
    b.device_id,
    b.consumer_id,
    'month4' as month_bucket,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) then 1 else 0 end) as any_order,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_nv_order = 1 then 1 else 0 end) as any_order_nv,
    max(case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_nv_order = 0 then 1 else 0 end) as any_order_non_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) then o.delivery_id end) as orders,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_nv_order = 1 then o.delivery_id end) as orders_nv,
    count(distinct case when o.order_timestamp >  dateadd('day', 90, b.join_time) and o.order_timestamp <= dateadd('day', 120, b.join_time) and o.is_nv_order = 0 then o.delivery_id end) as orders_non_nv
  from base_devices b
  left join orders o on o.device_id = b.device_id and o.consumer_id = b.consumer_id
  group by b.device_id, b.consumer_id
)
, device_monthly as (
  select 
    m.device_id,
    m.month_bucket,
    m.orders,
    m.orders_nv,
    m.orders_non_nv,
    m.any_order,
    m.any_order_nv,
    m.any_order_non_nv
  from monthly m
)
, aggregated as (
  -- Aggregate to cohort_name x cohort_flag x month_bucket and compute per order_type
  select 
    c.cohort_name,
    c.cohort_flag,
    dm.month_bucket,
    'overall' as order_type,
    count(distinct c.device_id) as devices,
    sum(dm.any_order) as consumers_retained,
    sum(dm.orders) as distinct_deliveries,
    sum(dm.orders) / nullif(count(distinct c.device_id), 0) as order_rate,
    sum(dm.any_order) / nullif(count(distinct c.device_id), 0) as retention
  from cohorts c
  left join device_monthly dm on dm.device_id = c.device_id
  group by c.cohort_name, c.cohort_flag, dm.month_bucket

  union all

  select 
    c.cohort_name,
    c.cohort_flag,
    dm.month_bucket,
    'nv_order' as order_type,
    count(distinct c.device_id) as devices,
    sum(dm.any_order_nv) as consumers_retained,
    sum(dm.orders_nv) as distinct_deliveries,
    sum(dm.orders_nv) / nullif(count(distinct c.device_id), 0) as order_rate,
    sum(dm.any_order_nv) / nullif(count(distinct c.device_id), 0) as retention
  from cohorts c
  left join device_monthly dm on dm.device_id = c.device_id
  group by c.cohort_name, c.cohort_flag, dm.month_bucket

  union all

  select 
    c.cohort_name,
    c.cohort_flag,
    dm.month_bucket,
    'non_nv_order' as order_type,
    count(distinct c.device_id) as devices,
    sum(dm.any_order_non_nv) as consumers_retained,
    sum(dm.orders_non_nv) as distinct_deliveries,
    sum(dm.orders_non_nv) / nullif(count(distinct c.device_id), 0) as order_rate,
    sum(dm.any_order_non_nv) / nullif(count(distinct c.device_id), 0) as retention
  from cohorts c
  left join device_monthly dm on dm.device_id = c.device_id
  group by c.cohort_name, c.cohort_flag, dm.month_bucket
)
select 
  a.*,
  -- Ratios: cohort_flag=1 vs 0 within cohort_name x month_bucket x order_type
  max(case when a.cohort_flag = 1 then a.order_rate end) over (partition by a.cohort_name, a.month_bucket, a.order_type)
    / nullif(max(case when a.cohort_flag = 0 then a.order_rate end) over (partition by a.cohort_name, a.month_bucket, a.order_type), 0) as ratio_order_rate_cohort_over_non,
  max(case when a.cohort_flag = 0 then a.order_rate end) over (partition by a.cohort_name, a.month_bucket, a.order_type)
    / nullif(max(case when a.cohort_flag = 1 then a.order_rate end) over (partition by a.cohort_name, a.month_bucket, a.order_type), 0) as ratio_order_rate_non_over_cohort,
  max(case when a.cohort_flag = 1 then a.retention end) over (partition by a.cohort_name, a.month_bucket, a.order_type)
    / nullif(max(case when a.cohort_flag = 0 then a.retention end) over (partition by a.cohort_name, a.month_bucket, a.order_type), 0) as ratio_retention_cohort_over_non,
  max(case when a.cohort_flag = 0 then a.retention end) over (partition by a.cohort_name, a.month_bucket, a.order_type)
    / nullif(max(case when a.cohort_flag = 1 then a.retention end) over (partition by a.cohort_name, a.month_bucket, a.order_type), 0) as ratio_retention_non_over_cohort
from aggregated a
);

-- Preview
select * from proddb.fionafan.nv_dp_orders_performance_from_device order by cohort_name, month_bucket, order_type, cohort_flag desc;


