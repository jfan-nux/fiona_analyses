create or replace table proddb.fionafan.speed_store_card_and_row_experiment_exposures as (

select * from METRICS_REPO.PUBLIC.speed_store_card_and_row_experiment__ios_Users_exposures
where experiment_group in ('treatment1','control')
);
create or replace table proddb.fionafan.speed_store_card_and_row_experiment_exposures_t3 as (

select * from METRICS_REPO.PUBLIC.speed_store_card_and_row_experiment__ios_Users_exposures
where experiment_group in ('treatment3','control')
);

select count(1) from proddb.fionafan.speed_store_card_and_row_experiment_exposures;



-- estimated time for delivery vs actual time for delivery, how does the gap change over day for treatment and control, and overall separately?

-- Overall (all variants combined)
select
    date_trunc('day', d.created_at) as order_date,
    count(distinct d.delivery_id) as total_deliveries,
    avg(datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as avg_eta_gap_minutes,
    median(datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as median_eta_gap_minutes,
    -- percentile_cont(0.01) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p1_eta_gap_minutes,
        -- percentile_cont(0.001) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p01_eta_gap_minutes
    percentile_cont(0.25) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p25_eta_gap_minutes,
    percentile_cont(0.75) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p75_eta_gap_minutes
from dimension_deliveries d
inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
    on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
where d.created_at >='2025-09-17' and d.actual_delivery_time is not null
    and d.quoted_delivery_time is not null
    and d.is_filtered_core = true
    and datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)>-90
group by 1
order by 1;

-- By experiment group (treatment vs control)
select
    e.experiment_group,
    date_trunc('day', d.created_at) as order_date,
    count(distinct d.delivery_id) as total_deliveries,
    avg(datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as avg_eta_gap_minutes,
    median(datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as median_eta_gap_minutes,
    percentile_cont(0.25) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p25_eta_gap_minutes,
    percentile_cont(0.75) within group (order by datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)) as p75_eta_gap_minutes
from dimension_deliveries d
inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
    on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
where d.created_at >='2025-09-17' and d.actual_delivery_time is not null
    and d.quoted_delivery_time is not null
    and d.is_filtered_core = true
    and datediff('minute', d.quoted_delivery_time, d.actual_delivery_time)>-90
group by 1, 2
order by 1, 2;

select top 10 created_at, quoted_delivery_time, actual_delivery_time from dimension_deliveries where created_at >='2025-09-17';

-- m_card_view events after 9/17 filtered for exposures
create or replace table proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events as (

select 
    mcv.*, e.experiment_group, e.first_exposure_time
from iguazu.consumer.m_card_click mcv
inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
    on mcv.consumer_id = e.bucket_key
where mcv.timestamp >= '2025-09-17'
    and mcv.timestamp > e.first_exposure_time
    );

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store as (

select 
    mcv.*, e.experiment_group, e.first_exposure_time
from iguazu.consumer.m_card_click mcv
inner join METRICS_REPO.PUBLIC.speed_store_card_and_row_experiment__ios_Users_exposures e
    on mcv.consumer_id = e.bucket_key
where mcv.timestamp >= '2025-09-17'
    and mcv.timestamp > e.first_exposure_time
    and store_id is not null
    );
 
select is_component_catalog,speed_type, store_cnt/sum(store_cnt) over () as store_cnt_pct, store_cnt, cnt/sum(cnt) over () as cnt_pct,cnt, avg_vertical_position 
from (
select other_properties:is_component_catalog as is_component_catalog, other_properties:speed_type as speed_type, count(distinct a.store_id) store_cnt, count(1) cnt, avg(a.vertical_position) as avg_vertical_position
from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store a
inner join dimension_store b on a.store_id = b.store_id
where page = 'explore_page' and b.is_restaurant = 1 and a.experiment_group in ('treatment1','control') and container = 'store'
group by all order by 2 desc
);

select distinct container from iguazu.consumer.m_card_click where ingest_timestamp > '2025-10-01' and page = 'explore_page' limit 100;

select container, count(distinct store_id) store_cnt, count(1) cnt from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where other_properties:speed_type is not null and page = 'explore_page' group by all order by store_cnt desc;

select CAROUSEL_NAME, count(distinct store_id) store_cnt, count(1) cnt from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where other_properties:speed_type is not null and page = 'explore_page' and container = 'cluster' group by all order by store_cnt desc;

select top 100 * from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where other_properties:speed_type is not null and page = 'explore_page' and container = 'cluster' ;

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores as (
with base as (
select  store_id, case when other_properties:speed_type in ('FASTEST') then 1 else 0 end as is_fast, count(1) cnt
from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where  experiment_group in ('treatment1','control') 
group by all)
select store_id, is_fast, cnt/sum(cnt) over (partition by store_id) as speed_type_pct from base
);

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_speed_type as (


);

select  case when speed_type_NUM = 1 then 'FASTEST' 
when speed_type_NUM = 2 then 'FAST' when speed_type_NUM = 3 then 'NON_FAST' 
when speed_type_NUM = 4 then 'UNKNOWN' when speed_type_NUM = 5 then 'null' else 'OTHER' end speed_type, count(1) cnt 
from (select store_id, min(case speed_type when 'FASTEST' then 1 when 'FAST ' then 2 
when 'NON_FAST' then 3 when 'UNKNOWN' then 4 when 'null' then 5
ELSE 5 end) speed_type_NUM
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores 
group by all) group by all order by speed_type ;


select count(1) from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores;
select count(distinct store_id) from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores 
where is_fast = 1 and speed_type_pct>0.05;

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_fast_Restaurants as (
select * from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 1 and speed_type_pct>0.05

);

select case when speed_type_pct=1 then 1 else 0 end as never_tagged, count(1) cnt 
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 0 group by all order by never_tagged desc;

NEVER_TAGGED	CNT
1	343845
0	484553

select 1-343845/(343845+484553)

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_non_fast_Restaurants as (
select * from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 0 and speed_type_pct>0.95
);
select count(1) from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_non_fast_Restaurants;

-- Restaurant percentiles based on ETA (created_at to actual_delivery_time) for ASAP orders
create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_eta_percentiles as (
with restaurant_eta as (
    select
        d.store_id,
        s.name as store_name,
        count(distinct d.delivery_id) as total_deliveries,
g_eta_minutes,
        median(datediff('minute', d.created_at, d.actual_delivery_time)) as median_eta_minutes,
        percentile_cont(0.25) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p25_eta_minutes,
        percentile_cont(0.75) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p75_eta_minutes,
        percentile_cont(0.90) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p90_eta_minutes
    from dimension_deliveries d
    inner join dimension_store s
        on d.store_id = s.store_id and s.is_restaurant = 1
    where d.created_at >= dateadd('day', -28, '2025-09-17'::date)
        and d.created_at < '2025-09-17'::date
        and d.actual_delivery_time is not null
        and d.cancelled_at is null
        and d.is_consumer_pickup = false
        and d.is_asap = true
        and datediff('minute', d.created_at, d.actual_delivery_time) < 120
    group by 1, 2
    having count(distinct d.delivery_id) >= 10  -- At least 10 deliveries for stable estimates
),
restaurant_percentiles as (
    select
        store_id,
        store_name,
        total_deliveries,
        avg_eta_minutes,
        median_eta_minutes,
        p25_eta_minutes,
        p75_eta_minutes,
        p90_eta_minutes,
        -- Calculate percentile rank for each restaurant based on average ETA
        percent_rank() over (order by avg_eta_minutes) as percentile_rank,
        ntile(100) over (order by avg_eta_minutes) as percentile_bucket,
        ntile(10) over (order by avg_eta_minutes) as decile
    from restaurant_eta
)
select
* from restaurant_percentiles
);

select case when b.store_id is not null then 'FAST' else 'NON_FAST' end as speed_type,
 avg(a.percentile_rank),median(a.percentile_rank), avg(a.percentile_bucket), avg(a.p25_eta_minutes), avg(a.p75_eta_minutes), avg(a.median_eta_minutes)
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_eta_percentiles a
right join  proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_fast_Restaurants b on a.store_id = b.store_id
group by all
union
select case when b.store_id is not null then 'NOT FAST' else 'FAST' end as speed_type,
 avg(a.percentile_rank),median(a.percentile_rank), avg(a.percentile_bucket), avg(a.p25_eta_minutes), avg(a.p75_eta_minutes), avg(a.median_eta_minutes)
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_eta_percentiles a
right join  proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_non_fast_Restaurants b on a.store_id = b.store_id
group by all
;

select case when b.store_id is not null then 'exists previously' else 'not exist previously' end as exists_previously
, case when b.percentile_rank<0.5 then 'fast' else 'non-fast' end as speed_type_pct50
, case when b.percentile_rank<0.3 then 'fast' else 'non-fast' end as speed_type_pct30
, case when b.percentile_rank<0.1 then 'fast' else 'non-fast' end as speed_type_pct10
, count(distinct a.store_id) store_cnt, sum(total_deliveries) total_deliveries
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_non_fast_Restaurants a
left join proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_eta_percentiles b on a.store_id = b.store_id
group by all order by all
;


select t.tier_level, count(distinct a.store_id), avg(b.percentile_rank)
from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_eta_percentiles b 
inner join dimension_store a on a.store_id = b.store_id
left join proddb.luwang.market_tier t

      on t.submarket_id = a.submarket_id
where a.is_restaurant = 1
group by all
;

create or replace table proddb.fionafan.ffs_quoted_eta_percentiles as (
WITH base AS (
  SELECT
    d.store_id,
    ds.NAME AS store_name,
    d.created_at,
    d.actual_delivery_time,
    -- Quoted (point) from dimension_deliveries
    d.QUOTED_DELIVERY_TIME AS quoted_time_point,
    -- Upper bound from ETA range; fallback to quoted point if range upper missing (still allow both metrics to differ when available)
    er.QUOTED_DELIVERY_TIME_RANGE_MAX AS quoted_time_upper
  FROM edw.finance.dimension_deliveries d
  JOIN edw.merchant.dimension_store ds
    ON ds.store_id = d.store_id
  LEFT JOIN edw.logistics.dasher_delivery_eta_range er
    ON er.delivery_id = d.delivery_id
  WHERE
    d.created_at >= DATEADD(day, -28, CURRENT_DATE)
    AND d.created_at < CURRENT_DATE
    AND d.is_filtered_core = TRUE               -- delivered, non-cancelled, non-test, non-Drive
    AND d.country_id = 1                        -- US
    AND d.fulfillment_type ILIKE 'dasher%'      -- Dasher fulfilled
    AND COALESCE(d.is_consumer_pickup, FALSE) = FALSE
    AND COALESCE(ds.IS_RESTAURANT, 0) = 1       -- Restaurants
),
per_delivery AS (
  SELECT
    store_id,
    store_name,
    created_at,
    actual_delivery_time,
    quoted_time_point,
    quoted_time_upper,
    -- Use upper bound (when present) for ranking by quoted ETA minutes; fall back to quoted point
    DATEDIFF(
      minute,
      created_at,
      COALESCE(quoted_time_upper, quoted_time_point)
    ) AS quoted_eta_minutes_for_rank,
    -- Within quoted (point) and late windows
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= quoted_time_point THEN 1 ELSE 0 END AS within_quoted_0,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 5, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_5,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 10, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_10,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 20, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_20,
    -- Within upper (range max) and late windows
    CASE WHEN quoted_time_upper IS NOT NULL
         AND actual_delivery_time <= quoted_time_upper THEN 1 ELSE 0 END AS within_upper_0,
    CASE WHEN quoted_time_upper IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 5, quoted_time_upper) THEN 1 ELSE 0 END AS within_upper_plus_5,
    CASE WHEN quoted_time_upper IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 10, quoted_time_upper) THEN 1 ELSE 0 END AS within_upper_plus_10,
    CASE WHEN quoted_time_upper IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 20, quoted_time_upper) THEN 1 ELSE 0 END AS within_upper_plus_20
  FROM base
  WHERE quoted_time_point IS NOT NULL
),
per_store AS (
  SELECT
    store_id,
    ANY_VALUE(store_name) AS store_name,
    COUNT(*) AS deliveries_cnt,
    AVG(quoted_eta_minutes_for_rank) AS avg_quoted_eta_minutes,
    -- Percentages (0–1)
    AVG(within_quoted_0)        AS pct_within_quoted_0,
    AVG(within_quoted_plus_5)   AS pct_within_quoted_plus_5,
    AVG(within_quoted_plus_10)  AS pct_within_quoted_plus_10,
    AVG(within_quoted_plus_20)  AS pct_within_quoted_plus_20,
    AVG(within_upper_0)         AS pct_within_upper_0,
    AVG(within_upper_plus_5)    AS pct_within_upper_plus_5,
    AVG(within_upper_plus_10)   AS pct_within_upper_plus_10,
    AVG(within_upper_plus_20)   AS pct_within_upper_plus_20
  FROM per_delivery
  GROUP BY store_id
),
ranked AS (
  SELECT
    p.*,
    CASE
      WHEN CUME_DIST() OVER (ORDER BY avg_quoted_eta_minutes DESC) <= 0.30 THEN 1
      ELSE 0
    END AS is_top_30pct
  FROM per_store p
)
SELECT
  store_id,
  store_name,
  is_top_30pct,                -- 0/1 flag
  avg_quoted_eta_minutes,
  deliveries_cnt,
  -- Within quoted (point) and quoted +5/+10/+20
  pct_within_quoted_0,
  pct_within_quoted_plus_5,
  pct_within_quoted_plus_10,
  pct_within_quoted_plus_20,
  -- Within upper (range max) and upper +5/+10/+20
  pct_within_upper_0,
  pct_within_upper_plus_5,
  pct_within_upper_plus_10,
  pct_within_upper_plus_20
FROM ranked
-- Optional: stabilize by requiring a minimum number of deliveries per store in the 28d window
-- WHERE deliveries_cnt >= 20                             UYT
ORDER BY avg_quoted_eta_minutes DESC
);

-- Percentage of stores meeting each threshold for top 30% stores
-- Concatenated view showing all metrics (quoted_0, quoted_plus_5, etc.)
with base_stats as (
  select
    count(1) as total_stores,
    -- pct_within_quoted_0
    sum(case when pct_within_quoted_0 >= 0.9 then 1 else 0 end) / count(1) * 100 as quoted_0_pct_stores_within_90,
    sum(case when pct_within_quoted_0 >= 0.85 then 1 else 0 end) / count(1) * 100 as quoted_0_pct_stores_within_85,
    sum(case when pct_within_quoted_0 >= 0.8 then 1 else 0 end) / count(1) * 100 as quoted_0_pct_stores_within_80,
    -- pct_within_quoted_plus_5
    sum(case when pct_within_quoted_plus_5 >= 0.9 then 1 else 0 end) / count(1) * 100 as quoted_plus_5_pct_stores_within_90,
    sum(case when pct_within_quoted_plus_5 >= 0.85 then 1 else 0 end) / count(1) * 100 as quoted_plus_5_pct_stores_within_85,
    sum(case when pct_within_quoted_plus_5 >= 0.8 then 1 else 0 end) / count(1) * 100 as quoted_plus_5_pct_stores_within_80,
    -- pct_within_quoted_plus_10
    sum(case when pct_within_quoted_plus_10 >= 0.9 then 1 else 0 end) / count(1) * 100 as quoted_plus_10_pct_stores_within_90,
    sum(case when pct_within_quoted_plus_10 >= 0.85 then 1 else 0 end) / count(1) * 100 as quoted_plus_10_pct_stores_within_85,
    sum(case when pct_within_quoted_plus_10 >= 0.8 then 1 else 0 end) / count(1) * 100 as quoted_plus_10_pct_stores_within_80,
    -- pct_within_quoted_plus_20
    sum(case when pct_within_quoted_plus_20 >= 0.9 then 1 else 0 end) / count(1) * 100 as quoted_plus_20_pct_stores_within_90,
    sum(case when pct_within_quoted_plus_20 >= 0.85 then 1 else 0 end) / count(1) * 100 as quoted_plus_20_pct_stores_within_85,
    sum(case when pct_within_quoted_plus_20 >= 0.8 then 1 else 0 end) / count(1) * 100 as quoted_plus_20_pct_stores_within_80,
    -- pct_within_upper_0
    sum(case when pct_within_upper_0 >= 0.9 then 1 else 0 end) / count(1) * 100 as upper_0_pct_stores_within_90,
    sum(case when pct_within_upper_0 >= 0.85 then 1 else 0 end) / count(1) * 100 as upper_0_pct_stores_within_85,
    sum(case when pct_within_upper_0 >= 0.8 then 1 else 0 end) / count(1) * 100 as upper_0_pct_stores_within_80,
    -- pct_within_upper_plus_5
    sum(case when pct_within_upper_plus_5 >= 0.9 then 1 else 0 end) / count(1) * 100 as upper_plus_5_pct_stores_within_90,
    sum(case when pct_within_upper_plus_5 >= 0.85 then 1 else 0 end) / count(1) * 100 as upper_plus_5_pct_stores_within_85,
    sum(case when pct_within_upper_plus_5 >= 0.8 then 1 else 0 end) / count(1) * 100 as upper_plus_5_pct_stores_within_80,
    -- pct_within_upper_plus_10
    sum(case when pct_within_upper_plus_10 >= 0.9 then 1 else 0 end) / count(1) * 100 as upper_plus_10_pct_stores_within_90,
    sum(case when pct_within_upper_plus_10 >= 0.85 then 1 else 0 end) / count(1) * 100 as upper_plus_10_pct_stores_within_85,
    sum(case when pct_within_upper_plus_10 >= 0.8 then 1 else 0 end) / count(1) * 100 as upper_plus_10_pct_stores_within_80,
    -- pct_within_upper_plus_20
    sum(case when pct_within_upper_plus_20 >= 0.9 then 1 else 0 end) / count(1) * 100 as upper_plus_20_pct_stores_within_90,
    sum(case when pct_within_upper_plus_20 >= 0.85 then 1 else 0 end) / count(1) * 100 as upper_plus_20_pct_stores_within_85,
    sum(case when pct_within_upper_plus_20 >= 0.8 then 1 else 0 end) / count(1) * 100 as upper_plus_20_pct_stores_within_80
  from proddb.fionafan.ffs_quoted_eta_percentiles 
  where is_top_30pct = 1 and deliveries_cnt >= 20       
)
select 
  'pct_within_quoted_0' as metric_name,
  total_stores,
  quoted_0_pct_stores_within_90 as pct_stores_within_90,
  quoted_0_pct_stores_within_85 as pct_stores_within_85,
  quoted_0_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_quoted_plus_5' as metric_name,
  total_stores,
  quoted_plus_5_pct_stores_within_90 as pct_stores_within_90,
  quoted_plus_5_pct_stores_within_85 as pct_stores_within_85,
  quoted_plus_5_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_quoted_plus_10' as metric_name,
  total_stores,
  quoted_plus_10_pct_stores_within_90 as pct_stores_within_90,
  quoted_plus_10_pct_stores_within_85 as pct_stores_within_85,
  quoted_plus_10_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_quoted_plus_20' as metric_name,
  total_stores,
  quoted_plus_20_pct_stores_within_90 as pct_stores_within_90,
  quoted_plus_20_pct_stores_within_85 as pct_stores_within_85,
  quoted_plus_20_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_upper_0' as metric_name,
  total_stores,
  upper_0_pct_stores_within_90 as pct_stores_within_90,
  upper_0_pct_stores_within_85 as pct_stores_within_85,
  upper_0_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_upper_plus_5' as metric_name,
  total_stores,
  upper_plus_5_pct_stores_within_90 as pct_stores_within_90,
  upper_plus_5_pct_stores_within_85 as pct_stores_within_85,
  upper_plus_5_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_upper_plus_10' as metric_name,
  total_stores,
  upper_plus_10_pct_stores_within_90 as pct_stores_within_90,
  upper_plus_10_pct_stores_within_85 as pct_stores_within_85,
  upper_plus_10_pct_stores_within_80 as pct_stores_within_80
from base_stats
union all
select 
  'pct_within_upper_plus_20' as metric_name,
  total_stores,
  upper_plus_20_pct_stores_within_90 as pct_stores_within_90,
  upper_plus_20_pct_stores_within_85 as pct_stores_within_85,
  upper_plus_20_pct_stores_within_80 as pct_stores_within_80
from base_stats
order by metric_name;

create or replace table proddb.fionafan.ffs_quoted_eta_percentiles_per_store as (
WITH base AS (
  SELECT
    d.store_id,
    ds.NAME AS store_name,
    d.created_at,
    d.actual_delivery_time,
    d.QUOTED_DELIVERY_TIME AS quoted_time_point,
    d.delivery_id
  FROM edw.finance.dimension_deliveries d
  JOIN edw.merchant.dimension_store ds
    ON ds.store_id = d.store_id
  WHERE
    d.created_at >= DATEADD(day, -28, CURRENT_DATE)
    AND d.created_at < CURRENT_DATE
    AND d.is_filtered_core = TRUE               -- delivered, non-cancelled, non-test, non-Drive
    AND d.fulfillment_type ILIKE 'dasher%'      -- Dasher fulfilled
    AND COALESCE(d.is_consumer_pickup, FALSE) = FALSE
    AND COALESCE(ds.IS_RESTAURANT, 0) = 1       -- Restaurants
),
per_delivery AS (
  SELECT
    store_id,
    store_name,
    created_at,
    actual_delivery_time,
    quoted_time_point,
    delivery_id,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= quoted_time_point THEN 1 ELSE 0 END AS within_quoted_0,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 5, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_5,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 10, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_10,
    CASE WHEN quoted_time_point IS NOT NULL
         AND actual_delivery_time <= DATEADD(minute, 20, quoted_time_point) THEN 1 ELSE 0 END AS within_quoted_plus_20
  FROM base
  WHERE quoted_time_point IS NOT NULL
),
per_store AS (
  SELECT
    store_id,
    ANY_VALUE(store_name) AS store_name,
    COUNT(distinct delivery_id) AS deliveries_cnt,
    AVG(within_quoted_0)        AS pct_within_quoted_0,
    AVG(within_quoted_plus_5)   AS pct_within_quoted_plus_5,
    AVG(within_quoted_plus_10)  AS pct_within_quoted_plus_10,
    AVG(within_quoted_plus_20)  AS pct_within_quoted_plus_20
  FROM per_delivery
  GROUP BY store_id
)
SELECT
  store_id,
  store_name,
  deliveries_cnt,
  -- Within quoted (point) and quoted +5/+10/+20
  pct_within_quoted_0,
  pct_within_quoted_plus_5,
  pct_within_quoted_plus_10,
  pct_within_quoted_plus_20
FROM per_store
-- Optional: stabilize by requiring a minimum number of deliveries per store in the 28d window
WHERE deliveries_cnt >= 20
);

create or replace table proddb.fionafan.ffs_static_eta_events as (


    WITH base AS (
  SELECT
    TIME_SLICE(IGUAZU_SENT_AT, 5, 'MINUTE') AS time_bin,
    CONSUMER_ID,
    STORE_ID,
    STORE_BUSINESS_ID,
    FINAL_DELIVERY_ETA_SECONDS AS quoted_eta_seconds,
    IGUAZU_SENT_AT,
    ROW_NUMBER() OVER (
      PARTITION BY TIME_SLICE(IGUAZU_SENT_AT, 5, 'MINUTE'), CONSUMER_ID, STORE_ID
      ORDER BY IGUAZU_SENT_AT DESC
    ) AS rn_latest_in_bin
  FROM IGUAZU.SERVER_EVENTS_PRODUCTION.STATIC_ETA_EVENT_ICE
  WHERE
    IGUAZU_SENT_AT >= DATEADD(day, -2, CURRENT_TIMESTAMP())
    AND IGUAZU_SENT_AT < CURRENT_TIMESTAMP()
    AND CONSUMER_ID IS NOT NULL
    AND STORE_ID IS NOT NULL
    AND FINAL_DELIVERY_ETA_SECONDS IS NOT NULL
),
dedup AS (
  SELECT
    time_bin,
    CONSUMER_ID,
    STORE_ID,
    STORE_BUSINESS_ID,
    quoted_eta_seconds
  FROM base
  WHERE rn_latest_in_bin = 1
),
with_p30 AS (
  SELECT
    d.*,
    PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY quoted_eta_seconds)
      OVER (PARTITION BY time_bin, CONSUMER_ID) AS p30_eta
  FROM dedup d
)
SELECT
  time_bin,
  CONSUMER_ID,
  STORE_ID,
  STORE_BUSINESS_ID,
  quoted_eta_seconds,
  1 AS is_fastest_top30
FROM with_p30
WHERE quoted_eta_seconds <= p30_eta
ORDER BY time_bin, CONSUMER_ID, quoted_eta_seconds ASC
);


create or replace table proddb.fionafan.ffs_static_eta_events_w_store as (


select a.*,
  deliveries_cnt,
  -- Within quoted (point) and quoted +5/+10/+20
  pct_within_quoted_0,
  pct_within_quoted_plus_5,
  pct_within_quoted_plus_10,
  pct_within_quoted_plus_20,
  -- Threshold checks for pct_within_quoted_0
  (pct_within_quoted_0 >= 0.85 and deliveries_cnt > 20) as pct_within_quoted_0_exceed_85,
  (pct_within_quoted_0 >= 0.90 and deliveries_cnt > 20) as pct_within_quoted_0_exceed_90,
  (pct_within_quoted_0 >= 0.95 and deliveries_cnt > 20) as pct_within_quoted_0_exceed_95,
  -- Threshold checks for pct_within_quoted_plus_5
  (pct_within_quoted_plus_5 >= 0.85 and deliveries_cnt > 20) as pct_within_quoted_plus_5_exceed_85,
  (pct_within_quoted_plus_5 >= 0.90 and deliveries_cnt > 20) as pct_within_quoted_plus_5_exceed_90,
  (pct_within_quoted_plus_5 >= 0.95 and deliveries_cnt > 20) as pct_within_quoted_plus_5_exceed_95,
  -- Threshold checks for pct_within_quoted_plus_10
  (pct_within_quoted_plus_10 >= 0.85 and deliveries_cnt > 20) as pct_within_quoted_plus_10_exceed_85,
  (pct_within_quoted_plus_10 >= 0.90 and deliveries_cnt > 20) as pct_within_quoted_plus_10_exceed_90,
  (pct_within_quoted_plus_10 >= 0.95 and deliveries_cnt > 20) as pct_within_quoted_plus_10_exceed_95,
  -- Threshold checks for pct_within_quoted_plus_20
  (pct_within_quoted_plus_20 >= 0.85 and deliveries_cnt > 20) as pct_within_quoted_plus_20_exceed_85,
  (pct_within_quoted_plus_20 >= 0.90 and deliveries_cnt > 20) as pct_within_quoted_plus_20_exceed_90,
  (pct_within_quoted_plus_20 >= 0.95 and deliveries_cnt > 20) as pct_within_quoted_plus_20_exceed_95
from proddb.fionafan.ffs_static_eta_events a
left join proddb.fionafan.ffs_quoted_eta_percentiles b 
on a.store_id::varchar = b.store_id::varchar
);

select * from proddb.fionafan.ffs_static_eta_events_w_store limit 10;

create or replace table proddb.fionafan.ffs_static_eta_events_w_store_cuisine as (
select time_bin, consumer_id, a.store_id, pct_within_quoted_plus_5_exceed_85, b.primary_tag_name, b.cuisine_type
from proddb.fionafan.ffs_static_eta_events_w_store a
left join edw.merchant.dimension_store b on a.store_id::varchar = b.store_id::varchar
);

select time_bin, consumer_id, store_id, count(1) cnt 
from proddb.fionafan.ffs_static_eta_events_w_store_cuisine group by all having cnt>1;

-- Flatten comma-separated columns and aggregate by tag/cuisine
create or replace table proddb.fionafan.ffs_cuisine_tag_consumer_store_counts as (
with flattened_tags as (
  select 
    consumer_id,
    store_id,
    pct_within_quoted_plus_5_exceed_85,
    trim(tag.value) as tag_value,
    'primary_tag' as tag_type
  from proddb.fionafan.ffs_static_eta_events_w_store_cuisine,
    lateral split_to_table(primary_tag_name, ',') as tag
  where primary_tag_name is not null
  
  union all
  
  select 
    consumer_id,
    store_id,
    pct_within_quoted_plus_5_exceed_85,
    trim(cuisine.value) as tag_value,
    'cuisine_type' as tag_type
  from proddb.fionafan.ffs_static_eta_events_w_store_cuisine,
    lateral split_to_table(cuisine_type, ',') as cuisine
  where cuisine_type is not null
)
select 
  tag_type,
  tag_value,
  count(distinct consumer_id) as distinct_consumers,
  count(distinct store_id) as distinct_stores,
  count(*) as total_records,
  count(distinct case when pct_within_quoted_plus_5_exceed_85 = true then consumer_id end) as distinct_consumers_exceed_85,
  count(distinct case when pct_within_quoted_plus_5_exceed_85 = true then store_id end) as distinct_stores_exceed_85,
  count(case when pct_within_quoted_plus_5_exceed_85 = true then 1 end) as total_records_exceed_85
from flattened_tags
group by all
order by distinct_consumers desc, distinct_stores desc
);

-- Preview results with fuzzy matching using JAROWINKLER_SIMILARITY (aggregated)
with matched_terms as (
  select 
    a.tag_value, 
    a.distinct_consumers, 
    a.distinct_stores, 
    a.total_records,
    a.distinct_consumers_exceed_85, 
    a.distinct_stores_exceed_85, 
    a.total_records_exceed_85,
    b.ordering, 
    b.search_term as matched_search_term,
    jarowinkler_similarity(lower(a.tag_value), lower(b.search_term)) as similarity_score
  from proddb.fionafan.ffs_cuisine_tag_consumer_store_counts a
  left join proddb.fionafan.search_terms b 
    on jarowinkler_similarity(lower(a.tag_value), lower(b.search_term)) >= 90
  where tag_type = 'primary_tag'
)
select 
  tag_value,
  listagg(matched_search_term || ' (' || similarity_score || ')', ', ') 
    within group (order by similarity_score desc) as matched_search_terms,
  listagg(ordering, ', ') within group (order by similarity_score desc) as orderings,
  distinct_consumers, 
  distinct_stores, 
  total_records,
  distinct_consumers_exceed_85, 
  distinct_stores_exceed_85, 
  total_records_exceed_85
from matched_terms
group by tag_value, distinct_consumers, distinct_stores, total_records,
  distinct_consumers_exceed_85, distinct_stores_exceed_85, total_records_exceed_85
order by distinct_stores desc
;
select count(distinct store_id), count(distinct case when pct_within_quoted_plus_5_exceed_85 = true then store_id end) 
from proddb.fionafan.ffs_static_eta_events_w_store_cuisine;

-- Aggregate to time_bin - consumer_id level
create or replace table proddb.fionafan.ffs_static_eta_events_consumer_agg as (
select 
  time_bin,
  consumer_id,
  count(*) as total_stores,
  -- Percentage of stores exceeding thresholds for pct_within_quoted_0
  avg(case when pct_within_quoted_0_exceed_85 then 1 else 0 end) as pct_stores_quoted_0_exceed_85,
  avg(case when pct_within_quoted_0_exceed_90 then 1 else 0 end) as pct_stores_quoted_0_exceed_90,
  avg(case when pct_within_quoted_0_exceed_95 then 1 else 0 end) as pct_stores_quoted_0_exceed_95,
  -- Percentage of stores exceeding thresholds for pct_within_quoted_plus_5
  avg(case when pct_within_quoted_plus_5_exceed_85 then 1 else 0 end) as pct_stores_quoted_plus_5_exceed_85,
  avg(case when pct_within_quoted_plus_5_exceed_90 then 1 else 0 end) as pct_stores_quoted_plus_5_exceed_90,
  avg(case when pct_within_quoted_plus_5_exceed_95 then 1 else 0 end) as pct_stores_quoted_plus_5_exceed_95,
  -- Percentage of stores exceeding thresholds for pct_within_quoted_plus_10
  avg(case when pct_within_quoted_plus_10_exceed_85 then 1 else 0 end) as pct_stores_quoted_plus_10_exceed_85,
  avg(case when pct_within_quoted_plus_10_exceed_90 then 1 else 0 end) as pct_stores_quoted_plus_10_exceed_90,
  avg(case when pct_within_quoted_plus_10_exceed_95 then 1 else 0 end) as pct_stores_quoted_plus_10_exceed_95,
  -- Percentage of stores exceeding thresholds for pct_within_quoted_plus_20
  avg(case when pct_within_quoted_plus_20_exceed_85 then 1 else 0 end) as pct_stores_quoted_plus_20_exceed_85,
  avg(case when pct_within_quoted_plus_20_exceed_90 then 1 else 0 end) as pct_stores_quoted_plus_20_exceed_90,
  avg(case when pct_within_quoted_plus_20_exceed_95 then 1 else 0 end) as pct_stores_quoted_plus_20_exceed_95
from proddb.fionafan.ffs_static_eta_events_w_store
group by all
);


grant all privileges on table proddb.fionafan.ffs_static_eta_events_consumer_agg to role public;

select 
--   time_bin,
  -- pct_within_quoted_0 metrics
  avg(pct_stores_quoted_0_exceed_85) as avg_pct_stores_quoted_0_exceed_85,
  avg(case when pct_stores_quoted_0_exceed_85 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_0_exceed_85_gte_03,
  avg(case when pct_stores_quoted_0_exceed_85 > 0 then 1 else 0 end) as pct_consumer_quoted_0_exceed_85_gt_0,
  avg(pct_stores_quoted_0_exceed_90) as avg_pct_stores_quoted_0_exceed_90,
  avg(case when pct_stores_quoted_0_exceed_90 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_0_exceed_90_gte_03,
  avg(case when pct_stores_quoted_0_exceed_90 > 0 then 1 else 0 end) as pct_consumer_quoted_0_exceed_90_gt_0,
--   avg(pct_stores_quoted_0_exceed_95) as avg_pct_stores_quoted_0_exceed_95,
--   avg(case when pct_stores_quoted_0_exceed_95 < 0.5 then 1 else 0 end) as pct_consumer_quoted_0_exceed_95_lt_05,
  -- pct_within_quoted_plus_5 metrics
  avg(pct_stores_quoted_plus_5_exceed_85) as avg_pct_stores_quoted_plus_5_exceed_85,
  avg(case when pct_stores_quoted_plus_5_exceed_85 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_5_exceed_85_gte_03,
  avg(case when pct_stores_quoted_plus_5_exceed_85 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_5_exceed_85_gt_0,
  avg(pct_stores_quoted_plus_5_exceed_90) as avg_pct_stores_quoted_plus_5_exceed_90,
  avg(case when pct_stores_quoted_plus_5_exceed_90 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_5_exceed_90_gte_03,
  avg(case when pct_stores_quoted_plus_5_exceed_90 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_5_exceed_90_gt_0,
--   avg(pct_stores_quoted_plus_5_exceed_95) as avg_pct_stores_quoted_plus_5_exceed_95,
--   avg(case when pct_stores_quoted_plus_5_exceed_95 < 0.5 then 1 else 0 end) as pct_consumer_quoted_plus_5_exceed_95_lt_05,
  -- pct_within_quoted_plus_10 metrics
  avg(pct_stores_quoted_plus_10_exceed_85) as avg_pct_stores_quoted_plus_10_exceed_85,
  avg(case when pct_stores_quoted_plus_10_exceed_85 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_10_exceed_85_gte_03,
  avg(case when pct_stores_quoted_plus_10_exceed_85 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_10_exceed_85_gt_0,
  avg(pct_stores_quoted_plus_10_exceed_90) as avg_pct_stores_quoted_plus_10_exceed_90,
  avg(case when pct_stores_quoted_plus_10_exceed_90 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_10_exceed_90_gte_03,
  avg(case when pct_stores_quoted_plus_10_exceed_90 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_10_exceed_90_gt_0,
--   avg(pct_stores_quoted_plus_10_exceed_95) as avg_pct_stores_quoted_plus_10_exceed_95,
--   avg(case when pct_stores_quoted_plus_10_exceed_95 < 0.5 then 1 else 0 end) as pct_consumer_quoted_plus_10_exceed_95_lt_05,
  -- pct_within_quoted_plus_20 metrics
  avg(pct_stores_quoted_plus_20_exceed_85) as avg_pct_stores_quoted_plus_20_exceed_85,
  avg(case when pct_stores_quoted_plus_20_exceed_85 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_20_exceed_85_gte_03,
  avg(case when pct_stores_quoted_plus_20_exceed_85 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_20_exceed_85_gt_0,
  avg(pct_stores_quoted_plus_20_exceed_90) as avg_pct_stores_quoted_plus_20_exceed_90,
  avg(case when pct_stores_quoted_plus_20_exceed_90 >= 0.3 then 1 else 0 end) as pct_consumer_quoted_plus_20_exceed_90_gte_03,
  avg(case when pct_stores_quoted_plus_20_exceed_90 > 0 then 1 else 0 end) as pct_consumer_quoted_plus_20_exceed_90_gt_0
--   avg(pct_stores_quoted_plus_20_exceed_95) as avg_pct_stores_quoted_plus_20_exceed_95,
--   avg(case when pct_stores_quoted_plus_20_exceed_95 < 0.5 then 1 else 0 end) as pct_consumer_quoted_plus_20_exceed_95_lt_05
from proddb.fionafan.ffs_static_eta_events_consumer_agg
group by all;

-- Create search terms table
CREATE OR REPLACE TABLE proddb.fionafan.search_terms AS
SELECT * FROM (
  SELECT 1 AS ordering, 'fast food' AS search_term, 62621659 AS search_sessions UNION ALL
  SELECT 2, 'pizza', 33150320 UNION ALL
  SELECT 3, 'breakfast', 25417133 UNION ALL
  SELECT 4, 'burgers', 23726661 UNION ALL
  SELECT 5, 'mexican', 23952493 UNION ALL
  SELECT 6, 'chicken', 19382966 UNION ALL
  SELECT 7, 'desserts', 16590060 UNION ALL
  SELECT 8, 'chinese', 18925214 UNION ALL
  SELECT 9, 'healthy', 15912768 UNION ALL
  SELECT 10, 'sandwiches', 12034670 UNION ALL
  SELECT 11, 'comfort food', 10833441 UNION ALL
  SELECT 12, 'coffee', 7316016 UNION ALL
  SELECT 13, 'sushi', 7133486 UNION ALL
  SELECT 14, 'asian', 6253811 UNION ALL
  SELECT 15, 'italian', 5151465 UNION ALL
  SELECT 16, 'seafood', 3985465 UNION ALL
  SELECT 17, 'indian', 3558474 UNION ALL
  SELECT 18, 'steak', 3555529 UNION ALL
  SELECT 19, 'soup', 3535883 UNION ALL
  SELECT 20, 'thai', 3612238 UNION ALL
  SELECT 21, 'barbecue', 1962027 UNION ALL
  SELECT 22, 'salad', 1807113 UNION ALL
  SELECT 23, 'ramen', 2302550 UNION ALL
  SELECT 24, 'vegan', 1349364 UNION ALL
  SELECT 25, 'pho', 1792534 UNION ALL
  SELECT 26, 'japanese', 1760403 UNION ALL
  SELECT 27, 'noodles', 1405550 UNION ALL
  SELECT 28, 'bubble tea', 1093204 UNION ALL
  SELECT 29, 'smoothie', 1121964 UNION ALL
  SELECT 30, 'american', 1233694 UNION ALL
  SELECT 31, 'korean', 1159493 UNION ALL
  SELECT 32, 'greek', 983495 UNION ALL
  SELECT 33, 'southern', 959016 UNION ALL
  SELECT 34, 'halal', 878305 UNION ALL
  SELECT 35, 'poke', 729064 UNION ALL
  SELECT 36, 'bakery', 596908 UNION ALL
  SELECT 37, 'latin american', 506611 UNION ALL
  SELECT 38, 'vietnamese', 505632 UNION ALL
  SELECT 39, 'middle eastern', 412650 UNION ALL
  SELECT 40, 'filipino', 337363 UNION ALL
  SELECT 41, 'african', 371140 UNION ALL
  SELECT 42, 'spanish', 319082 UNION ALL
  SELECT 43, 'brazilian', 185115 UNION ALL
  SELECT 44, 'gastropubs', 199811 UNION ALL
  SELECT 45, 'peruvian', 132549 UNION ALL
  SELECT 46, 'pakistani', 158705 UNION ALL
  SELECT 47, 'german', 145763 UNION ALL
  SELECT 48, 'russian', 170403 UNION ALL
  SELECT 49, 'british', 128134 UNION ALL
  SELECT 50, 'european', 106898 UNION ALL
  SELECT 51, 'belgian', 107452 UNION ALL
  SELECT 52, 'ethiopian', 79604 UNION ALL
  SELECT 53, 'french', 65730 UNION ALL
  SELECT 54, 'poutineries', 70798 UNION ALL
  SELECT 55, 'burmese', 75940 UNION ALL
  SELECT 56, 'snacks', 165920 UNION ALL
  SELECT 57, 'flowers', 75229 UNION ALL
  SELECT 58, 'irish', 44044 UNION ALL
  SELECT 59, 'argentine', 37395 UNION ALL
  SELECT 60, 'tapas', 43624 UNION ALL
  SELECT 61, 'australian', 96568 UNION ALL
  SELECT 62, 'drinks', 212400 UNION ALL
  SELECT 63, 'kosher', 29664 UNION ALL
  SELECT 64, 'convenience', 7621 UNION ALL
  SELECT 65, 'dessert', 505 UNION ALL
  SELECT 66, 'burger', 418 UNION ALL
  SELECT 67, 'sandwich', 341 UNION ALL
  SELECT 68, 'pasta', 135 UNION ALL
  SELECT 69, 'cake', 130 UNION ALL
  SELECT 70, 'donut', 113 UNION ALL
  SELECT 71, 'cookie', 105 UNION ALL
  SELECT 72, 'ice cream', 92 UNION ALL
  SELECT 73, 'milkshake', 74 UNION ALL
  SELECT 74, 'chicken wings', 77 UNION ALL
  SELECT 75, 'taco', 83 UNION ALL
  SELECT 76, 'burrito', 75 UNION ALL
  SELECT 77, 'wine', 8822 UNION ALL
  SELECT 78, 'cheesecake', 46 UNION ALL
  SELECT 79, 'waffle', 47 UNION ALL
  SELECT 80, 'wings', 47 UNION ALL
  SELECT 81, 'bagel', 37 UNION ALL
  SELECT 82, 'acai bowl', 30 UNION ALL
  SELECT 83, 'breakfast burrito', 19 UNION ALL
  SELECT 84, 'shrimp', 19 UNION ALL
  SELECT 85, 'sonic', 34 UNION ALL
  SELECT 86, 'cheesesteak', 33 UNION ALL
  SELECT 87, 'nachos', 27 UNION ALL
  SELECT 88, 'fried chicken', 25 UNION ALL
  SELECT 89, 'food', 26 UNION ALL
  SELECT 90, 'birria', 23 UNION ALL
  SELECT 91, 'soul food', 23 UNION ALL
  SELECT 92, 'tacos', 25 UNION ALL
  SELECT 93, 'enchilada', 22 UNION ALL
  SELECT 94, 'fried rice', 20 UNION ALL
  SELECT 95, 'bbq', 26 UNION ALL
  SELECT 96, 'submarine sandwich', 18 UNION ALL
  SELECT 97, 'pancake', 20 UNION ALL
  SELECT 98, 'club sandwich', 17 UNION ALL
  SELECT 99, 'cupcake', 17 UNION ALL
  SELECT 100, 'dumplings', 14 UNION ALL
  SELECT 101, 'brownie', 15 UNION ALL
  SELECT 102, 'breakfast sandwich', 13 UNION ALL
  SELECT 103, 'donuts', 30 UNION ALL
  SELECT 104, 'olive garden', 63 UNION ALL
  SELECT 105, 'waffle house', 39 UNION ALL
  SELECT 106, 'taco bell', 55 UNION ALL
  SELECT 107, 'chicken noodle soup', 16 UNION ALL
  SELECT 108, 'biryani', 15 UNION ALL
  SELECT 109, 'starbucks', 22 UNION ALL
  SELECT 110, 'blt', 13 UNION ALL
  SELECT 111, 'chilis', 13 UNION ALL
  SELECT 112, 'gyro', 11 UNION ALL
  SELECT 113, 'quesadilla', 17 UNION ALL
  SELECT 114, 'fajita', 15 UNION ALL
  SELECT 115, 'frappe', 11 UNION ALL
  SELECT 116, 'boba', 18 UNION ALL
  SELECT 117, 'fish', 19 UNION ALL
  SELECT 118, 'fries', 9 UNION ALL
  SELECT 119, 'kfc', 18 UNION ALL
  SELECT 120, 'fried fish', 8 UNION ALL
  SELECT 121, 'pad thai', 12 UNION ALL
  SELECT 122, 'fish and chips', 11 UNION ALL
  SELECT 123, 'curry', 10 UNION ALL
  SELECT 124, 'chicken sandwich', 10 UNION ALL
  SELECT 125, 'chicken nuggets', 8 UNION ALL
  SELECT 126, 'mcdonald''s', 35 UNION ALL
  SELECT 127, 'lunch', 8 UNION ALL
  SELECT 128, 'alcohol', 48 UNION ALL
  SELECT 129, 'greek salad', 9 UNION ALL
  SELECT 130, 'panini', 9 UNION ALL
  SELECT 131, 'chicken parmesan', 10 UNION ALL
  SELECT 132, 'pizza hut', 27 UNION ALL
  SELECT 133, 'dairy queen', 12 UNION ALL
  SELECT 134, 'burger king', 18 UNION ALL
  SELECT 135, 'cook out', 8 UNION ALL
  SELECT 136, 'thai curry', 6 UNION ALL
  SELECT 137, 'general tso''s chicken', 7 UNION ALL
  SELECT 138, 'pancakes', 7 UNION ALL
  SELECT 139, 'margarita', 8 UNION ALL
  SELECT 140, 'asian soup', 6 UNION ALL
  SELECT 141, 'orange chicken', 7 UNION ALL
  SELECT 142, 'jack in the box', 6 UNION ALL
  SELECT 143, 'hummus', 8 UNION ALL
  SELECT 144, 'chocolate', 6 UNION ALL
  SELECT 145, 'cheese curds', 5 UNION ALL
  SELECT 146, 'wendys', 22 UNION ALL
  SELECT 147, 'panera', 13 UNION ALL
  SELECT 148, 'brussel sprouts', 4 UNION ALL
  SELECT 149, 'shawarma', 5 UNION ALL
  SELECT 150, 'alfredo', 6 UNION ALL
  SELECT 151, 'udon', 7 UNION ALL
  SELECT 152, 'hash browns', 4 UNION ALL
  SELECT 153, 'macchiato', 6 UNION ALL
  SELECT 154, 'subs', 8 UNION ALL
  SELECT 155, 'cupcakes', 6 UNION ALL
  SELECT 156, 'fruit', 6 UNION ALL
  SELECT 157, 'sub', 7 UNION ALL
  SELECT 158, 'queso', 5 UNION ALL
  SELECT 159, 'thai soup', 8 UNION ALL
  SELECT 160, 'tomato soup', 9 UNION ALL
  SELECT 161, 'korean fried chicken', 5 UNION ALL
  SELECT 162, 'cookies', 9 UNION ALL
  SELECT 163, 'dunkin', 9 UNION ALL
  SELECT 164, 'loaded fries', 4 UNION ALL
  SELECT 165, 'gyros', 3 UNION ALL
  SELECT 166, 'spam', 3 UNION ALL
  SELECT 167, 'cauliflower', 3 UNION ALL
  SELECT 168, 'restaurants', 6 UNION ALL
  SELECT 169, 'deals', 8 UNION ALL
  SELECT 170, 'chick', 4 UNION ALL
  SELECT 171, 'milk tea', 5 UNION ALL
  SELECT 172, 'vegetarian', 2333 UNION ALL
  SELECT 173, 'cafe', 4 UNION ALL
  SELECT 174, 'latte', 6 UNION ALL
  SELECT 175, 'thai fried rice', 3
);
