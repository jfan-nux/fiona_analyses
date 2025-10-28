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
