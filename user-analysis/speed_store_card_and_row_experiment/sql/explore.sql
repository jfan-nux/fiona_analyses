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