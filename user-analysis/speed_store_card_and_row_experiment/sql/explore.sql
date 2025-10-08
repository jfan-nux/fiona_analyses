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
 

select experiment_group, other_properties:speed_type as speed_type, count(distinct store_id), count(1) cnt, avg(vertical_position) as avg_vertical_position
from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where page = 'explore_page'
group by 1, 2 order by 2 desc;
select CONTAINER, count(distinct store_id) store_cnt, count(1) cnt from proddb.fionafan.speed_store_card_and_row_experiment_m_card_click_events_store 
where other_properties:speed_type is  null and page = 'explore_page' group by all order by store_cnt desc;


create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores as (
with base as (
select  store_id, case when other_properties:speed_type in ('FASTEST','FAST') then 1 else 0 end as is_fast, count(1) cnt
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

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_fast_Restaurants as (
select * from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where speed_type in ('FASTEST','FAST') 
 and speed_type_pct>0.08
);

select count(distinct store_id) from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 0 and speed_type_pct>0.9;

create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_fast_Restaurants as (
select * from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 1 and speed_type_pct>0.1
);


create or replace table proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores_rx_non_fast_Restaurants as (
select * from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores where is_fast = 0 and speed_type_pct>0.9
);

-- Restaurant percentiles based on ETA (created_at to actual_delivery_time) for ASAP orders
with restaurant_eta as (
    select
        d.store_id,
        s.name as store_name,
        count(distinct d.delivery_id) as total_deliveries,
        avg(datediff('minute', d.created_at, d.actual_delivery_time)) as avg_eta_minutes,
        median(datediff('minute', d.created_at, d.actual_delivery_time)) as median_eta_minutes,
        percentile_cont(0.25) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p25_eta_minutes,
        percentile_cont(0.75) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p75_eta_minutes,
        percentile_cont(0.90) within group (order by datediff('minute', d.created_at, d.actual_delivery_time)) as p90_eta_minutes
    from dimension_deliveries d
    inner join dimension_stores s
        on d.store_id = s.id and s.is_restaurant = 1
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
    percentile_bucket,
    decile,
    count(distinct store_id) as num_restaurants,
    avg(total_deliveries) as avg_deliveries_per_restaurant,
    avg(avg_eta_minutes) as avg_eta_minutes,
    min(avg_eta_minutes) as min_eta_minutes,
    max(avg_eta_minutes) as max_eta_minutes,
    avg(median_eta_minutes) as avg_median_eta_minutes,
    avg(p25_eta_minutes) as avg_p25_eta_minutes,
    avg(p75_eta_minutes) as avg_p75_eta_minutes,
    avg(p90_eta_minutes) as avg_p90_eta_minutes
from restaurant_percentiles
group by 1, 2
order by percentile_bucket;
