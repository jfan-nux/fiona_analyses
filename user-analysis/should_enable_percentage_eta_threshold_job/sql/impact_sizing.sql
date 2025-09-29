-- Impact sizing: iOS exposures by lifestage
-- Sources:
--   - METRICS_REPO.PUBLIC.should_enable_percentage_eta_threshold_job_ios_exposures
--   - edw.growth.consumer_growth_accounting_scd3
-- Outputs (materialized under proddb.fionafan):
--   - proddb.fionafan.eta_threshold_ios_exposures_consumer
--   - proddb.fionafan.eta_threshold_ios_exposures_lifestage_sizing

-- Consumer-level iOS exposures joined to SCD lifestage at exposure date
-- select count(1) from METRICS_REPO.PUBLIC.should_enable_percentage_eta_threshold_job_ios_exposures;
WITH exposure AS
(SELECT  ee.tag
               , ee.result
               , ee.bucket_key
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
               , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) EXPOSURE_TIME
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = 'should_enable_percentage_eta_threshold_job'
AND experiment_version::INT = 5
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN '2025-09-10' AND '2025-10-30'
GROUP BY all
)


create or replace table proddb.fionafan.eta_threshold_ios_exposures_consumer as


with raw as (
    select
        bucket_key as consumer_id,
        experiment_version,
        experiment_group,
        segment,
        first_exposure_time,
        first_exposure_time::date as exposure_date
    from METRICS_REPO.PUBLIC.should_enable_percentage_eta_threshold_job_ios_exposures
    -- where segment = 'iOS Users'
)
, dedup as (
    select
        consumer_id,
        experiment_version,
        experiment_group,
        segment,
        first_exposure_time,
        exposure_date,
        row_number() over (
            partition by consumer_id
            order by first_exposure_time asc
        ) as rn
    from raw
)
, first_exposure as (
    select
        consumer_id,
        experiment_version,
        experiment_group,
        segment,
        first_exposure_time,
        exposure_date
    from dedup
    where rn = 1
)
select
    fe.consumer_id,
    fe.experiment_version,
    fe.experiment_group,
    fe.segment,
    fe.first_exposure_time,
    fe.exposure_date,
    s.lifestage
from first_exposure fe
left join edw.growth.consumer_growth_accounting_scd3 s
  on s.consumer_id = fe.consumer_id
 and fe.exposure_date >= s.scd_start_date
 and (s.scd_end_date is null or fe.exposure_date <= s.scd_end_date)
;


-- Lifestage at time of first session per consumer (global first session)
create or replace table proddb.fionafan.consumer_first_session_lifestage as
with first_session as (
    select
        user_id::string as consumer_id,
        min(start_ts) as first_session_start_ts,
        min(start_ts)::date as first_session_date
    from proddb.tyleranderson.sessions
    where  event_date between '2025-07-01'::date and '2025-07-31'::date
    and user_id is not null
    group by all
)
select
    fs.consumer_id,
    fs.first_session_start_ts,
    fs.first_session_date,
    s.lifestage as lifestage_at_first_session
from first_session fs
left join edw.growth.consumer_growth_accounting_scd3 s
  on s.consumer_id::string = fs.consumer_id
  and s.consumer_id is not null
 and fs.first_session_date >= s.scd_start_date
 and (s.scd_end_date is null or fs.first_session_date <= s.scd_end_date)
;

create or replace table proddb.fionafan.consumer_first_session_2024 as
with first_session as (
    select
        user_id::string as consumer_id,
        min(start_ts) as first_session_start_ts,
        min(start_ts)::date as first_session_date
    from proddb.tyleranderson.sessions
    where  event_date between '2024-07-01'::date and '2024-07-31'::date
    and user_id is not null
    group by all
)
select
    fs.consumer_id,
    fs.first_session_start_ts,
    fs.first_session_date,
    s.lifestage as lifestage_at_first_session
from first_session fs
left join edw.growth.consumer_growth_accounting_scd3 s
  on s.consumer_id::string = fs.consumer_id
  and s.consumer_id is not null
 and fs.first_session_date >= s.scd_start_date
 and (s.scd_end_date is null or fs.first_session_date <= s.scd_end_date)
;

CREATE OR REPLACE TABLE proddb.fionafan.consumer_day_orders_20250701_20250901 AS
WITH deliveries AS (
    SELECT
        dd.creator_id AS consumer_id,
        dd.active_date AS order_date,
        dd.delivery_id
    FROM proddb.public.dimension_deliveries dd
    WHERE dd.is_filtered_core = TRUE
      AND dd.active_date BETWEEN '2025-07-01'::date AND '2025-09-01'::date
)
SELECT
    consumer_id,
    order_date,
    COUNT(DISTINCT delivery_id) AS orders_on_day
FROM deliveries
GROUP BY all
ORDER BY consumer_id, order_date;
CREATE OR REPLACE TABLE proddb.fionafan.consumer_day_orders_20240701_20250901 AS

WITH deliveries AS (
    SELECT
        dd.creator_id AS consumer_id,
        dd.active_date AS order_date,
        dd.delivery_id
    FROM proddb.public.dimension_deliveries dd
    WHERE dd.is_filtered_core = TRUE
      AND dd.active_date BETWEEN '2024-07-01'::date AND '2025-09-01'::date
)
SELECT
    consumer_id,
    order_date,
    COUNT(DISTINCT delivery_id) AS orders_on_day
FROM deliveries
GROUP BY all
ORDER BY consumer_id, order_date;

-- 28d conversion after first session by lifestage
create or replace table proddb.fionafan.first_session_lifestage_conv28d as
with base as (
    select
        c.consumer_id,
        c.first_session_date,
        c.lifestage_at_first_session as lifestage
    from proddb.fionafan.consumer_first_session_lifestage c
)
, orders_in_28d as (
    select
        b.consumer_id,
        min(case when o.order_date >= b.first_session_date
                  and o.order_date < dateadd(day, 28, b.first_session_date)
                  then 1 else 0 end) as has_order_28d,
    sum( case when o.order_date >= b.first_session_date
                  and o.order_date < dateadd(day, 28, b.first_session_date)
                  then orders_on_day else 0 end) as order_cnt_28d
    from base b
    inner join proddb.fionafan.consumer_day_orders_20250701_20250901 o
      on o.consumer_id::string = b.consumer_id::string
     and o.order_date >= b.first_session_date
     and o.order_date < dateadd(day, 28, b.first_session_date)
    group by all
)
select b.*, coalesce(o.has_order_28d, 0) as has_order_28d, coalesce(o.order_cnt_28d, 0) as order_cnt_28d
from base b left join orders_in_28d o on o.consumer_id::string = b.consumer_id::string;

select
    lifestage,
    count(distinct consumer_id) as users,
    count(distinct case when has_order_28d = 1 then consumer_id end) as converters_28d,
    count(distinct case when has_order_28d = 1 then consumer_id end) / nullif(count(distinct consumer_id), 0)::float as conv_28d,
    sum( case when has_order_28d = 1 then order_cnt_28d end) / nullif(count(distinct case when has_order_28d = 1 then consumer_id end), 0)::float as ord_freq_28d
from proddb.fionafan.first_session_lifestage_conv28d

group by lifestage
order by conv_28d desc;





-- Consumer-level retention after first session (2024 cohort): 28d and 2–12 months
create or replace table proddb.fionafan.consumer_first_session_2024_retention_flags as
with base as (
  select consumer_id, first_session_date, lifestage_at_first_session as lifestage
  from proddb.fionafan.consumer_first_session_2024
),
orders as (
  select o.consumer_id::string as consumer_id, o.order_date
  from proddb.fionafan.consumer_day_orders_20240701_20250901 o
  join base b
    on o.consumer_id::string = b.consumer_id
   and o.order_date >= b.first_session_date
   and o.order_date < dateadd(month, 12, b.first_session_date)
)
select
  b.consumer_id,
  b.lifestage,
  b.first_session_date,
  -- 28d window
  max(case when o.order_date >= b.first_session_date
             and o.order_date < dateadd(day, 28, b.first_session_date)
           then 1 else 0 end) as r_28d,
  -- month-by-month, non-cumulative
  max(case when o.order_date >= b.first_session_date
             and o.order_date < dateadd(month, 1, b.first_session_date)
           then 1 else 0 end) as r_1mo,
  max(case when o.order_date >= dateadd(month, 1, b.first_session_date)
             and o.order_date < dateadd(month, 2, b.first_session_date)
           then 1 else 0 end) as r_2mo,
  max(case when o.order_date >= dateadd(month, 2, b.first_session_date)
             and o.order_date < dateadd(month, 3, b.first_session_date)
           then 1 else 0 end) as r_3mo,
  max(case when o.order_date >= dateadd(month, 3, b.first_session_date)
             and o.order_date < dateadd(month, 4, b.first_session_date)
           then 1 else 0 end) as r_4mo,
  max(case when o.order_date >= dateadd(month, 4, b.first_session_date)
             and o.order_date < dateadd(month, 5, b.first_session_date)
           then 1 else 0 end) as r_5mo,
  max(case when o.order_date >= dateadd(month, 5, b.first_session_date)
             and o.order_date < dateadd(month, 6, b.first_session_date)
           then 1 else 0 end) as r_6mo,
  max(case when o.order_date >= dateadd(month, 6, b.first_session_date)
             and o.order_date < dateadd(month, 7, b.first_session_date)
           then 1 else 0 end) as r_7mo,
  max(case when o.order_date >= dateadd(month, 7, b.first_session_date)
             and o.order_date < dateadd(month, 8, b.first_session_date)
           then 1 else 0 end) as r_8mo,
  max(case when o.order_date >= dateadd(month, 8, b.first_session_date)
             and o.order_date < dateadd(month, 9, b.first_session_date)
           then 1 else 0 end) as r_9mo,
  max(case when o.order_date >= dateadd(month, 9, b.first_session_date)
             and o.order_date < dateadd(month, 10, b.first_session_date)
           then 1 else 0 end) as r_10mo,
  max(case when o.order_date >= dateadd(month, 10, b.first_session_date)
             and o.order_date < dateadd(month, 11, b.first_session_date)
           then 1 else 0 end) as r_11mo,
  max(case when o.order_date >= dateadd(month, 11, b.first_session_date)
             and o.order_date < dateadd(month, 12, b.first_session_date)
           then 1 else 0 end) as r_12mo
from base b
left join orders o on o.consumer_id = b.consumer_id
group by all;

-- Aggregated retention by lifestage (2024 cohort)
-- create or replace table proddb.fionafan.consumer_first_session_2024_retention_by_lifestage as
select
    lifestage,
    count(distinct consumer_id) as users,
    avg(r_28d) as r_28d,
    avg(r_2mo) as r_2mo,
    avg(r_3mo) as r_3mo,
    avg(r_4mo) as r_4mo,
    avg(r_5mo) as r_5mo,
    avg(r_6mo) as r_6mo,
    avg(r_7mo) as r_7mo,
    avg(r_8mo) as r_8mo,
    avg(r_9mo) as r_9mo,
    avg(r_10mo) as r_10mo,
    avg(r_11mo) as r_11mo,
    avg(r_12mo) as r_12mo
from proddb.fionafan.consumer_first_session_2024_retention_flags
where r_28d = 1
group by all
order by all;



create or replace table proddb.fionafan.eta_threshold_ios_exposures_consumer_null as
with  first_exposure as (
    select * from proddb.fionafan.eta_threshold_ios_exposures_consumer where lifestage is null
)
select
    fe.consumer_id,
    fe.experiment_version,
    fe.experiment_group,
    fe.segment,
    fe.first_exposure_time,
    fe.exposure_date,
    s.lifestage
from first_exposure fe
left join edw.growth.consumer_growth_accounting_scd3 s
  on s.consumer_id = fe.consumer_id
 and fe.exposure_date+ interval '24 hour' >= s.scd_start_date
 and (s.scd_end_date is null or fe.exposure_date+ interval '24 hour' <= s.scd_end_date)
;


-- Lifestage sizing for exposed iOS consumers with percentages
with agg as (
select
    lifestage,
    count(distinct consumer_id) as exposed_consumers,
    count(*) as exposure_rows
from proddb.fionafan.eta_threshold_ios_exposures_consumer
where lifestage is not null
group by all
)
select
    lifestage,
    exposed_consumers,
    -- exposure_rows,
    exposed_consumers / nullif(sum(exposed_consumers) over (), 0)::float as pct_exposed_consumers,
    -- exposure_rows / nullif(sum(exposure_rows) over (), 0)::float as pct_exposure_rows
from agg
order by all;
select * from proddb.fionafan.eta_threshold_ios_exposures_consumer_null
where lifestage is null limit 10;

select * from edw.growth.consumer_growth_accounting_scd3  a 
inner join proddb.fionafan.eta_threshold_ios_exposures_consumer_null b
on a.consumer_id = b.consumer_id and b.first_exposure_time >= a.scd_start_date 
and (b.first_exposure_time <= a.scd_end_date or a.scd_end_date is null)
where b.lifestage is null ;

limit 10;
select count(distinct b.consumer_id) from proddb.fionafan.consumer_day_orders_20240630_20250930  a
inner join proddb.fionafan.eta_threshold_ios_exposures_consumer_null b
on a.consumer_id = b.consumer_id
where b.lifestage is null limit 10;
select count(distinct consumer_id) from proddb.fionafan.eta_threshold_ios_exposures_consumer_null;

create or replace table proddb.fionafan.eta_threshold_ios_exposures_consumer_null_days_before_exposure as (
select a.*,created_at, datediff('day', created_at, first_exposure_time::date) as days_before_exposure, is_guest
from proddb.fionafan.eta_threshold_ios_exposures_consumer_null a
inner join dimension_consumer b
on a.consumer_id = b.user_id
where a.lifestage is null
);

select days_before_exposure, count(distinct consumer_id) 
from proddb.fionafan.eta_threshold_ios_exposures_consumer_null_days_before_exposure
group by all
order by days_before_exposure;

select case when days_before_exposure <0 then 'before_exposure' else 'after_exposure' end as tenure_vs_exposure, is_guest,count(1) 
from proddb.fionafan.eta_threshold_ios_exposures_consumer_null_days_before_exposure  group by all order by all;

select case when days_before_exposure between 0 and 7 then '0-7 days'
when days_before_exposure between 8 and 30 then '8-30 days'
when days_before_exposure between 31 and 90 then '31-90 days'
when days_before_exposure between 91 and 180 then '91-180 days'
when days_before_exposure between 181 and 365 then '181-365 days'
when days_before_exposure > 365 then 'more_than_365_days'
else 'other' end as tenure_vs_exposure, is_guest,count(1) 
from proddb.fionafan.eta_threshold_ios_exposures_consumer_null_days_before_exposure where days_before_exposure is not null group by all order by all;

select * from dimension_consumer where user_id = '1125900356179353' limit 10;

select * from 
select * from proddb.fionafan.eta_threshold_ios_exposures_consumer where consumer_id = '1125900362944067' limit 10;