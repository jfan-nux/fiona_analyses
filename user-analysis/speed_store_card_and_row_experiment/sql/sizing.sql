
create or replace table proddb.fionafan.consumer_first_session_2024 as
with first_session as (
    select
        user_id::string as consumer_id,
        min(event_date) as first_session_start_ts,
        min(event_date)::date as first_session_date
    from proddb.public.fact_unique_visitors_full_pt
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
create or replace table proddb.fionafan.consumer_first_session_lifestage as
with first_session as (
    select
        user_id::string as consumer_id,
        min(event_date) as first_session_start_ts,
        min(event_date)::date as first_session_date
    from proddb.public.fact_unique_visitors_full_pt
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
    inner join proddb.fionafan.consumer_day_orders_20240701_20250901 o
      on o.consumer_id::string = b.consumer_id::string
      AND o.order_date between '2025-07-01'::date and '2025-09-01'::date
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
create or replace table proddb.fionafan.consumer_july_2024_retention_flags as
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


SELECT 
  a.dimension_value AS Lifestage,
  a.exposures AS Exposures,
  a.metric_impact_absolute AS Impact_Abs
FROM dimension_experiment_analysis_results a 

INNER JOIN 
dimension_experiment_analyses b 
ON a.analysis_name=b.analysis_name
WHERE TRUE 
AND metric_name='consumers_mau'
AND dimension_name='cx_lifestage'
AND variant_name = 'treatment1'
AND dimension_value IS NOT NULL
AND metric_impact_absolute IS NOT NULL
AND b.health_check_result_detailed:imbalance::VARCHAR='PASSED'
AND b.health_check_result_detailed:flicker::VARCHAR='PASSED'
AND a.analysis_name='speed_store_card_and_row_experiment__ios_Users'
ORDER BY dimension_value;