-- Fastest Near You (FNY) views for experiment population and post-exposure order rates
-- Sources:
--   - segment_events_raw.consumer_production.m_card_view
--   - proddb.fionafan.eta_threshold_ios_exposures_consumer (experiment population; first_exposure_time)
--   - proddb.public.dimension_store (store → submarket)
--   - proddb.luwang.market_tier (submarket → tier_level)
--   - proddb.public.dimension_deliveries (orders; used for post-exposure order rates)

-- Identify FNY stores (optional helper; not required for main view extraction)
select event_date, count(distinct consumer_id) consumer_cnt, avg(vertical_position ) vertical_position
from iguazu.consumer.m_card_view
where 1=1
and timestamp >= '2025-06-10'::date
and vertical_position is not null
and container_name ILIKE 'fastest near you'
group by all
;
-- and ETA is not null
-- and (REGEXP_LIKE(badges, '(^|\W)\d+\s*min?(\W|$)', 'i')) --badges ilike '%fast%' or 
-- and badges = '5 min'
-- and store_id is not null and item_id is null
;
select experiment_group, count(1) views, count(distinct consumer_id) 
from proddb.fionafan.eta_threshold_ios_exposures_consumer group by all;

create or replace table proddb.fionafan.fny_views_experiment as


with exposures as (
    select
        consumer_id,
        experiment_version,
        experiment_group,
        segment,
        first_exposure_time,
        exposure_date,
        lifestage
    from proddb.fionafan.eta_threshold_ios_exposures_consumer
)
, fny_views_raw as (
    select
        v.timestamp as view_ts,
        v.CONTEXT_TIMEZONE,
        v.event_date as view_event_date,
        v.user_id as consumer_id,
        v.dd_device_id,
        -- v.platform,
        v.store_id,
        v.container_name, 
        v.badges,
        v.card_position,
        v.position,
        v.vertical_position,
        v.eta,
        v.delivery_eta,
        v.eta_icon,
        v.QUERY,
        v.TILE_NAME,
        v.CAROUSEL_ID,
        v.DESCRIPTION,
        v.STORE_DISPLAY_ASAP_TIME,
        v.asap_time
        -- v.OTHER_PROPERTIES:speed_type as speed_type
    from iguazu.consumer.m_card_view v
    where 1=1
    and v.timestamp >= '2025-09-10'::date - 1
    -- and v.container_name ilike 'fastest near you'
    -- and v.badges in ('fastest near you', 'all stores')
      and v.store_id is not null
      and v.user_id is not null
)
, fny_views_with_exposure as (
    select
        e.consumer_id,
        e.experiment_version,
        e.experiment_group,
        e.segment,
        e.first_exposure_time,
        e.exposure_date,
        e.lifestage,
        r.view_ts,
        r.CONTEXT_TIMEZONE,
        r.view_event_date,
        r.dd_device_id,
        -- r.platform,
        r.store_id,
        r.badges,
        r.eta,
        r.delivery_eta,
        r.eta_icon,
        r.container_name,
        r.card_position,
        r.position,
        r.vertical_position,
        r.QUERY,
        r.TILE_NAME,
        r.CAROUSEL_ID,
        r.DESCRIPTION,
        r.STORE_DISPLAY_ASAP_TIME,
        r.asap_time
        -- r.speed_type
    from exposures e
    join fny_views_raw r
      on r.consumer_id = e.consumer_id
     and r.view_ts >= e.first_exposure_time  -- only views after exposure
)
, views_with_market_tier as (
    select
        f.*,
        ds.submarket_id,
        t.tier_level as market_tier
    from fny_views_with_exposure f
    left join proddb.public.dimension_store ds
      on ds.store_id = f.store_id
    left join proddb.luwang.market_tier t
      on t.submarket_id = ds.submarket_id
)
-- Materialize view-level table with all exposure attributes retained
select * from views_with_market_tier;


select container_name, experiment_group, avg(vertical_position), count(distinct consumer_id) from proddb.fionafan.fny_views_experiment where container_name ILIKE 'fastest near you' group by all;

select * from proddb.fionafan.fny_views_experiment where consumer_id = '1125900347207576' limit 10;
select store_display_asap_time, count(distinct consumer_id) as users
from proddb.fionafan.fny_views_experiment
where container_name ilike '%faster%'
group by all;

select vertical_position, count(1) from proddb.fionafan.fny_views_experiment group by all;
-- Post-exposure order rates at exposure (consumer) level
create or replace table proddb.fionafan.fny_exposure_order_rates as
with base as (
    select distinct
        e.consumer_id,
        e.experiment_version,
        e.experiment_group,
        e.segment,
        e.first_exposure_time,
        e.exposure_date,
        e.lifestage
    from proddb.fionafan.eta_threshold_ios_exposures_consumer e
)
, views_any as (
    select
        consumer_id,
        min(1) as had_fny_view
    from proddb.fionafan.fny_views_experiment
    where container_name ILIKE 'fastest near you'
    group by all
)
, orders_any as (
    select
        b.consumer_id,
        min(1) as has_order_any,
        count(distinct dd.delivery_id) as order_cnt_any
    from base b
    join proddb.public.dimension_deliveries dd
      on dd.creator_id::string = b.consumer_id
     and dd.is_filtered_core = true
     and dd.created_at >= b.first_exposure_time
    group by all
)
, orders_24h as (
    select
        b.consumer_id,
        min(1) as has_order_24h,
        count(distinct dd.delivery_id) as order_cnt_24h
    from base b
    join proddb.public.dimension_deliveries dd
      on dd.creator_id::string = b.consumer_id
     and dd.is_filtered_core = true
     and dd.created_at >= b.first_exposure_time
     and dd.created_at < dateadd(hour, 24, b.first_exposure_time)
    group by all
)
select
    b.consumer_id,
    b.experiment_version,
    b.experiment_group,
    b.segment,
    b.first_exposure_time,
    b.exposure_date,
    b.lifestage,
    coalesce(v.had_fny_view, 0) as had_fny_view,
    coalesce(o24.has_order_24h, 0) as has_order_24h_post_exposure,
    coalesce(oa.has_order_any, 0) as has_order_overall_post_exposure,
    coalesce(o24.order_cnt_24h, 0) as order_cnt_24h_post_exposure,
    coalesce(oa.order_cnt_any, 0) as order_cnt_overall_post_exposure
from base b
left join views_any v on v.consumer_id = b.consumer_id
left join orders_24h o24 on o24.consumer_id = b.consumer_id
left join orders_any oa  on oa.consumer_id = b.consumer_id
;
-- select consumer_id, count(1) cnt from proddb.fionafan.fny_exposure_order_rates group by all having cnt>1;
select experiment_group, avg(order_cnt_24h_post_exposure), avg(order_cnt_overall_post_exposure)
from proddb.fionafan.fny_exposure_order_rates group by all;
-- Optional: summary rates
-- select lifestage, avg(has_order_24h_post_exposure) as order_rate_24h, avg(has_order_overall_post_exposure) as order_rate_overall
-- from proddb.fionafan.fny_exposure_order_rates
-- group by all
-- order by all;
-- select consumer_id, count(1) cnt from proddb.fionafan.fny_exposure_order_rates group by all having cnt>1;

-- Segment-level: avg 24h/overall, avg 24h/overall for had_fny_view=1, and lifts
select
    'user type' as segment_type,
    segment,
    avg(order_cnt_24h_post_exposure) as order_rate_24h_overall,
    avg(order_cnt_overall_post_exposure) as order_rate_overall,
    avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view,
    avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_treatment2,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_treatment2,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct,
    ((avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct_view,
    ((avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct_view
from proddb.fionafan.fny_exposure_order_rates
where segment not in ('Developers')
group by segment

-- Lifestage-level: avg 24h/overall, avg 24h/overall for had_fny_view=1, and lifts
union all
select
    'lifestage' as segment_type,
    lifestage as segment,
    avg(order_cnt_24h_post_exposure) as order_rate_24h_overall,
    avg(order_cnt_overall_post_exposure) as order_rate_overall,
    avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view,
    avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_treatment2,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_treatment2,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct,
    ((avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct_view,
    ((avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct_view
from proddb.fionafan.fny_exposure_order_rates
group by lifestage

union all
select
    -- lifestage,
    'overall' as segment_type,
    'overall' as segment,
    avg(order_cnt_24h_post_exposure) as order_rate_24h_overall,
    avg(order_cnt_overall_post_exposure) as order_rate_overall,
    avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view,
    avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_fny_view_treatment2,
    avg(case when had_fny_view = 1 and lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_control,
    avg(case when had_fny_view = 1 and experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_fny_view_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end) as order_rate_24h_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end) as order_rate_24h_treatment2,
    avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end) as order_rate_overall_control,
    avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end) as order_rate_overall_treatment2,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_24h_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct,
    ((avg(case when experiment_group = 'Treatment2' then order_cnt_overall_post_exposure end)
      / nullif(avg(case when lower(experiment_group) = 'control' then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct,
    ((avg(case when had_fny_view = 1 then order_cnt_24h_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_24h_post_exposure end), 0)) - 1) as lift_24h_pct_view,
    ((avg(case when had_fny_view = 1 then order_cnt_overall_post_exposure end)
      / nullif(avg(case when had_fny_view = 0 then order_cnt_overall_post_exposure end), 0)) - 1) as lift_overall_pct_view
from proddb.fionafan.fny_exposure_order_rates
order by all
;



-- select experiment_group,market_tier, sum(order_cnt_24h_after_view)/count(1) 
-- from proddb.fionafan.fny_view_order_rates 
-- -- where lifestage = 'Very Churned'
-- group by all;

-- Market-tier lift (treatment over control) on 24h post-view order rate
-- create or replace view proddb.fionafan.fny_view_order_rates_by_tier as
with enriched as (
  select
      uds.*,
      e.experiment_group,
      e.segment,
      e.lifestage,
      ds.submarket_id,
      t.tier_level as market_tier
  from proddb.fionafan.fny_user_day_store_24h_orders uds
  left join proddb.fionafan.eta_threshold_ios_exposures_consumer e
    on e.consumer_id = uds.consumer_id
  left join proddb.public.dimension_store ds
    on ds.store_id = uds.store_id
  left join proddb.luwang.market_tier t
    on t.submarket_id = ds.submarket_id
)
select
    market_tier,
    count(*) as user_store_days,
    -- orders per user-store-day within 24h of first view
    avg(order_cnt_24h_same_store) as order_rate_24h_overall,
    avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_control,
    avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_treatment,
    (avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
     / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) as lift_treatment_over_control,
    ((avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
      / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) - 1) as lift_pct
from enriched
where lifestage = 'Very Churned'
group by market_tier
order by market_tier;

-- Market-tier x local hour-of-day lift (treatment over control) on 24h post-view order rate
-- Local time derived from submarket timezone; fallback to America/Los_Angeles if missing
-- create or replace view proddb.fionafan.fny_view_order_rates_by_tier_hod as
with enriched as (
  select
      uds.*,
      e.experiment_group,
      e.segment,
      e.lifestage,
      v.CONTEXT_TIMEZONE
  from proddb.fionafan.fny_user_day_store_24h_orders uds
  left join proddb.fionafan.eta_threshold_ios_exposures_consumer e

    on e.consumer_id = uds.consumer_id
  left join proddb.fionafan.fny_views_experiment v
    on v.consumer_id = uds.consumer_id
   and v.store_id    = uds.store_id
   and v.view_ts     = uds.session_start_ts
)
, with_tz as (
  select
    r.*,
    extract(hour from convert_timezone('UTC', coalesce(r.CONTEXT_TIMEZONE, 'America/Los_Angeles'), r.session_start_ts)) as local_hour
  from enriched r
)
select
  local_hour,
  count(*) as user_store_days,
  -- orders per user-store-day within 24h
  avg(order_cnt_24h_same_store) as order_rate_24h_overall,
  avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_control,
  avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_treatment,
  (avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
   / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) as lift_treatment_over_control,
  ((avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
    / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) - 1) as lift_pct
from with_tz
-- where lifestage = 'Very Churned'
group by all order by all;


-- Segment-level order rate (orders per view) and lift using view-level counts
with enriched as (
  select
      uds.*,
      e.experiment_group,
      e.segment,
      e.lifestage
  from proddb.fionafan.fny_user_day_store_24h_orders uds
  left join proddb.fionafan.eta_threshold_ios_exposures_consumer e
    on e.consumer_id = uds.consumer_id
)
select
  segment,
  -- orders per user-store-day (24h)
  avg(order_cnt_24h_same_store) as order_rate_24h_overall,
  avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_control,
  avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end) as order_rate_24h_treatment,
  (avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
   / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) as lift_24h_ratio,
  ((avg(case when lower(experiment_group) not like 'control%' then order_cnt_24h_same_store end)
    / nullif(avg(case when lower(experiment_group) like 'control%' then order_cnt_24h_same_store end), 0)) - 1) as lift_24h_pct
from enriched
-- where lifestage = 'Very Churned'
group by segment
order by segment;


-- FNY user-day-store 24h orders (same store) after first view
create or replace table proddb.fionafan.fny_user_day_store_24h_orders as
with base as (
  select
      consumer_id::string                         as consumer_id,
      store_id,
      date_trunc('day', view_ts)                  as view_day,
      min(view_ts)                                as first_view_ts,
      count(*)                                    as view_events
  from proddb.fionafan.fny_views_experiment
  where container_name ilike 'fastest near you'
  group by consumer_id::string, store_id, date_trunc('day', view_ts)
),
orders_24h as (
  select
      b.consumer_id,
      b.store_id,
      b.view_day,
      count(distinct dd.delivery_id)              as order_cnt_24h_same_store
  from base b
  join proddb.public.dimension_deliveries dd
    on dd.creator_id::string = b.consumer_id
   and dd.store_id = b.store_id
   and dd.is_filtered_core = true
   and dd.created_at >= b.first_view_ts
   and dd.created_at <  dateadd(hour, 24, b.first_view_ts)
  group by b.consumer_id, b.store_id, b.view_day
)
select
    b.consumer_id,
    b.view_day,
    b.store_id,
    b.first_view_ts                               as session_start_ts,
    b.view_events,
    coalesce(o.order_cnt_24h_same_store, 0)       as order_cnt_24h_same_store,
    case when coalesce(o.order_cnt_24h_same_store, 0) > 0 then 1 else 0 end
                                                  as has_order_24h_same_store
from base b
left join orders_24h o
  on o.consumer_id = b.consumer_id
 and o.store_id    = b.store_id
 and o.view_day    = b.view_day;

 select * from proddb.fionafan.fny_daily_trend ;