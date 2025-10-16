-- Analysis: Delivery count and ETA metrics by market tier
-- Shows how experiment performs in different market tiers (Tier 1, 2, 3, etc.)

-- Step 1: Calculate store historical ETA percentiles (pre-experiment)
with store_historical_eta as (
    select
        d.store_id,
        avg(datediff('minute', d.created_at, d.actual_delivery_time)) as avg_eta_minutes,
        count(distinct d.delivery_id) as historical_deliveries,
        percent_rank() over (order by avg(datediff('minute', d.created_at, d.actual_delivery_time))) as percentile_rank
    from dimension_deliveries d
    where d.created_at >= '2025-08-22'  -- 1 month before experiment
        and d.created_at < '2025-09-22'  -- Before experiment start
        and d.actual_delivery_time is not null
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and datediff('minute', d.created_at, d.actual_delivery_time) < 90
    group by 1
    having count(distinct d.delivery_id) >= 100
),

-- Step 2: Get experiment period data with market tier
experiment_data as (
    select
        e.experiment_group,
        coalesce(cast(mt.tier_level as varchar), 'Unknown') as market_tier,
        d.delivery_id,
        d.creator_id,
        d.created_at,
        d.quoted_delivery_time,
        d.actual_delivery_time,
        datediff('minute', d.created_at, d.actual_delivery_time) as actual_eta_minutes,
        datediff('minute', d.created_at, d.quoted_delivery_time) as quoted_eta_minutes,
        she.percentile_rank as store_percentile_rank,
        she.avg_eta_minutes as store_avg_eta
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key 
        and d.created_at > e.first_exposure_time
    inner join dimension_store ds
        on d.store_id = ds.store_id
    left join proddb.luwang.market_tier mt
        on mt.submarket_id = ds.submarket_id
    left join store_historical_eta she
        on d.store_id = she.store_id
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and ds.is_restaurant = 1
        and d.actual_delivery_time is not null
        and d.quoted_delivery_time is not null
        and datediff('minute', d.created_at, d.actual_delivery_time) < 90
),

-- Step 3: Calculate metrics by market tier and experiment group
tier_metrics as (
    select
        market_tier,
        experiment_group,
        avg(store_percentile_rank) as avg_store_percentile,
        avg(store_avg_eta) as avg_store_eta,
        count(distinct delivery_id) as deliveries,
        count(distinct creator_id) as consumers,
        count(distinct case when store_percentile_rank is not null then delivery_id end) as deliveries_with_eta_data,
        avg(actual_eta_minutes) as avg_actual_eta,
        avg(quoted_eta_minutes) as avg_quoted_eta
    from experiment_data
    group by 1, 2
),

-- Step 4: Calculate lift by market tier
lift_by_tier as (
    select
        market_tier,
        max(case when experiment_group = 'treatment1' then avg_store_percentile end) as treatment_store_percentile,
        max(case when experiment_group = 'control' then avg_store_percentile end) as control_store_percentile,
        max(case when experiment_group = 'treatment1' then avg_store_eta end) as treatment_avg_store_eta,
        max(case when experiment_group = 'control' then avg_store_eta end) as control_avg_store_eta,
        -- Delivery metrics
        max(case when experiment_group = 'treatment1' then deliveries end) as treatment_deliveries,
        max(case when experiment_group = 'control' then deliveries end) as control_deliveries,
        ((max(case when experiment_group = 'treatment1' then deliveries end) - 
          max(case when experiment_group = 'control' then deliveries end)) / 
         nullif(max(case when experiment_group = 'control' then deliveries end), 0) * 100) as delivery_lift_pct,
        -- Consumer metrics
        max(case when experiment_group = 'treatment1' then consumers end) as treatment_consumers,
        max(case when experiment_group = 'control' then consumers end) as control_consumers,
        ((max(case when experiment_group = 'treatment1' then consumers end) - 
          max(case when experiment_group = 'control' then consumers end)) / 
         nullif(max(case when experiment_group = 'control' then consumers end), 0) * 100) as consumer_lift_pct,
        -- ETA metrics
        max(case when experiment_group = 'treatment1' then avg_actual_eta end) as treatment_actual_eta,
        max(case when experiment_group = 'control' then avg_actual_eta end) as control_actual_eta,
        ((max(case when experiment_group = 'treatment1' then avg_actual_eta end) - 
          max(case when experiment_group = 'control' then avg_actual_eta end)) / 
         nullif(max(case when experiment_group = 'control' then avg_actual_eta end), 0) * 100) as actual_eta_lift_pct,
        max(case when experiment_group = 'treatment1' then avg_quoted_eta end) as treatment_quoted_eta,
        max(case when experiment_group = 'control' then avg_quoted_eta end) as control_quoted_eta,
        ((max(case when experiment_group = 'treatment1' then avg_quoted_eta end) - 
          max(case when experiment_group = 'control' then avg_quoted_eta end)) / 
         nullif(max(case when experiment_group = 'control' then avg_quoted_eta end), 0) * 100) as quoted_eta_lift_pct
    from tier_metrics
    group by 1
)

select
    market_tier,
    treatment_store_percentile,
    control_store_percentile,
    treatment_avg_store_eta,
    control_avg_store_eta,
    treatment_deliveries,
    control_deliveries,
    delivery_lift_pct,
    treatment_consumers,
    control_consumers,
    consumer_lift_pct,
    treatment_actual_eta,
    control_actual_eta,
    actual_eta_lift_pct,
    treatment_quoted_eta,
    control_quoted_eta,
    quoted_eta_lift_pct
from lift_by_tier
order by 
    case 
        when market_tier = 'Unknown' then 999
        else try_cast(market_tier as integer)
    end;

