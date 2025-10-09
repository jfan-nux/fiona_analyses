-- Analysis: Delivery count and consumer count lift by store's historical ETA percentile
-- Shows if lift varies by store ETA performance (fast vs slow stores)

-- Step 1: Calculate historical ETA percentiles by store (pre-experiment period)
with store_historical_eta as (
    select
        d.store_id,
        avg(datediff('minute', d.created_at, d.actual_delivery_time)) as avg_eta_minutes,
        count(distinct d.delivery_id) as historical_deliveries
    from dimension_deliveries d
    where d.created_at >= '2025-08-22'  -- 1 month before experiment start
        and d.created_at < '2025-09-22'  -- Before experiment start
        and d.actual_delivery_time is not null
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and datediff('minute', d.created_at, d.actual_delivery_time) < 90
    group by 1
    having count(distinct d.delivery_id) >= 100  -- Minimum volume for reliable percentile
),

-- Step 2: Assign percentile buckets to stores
store_eta_percentiles as (
    select
        store_id,
        avg_eta_minutes,
        historical_deliveries,
        ntile(10) over (order by avg_eta_minutes) as eta_percentile_decile
    from store_historical_eta
),

-- Step 3: Assign bucket labels based on deciles
store_eta_with_buckets as (
    select
        store_id,
        avg_eta_minutes,
        historical_deliveries,
        eta_percentile_decile,
        case
            when eta_percentile_decile <= 2 then 'D1-2: Fastest (0-20%)'
            when eta_percentile_decile <= 5 then 'D3-5: Fast (20-50%)'
            when eta_percentile_decile <= 7 then 'D6-7: Slow (50-70%)'
            else 'D8-10: Slowest (70-100%)'
        end as eta_percentile_bucket
    from store_eta_percentiles
),

-- Step 4: Get experiment data and join with store ETA percentiles
experiment_data as (
    select
        e.experiment_group,
        sep.eta_percentile_bucket,
        sep.eta_percentile_decile,
        d.delivery_id,
        d.creator_id,
        sep.avg_eta_minutes as store_avg_eta
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key 
        and d.created_at > e.first_exposure_time
    inner join store_eta_with_buckets sep
        on d.store_id = sep.store_id
    left join edw.dasher.dimension_dasher_applicants dda
        on dda.dasher_id = d.dasher_id
    left join edw.drive.fact_drive_core_delivery_metrics dm
        on d.delivery_id = dm.delivery_id
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and dm.delivery_id is null
),

-- Step 5: Calculate metrics by ETA percentile bucket
percentile_metrics as (
    select
        eta_percentile_bucket,
        eta_percentile_decile,
        experiment_group,
        avg(store_avg_eta) as avg_store_eta,
        count(distinct delivery_id) as deliveries,
        count(distinct creator_id) as consumers
    from experiment_data
    group by 1, 2, 3
),

-- Step 6: Calculate lift by percentile bucket
lift_by_percentile as (
    select
        eta_percentile_bucket,
        eta_percentile_decile,
        max(avg_store_eta) as avg_store_eta_minutes,
        max(case when experiment_group = 'treatment1' then deliveries end) as treatment_deliveries,
        max(case when experiment_group = 'control' then deliveries end) as control_deliveries,
        max(case when experiment_group = 'treatment1' then consumers end) as treatment_consumers,
        max(case when experiment_group = 'control' then consumers end) as control_consumers,
        -- Delivery count lift
        (max(case when experiment_group = 'treatment1' then deliveries end) - 
         max(case when experiment_group = 'control' then deliveries end)) as delivery_diff,
        ((max(case when experiment_group = 'treatment1' then deliveries end) - 
          max(case when experiment_group = 'control' then deliveries end)) / 
         nullif(max(case when experiment_group = 'control' then deliveries end), 0) * 100) as delivery_lift_pct,
        -- Consumer count lift
        (max(case when experiment_group = 'treatment1' then consumers end) - 
         max(case when experiment_group = 'control' then consumers end)) as consumer_diff,
        ((max(case when experiment_group = 'treatment1' then consumers end) - 
          max(case when experiment_group = 'control' then consumers end)) / 
         nullif(max(case when experiment_group = 'control' then consumers end), 0) * 100) as consumer_lift_pct
    from percentile_metrics
    group by 1, 2
)

select
    eta_percentile_bucket,
    eta_percentile_decile,
    avg_store_eta_minutes,
    treatment_deliveries,
    control_deliveries,
    delivery_diff,
    delivery_lift_pct,
    treatment_consumers,
    control_consumers,
    consumer_diff,
    consumer_lift_pct
from lift_by_percentile
order by eta_percentile_decile;

