-- Analysis: Order count lift by speed_type_pct for fastest stores
-- Shows how lift varies by the percentage of time a store is tagged as fastest
-- Rounded to 2 decimal places (0.01, 0.02, 0.03, etc.)
-- Note: is_fast = 1 means the store has 'fastest' tag

with tagged_fast_stores as (
    select 
        store_id,
        speed_type_pct,
        round(speed_type_pct, 2) as speed_type_pct_rounded
    from proddb.fionafan.speed_store_card_and_row_experiment_tagged_stores
    where is_fast = 1
),

experiment_data as (
    select
        e.experiment_group,
        ts.speed_type_pct_rounded,
        d.delivery_id,
        d.creator_id
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key 
        and d.created_at > e.first_exposure_time
    inner join tagged_fast_stores ts
        on d.store_id = ts.store_id
    left join edw.dasher.dimension_dasher_applicants dda
        on dda.dasher_id = d.dasher_id
    left join edw.drive.fact_drive_core_delivery_metrics dm
        on d.delivery_id = dm.delivery_id
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and dm.delivery_id is null
),

metrics_by_speed_pct as (
    select
        speed_type_pct_rounded,
        experiment_group,
        count(distinct delivery_id) as deliveries,
        count(distinct creator_id) as consumers
    from experiment_data
    group by 1, 2
),

lift_by_speed_pct as (
    select
        speed_type_pct_rounded,
        max(case when experiment_group = 'treatment1' then deliveries end) as treatment_deliveries,
        max(case when experiment_group = 'control' then deliveries end) as control_deliveries,
        ((max(case when experiment_group = 'treatment1' then deliveries end) - 
          max(case when experiment_group = 'control' then deliveries end)) / 
         nullif(max(case when experiment_group = 'control' then deliveries end), 0) * 100) as delivery_lift_pct,
        max(case when experiment_group = 'treatment1' then consumers end) as treatment_consumers,
        max(case when experiment_group = 'control' then consumers end) as control_consumers,
        ((max(case when experiment_group = 'treatment1' then consumers end) - 
          max(case when experiment_group = 'control' then consumers end)) / 
         nullif(max(case when experiment_group = 'control' then consumers end), 0) * 100) as consumer_lift_pct
    from metrics_by_speed_pct
    group by 1
)

select
    speed_type_pct_rounded,
    treatment_deliveries,
    control_deliveries,
    delivery_lift_pct,
    treatment_consumers,
    control_consumers,
    consumer_lift_pct
from lift_by_speed_pct
where treatment_deliveries is not null 
    and control_deliveries is not null
order by speed_type_pct_rounded;

