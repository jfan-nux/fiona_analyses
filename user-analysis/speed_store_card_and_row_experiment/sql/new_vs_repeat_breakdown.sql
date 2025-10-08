-- Daily breakdown of new vs repeat purchasers
-- Shows volume split and trends over time

with experiment_orders as (
    select
        e.experiment_group,
        d.creator_id,
        d.delivery_id,
        d.created_at,
        date_trunc('day', d.created_at) as order_date
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    where d.created_at >= '2025-09-17'
        and d.is_filtered_core = true
),
-- Identify first order for each consumer during experiment period
first_order_per_consumer as (
    select
        experiment_group,
        creator_id,
        min(created_at) as first_order_time,
        date_trunc('day', min(created_at)) as first_order_date
    from experiment_orders
    group by 1, 2
),
-- Classify each order as new purchaser (first order) or repeat purchaser
classified_orders as (
    select
        eo.experiment_group,
        eo.order_date,
        eo.delivery_id,
        eo.creator_id,
        case
            when eo.order_date = fo.first_order_date and eo.created_at = fo.first_order_time then 'new_purchaser'
            else 'repeat_purchaser'
        end as purchaser_type
    from experiment_orders eo
    inner join first_order_per_consumer fo
        on eo.creator_id = fo.creator_id and eo.experiment_group = fo.experiment_group
),
-- Daily metrics by purchaser type
daily_metrics as (
    select
        experiment_group,
        order_date,
        purchaser_type,
        count(distinct delivery_id) as total_deliveries,
        count(distinct creator_id) as unique_consumers
    from classified_orders
    group by 1, 2, 3
),
-- Pivot and calculate percentages
treatment_control_comparison as (
    select
        order_date,
        purchaser_type,
        max(case when experiment_group = 'treatment1' then total_deliveries end) as treatment_deliveries,
        max(case when experiment_group = 'control' then total_deliveries end) as control_deliveries,
        max(case when experiment_group = 'treatment1' then unique_consumers end) as treatment_consumers,
        max(case when experiment_group = 'control' then unique_consumers end) as control_consumers
    from daily_metrics
    group by 1, 2
),
-- Calculate totals per day for percentage calculation
daily_totals as (
    select
        order_date,
        sum(treatment_deliveries) as treatment_total_deliveries,
        sum(control_deliveries) as control_total_deliveries,
        sum(treatment_consumers) as treatment_total_consumers,
        sum(control_consumers) as control_total_consumers
    from treatment_control_comparison
    group by 1
)
select
    tc.order_date,
    tc.purchaser_type,

    -- Treatment metrics
    tc.treatment_deliveries,
    tc.treatment_consumers,
    tc.treatment_deliveries / nullif(dt.treatment_total_deliveries, 0) * 100 as treatment_pct_of_deliveries,
    tc.treatment_consumers / nullif(dt.treatment_total_consumers, 0) * 100 as treatment_pct_of_consumers,

    -- Control metrics
    tc.control_deliveries,
    tc.control_consumers,
    tc.control_deliveries / nullif(dt.control_total_deliveries, 0) * 100 as control_pct_of_deliveries,
    tc.control_consumers / nullif(dt.control_total_consumers, 0) * 100 as control_pct_of_consumers,

    -- Lift calculations
    (tc.treatment_deliveries - tc.control_deliveries) / nullif(tc.control_deliveries, 0) * 100 as delivery_lift_pct,
    (tc.treatment_consumers - tc.control_consumers) / nullif(tc.control_consumers, 0) * 100 as consumer_lift_pct

from treatment_control_comparison tc
inner join daily_totals dt on tc.order_date = dt.order_date
where tc.order_date >= '2025-09-22'  -- Filter for plotting from 9/22
order by tc.order_date, tc.purchaser_type;
