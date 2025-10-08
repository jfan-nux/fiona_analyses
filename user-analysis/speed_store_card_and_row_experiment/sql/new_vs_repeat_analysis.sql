-- New vs Repeat Purchaser Analysis: Treatment vs Control
-- Comparing behavior of new purchasers (first order during experiment) vs repeat purchasers

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
        min(created_at) as first_order_time
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
            when eo.created_at = fo.first_order_time then 'new_purchaser'
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
        count(distinct creator_id) as unique_consumers,
        count(distinct delivery_id) / nullif(count(distinct creator_id), 0) as order_frequency
    from classified_orders
    group by 1, 2, 3
),
-- Pivot to compare treatment vs control
treatment_control_comparison as (
    select
        order_date,
        purchaser_type,
        max(case when experiment_group = 'treatment1' then total_deliveries end) as treatment_deliveries,
        max(case when experiment_group = 'control' then total_deliveries end) as control_deliveries,
        max(case when experiment_group = 'treatment1' then unique_consumers end) as treatment_consumers,
        max(case when experiment_group = 'control' then unique_consumers end) as control_consumers,
        max(case when experiment_group = 'treatment1' then order_frequency end) as treatment_order_freq,
        max(case when experiment_group = 'control' then order_frequency end) as control_order_freq
    from daily_metrics
    group by 1, 2
),
-- Daily results
daily_results as (
    select
        order_date,
        purchaser_type,
        'daily' as metric_type,

        -- Delivery metrics
        treatment_deliveries,
        control_deliveries,
        (treatment_deliveries - control_deliveries) / nullif(control_deliveries, 0) * 100 as delivery_lift_pct,

        -- Consumer metrics
        treatment_consumers,
        control_consumers,
        (treatment_consumers - control_consumers) / nullif(control_consumers, 0) * 100 as consumer_lift_pct,

        -- Order frequency metrics
        treatment_order_freq,
        control_order_freq,
        (treatment_order_freq - control_order_freq) / nullif(control_order_freq, 0) * 100 as order_freq_lift_pct

    from treatment_control_comparison
    where order_date >= '2025-09-22'  -- Filter for plotting from 9/22
),
-- Overall aggregation
overall_results as (
    select
        null as order_date,
        purchaser_type,
        'overall' as metric_type,

        -- Total deliveries
        sum(treatment_deliveries) as treatment_deliveries,
        sum(control_deliveries) as control_deliveries,
        (sum(treatment_deliveries) - sum(control_deliveries)) / nullif(sum(control_deliveries), 0) * 100 as delivery_lift_pct,

        -- Total consumers (sum across days, though with overlap)
        sum(treatment_consumers) as treatment_consumers,
        sum(control_consumers) as control_consumers,
        (sum(treatment_consumers) - sum(control_consumers)) / nullif(sum(control_consumers), 0) * 100 as consumer_lift_pct,

        -- Average order frequency
        sum(treatment_deliveries) / nullif(sum(treatment_consumers), 0) as treatment_order_freq,
        sum(control_deliveries) / nullif(sum(control_consumers), 0) as control_order_freq,
        ((sum(treatment_deliveries) / nullif(sum(treatment_consumers), 0)) - (sum(control_deliveries) / nullif(sum(control_consumers), 0))) 
            / nullif((sum(control_deliveries) / nullif(sum(control_consumers), 0)), 0) * 100 as order_freq_lift_pct

    from treatment_control_comparison
    where order_date >= '2025-09-22'
    group by 2
)
-- Union daily and overall
select * from daily_results
union all
select * from overall_results
order by purchaser_type, metric_type desc, order_date;
