-- Analysis: quoted time from order placement and actual delivery time from order placement
-- Comparing treatment vs control by day with statistical significance testing

with daily_metrics as (
    select
        e.experiment_group,
        date_trunc('day', d.created_at) as order_date,
        count(distinct d.delivery_id) as total_deliveries,
        count(distinct d.creator_id) as unique_consumers,
        -- Time from order creation to quoted delivery time (from dimension_deliveries)
        avg(datediff('minute', d.created_at, d.quoted_delivery_time)) as avg_quoted_minutes,
        stddev(datediff('minute', d.created_at, d.quoted_delivery_time)) as std_quoted_minutes,
        -- Time from order creation to quoted delivery time range (min and max from ETA range table)
        avg(datediff('minute', d.created_at, r.quoted_delivery_time_range_min)) as avg_qdt_min_minutes,
        stddev(datediff('minute', d.created_at, r.quoted_delivery_time_range_min)) as std_qdt_min_minutes,
        avg(datediff('minute', d.created_at, r.quoted_delivery_time_range_max)) as avg_qdt_max_minutes,
        stddev(datediff('minute', d.created_at, r.quoted_delivery_time_range_max)) as std_qdt_max_minutes,
        -- Time from order creation to actual delivery time
        avg(datediff('minute', d.created_at, d.actual_delivery_time)) as avg_actual_minutes,
        stddev(datediff('minute', d.created_at, d.actual_delivery_time)) as std_actual_minutes,
        -- R2C: Restaurant to Customer (pickup to delivery)
        avg(datediff('sec', d.actual_pickup_time, d.actual_delivery_time)/60.0) as avg_r2c_minutes,
        stddev(datediff('sec', d.actual_pickup_time, d.actual_delivery_time)/60.0) as std_r2c_minutes,
        -- D2C: Dasher confirmed to Customer (dasher confirmed to delivery)
        avg(datediff('sec', d.dasher_confirmed_time, d.actual_delivery_time)/60.0) as avg_d2c_minutes,
        stddev(datediff('sec', d.dasher_confirmed_time, d.actual_delivery_time)/60.0) as std_d2c_minutes
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    inner join edw.logistics.dasher_delivery_eta_range r
        on d.delivery_id = r.delivery_id
    left join edw.dasher.dimension_dasher_applicants dda
        on dda.dasher_id = d.dasher_id
    left join edw.drive.fact_drive_core_delivery_metrics dm
        on d.delivery_id = dm.delivery_id
    where d.created_at >= '2025-09-22'
        and d.actual_delivery_time is not null
        and d.quoted_delivery_time is not null
        and r.quoted_delivery_time_range_min is not null
        and r.quoted_delivery_time_range_max is not null
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false
        and nvl(d.fulfillment_type, '') not in ('merchant_fleet', 'shipping')
        and dm.delivery_id is null
        and datediff('minute', d.created_at, d.quoted_delivery_time) < 90
        and datediff('minute', d.created_at, r.quoted_delivery_time_range_min) < 90
        and datediff('minute', d.created_at, r.quoted_delivery_time_range_max) < 90
        and datediff('minute', d.created_at, d.actual_delivery_time) < 90
    group by 1, 2
),
-- Simplified: Use daily unique consumers as proxy for MAU trend
-- (true 28-day rolling MAU is too expensive to compute)

treatment_control_comparison as (
    select
        dm.order_date,
        max(case when dm.experiment_group = 'treatment1' then dm.total_deliveries end) as treatment_deliveries,
        max(case when dm.experiment_group = 'control' then dm.total_deliveries end) as control_deliveries,
        max(case when dm.experiment_group = 'treatment1' then dm.unique_consumers end) as treatment_consumers,
        max(case when dm.experiment_group = 'control' then dm.unique_consumers end) as control_consumers,
        -- Quoted time (from dimension_deliveries)
        max(case when dm.experiment_group = 'treatment1' then dm.avg_quoted_minutes end) as treatment_quoted_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_quoted_minutes end) as control_quoted_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_quoted_minutes end) as treatment_quoted_std,
        max(case when dm.experiment_group = 'control' then dm.std_quoted_minutes end) as control_quoted_std,
        -- QDT range min
        max(case when dm.experiment_group = 'treatment1' then dm.avg_qdt_min_minutes end) as treatment_qdt_min_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_qdt_min_minutes end) as control_qdt_min_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_qdt_min_minutes end) as treatment_qdt_min_std,
        max(case when dm.experiment_group = 'control' then dm.std_qdt_min_minutes end) as control_qdt_min_std,
        -- QDT range max
        max(case when dm.experiment_group = 'treatment1' then dm.avg_qdt_max_minutes end) as treatment_qdt_max_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_qdt_max_minutes end) as control_qdt_max_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_qdt_max_minutes end) as treatment_qdt_max_std,
        max(case when dm.experiment_group = 'control' then dm.std_qdt_max_minutes end) as control_qdt_max_std,
        -- Actual delivery time
        max(case when dm.experiment_group = 'treatment1' then dm.avg_actual_minutes end) as treatment_actual_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_actual_minutes end) as control_actual_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_actual_minutes end) as treatment_actual_std,
        max(case when dm.experiment_group = 'control' then dm.std_actual_minutes end) as control_actual_std,
        -- R2C metrics
        max(case when dm.experiment_group = 'treatment1' then dm.avg_r2c_minutes end) as treatment_r2c_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_r2c_minutes end) as control_r2c_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_r2c_minutes end) as treatment_r2c_std,
        max(case when dm.experiment_group = 'control' then dm.std_r2c_minutes end) as control_r2c_std,
        -- D2C metrics
        max(case when dm.experiment_group = 'treatment1' then dm.avg_d2c_minutes end) as treatment_d2c_minutes,
        max(case when dm.experiment_group = 'control' then dm.avg_d2c_minutes end) as control_d2c_minutes,
        max(case when dm.experiment_group = 'treatment1' then dm.std_d2c_minutes end) as treatment_d2c_std,
        max(case when dm.experiment_group = 'control' then dm.std_d2c_minutes end) as control_d2c_std
    from daily_metrics dm
    group by 1
),
-- Daily results
daily_results as (
select
    order_date,
        'daily' as metric_type,
    treatment_deliveries,
    control_deliveries,
    treatment_deliveries - control_deliveries as delivery_count_diff,
    -- Delivery count lift percentage
    (treatment_deliveries - control_deliveries) / nullif(control_deliveries, 0) * 100 as delivery_count_lift_pct,

    -- Daily unique consumer metrics (proxy for MAU trend)
    treatment_consumers,
    control_consumers,
    treatment_consumers - control_consumers as consumer_count_diff,
    (treatment_consumers - control_consumers) / nullif(control_consumers, 0) * 100 as consumer_count_lift_pct,

    -- Quoted time metrics (from dimension_deliveries)
    treatment_quoted_minutes,
    control_quoted_minutes,
    treatment_quoted_minutes - control_quoted_minutes as quoted_minutes_diff,
    (treatment_quoted_minutes - control_quoted_minutes) / nullif(control_quoted_minutes, 0) * 100 as quoted_minutes_lift_pct,
    treatment_quoted_std,
    control_quoted_std,
    case
        when abs(treatment_quoted_minutes - control_quoted_minutes) /
             sqrt(power(treatment_quoted_std, 2)/treatment_deliveries + power(control_quoted_std, 2)/control_deliveries) > 1.96
        then 1
        else 0
    end as quoted_significant,

    -- QDT range min metrics
    treatment_qdt_min_minutes,
    control_qdt_min_minutes,
    treatment_qdt_min_minutes - control_qdt_min_minutes as qdt_min_minutes_diff,
    (treatment_qdt_min_minutes - control_qdt_min_minutes) / nullif(control_qdt_min_minutes, 0) * 100 as qdt_min_minutes_lift_pct,
    treatment_qdt_min_std,
    control_qdt_min_std,
    case
        when abs(treatment_qdt_min_minutes - control_qdt_min_minutes) /
             sqrt(power(treatment_qdt_min_std, 2)/treatment_deliveries + power(control_qdt_min_std, 2)/control_deliveries) > 1.96
        then 1
        else 0
    end as qdt_min_significant,

    -- QDT range max metrics
    treatment_qdt_max_minutes,
    control_qdt_max_minutes,
    treatment_qdt_max_minutes - control_qdt_max_minutes as qdt_max_minutes_diff,
    (treatment_qdt_max_minutes - control_qdt_max_minutes) / nullif(control_qdt_max_minutes, 0) * 100 as qdt_max_minutes_lift_pct,
    treatment_qdt_max_std,
    control_qdt_max_std,
    case
        when abs(treatment_qdt_max_minutes - control_qdt_max_minutes) /
             sqrt(power(treatment_qdt_max_std, 2)/treatment_deliveries + power(control_qdt_max_std, 2)/control_deliveries) > 1.96
        then 1
        else 0
    end as qdt_max_significant,

    -- Actual delivery time metrics
    treatment_actual_minutes,
    control_actual_minutes,
    treatment_actual_minutes - control_actual_minutes as actual_minutes_diff,
    (treatment_actual_minutes - control_actual_minutes) / nullif(control_actual_minutes, 0) * 100 as actual_minutes_lift_pct,
    treatment_actual_std,
    control_actual_std,
    case
        when abs(treatment_actual_minutes - control_actual_minutes) /
             sqrt(power(treatment_actual_std, 2)/treatment_deliveries + power(control_actual_std, 2)/control_deliveries) > 1.96
        then 1
        else 0
        end as actual_significant,

        -- R2C (Restaurant to Customer) metrics
        treatment_r2c_minutes,
        control_r2c_minutes,
        treatment_r2c_minutes - control_r2c_minutes as r2c_minutes_diff,
        (treatment_r2c_minutes - control_r2c_minutes) / nullif(control_r2c_minutes, 0) * 100 as r2c_minutes_lift_pct,
        treatment_r2c_std,
        control_r2c_std,
        case
            when abs(treatment_r2c_minutes - control_r2c_minutes) /
                 sqrt(power(treatment_r2c_std, 2)/treatment_deliveries + power(control_r2c_std, 2)/control_deliveries) > 1.96
            then 1
            else 0
        end as r2c_significant,

        -- D2C (Dasher confirmed to Customer) metrics
        treatment_d2c_minutes,
        control_d2c_minutes,
        treatment_d2c_minutes - control_d2c_minutes as d2c_minutes_diff,
        (treatment_d2c_minutes - control_d2c_minutes) / nullif(control_d2c_minutes, 0) * 100 as d2c_minutes_lift_pct,
        treatment_d2c_std,
        control_d2c_std,
        case
            when abs(treatment_d2c_minutes - control_d2c_minutes) /
                 sqrt(power(treatment_d2c_std, 2)/treatment_deliveries + power(control_d2c_std, 2)/control_deliveries) > 1.96
            then 1
            else 0
        end as d2c_significant,

        -- Create to Dash (actual_delivery - d2c = order creation to dasher confirmation)
        treatment_actual_minutes - treatment_d2c_minutes as treatment_create_to_dash_minutes,
        control_actual_minutes - control_d2c_minutes as control_create_to_dash_minutes,
        (treatment_actual_minutes - treatment_d2c_minutes) - (control_actual_minutes - control_d2c_minutes) as create_to_dash_minutes_diff,
        ((treatment_actual_minutes - treatment_d2c_minutes) - (control_actual_minutes - control_d2c_minutes)) / 
            nullif((control_actual_minutes - control_d2c_minutes), 0) * 100 as create_to_dash_minutes_lift_pct,

        -- Create to Restaurant (actual_delivery - r2c = order creation to pickup)
        treatment_actual_minutes - treatment_r2c_minutes as treatment_create_to_restaurant_minutes,
        control_actual_minutes - control_r2c_minutes as control_create_to_restaurant_minutes,
        (treatment_actual_minutes - treatment_r2c_minutes) - (control_actual_minutes - control_r2c_minutes) as create_to_restaurant_minutes_diff,
        ((treatment_actual_minutes - treatment_r2c_minutes) - (control_actual_minutes - control_r2c_minutes)) / 
            nullif((control_actual_minutes - control_r2c_minutes), 0) * 100 as create_to_restaurant_minutes_lift_pct
from treatment_control_comparison
),
-- Overall aggregation using weighted averages for time metrics
overall_results as (
    select
        null as order_date,
        'overall' as metric_type,
        
        -- Total deliveries
        sum(treatment_deliveries) as treatment_deliveries,
        sum(control_deliveries) as control_deliveries,
        sum(treatment_deliveries) - sum(control_deliveries) as delivery_count_diff,
        (sum(treatment_deliveries) - sum(control_deliveries)) / nullif(sum(control_deliveries), 0) * 100 as delivery_count_lift_pct,

        -- Total consumers
        sum(treatment_consumers) as treatment_consumers,
        sum(control_consumers) as control_consumers,
        sum(treatment_consumers) - sum(control_consumers) as consumer_count_diff,
        (sum(treatment_consumers) - sum(control_consumers)) / nullif(sum(control_consumers), 0) * 100 as consumer_count_lift_pct,

        -- Weighted average quoted time
        sum(treatment_quoted_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_quoted_minutes,
        sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_quoted_minutes,
        (sum(treatment_quoted_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as quoted_minutes_diff,
        ((sum(treatment_quoted_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as quoted_minutes_lift_pct,
        null as treatment_quoted_std,
        null as control_quoted_std,
        null as quoted_significant,

        -- Weighted average QDT min
        sum(treatment_qdt_min_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_qdt_min_minutes,
        sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_qdt_min_minutes,
        (sum(treatment_qdt_min_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as qdt_min_minutes_diff,
        ((sum(treatment_qdt_min_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as qdt_min_minutes_lift_pct,
        null as treatment_qdt_min_std,
        null as control_qdt_min_std,
        null as qdt_min_significant,

        -- Weighted average QDT max
        sum(treatment_qdt_max_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_qdt_max_minutes,
        sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_qdt_max_minutes,
        (sum(treatment_qdt_max_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as qdt_max_minutes_diff,
        ((sum(treatment_qdt_max_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as qdt_max_minutes_lift_pct,
        null as treatment_qdt_max_std,
        null as control_qdt_max_std,
        null as qdt_max_significant,

        -- Weighted average actual time
        sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_actual_minutes,
        sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_actual_minutes,
        (sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as actual_minutes_diff,
        ((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as actual_minutes_lift_pct,
        null as treatment_actual_std,
        null as control_actual_std,
        null as actual_significant,

        -- Weighted average R2C
        sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_r2c_minutes,
        sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_r2c_minutes,
        (sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as r2c_minutes_diff,
        ((sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as r2c_minutes_lift_pct,
        null as treatment_r2c_std,
        null as control_r2c_std,
        null as r2c_significant,

        -- Weighted average D2C
        sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_d2c_minutes,
        sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_d2c_minutes,
        (sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as d2c_minutes_diff,
        ((sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as d2c_minutes_lift_pct,
        null as treatment_d2c_std,
        null as control_d2c_std,
        null as d2c_significant,

        -- Create to Dash (actual_delivery - d2c)
        (sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) as treatment_create_to_dash_minutes,
        (sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as control_create_to_dash_minutes,
        ((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0))) -
        ((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) as create_to_dash_minutes_diff,
        (((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_d2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0))) -
        ((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)))) /
        nullif(((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_d2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))), 0) * 100 as create_to_dash_minutes_lift_pct,

        -- Create to Restaurant (actual_delivery - r2c)
        (sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) as treatment_create_to_restaurant_minutes,
        (sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) as control_create_to_restaurant_minutes,
        ((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0))) -
        ((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) as create_to_restaurant_minutes_diff,
        (((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(treatment_r2c_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0))) -
        ((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)))) /
        nullif(((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)) - 
            (sum(control_r2c_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))), 0) * 100 as create_to_restaurant_minutes_lift_pct
    from treatment_control_comparison
)
-- Union daily and overall
select * from daily_results
union all
select * from overall_results
order by metric_type desc, order_date;



