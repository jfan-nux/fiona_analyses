-- Analysis: ETA metrics by R2C (restaurant-to-consumer) distance buckets
-- Comparing treatment vs control broken down by delivery distance

with base_deliveries as (
    select
        e.experiment_group,
        d.delivery_id,
        d.creator_id,
        d.created_at,
        date_trunc('day', d.created_at) as order_date,
        d.quoted_delivery_time,
        d.actual_delivery_time,
        r.quoted_delivery_time_range_min,
        r.quoted_delivery_time_range_max,
        fdd.ROAD_R2C_DISTANCE * 0.0006211371 as r2c_miles,  -- Convert meters to miles
        -- Create R2C distance buckets
        case
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 1 then '0-1 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 2 then '1-2 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 3 then '2-3 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 5 then '3-5 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 8 then '5-8 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 10 then '8-10 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 13 then '10-13 miles'
            when fdd.ROAD_R2C_DISTANCE * 0.0006211371 < 15 then '13-15 miles'
            else '15+ miles'
        end as r2c_bucket
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    inner join edw.logistics.dasher_delivery_eta_range r
        on d.delivery_id = r.delivery_id
    left join proddb.public.fact_delivery_distances fdd
        on d.delivery_id = fdd.delivery_id
    where d.created_at >= '2025-09-22'
        and d.actual_delivery_time is not null
        and d.quoted_delivery_time is not null
        and r.quoted_delivery_time_range_min is not null
        and r.quoted_delivery_time_range_max is not null
        and d.is_filtered_core = true
        and d.is_consumer_pickup = false  -- Exclude pickup orders
        and nvl(d.fulfillment_type, '') not in ('merchant_fleet', 'shipping')
        and fdd.ROAD_R2C_DISTANCE is not null
        and fdd.ROAD_R2C_DISTANCE > 0
        and datediff('minute', d.created_at, d.quoted_delivery_time) < 90
        and datediff('minute', d.created_at, r.quoted_delivery_time_range_min) < 90
        and datediff('minute', d.created_at, r.quoted_delivery_time_range_max) < 90
        and datediff('minute', d.created_at, d.actual_delivery_time) < 90
),
-- Daily metrics by R2C bucket
daily_metrics as (
    select
        experiment_group,
        order_date,
        r2c_bucket,
        count(distinct delivery_id) as total_deliveries,
        count(distinct creator_id) as unique_consumers,
        -- Time from order creation to quoted delivery time
        avg(datediff('minute', created_at, quoted_delivery_time)) as avg_quoted_minutes,
        stddev(datediff('minute', created_at, quoted_delivery_time)) as std_quoted_minutes,
        -- QDT range min
        avg(datediff('minute', created_at, quoted_delivery_time_range_min)) as avg_qdt_min_minutes,
        stddev(datediff('minute', created_at, quoted_delivery_time_range_min)) as std_qdt_min_minutes,
        -- QDT range max
        avg(datediff('minute', created_at, quoted_delivery_time_range_max)) as avg_qdt_max_minutes,
        stddev(datediff('minute', created_at, quoted_delivery_time_range_max)) as std_qdt_max_minutes,
        -- Actual delivery time
        avg(datediff('minute', created_at, actual_delivery_time)) as avg_actual_minutes,
        stddev(datediff('minute', created_at, actual_delivery_time)) as std_actual_minutes,
        -- R2C distance (restaurant to consumer road miles)
        avg(r2c_miles) as avg_r2c_miles
    from base_deliveries
    group by 1, 2, 3
),
-- Pivot to compare treatment vs control
treatment_control_comparison as (
    select
        dm.order_date,
        dm.r2c_bucket,
        max(case when dm.experiment_group = 'treatment1' then dm.total_deliveries end) as treatment_deliveries,
        max(case when dm.experiment_group = 'control' then dm.total_deliveries end) as control_deliveries,
        max(case when dm.experiment_group = 'treatment1' then dm.unique_consumers end) as treatment_consumers,
        max(case when dm.experiment_group = 'control' then dm.unique_consumers end) as control_consumers,
        -- Quoted time
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
        -- R2C distance
        max(case when dm.experiment_group = 'treatment1' then dm.avg_r2c_miles end) as treatment_r2c_miles,
        max(case when dm.experiment_group = 'control' then dm.avg_r2c_miles end) as control_r2c_miles
    from daily_metrics dm
    group by 1, 2
),
-- Daily results
daily_results as (
    select
        order_date,
        r2c_bucket,
        'daily' as metric_type,
        treatment_deliveries,
        control_deliveries,
        (treatment_deliveries - control_deliveries) / nullif(control_deliveries, 0) * 100 as delivery_count_lift_pct,
        
        treatment_consumers,
        control_consumers,
        (treatment_consumers - control_consumers) / nullif(control_consumers, 0) * 100 as consumer_count_lift_pct,
        
        treatment_quoted_minutes,
        control_quoted_minutes,
        (treatment_quoted_minutes - control_quoted_minutes) / nullif(control_quoted_minutes, 0) * 100 as quoted_minutes_lift_pct,
        
        treatment_qdt_min_minutes,
        control_qdt_min_minutes,
        (treatment_qdt_min_minutes - control_qdt_min_minutes) / nullif(control_qdt_min_minutes, 0) * 100 as qdt_min_minutes_lift_pct,
        
        treatment_qdt_max_minutes,
        control_qdt_max_minutes,
        (treatment_qdt_max_minutes - control_qdt_max_minutes) / nullif(control_qdt_max_minutes, 0) * 100 as qdt_max_minutes_lift_pct,
        
        treatment_actual_minutes,
        control_actual_minutes,
        (treatment_actual_minutes - control_actual_minutes) / nullif(control_actual_minutes, 0) * 100 as actual_minutes_lift_pct,
        
        treatment_r2c_miles,
        control_r2c_miles
    from treatment_control_comparison
),
-- Overall aggregation by R2C bucket using weighted averages
overall_results as (
    select
        null as order_date,
        r2c_bucket,
        'overall' as metric_type,
        
        -- Total deliveries
        sum(treatment_deliveries) as treatment_deliveries,
        sum(control_deliveries) as control_deliveries,
        (sum(treatment_deliveries) - sum(control_deliveries)) / nullif(sum(control_deliveries), 0) * 100 as delivery_count_lift_pct,
        
        -- Total consumers
        sum(treatment_consumers) as treatment_consumers,
        sum(control_consumers) as control_consumers,
        (sum(treatment_consumers) - sum(control_consumers)) / nullif(sum(control_consumers), 0) * 100 as consumer_count_lift_pct,
        
        -- Weighted average quoted time
        sum(treatment_quoted_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_quoted_minutes,
        sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_quoted_minutes,
        ((sum(treatment_quoted_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_quoted_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as quoted_minutes_lift_pct,
        
        -- Weighted average QDT min
        sum(treatment_qdt_min_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_qdt_min_minutes,
        sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_qdt_min_minutes,
        ((sum(treatment_qdt_min_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_qdt_min_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as qdt_min_minutes_lift_pct,
        
        -- Weighted average QDT max
        sum(treatment_qdt_max_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_qdt_max_minutes,
        sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_qdt_max_minutes,
        ((sum(treatment_qdt_max_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_qdt_max_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as qdt_max_minutes_lift_pct,
        
        -- Weighted average actual time
        sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_actual_minutes,
        sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0) as control_actual_minutes,
        ((sum(treatment_actual_minutes * treatment_deliveries) / nullif(sum(treatment_deliveries), 0)) - 
            (sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0))) / 
            nullif((sum(control_actual_minutes * control_deliveries) / nullif(sum(control_deliveries), 0)), 0) * 100 as actual_minutes_lift_pct,
        
        -- Weighted average R2C distance
        sum(treatment_r2c_miles * treatment_deliveries) / nullif(sum(treatment_deliveries), 0) as treatment_r2c_miles,
        sum(control_r2c_miles * control_deliveries) / nullif(sum(control_deliveries), 0) as control_r2c_miles
    from treatment_control_comparison
    group by 2
)
-- Union daily and overall
select * from daily_results
union all
select * from overall_results
order by r2c_bucket, metric_type desc, order_date;

