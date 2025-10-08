-- Express Pay Analysis: Treatment vs Control
-- Analyzing express pay (speed sensitive) adoption and order behavior

with daily_metrics as (
    select
        e.experiment_group,
        date_trunc('day', d.created_at) as order_date,
        -- Overall delivery metrics
        count(distinct d.delivery_id) as total_deliveries,
        count(distinct d.creator_id) as unique_consumers,
        -- Express pay delivery metrics
        count(distinct case when d.express_pay > 0 then d.delivery_id end) as express_deliveries,
        count(distinct case when d.express_pay > 0 then d.creator_id end) as express_consumers,
        -- Express pay percentages
        express_deliveries / nullif(total_deliveries, 0) * 100 as pct_deliveries_express,
        express_consumers / nullif(unique_consumers, 0) * 100 as pct_consumers_using_express
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
    group by 1, 2
),
-- Identify consumers who used express pay at least once during experiment
express_user_cohort as (
    select distinct
        e.experiment_group,
        d.creator_id
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
        and d.express_pay > 0
),
-- Order metrics for express pay users only
express_user_metrics as (
    select
        e.experiment_group,
        date_trunc('day', d.created_at) as order_date,
        count(distinct d.delivery_id) as express_user_total_orders,
        count(distinct d.creator_id) as express_user_active_consumers,
        count(distinct d.delivery_id) / nullif(count(distinct d.creator_id), 0) as express_user_order_frequency
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    inner join express_user_cohort euc
        on d.creator_id = euc.creator_id and e.experiment_group = euc.experiment_group
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
    group by 1, 2
),
treatment_control_comparison as (
    select
        dm.order_date,
        -- Overall metrics
        max(case when dm.experiment_group = 'treatment1' then dm.total_deliveries end) as treatment_deliveries,
        max(case when dm.experiment_group = 'control' then dm.total_deliveries end) as control_deliveries,
        max(case when dm.experiment_group = 'treatment1' then dm.unique_consumers end) as treatment_consumers,
        max(case when dm.experiment_group = 'control' then dm.unique_consumers end) as control_consumers,

        -- Express pay adoption metrics
        max(case when dm.experiment_group = 'treatment1' then dm.express_deliveries end) as treatment_express_deliveries,
        max(case when dm.experiment_group = 'control' then dm.express_deliveries end) as control_express_deliveries,
        max(case when dm.experiment_group = 'treatment1' then dm.express_consumers end) as treatment_express_consumers,
        max(case when dm.experiment_group = 'control' then dm.express_consumers end) as control_express_consumers,
        max(case when dm.experiment_group = 'treatment1' then dm.pct_deliveries_express end) as treatment_pct_deliveries_express,
        max(case when dm.experiment_group = 'control' then dm.pct_deliveries_express end) as control_pct_deliveries_express,
        max(case when dm.experiment_group = 'treatment1' then dm.pct_consumers_using_express end) as treatment_pct_consumers_express,
        max(case when dm.experiment_group = 'control' then dm.pct_consumers_using_express end) as control_pct_consumers_express,

        -- Express pay user cohort metrics
        max(case when eum.experiment_group = 'treatment1' then eum.express_user_total_orders end) as treatment_express_user_orders,
        max(case when eum.experiment_group = 'control' then eum.express_user_total_orders end) as control_express_user_orders,
        max(case when eum.experiment_group = 'treatment1' then eum.express_user_active_consumers end) as treatment_express_user_active,
        max(case when eum.experiment_group = 'control' then eum.express_user_active_consumers end) as control_express_user_active,
        max(case when eum.experiment_group = 'treatment1' then eum.express_user_order_frequency end) as treatment_express_user_freq,
        max(case when eum.experiment_group = 'control' then eum.express_user_order_frequency end) as control_express_user_freq
    from daily_metrics dm
    left join express_user_metrics eum on dm.order_date = eum.order_date and dm.experiment_group = eum.experiment_group
    group by 1
)
select
    order_date,

    -- Overall volume
    treatment_deliveries,
    control_deliveries,
    (treatment_deliveries - control_deliveries) / nullif(control_deliveries, 0) * 100 as delivery_lift_pct,

    treatment_consumers,
    control_consumers,
    (treatment_consumers - control_consumers) / nullif(control_consumers, 0) * 100 as consumer_lift_pct,

    -- Express pay adoption: % of deliveries with express pay
    treatment_pct_deliveries_express,
    control_pct_deliveries_express,
    treatment_pct_deliveries_express - control_pct_deliveries_express as express_delivery_pct_diff,

    -- Express pay adoption: % of consumers using express pay
    treatment_pct_consumers_express,
    control_pct_consumers_express,
    treatment_pct_consumers_express - control_pct_consumers_express as express_consumer_pct_diff,

    -- Express pay user cohort: absolute counts
    treatment_express_user_orders,
    control_express_user_orders,
    (treatment_express_user_orders - control_express_user_orders) / nullif(control_express_user_orders, 0) * 100 as express_user_order_lift_pct,

    treatment_express_user_active,
    control_express_user_active,
    (treatment_express_user_active - control_express_user_active) / nullif(control_express_user_active, 0) * 100 as express_user_active_lift_pct,

    -- Express pay user cohort: order frequency (orders per active consumer)
    treatment_express_user_freq,
    control_express_user_freq,
    (treatment_express_user_freq - control_express_user_freq) / nullif(control_express_user_freq, 0) * 100 as express_user_freq_lift_pct

from treatment_control_comparison
order by order_date;
