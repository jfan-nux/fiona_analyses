-- ASAP Usage Analysis: Treatment vs Control
-- Analyzing ASAP adoption and order behavior for ASAP users

with daily_metrics as (
    select
        e.experiment_group,
        date_trunc('day', d.created_at) as order_date,
        -- Overall delivery metrics
        count(distinct d.delivery_id) as total_deliveries,
        count(distinct d.creator_id) as unique_consumers,
        -- ASAP delivery metrics
        count(distinct case when d.is_asap = 1 then d.delivery_id end) as asap_deliveries,
        count(distinct case when d.is_asap = 1 then d.creator_id end) as asap_consumers,
        -- ASAP percentages
        asap_deliveries / nullif(total_deliveries, 0) * 100 as pct_deliveries_asap,
        asap_consumers / nullif(unique_consumers, 0) * 100 as pct_consumers_using_asap
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
    group by 1, 2
),
-- Identify consumers who used ASAP at least once during experiment
asap_user_cohort as (
    select distinct
        e.experiment_group,
        d.creator_id
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    where d.created_at >= '2025-09-22'
        and d.is_filtered_core = true
        and d.is_asap = 1
),
-- Order metrics for ASAP users only
asap_user_metrics as (
    select
        e.experiment_group,
        date_trunc('day', d.created_at) as order_date,
        count(distinct d.delivery_id) as asap_user_total_orders,
        count(distinct d.creator_id) as asap_user_active_consumers,
        count(distinct d.delivery_id) / nullif(count(distinct d.creator_id), 0) as asap_user_order_frequency
    from dimension_deliveries d
    inner join proddb.fionafan.speed_store_card_and_row_experiment_exposures e
        on d.creator_id = e.bucket_key and d.created_at > e.first_exposure_time
    inner join asap_user_cohort auc
        on d.creator_id = auc.creator_id and e.experiment_group = auc.experiment_group
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

        -- ASAP adoption metrics
        max(case when dm.experiment_group = 'treatment1' then dm.asap_deliveries end) as treatment_asap_deliveries,
        max(case when dm.experiment_group = 'control' then dm.asap_deliveries end) as control_asap_deliveries,
        max(case when dm.experiment_group = 'treatment1' then dm.asap_consumers end) as treatment_asap_consumers,
        max(case when dm.experiment_group = 'control' then dm.asap_consumers end) as control_asap_consumers,
        max(case when dm.experiment_group = 'treatment1' then dm.pct_deliveries_asap end) as treatment_pct_deliveries_asap,
        max(case when dm.experiment_group = 'control' then dm.pct_deliveries_asap end) as control_pct_deliveries_asap,
        max(case when dm.experiment_group = 'treatment1' then dm.pct_consumers_using_asap end) as treatment_pct_consumers_asap,
        max(case when dm.experiment_group = 'control' then dm.pct_consumers_using_asap end) as control_pct_consumers_asap,

        -- ASAP user cohort metrics
        max(case when aum.experiment_group = 'treatment1' then aum.asap_user_total_orders end) as treatment_asap_user_orders,
        max(case when aum.experiment_group = 'control' then aum.asap_user_total_orders end) as control_asap_user_orders,
        max(case when aum.experiment_group = 'treatment1' then aum.asap_user_active_consumers end) as treatment_asap_user_active,
        max(case when aum.experiment_group = 'control' then aum.asap_user_active_consumers end) as control_asap_user_active,
        max(case when aum.experiment_group = 'treatment1' then aum.asap_user_order_frequency end) as treatment_asap_user_freq,
        max(case when aum.experiment_group = 'control' then aum.asap_user_order_frequency end) as control_asap_user_freq
    from daily_metrics dm
    left join asap_user_metrics aum on dm.order_date = aum.order_date and dm.experiment_group = aum.experiment_group
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

    -- ASAP adoption: % of deliveries that are ASAP
    treatment_pct_deliveries_asap,
    control_pct_deliveries_asap,
    treatment_pct_deliveries_asap - control_pct_deliveries_asap as asap_delivery_pct_diff,

    -- ASAP adoption: % of consumers using ASAP
    treatment_pct_consumers_asap,
    control_pct_consumers_asap,
    treatment_pct_consumers_asap - control_pct_consumers_asap as asap_consumer_pct_diff,

    -- ASAP user cohort: absolute counts
    treatment_asap_user_orders,
    control_asap_user_orders,
    (treatment_asap_user_orders - control_asap_user_orders) / nullif(control_asap_user_orders, 0) * 100 as asap_user_order_lift_pct,

    treatment_asap_user_active,
    control_asap_user_active,
    (treatment_asap_user_active - control_asap_user_active) / nullif(control_asap_user_active, 0) * 100 as asap_user_active_lift_pct,

    -- ASAP user cohort: order frequency (orders per active consumer)
    treatment_asap_user_freq,
    control_asap_user_freq,
    (treatment_asap_user_freq - control_asap_user_freq) / nullif(control_asap_user_freq, 0) * 100 as asap_user_freq_lift_pct

from treatment_control_comparison
order by order_date;

