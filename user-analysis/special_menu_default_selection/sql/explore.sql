WITH exposure AS (
  SELECT
    *
  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures
), core_quality_events AS (
  SELECT
    *
  FROM metrics_repo.public.core_quality_events AS core_quality_events
  WHERE
    CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)
), core_quality_events_NonWindow_exp_metrics AS (
  SELECT
    TO_CHAR(exposure.bucket_key) AS bucket_key,
    exposure.experiment_group,

    /* Numerators for cancel-related metrics */
    SUM(is_cancelled_core)                                   AS core_quality_cancelled_core_numerator,
    COUNT(is_cancelled_core)                                 AS core_quality_cancelled_core_numerator_count,

    SUM(is_mx_induced_cancel_category)                       AS core_quality_mx_induced_cancel_numerator,
    COUNT(is_mx_induced_cancel_category)                     AS core_quality_mx_induced_cancel_numerator_count,

    SUM(is_cx_induced_cancel_category)                       AS core_quality_cx_induced_cancel_numerator,
    COUNT(is_cx_induced_cancel_category)                     AS core_quality_cx_induced_cancel_numerator_count,

    SUM(is_logistics_induced_cancel_category)                AS core_quality_logistics_induced_cancel_numerator,
    COUNT(is_logistics_induced_cancel_category)              AS core_quality_logistics_induced_cancel_numerator_count,

    SUM(is_pos_error_induced_cancel_category)                AS core_quality_pos_error_induced_cancel_numerator,
    COUNT(is_pos_error_induced_cancel_category)              AS core_quality_pos_error_induced_cancel_numerator_count,

    SUM(is_ottl_cancel)                                      AS core_quality_ottl_cancel_numerator,
    COUNT(is_ottl_cancel)                                    AS core_quality_ottl_cancel_numerator_count,

    SUM(is_store_closed_cancel)                              AS core_quality_store_closed_cancel_numerator,
    COUNT(is_store_closed_cancel)                            AS core_quality_store_closed_cancel_numerator_count,

    SUM(is_pos_proactive_cancel)                             AS core_quality_pos_proactive_cancel_numerator,
    COUNT(is_pos_proactive_cancel)                           AS core_quality_pos_proactive_cancel_numerator_count,

    SUM(is_item_unavailable_cancel)                          AS core_quality_item_unavailable_cancel_numerator,
    COUNT(is_item_unavailable_cancel)                        AS core_quality_item_unavailable_cancel_numerator_count,

    SUM(is_wrong_dx_handoff_cancel)                          AS core_quality_wrong_dx_handoff_cancel_numerator,
    COUNT(is_wrong_dx_handoff_cancel)                        AS core_quality_wrong_dx_handoff_cancel_numerator_count,

    /* Common denominator for all cancel-related metrics */
    SUM(is_cancelled_denom)                                  AS core_quality_cancel_common_denominator,
    COUNT(is_cancelled_denom)                                AS core_quality_cancel_common_denominator_count
  FROM exposure
  LEFT JOIN core_quality_events
    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)
    AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)
  WHERE 1 = 1
  GROUP BY ALL
)

/* Aggregate to treatment/control level */
SELECT
  experiment_group,

  /* shared denominator */
  SUM(core_quality_cancel_common_denominator)        AS cancel_denominator,
  SUM(core_quality_cancel_common_denominator_count)  AS cancel_denominator_count,

  /* numerators by cancel type */
  SUM(core_quality_cancelled_core_numerator)         AS cancelled_core_numerator,
  SUM(core_quality_mx_induced_cancel_numerator)      AS mx_induced_cancel_numerator,
  SUM(core_quality_cx_induced_cancel_numerator)      AS cx_induced_cancel_numerator,
  SUM(core_quality_logistics_induced_cancel_numerator) AS logistics_induced_cancel_numerator,
  SUM(core_quality_pos_error_induced_cancel_numerator) AS pos_error_induced_cancel_numerator,
  SUM(core_quality_ottl_cancel_numerator)            AS ottl_cancel_numerator,
  SUM(core_quality_store_closed_cancel_numerator)    AS store_closed_cancel_numerator,
  SUM(core_quality_pos_proactive_cancel_numerator)   AS pos_proactive_cancel_numerator,
  SUM(core_quality_item_unavailable_cancel_numerator) AS item_unavailable_cancel_numerator,
  SUM(core_quality_wrong_dx_handoff_cancel_numerator) AS wrong_dx_handoff_cancel_numerator
FROM core_quality_events_NonWindow_exp_metrics
GROUP BY experiment_group
ORDER BY experiment_group;


WITH exposure AS (
  SELECT
    *
  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures
), core_quality_events AS (
  SELECT
    *
  FROM metrics_repo.public.core_quality_events AS core_quality_events
  WHERE
    CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)
)

select *
FROM exposure
  LEFT JOIN core_quality_events
    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)
    AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)
    where is_pos_error_induced_cancel_category = 1 and experiment_group = 'treatment' and segment = 'All Users IOS';





-- Replace YOUR_HOURS_TABLE with your table (columns: menu_id, store_id, day_of_week, start_time, end_time)
-- Uses proddb.public.dimension_deliveries for cancelled orders and local timezone conversion

create or replace table proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders as (

WITH exposure AS (
  SELECT
    *
  FROM METRICS_REPO.PUBLIC.special_menu_default_selection___All_Users_IOS_exposures
), core_quality_events AS (
  SELECT
    *
  FROM metrics_repo.public.core_quality_events AS core_quality_events
  WHERE
    CAST(core_quality_events.event_ts AS DATETIME) BETWEEN CAST('2025-09-16T00:00:00' AS DATETIME) AND CAST('2025-10-30T00:00:00' AS DATETIME)
),
deliveries as (
select distinct delivery_id, experiment_group
FROM exposure
  LEFT JOIN core_quality_events
    ON TO_CHAR(exposure.bucket_key) = TO_CHAR(core_quality_events.device_id)
    AND exposure.first_exposure_time <= DATEADD(SECOND, 10, core_quality_events.event_ts)
    where is_pos_error_induced_cancel_category = 1 and segment = 'All Users IOS'
),

cancels AS (
  SELECT
    dd.delivery_id, dd.delivery_uuid, d.experiment_group,
    dd.store_id,
    dd.menu_id,
    dd.store_name,
    CONVERT_TIMEZONE('UTC', dd.timezone, COALESCE(dd.actual_order_place_time, dd.created_at)) AS local_ts,
    TO_TIME(CONVERT_TIMEZONE('UTC', dd.timezone, COALESCE(dd.actual_order_place_time, dd.created_at))) AS local_time,
    UPPER(TO_CHAR(CONVERT_TIMEZONE('UTC', dd.timezone, COALESCE(dd.actual_order_place_time, dd.created_at)), 'DY')) AS local_day_abbrev,
    actual_order_place_time,
    coalesce(
      dd.IS_SUBSCRIPTION_DISCOUNT_APPLIED,
      dd.FIRST_DELIVERY_DISCOUNT,
      dd.SNAP_EBT_DISCOUNT,
      dd.UPSELL_DELIVERY_DISCOUNT,
      dd.RETAIL_LARGE_ORDER_SERVICE_FEE_DISCOUNT,
      dd.RETAIL_LARGE_ORDER_DELIVERY_FEE_DISCOUNT,
      dd.SNAP_EBT_DISCOUNT_LOCAL,
      0
    ) as discount
  FROM proddb.public.dimension_deliveries dd

  inner join deliveries d
    on dd.delivery_id = d.delivery_id
  WHERE dd.cancelled_at IS NOT NULL
    AND COALESCE(dd.actual_order_place_time, dd.created_at) IS NOT NULL
    and actual_order_place_time>='2025-09-16T00:00:00'
),
hours_norm AS (
  SELECT
    menu_id,
    store_id,
    /* normalize hours: replace 24:00:00 safely */
    CAST(CASE WHEN start_time IN ('24:00:00','24:00') THEN '00:00:00' ELSE start_time END AS TIME) AS start_time_norm,
    CAST(CASE WHEN end_time   IN ('24:00:00','24:00') THEN '23:59:59' ELSE end_time   END AS TIME) AS end_time_norm,
    UPPER(day_of_week) AS day_of_week_abbrev
  FROM edw.merchant.dimension_menu_open_hours

),
joined AS (
  SELECT
    c.delivery_id, c.delivery_uuid, c.experiment_group,
    c.store_id,
    c.menu_id,
    c.store_name,
    c.local_ts,
    c.local_time,
    h.start_time_norm,
    h.end_time_norm,
    c.actual_order_place_time,
    c.discount,
    /* same-day vs wraps-past-midnight window */
    CASE
      WHEN h.start_time_norm <= h.end_time_norm
        THEN (c.local_time BETWEEN h.start_time_norm AND h.end_time_norm)
      ELSE (c.local_time >= h.start_time_norm OR c.local_time <= h.end_time_norm)
    END AS within_hours
  FROM cancels c
  INNER JOIN hours_norm h
    ON h.store_id = c.store_id
   AND h.menu_id  = c.menu_id
   AND h.day_of_week_abbrev = c.local_day_abbrev
)
SELECT *
FROM joined
ORDER BY local_ts);

select b.experiment_group, is_active,count(1)
from edw.merchant.dimension_menu a
inner join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders b 
on a.store_id = b.store_id and a.menu_id = b.menu_id
where b.within_hours = true
group by all
;


select a.*
from edw.merchant.dimension_menu a
inner join (select distinct store_id, menu_id from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders where within_hours = true) b 
on a.store_id = b.store_id and a.menu_id = b.menu_id
where is_active = false
group by all
;


SELECT
  within_hours,
  cnt,
  SUM(cnt) OVER () AS total,
  ROUND(cnt * 100.0 / NULLIF(SUM(cnt) OVER (), 0), 2) AS pct
FROM (
  SELECT within_hours, COUNT(*) AS cnt
  FROM proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders
  GROUP BY within_hours
)
ORDER BY within_hours DESC;

select * from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders;



select revenue_date::date,  count(*) 
from public.dispatch_error 

where revenue_date > '2025-09-17' 
  and category='proactive_cancel'  

group by 1
order by 1;


SELECT *
FROM fact_delivery_allocation a
inner join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders b
on a.delivery_id = b.delivery_id
where active_date>='2025-09-17'
;
create or replace table proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason as (

select c.*, b.bucket, b.provider, b.error_message, b.error_regex
from DOORDASH_POINTOFSALE.PUBLIC.MAINDB_ORDER a
join proddb.public.pos_failed_orders b on a.order_id = b.delivery_id

join dimension_deliveries dd on dd.delivery_id=b.delivery_id and dd.active_date between '2025-09-15' and '2025-09-27'
join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders c on dd.delivery_id = c.delivery_id

where a.CREATED_AT between '2025-09-15' and '2025-09-27'
and b.provider not ilike '%staging%'
and b.provider not ilike '%sandbox%'
  and dd.is_test = false
  and dd.active_date is not null
  and dd.is_from_store_to_us = false
  and dd.fulfillment_type not in ('merchant_fleet', 'shipping')
  and dd.source is distinct
from
  'ad_shadow'
);
select discount,within_hours, count(1) from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason a
inner join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores b
on a.store_id = b.store_id
group by all;
select bucket, count(1) from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason 

group by all;

select * from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason
where bucket in ('Item Unavailable', 'Invalid Order', 'Other') 
and store_name = 'Burger King (24787)'
;

-- where bucket = 'Item Unavailable';
select experiment_group, count(1) from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason 
where bucket in ('Item Unavailable', 'Invalid Order', 'Other') group by all;

create or replace table proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores as (

select store_id, store_name,sum(case when experiment_group ='treatment' then cnt else 0 end) as treatment_cnt, 
sum(case when experiment_group ='control' then cnt else 0 end) as control_cnt,
sum(case when experiment_group ='treatment' then cnt else 0 end) - sum(case when experiment_group ='control' then 1 else 0 end) as diff

from (
select  experiment_group, store_id,store_name,count(1) cnt from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason 

where bucket in ('Item Unavailable', 'Invalid Order', 'Other')
group by all 
) group by all having diff>0);
select * from  proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores;
select * from edw.merchant.dimension_store a 
inner join (select distinct store_id, store_name from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores) b 
on a.store_id = b.store_id
;

select deck_rank, count(1) from (
  select * from edw.merchant.dimension_store a 
inner join (select distinct store_id, store_name from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores) b 
on a.store_id = b.store_id
) 
group by all;


with base as (
  select
    bucket,
    experiment_group,
    count(1) as cnt
  from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason
  group by all
),
normalized as (
  select
    bucket,
    case
      when lower(experiment_group) like 'treat%' then 'treatment'
      when lower(experiment_group) like 'control%' then 'control'
      else null
    end as experiment_group,
    cnt
  from base
  where experiment_group is not null
),
group_totals as (
  select
    experiment_group,
    sum(cnt) as total_n
  from normalized
  group by all
),
shares as (
  select
    n.bucket,
    n.experiment_group,
    n.cnt,
    g.total_n,
    n.cnt / nullif(g.total_n, 0) as share
  from normalized n
  join group_totals g using (experiment_group)
)
select
  bucket,
  coalesce(max(case when experiment_group = 'treatment' then share end), 0) as treatment_share,
  coalesce(max(case when experiment_group = 'control' then share end), 0) as control_share,
  coalesce(max(case when experiment_group = 'treatment' then share end), 0)
    - coalesce(max(case when experiment_group = 'control' then share end), 0) as share_diff,
  abs(
    coalesce(max(case when experiment_group = 'treatment' then share end), 0)
    - coalesce(max(case when experiment_group = 'control' then share end), 0)
  ) as abs_share_diff,
  coalesce(max(case when experiment_group = 'treatment' then cnt end), 0) as treatment_n,
  coalesce(max(case when experiment_group = 'control' then cnt end), 0) as control_n
from shares
group by all
order by abs_share_diff desc;





create or replace table proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_items as (
with deliveries as (
  select *
  from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason
),
items as (
  select
    d.*,
    doi.item_id,
    doi.item_name,
    -- doi.store_id,
    -- doi.store_name,
    doi.order_item_quantity as quantity,
    doi.unit_price as unit_price_cents,
    (doi.unit_price * doi.order_item_quantity) as item_subtotal_cents
  from deliveries d
  join proddb.public.dimension_order_item doi
    on doi.delivery_id = d.delivery_id
  where doi.created_at between '2025-09-15' and '2025-09-27'
)
select
  *,
  item_subtotal_cents / 100.0 as item_subtotal,
  sum(item_subtotal_cents) over (partition by delivery_id) / 100.0 as delivery_gov
from items
order by delivery_id, item_name);


create or replace table proddb.fionafan.special_menu_item_unavailable as (
select delivery_id,max(item_not_in_Menu) as item_not_in_Menu from (
select
  a.*,
  case when b.item_id is null then 1 else 0 end as item_not_in_Menu
from proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_items a
left join edw.merchant.dimension_menu_item b
  on a.item_id = b.item_id
 and a.menu_id = b.menu_id
) 
group by all
);

select * from edw.merchant.dimension_menu_item where menu_id = '18469951' and item_id = '6495959049';


select within_hours,discount, item_not_in_Menu, count(1) from (
select distinct c.*,a.item_not_in_Menu from proddb.fionafan.special_menu_item_unavailable a 

inner join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason c
on a.delivery_id = c.delivery_id
inner join proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_stores b  
on c.store_id = b.store_id
) group by all;


select * from 
proddb.fionafan.special_menu_default_selection_pos_error_induced_cancel_orders_reason_top_items where delivery_id = '202095493227';


select * from proddb.fionafan.document_index_community;

select * from proddb.fionafan.chunk_index_community;