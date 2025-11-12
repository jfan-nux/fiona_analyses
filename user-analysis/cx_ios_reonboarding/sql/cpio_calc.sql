-- Create table with campaign names
CREATE OR REPLACE TEMP TABLE ep_names_list AS
SELECT * FROM (
  SELECT 1 AS ep_id, 'ep_consumer_dormant_late_bloomers_us_v1' AS ep_name UNION ALL
  SELECT 2, 'ep_consumer_dormant_winback_us_v1' UNION ALL
  SELECT 3, 'ep_consumer_dewo_phase2_us_v1' UNION ALL
  SELECT 4, 'ep_consumer_dewo_phase3_us_v1' UNION ALL
  SELECT 5, 'ep_consumer_dewo_phase1_retarget_us_v1' UNION ALL
  SELECT 6, 'ep_consumer_ml_churn_prevention_us_v1_p1_active' UNION ALL
  SELECT 7, 'ep_consumer_ml_churn_prevention_us_v1_p1_dormant' UNION ALL
  SELECT 8, 'ep_consumer_ml_churn_prevention_us_v1_p2_active_active' UNION ALL
  SELECT 9, 'ep_consumer_ml_churn_prevention_us_v1_p2_active_dormant' UNION ALL
  SELECT 10, 'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_active' UNION ALL
  SELECT 11, 'ep_consumer_ml_churn_prevention_us_v1_p2_dormant_dormant' UNION ALL
  SELECT 12, 'ep_consumer_dormant_churned_browsers_us_v1' UNION ALL
  SELECT 13, 'ep_consumer_enhanced_rxauto_90d_us_v1' UNION ALL
  SELECT 14, 'ep_consumer_enhanced_rxauto_120d_test_us_v1' UNION ALL
  SELECT 15, 'ep_consumer_enhanced_rxauto_150day_test_us_v1' UNION ALL
  SELECT 16, 'ep_consumer_enhanced_rxauto_180day_test_us_v1' UNION ALL
  SELECT 17, 'ep_consumer_churned_btm_pickup_exclude_test_us_v1' UNION ALL
  SELECT 18, 'ep_consumer_churned_latebloomers_auto_ctc_test_us' UNION ALL
  SELECT 19, 'ep_consumer_rx_reachability_auto_us' UNION ALL
  SELECT 20, 'ep_consumer_repeatchurned_us' UNION ALL
  SELECT 21, 'ep_consumer_very_churned_med_vp_us_v1' UNION ALL
  SELECT 22, 'ep_consumer_super_churned_low_vp_us_v1' UNION ALL
  SELECT 23, 'ep_consumer_super_churned_med_vp_us_v1' UNION ALL
  SELECT 24, 'ep_consumer_churned_low_vp_us_v1' UNION ALL
  SELECT 25, 'ep_consumer_churned_med_vp_us_v1'
);

-- create or replace temp table campaign_names as (
-- select distinct a.campaign_name, promo_codes, campaign_variant
-- from edw.consumer.campaign_analyzer_campaigns_raw a
-- -- inner join ep_names_list b
-- --     on lower(a.campaign_variant::varchar) = lower(b.campaign_name::varchar)
-- where exists (
--   select 1 
--   from ep_names_list cnl
--   where a.campaign_variant ilike '%' || cnl.campaign_name || '%'
-- )
-- );

-- can be read from fionafan.campaign_analyzer_exposures_all
create or replace temp table campaign_analyzer_exposures_all as (
select cae.*
from edw.consumer.campaign_analyzer_exposures cae
where exposure_date between '2025-09-08' and '2025-11-03' 
and exists (
  select 1 
  from ep_names_list cnl
  where cae.campaign_name ilike '%' || cnl.ep_name || '%'
)
);
CREATE OR REPLACE TEMP TABLE exposure_all AS (

  SELECT DISTINCT
    ee.tag,
    ee.result,
    -- somtimes the bucketkey is device_id, in which case use custom_attributes:userId as the consuemr_id
    -- ee.bucket_key::varchar AS dd_device_id,
    custom_attributes:userId as consumer_id,
    
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date) AS day,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)) as ts
  FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
  WHERE ee.experiment_name = 'cx_ios_reonboarding'
    -- AND ee.experiment_version::INT = 1
    AND ee.tag <> 'overridden'
    AND convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)
        BETWEEN '2025-09-08' AND '2025-11-03'
  GROUP BY 1,2,3
);


create or replace temp table exposure_all_new as (

select e.*, c.consumer_bucket, c.CAMPAIGN_NAME, c.campaign_promo_codes
from exposure_all e
inner join fionafan.campaign_analyzer_exposures_all c
    on e.consumer_id::varchar = c.consumer_id::varchar
);

-- Create temp table with list of promo codes
create or replace temp table promo_codes_list as (
select distinct
  f.value::varchar as promo_code
from exposure_all_new e,
  lateral flatten(input => e.campaign_promo_codes) f
where e.campaign_promo_codes is not null
  and array_size(e.campaign_promo_codes) > 0
);


create or replace temp table checkout_all_new as (

    SELECT e.tag
        , e.consumer_id
        , e.day AS exposure_date
        , case when e.consumer_bucket ilike '%control%' then 'control' else 'treatment' end as consumer_bucket
        , dd.delivery_ID
        , dd.variable_profit *0.01 as VP
        , dd.active_date
        , p.delivery_id as promo_delivery_id
        , coalesce(p.fda_other_promotions_base,0)/100.0 as total_discount_amount
    FROM exposure_all_new e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-09-08' AND '2025-11-03'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    -- AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND promo_code in (select promo_code from promo_codes_list)
);

select tag, consumer_bucket, count(distinct consumer_id) total_consumers , count(distinct delivery_id) total_orders
, sum(VP) as total_VP
,count(distinct promo_delivery_id) as promo_delivery_id_count
,count(distinct case when promo_delivery_id is not null then consumer_id end) as promo_consumers_count
,sum(total_discount_amount) as total_discount_amount
,count(distinct case when promo_delivery_id is not null then vp end) as promo_order_vp

from checkout_all_new group by all;

CREATE OR REPLACE TEMP TABLE VP_per_Cx_all AS
SELECT tag
        , consumer_id
        , consumer_bucket
        , SUM(VP) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP_per_cx
FROM checkout_all_new
GROUP BY 1,2,3
;
CREATE OR REPLACE TEMP TABLE VP_all AS
SELECT tag, consumer_bucket
        , SUM(D28_VP_per_cx) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP
FROM VP_per_Cx_all
GROUP BY 1,2
;
CREATE OR REPLACE TEMP TABLE D28_OR_all AS
SELECT tag, consumer_bucket
       , COUNT(DISTINCT DELIVERY_ID) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_OR
FROM checkout_all_new
GROUP BY 1,2
;
--------- Performance of All exposure
CREATE OR REPLACE TEMP TABLE overall_perf AS

SELECT o.tag , o.consumer_bucket
        , o.d28_or AS d28_or_overall
        , D28_VP AS D28_VP_overall
FROM D28_OR_all o
LEFT JOIN VP_all v
    ON o.tag = v.tag
    AND o.consumer_bucket = v.consumer_bucket
--    AND o.wk = v.wk
ORDER BY 1,2
;
CREATE OR REPLACE TEMP TABLE overall_cpio AS
SELECT p1.*
        , (p1.D28_VP_overall - p2.D28_VP_overall) / -(p1.d28_or_overall - p2.d28_or_overall) AS d28_CPIO_overall
FROM overall_perf p1
LEFT JOIN overall_perf p2
--    ON p1.wk = p2.wk
    ON p1.consumer_bucket != p2.consumer_bucket
    AND p1.consumer_bucket <>'control'
    and p1.tag = p2.tag
ORDER BY 1, 2
;
SELECT * FROM overall_perf;
select * from overall_cpio order by 1,2;

-- ====================================================================
-- CTE VERSION (same logic as temp tables above)
-- ====================================================================
with VP_per_Cx_all as (
  SELECT tag
          , consumer_id
          , consumer_bucket
          , SUM(VP) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP_per_cx
  FROM checkout_all_new
  GROUP BY 1,2,3
),
VP_all as (
  SELECT tag, consumer_bucket
          , SUM(D28_VP_per_cx) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP
  FROM VP_per_Cx_all
  GROUP BY 1,2
),
D28_OR_all as (
  SELECT tag, consumer_bucket
         , COUNT(DISTINCT DELIVERY_ID) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_OR
  FROM checkout_all_new
  GROUP BY 1,2
),
--------- Performance of All exposure
overall_perf as (
  SELECT o.tag , o.consumer_bucket
          , o.d28_or AS d28_or_overall
          , D28_VP AS D28_VP_overall
  FROM D28_OR_all o
  LEFT JOIN VP_all v
      ON o.tag = v.tag
      AND o.consumer_bucket = v.consumer_bucket
  --    AND o.wk = v.wk
)
select * from overall_perf order by tag, consumer_bucket;

