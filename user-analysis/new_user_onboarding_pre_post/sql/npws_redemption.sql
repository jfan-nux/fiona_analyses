
----------CPIO
--------------------- experiment exposure (consumer-level, filtered to consumers in start_page_view)
CREATE OR REPLACE TEMP TABLE start_page_view AS (
  -- the filter universe: consumers who viewed the onboarding start promo page in the window
  SELECT DISTINCT
    consumer_id::varchar AS consumer_id,
    CAST(iguazu_timestamp AS DATE) AS day
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-10-15' AND '2025-10-28'
);

CREATE OR REPLACE TEMP TABLE exposure_all AS (

  SELECT DISTINCT 
    ee.tag,
    ee.result,
    ee.bucket_key::varchar AS consumer_id,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date) AS day,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)) as ts 
  FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
  INNER JOIN start_page_view spv
    ON ee.bucket_key::varchar = spv.consumer_id::varchar
  WHERE ee.experiment_name = 'enable_post_onboarding_in_consumer_targeting'
    AND ee.experiment_version::INT = 1
    AND ee.tag <> 'overridden'
    AND convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)
        BETWEEN '2025-10-15' AND '2025-10-28'
  GROUP BY 1,2,3
);
create or replace temp table exposure_all_new as (


select e.*, c.consumer_bucket, c.CAMPAIGN_NAME
from exposure_all e
inner join edw.consumer.campaign_analyzer_exposures c
    on e.consumer_id = c.consumer_id
    and c.CAMPAIGN_NAME ilike '%npws_redemption_window_q225%'
);
create OR REPLACE TEMP TABLE checkout_all AS ( 

SELECT  e.tag
        , e.consumer_id 
        , e.day AS exposure_date
        , case when e.consumer_bucket = 'npws_control' then 'control' else 'npws_treatment' end as consumer_bucket
        , dd.delivery_ID
        , dd.variable_profit *0.01 as VP
        , dd.active_date
FROM exposure_all_new e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
);
create OR REPLACE TEMP TABLE checkout_all_old AS ( 

SELECT  e.tag
        , e.consumer_id 
        , e.day AS exposure_date
        , dd.delivery_ID
        , dd.variable_profit *0.01 as VP
        , dd.active_date
        , p.delivery_id as promo_delivery_id
        , coalesce(p.fda_other_promotions_base,0)/100.0 as total_discount_amount
FROM exposure_all e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND promo_code ilike '%NEW40OFF%'
    -- AND p.campaign_or_promo_name ilike '%npws%'e
);

create or replace temp table checkout_all_new as (
    SELECT e.tag
        , e.consumer_id 
        , e.day AS exposure_date
        , case when e.consumer_bucket = 'npws_control' then 'control' else 'treatment' end as consumer_bucket
        , dd.delivery_ID
        , dd.variable_profit *0.01 as VP
        , dd.active_date
        , p.delivery_id as promo_delivery_id
        , coalesce(p.fda_other_promotions_base,0)/100.0 as total_discount_amount
    FROM exposure_all_new e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    -- AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND promo_code ilike '%NEW40OFF%'
);


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
select * from overall_cpio;




-------- Redemption (filtered to start_page_view)

SELECT e.tag
      , COUNT(DISTINCT e.consumer_id) as exposure
      , COUNT(DISTINCT p.delivery_id) as npws_redeems
      , npws_redeems / exposure as npws_redemption_rate
      , sum(coalesce(p.fda_other_promotions_base,0)) as fda_discount_amount
, sum(coalesce(p.discounts_total_amount,0)) as total_discount_amount
FROM exposure_all e
LEFT JOIN proddb.public.dimension_deliveries dd 
  ON e.consumer_id::varchar = dd.creator_id::varchar
  AND dd.is_filtered_core = 1
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) >= e.ts
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
--   AND p.promo_code IN ('NEW40OFF', 'HOT40OFF1', 'HOT50OFF1')
    AND p.campaign_or_promo_name ilike '%npws%'
WHERE e.tag NOT IN ('internal_test', 'reserved')
GROUP BY 1
ORDER BY 1;



SELECT e.tag
      , COUNT(DISTINCT e.consumer_id) as exposure
      , COUNT(DISTINCT p.delivery_id) as npws_redeems
      , npws_redeems / exposure as npws_redemption_rate
      , sum(coalesce(p.fda_other_promotions_base,0)) as fda_discount_amount
-- , sum(coalesce(p.discounts_total_amount,0)) as total_discount_amount
FROM exposure_all_new e

LEFT JOIN proddb.public.dimension_deliveries dd 
  ON e.consumer_id::varchar = dd.creator_id::varchar
  AND dd.is_filtered_core = 1
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) >= e.ts
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND p.promo_code IN ('NEW40OFF', 'HOT40OFF1', 'HOT50OFF1')
    -- AND p.campaign_or_promo_name ilike '%npws%'
WHERE e.tag NOT IN ('internal_test', 'reserved')
GROUP BY 1
ORDER BY 1;



CREATE OR REPLACE TEMP TABLE onboarding_top_of_funnel AS (
  -- the filter universe: consumers who viewed the onboarding start promo page in the window
  SELECT DISTINCT
    consumer_id::varchar AS consumer_id,
    CAST(iguazu_timestamp AS DATE) AS day
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-03-01' AND current_date
);

SELECT e.day
, count(distinct e.consumer_id) as onboarded_consumers
, count(distinct dd.delivery_id) as all_orders
, COUNT(DISTINCT p.delivery_id) as npws_redeems
, count(distinct case when dd.delivery_id is not null then e.consumer_id end) as consumers_with_orders
, count(distinct case when p.delivery_id is not null then e.consumer_id end) as consumers_with_npws_orders
, COUNT(DISTINCT p.delivery_id) / count(distinct e.consumer_id) as npws_redemption_rate
, sum(coalesce(p.fda_other_promotions_base,0)) as fda_discount_amount
, sum(coalesce(p.discounts_total_amount,0)) as total_discount_amount
FROM onboarding_top_of_funnel e
LEFT JOIN proddb.public.dimension_deliveries dd 
  ON e.consumer_id::varchar = dd.creator_id::varchar
  AND dd.is_filtered_core = 1
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) BETWEEN '2025-03-01' AND current_date
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) >= e.day
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p

  ON p.delivery_id = dd.delivery_id
--   AND p.promo_code IN ('NEW40OFF', 'HOT40OFF1', 'HOT50OFF1')
  AND promo_code ilike '%NEW40OFF%'
    -- AND p.campaign_or_promo_name ilike '%npws%'
GROUP BY 1
ORDER BY 1;


select CAMPAIGN_NAME, consumer_bucket, count(1) from edw.consumer.campaign_analyzer_exposures 

where CAMPAIGN_NAME ILIKE '%npws-fobt-interim-40off1%' group by all limit 10;

select CAMPAIGN_NAME, consumer_bucket, count(1) from edw.consumer.campaign_analyzer_exposures 

where CAMPAIGN_NAME = 'npws-fobt-interim-40off1' group by all limit 10;


select distinct consumer_bucket from exposure_all_new;
select tag, consumer_bucket
, count(distinct promo_delivery_id) as npws_redeems
, count(1) consumer_cnt, sum(total_discount_amount*100) total_discount_amount 
from checkout_all_new group by all order by all;
select tag, case when e.consumer_bucket = 'npws_control' then 'control' else 'treatment' end as consumer_bucket
, count(distinct e.consumer_id) consumer_cnt
, count(distinct p.delivery_id) as npws_redeems
, sum(coalesce(p.fda_other_promotions_base,0)) fda_discount_amount 
FROM exposure_all_new e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND promo_code ilike '%NEW40OFF%' group by all order by all;
SELECT e.tag,  case when e.consumer_bucket = 'npws_control' then 'control' else 'treatment' end as consumer_bucket
      , COUNT(DISTINCT e.consumer_id) as exposure
      , COUNT(DISTINCT p.delivery_id) as npws_redeems
      , npws_redeems / exposure as npws_redemption_rate
      , sum(coalesce(p.fda_other_promotions_base,0)) as fda_discount_amount
FROM exposure_all_new e
LEFT JOIN proddb.public.dimension_deliveries dd 
  ON e.consumer_id::varchar = dd.creator_id::varchar
  AND dd.is_filtered_core = 1
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
  AND convert_timezone('UTC','America/Los_Angeles', dd.created_at) >= e.ts
--   AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
LEFT JOIN proddb.public.fact_order_discounts_and_promotions_extended p
  ON p.delivery_id = dd.delivery_id
  AND promo_code ilike '%NEW40OFF%'
GROUP BY all
ORDER BY all;




---old query

CREATE OR REPLACE TEMP TABLE start_page_view AS (
  -- the filter universe: consumers who viewed the onboarding start promo page in the window
  SELECT DISTINCT
    consumer_id::varchar AS consumer_id,
    CAST(iguazu_timestamp AS DATE) AS day
  FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
  WHERE iguazu_timestamp BETWEEN '2025-10-15' AND '2025-10-27'
);

CREATE OR REPLACE TEMP TABLE exposure_all AS (
  SELECT DISTINCT 
    ee.tag,
    ee.result,
    ee.bucket_key::varchar AS consumer_id,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)::date) AS day,
    MIN(convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)) as ts 
  FROM PRODDB.PUBLIC.FACT_DEDUP_EXPERIMENT_EXPOSURE ee
  INNER JOIN start_page_view spv
    ON ee.bucket_key::varchar = spv.consumer_id::varchar
  WHERE ee.experiment_name = 'enable_post_onboarding_in_consumer_targeting'
    AND ee.experiment_version::INT = 1
    AND ee.tag <> 'overridden'
    AND convert_timezone('UTC','America/Los_Angeles', ee.EXPOSURE_TIME)
        BETWEEN '2025-10-15' AND '2025-10-27'
  GROUP BY 1,2,3
);
create or replace temp table exposure_all_new as (
select e.*, c.consumer_bucket
from exposure_all e
inner join edw.consumer.campaign_analyzer_exposures c
    on e.consumer_id = c.consumer_id
    and c.CAMPAIGN_NAME ilike '%npws_redemption_window_q225%'
);

create OR REPLACE TEMP TABLE checkout_all AS ( 

SELECT  e.tag
        , e.consumer_id 
        , e.day AS exposure_date
        -- , e.consumer_bucket
        , dd.delivery_ID
        , dd.variable_profit *0.01 as VP
        , dd.active_date
FROM exposure_all e
LEFT JOIN dimension_deliveries dd
ON e.consumer_id::varchar = dd.creator_id::varchar
    AND dd.is_filtered_core = 1
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) BETWEEN '2025-10-15' AND '2025-10-27'
    AND convert_timezone('UTC','America/Los_Angeles',dd.created_at) >= e.ts
    AND DATEDIFF('day',e.day,convert_timezone('UTC','America/Los_Angeles',dd.created_at)) BETWEEN 1 AND 28
);


CREATE OR REPLACE TEMP TABLE VP_per_Cx_all AS


SELECT tag
        , consumer_id
        -- , consumer_bucket
        , SUM(VP) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP_per_cx
FROM checkout_all
GROUP BY all
;     


CREATE OR REPLACE TEMP TABLE VP_all AS

SELECT tag
        , SUM(D28_VP_per_cx) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_VP
FROM VP_per_Cx_all
GROUP BY all
;    


CREATE OR REPLACE TEMP TABLE D28_OR_all AS
SELECT tag
       , COUNT(DISTINCT DELIVERY_ID) / NULLIF(COUNT(DISTINCT consumer_id),0) AS D28_OR
FROM checkout_all
GROUP BY all
;   


--------- Performance of All exposure
CREATE OR REPLACE TEMP TABLE overall_perf AS



SELECT o.tag 
        , o.d28_or AS d28_or_overall
        , D28_VP AS D28_VP_overall
FROM D28_OR_all o

LEFT JOIN VP_all v

    ON o.tag = v.tag
    -- AND o.consumer_bucket = v.consumer_bucket
    -- AND o.tag <>'control'
--    AND o.wk = v.wk
ORDER BY 1,2
;

CREATE OR REPLACE TEMP TABLE overall_cpio AS
SELECT p1.*
        , (p1.D28_VP_overall - p2.D28_VP_overall) / nullif((p1.d28_or_overall - p2.d28_or_overall),0) AS d28_CPIO_overall
FROM overall_perf p1
LEFT JOIN overall_perf p2
--    ON p1.wk = p2.wk
    -- ON p1.consumer_bucket != p2.consumer_bucket
    -- AND p1.consumer_bucket <>'npws_control'
    where 1=1
    and p1.tag ='variant_true'
    -- and p1.tag = p2.tag
ORDER BY 1, 2
;

SELECT * FROM overall_perf;
select * from overall_cpio;


select tag, case when consumer_bucket = 'npws_control' then 'control' else 'treatment' end as consumer_bucket,count(distinct consumer_id) from exposure_all_new group by  all;

select tag, count(distinct consumer_id) from exposure_all group by  all;


select 191666/333883;



select CAMPAIGN_NAME, consumer_bucket, count(1) 
from edw.consumer.campaign_analyzer_exposures where exposure_time between '2025-10-15' and '2025-10-27' group by all;
