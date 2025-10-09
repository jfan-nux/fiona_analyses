EDW.CONSUMER.EVENT_ATTRIBUTION

tyleranderson.sessions

tyleranderson.events_all


tyleranderson.store_impressions ;


select * from tyleranderson.events_all where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';

select * from tyleranderson.store_impressions where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';

select * from tyleranderson.sessions where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';


select source_page, page, source, preference_type, count(1) cnt, count(distinct consumer_id) cc from iGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where iguazu_timestamp>= '2025-08-25' group by all;


select source_page, page, source, preference_type, entity_id, entity_type from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' limit 100 ;

select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' and consumer_id = '1964336184' limit 100 ;
with base as (select consumer_id, entity_id, count(1) cnt 
from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' group by all)
select count(1), sum(case when cnt>1 then 1 else 0 end) as multiple_cnt
from base;
select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where consumer_id = '1965216620' and entity_id = 'chinese'  limit;

-- Most selected items (entities) on onboarding preference page
with latest_adds as (
  select consumer_id, entity_id
  from (
    select *, row_number() over(partition by consumer_id, entity_id order by iguazu_event_time desc) as rn
    from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE
    where page = 'onboarding_preference_page'
  ) where rn = 1 and toggle_type = 'add'
)
select entity_id, count(distinct consumer_id) as consumers_selected,
       count(1) as selections
from latest_adds
group by all
order by consumers_selected desc, selections desc
limit 50;

create or replace table proddb.fionafan.preference_entity_cnt_distribution as (
with consumer_entity as( 
SELECT consumer_id, entity_id, toggle_type, page
FROM (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
  FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 
  where page = 'onboarding_preference_page' and iguazu_timestamp >= '2025-08-04'
) WHERE rn = 1 and toggle_type = 'add' 
)
, second_base as (
select consumer_id, page, count(distinct entity_id) entity_cnt
from consumer_entity
group by all
)
select * from second_base
);
select * from proddb.fionafan.preference_entity_cnt_distribution where entity_cnt>10 limit 10;

select * from iguazu.consumer.m_preference_toggle_ice where page = 'onboarding_preference_page' and consumer_id = '1125900330361742';
-- select 
--     page, 
--     entity_cnt,
--     count(1) cnt,
--     round(100.0 * count(1) / sum(count(1)) over (partition by page), 2) as pct
-- from second_base 
-- group by all 
-- order by all;




;
select * from proddb.fionafan.document_index_community limit 10;




EDW.CONSUMER.EVENT_ATTRIBUTION

tyleranderson.sessions

tyleranderson.events_all


tyleranderson.store_impressions ;


select * from tyleranderson.events_all where event_date = '2025-08-25' and user_id = '1125900281188900' limit 10;

select * from tyleranderson.store_impressions where event_date = '2025-08-25'and user_id = '1125900281188900';

select * from tyleranderson.sessions where event_date = '2025-08-25' and user_id = '1125900281188900';


select source_page, page, source, preference_type, count(1) cnt, count(distinct consumer_id) cc from iGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where iguazu_timestamp>= '2025-08-25' group by all;


select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' limit 100 ;

select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' and consumer_id = '1964336184' limit 100 ;
with base as (select consumer_id, entity_id, count(1) cnt 
from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' group by all)
select count(1), sum(case when cnt>1 then 1 else 0 end) as multiple_cnt
from base;
select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where consumer_id = '1965216620' and entity_id = 'chinese' limit 10;





with consumer_entity as( 
SELECT consumer_id, entity_id, toggle_type, page
FROM (
  SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
  FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 
) WHERE rn = 1 and toggle_type = 'add' 
)
, second_base as (
select consumer_id, page, count(distinct entity_id) entity_cnt
from consumer_entity
group by all
)
select page, avg(entity_cnt) avg_entity_cnt, count(distinct consumer_id) cnt from second_base group by all order by all;
-- select 
--     page, 
--     entity_cnt, 
--     count(1) cnt,
--     round(100.0 * count(1) / sum(count(1)) over (partition by page), 2) as pct
-- from second_base 
-- group by all 
-- order by all;


iguazu.consumer.m_onboarding_start_promo_page_view_ice

iguazu.consumer.m_onboarding_start_promo_page_click_ice
iguazu.consumer.M_onboarding_page_view_ice (filter on page = '')
iguazu.consumer.M_onboarding_page_click_ice (filter on page = '')
iguazu_consumer.m_onboarding_end_promo_page_view_ice
iguazu_consumer.m_onboarding_end_promo_page_click_ice;


segment_events_raw.consumer_production.m_att_system_tracking_authorized

segment_events_raw.consumer_production.m_att_system_tracking_declined

segment_events_raw.consumer_production.m_att_description_view_appear

segment_events_raw.consumer_production.m_att_description_view_allow_button_tap;

select context_device_id, min (convert_timezone(context_timezone,'America/Los_Angeles',sent_at)) pst_exposure_time
from segment_events_raw.consumer_production.m_att_system_tracking_declined 
where sent_at between '2025-08-04' and '2025-09-30' 
group by all limit 10;


select context_device_id, count(1) cnt from segment_events_raw.consumer_production.m_att_system_tracking_authorized 

group by all having cnt>1 limit 10;


select * from segment_events_raw.consumer_production.m_att_system_tracking_authorized  where context_device_id = 'AC63506D-2EC6-405C-A3BF-C9015F3B6F07' limit 10;

select distinct context_timezone from segment_events_raw.consumer_production.m_att_description_view_allow_button_tap ;



-- ===============================================================================================
-- SUMMARY: TREATMENT EXPRESSION, MATCHED VIEWS, REVENUE SHARE, AND CONVERSION LIFT
-- -----------------------------------------------------------------------------------------------
-- Outputs one-row summary with:
-- - total_treatment_users
-- - users_expressed_interest
-- - pct_revenue_from_expressed (of total treatment GOV)
-- - pct_of_expressed_with_match_views
-- - conv_rate_expressed_with_match
-- - conv_rate_expressed_no_match
-- - conversion_rate_ratio_with_vs_without_match
-- Relies on prepared tables in preference_scope.sql
--   proddb.fionafan.preference_experiment_data
--   proddb.fionafan.preference_toggle_ice_latest
--   proddb.fionafan.experiment_preference_m_card_view
--   proddb.fionafan.experiment_preference_orders
WITH treatment_users AS (
  SELECT DISTINCT
    ped.dd_device_ID_filtered,
    ped.user_id,
    ped.exposure_time,
    CASE WHEN pt.entity_ids IS NOT NULL AND pt.entity_ids <> '' THEN 1 ELSE 0 END AS expressed_interest,
    pt.entity_ids
  FROM proddb.fionafan.preference_experiment_data ped
  LEFT JOIN proddb.fionafan.preference_toggle_ice_latest pt
    ON ped.dd_device_ID_filtered = replace(lower(CASE WHEN pt.dd_device_id like 'dx_%' then pt.dd_device_id
                      else 'dx_'||pt.dd_device_id end), '-')
  WHERE ped.tag <> 'control'
),
orders_by_user AS (
  SELECT 
    dd_device_ID_filtered,
    MAX(CASE WHEN delivery_id IS NOT NULL THEN 1 ELSE 0 END) AS has_order,
    SUM(gov) AS user_gov
  FROM proddb.fionafan.experiment_preference_orders
  GROUP BY dd_device_ID_filtered
),
matched_views AS (
  SELECT 
    pemcv.dd_device_ID_filtered,
    MAX(CASE 
          WHEN pemcv.entity_ids IS NOT NULL AND pemcv.entity_ids <> '' AND (
               LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
            OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
            OR LOWER(pemcv.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
          ) THEN 1 ELSE 0 END) AS has_match_view
  FROM proddb.fionafan.experiment_preference_events pemcv --proddb.fionafan.experiment_preference_events proddb.fionafan.preference_experiment_m_card_view
  LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id
  GROUP BY pemcv.dd_device_ID_filtered
),
joined AS (
  SELECT 
    tu.dd_device_ID_filtered,
    tu.user_id,
    tu.expressed_interest,
    COALESCE(mv.has_match_view, 0) AS has_match_view,
    COALESCE(obu.has_order, 0) AS has_order,
    COALESCE(obu.user_gov, 0) AS user_gov
  FROM treatment_users tu
  LEFT JOIN matched_views mv ON tu.dd_device_ID_filtered = mv.dd_device_ID_filtered
  LEFT JOIN orders_by_user obu ON tu.dd_device_ID_filtered = obu.dd_device_ID_filtered
),
aggregates AS (
  SELECT 
    COUNT(DISTINCT dd_device_ID_filtered) AS treatment_users,
    COUNT(DISTINCT CASE WHEN expressed_interest = 1 THEN dd_device_ID_filtered END) AS expressed_interest_users,
    SUM(user_gov) AS total_revenue_treatment,
    SUM(CASE WHEN expressed_interest = 1 THEN user_gov ELSE 0 END) AS revenue_from_expressed,
    COUNT(DISTINCT CASE WHEN expressed_interest = 1 AND has_match_view = 1 THEN dd_device_ID_filtered END ) AS expressed_users_with_match_view,
    COUNT(DISTINCT CASE WHEN expressed_interest = 1 AND has_match_view = 0 THEN dd_device_ID_filtered END) AS expressed_users_without_match_view,
    AVG(CASE WHEN expressed_interest = 1 AND has_match_view = 1 THEN has_order END) AS conv_rate_expressed_with_match,
    AVG(CASE WHEN expressed_interest = 1 AND has_match_view = 0 THEN has_order END) AS conv_rate_expressed_no_match,
    AVG(CASE WHEN expressed_interest = 1 THEN has_match_view END) AS pct_expressed_with_match
  FROM joined
)
SELECT 
  treatment_users AS total_treatment_users,
  expressed_interest_users AS users_expressed_interest,
  expressed_interest_users/treatment_users AS pct_expressed_interest_users,
  ROUND(revenue_from_expressed * 100.0 / NULLIF(total_revenue_treatment, 0), 2) AS pct_revenue_from_expressed,
  ROUND(pct_expressed_with_match * 100.0, 2) AS pct_of_expressed_with_match_views,
  expressed_users_with_match_view,
  expressed_users_without_match_view,
  ROUND(expressed_users_with_match_view * 100.0 / NULLIF(expressed_interest_users, 0), 2) AS pct_expressed_users_with_match_view,
  ROUND(expressed_users_without_match_view * 100.0 / NULLIF(expressed_interest_users, 0), 2) AS pct_expressed_users_without_match_view,
  conv_rate_expressed_with_match,
  conv_rate_expressed_no_match,
  ROUND(conv_rate_expressed_with_match / NULLIF(conv_rate_expressed_no_match, 0), 3) AS conversion_rate_ratio_with_vs_without_match
FROM aggregates;


select tag, count(1)FROM proddb.fionafan.preference_experiment_data group by all;


-- ===============================================================================================
-- DIFF: Users with match in experiment_preference_events (m_card_view) but NOT
--       in preference_experiment_m_card_view (sanity examples)
-- ===============================================================================================
WITH epe_views AS (
  SELECT 
    epe.dd_device_ID_filtered,
    epe.user_id,
    epe.event_timestamp,
    epe.store_id,
    epe.store_name,
    epe.entity_ids,
    ds.primary_category_name,
    ds.primary_tag_name,
    ds.cuisine_type,
    CASE 
      WHEN epe.entity_ids IS NOT NULL AND epe.entity_ids <> '' AND (
           LOWER(epe.entity_ids) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
        OR LOWER(epe.entity_ids) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
        OR LOWER(epe.entity_ids) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
      ) THEN 1 ELSE 0 END AS is_match
  FROM proddb.fionafan.experiment_preference_events epe
  LEFT JOIN edw.merchant.dimension_store ds ON epe.store_id = ds.store_id
  WHERE epe.tag <> 'control'
    AND epe.entity_ids IS NOT NULL AND epe.entity_ids <> ''
    AND epe.event = 'm_card_view' -- compare view-to-view
),
pemcv_matches AS (
  SELECT DISTINCT pemcv.dd_device_ID_filtered, pref.entity_ids
  FROM proddb.fionafan.preference_experiment_m_card_view pemcv
  LEFT join proddb.fionafan.preference_toggle_ice_latest pref on pemcv.dd_device_ID_filtered = replace(lower(CASE WHEN pref.dd_device_id like 'dx_%' then pref.dd_device_id
                      else 'dx_'||pref.dd_device_id end), '-')

  LEFT JOIN edw.merchant.dimension_store ds ON pemcv.store_id = ds.store_id
  WHERE pemcv.tag <> 'control'
    AND coalesce(pemcv.entity_ids, pref.entity_ids) IS NOT NULL AND coalesce(pemcv.entity_ids, pref.entity_ids) <> ''
    AND (
         LOWER(coalesce(pemcv.entity_ids, pref.entity_ids)) LIKE '%' || LOWER(ds.primary_category_name) || '%' AND ds.primary_category_name IS NOT NULL
      OR LOWER(coalesce(pemcv.entity_ids, pref.entity_ids)) LIKE '%' || LOWER(ds.primary_tag_name) || '%' AND ds.primary_tag_name IS NOT NULL
      OR LOWER(coalesce(pemcv.entity_ids, pref.entity_ids)) LIKE '%' || LOWER(ds.cuisine_type) || '%' AND ds.cuisine_type IS NOT NULL
    )
)
SELECT 
  v.dd_device_ID_filtered,
  v.user_id,
  v.event_timestamp,
  v.store_id,
  v.store_name,
  coalesce(v.entity_ids, m.entity_ids) as entity_ids,
  v.primary_category_name,
  v.primary_tag_name,
  v.cuisine_type
FROM epe_views v
LEFT JOIN pemcv_matches m
  ON v.dd_device_ID_filtered = m.dd_device_ID_filtered
WHERE v.is_match = 1
  AND m.dd_device_ID_filtered IS NULL
  and v.dd_device_ID_filtered in (select dd_device_ID_filtered from proddb.fionafan.preference_experiment_m_card_view)
ORDER BY v.event_timestamp DESC
LIMIT 100;

select min (event_timestamp), max (event_timestamp) from proddb.fionafan.preference_experiment_m_card_view;
select * from proddb.fionafan.preference_experiment_m_card_view where dd_device_ID_filtered = 'dx_72e72e5037f142f0b4332d925c818e68';

select * from proddb.fionafan.experiment_preference_events where dd_device_ID_filtered = 'dx_72e72e5037f142f0b4332d925c818e68';

select * from  proddb.fionafan.preference_toggle_ice_latest where consumer_id = '1125900341545480';

select tag, count(distinct dd_device_ID_filtered), count(distinct case when entity_ids is not null then dd_device_ID_filtered else null end) from proddb.fionafan.preference_experiment_m_card_view group by all;

