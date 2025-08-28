EDW.CONSUMER.EVENT_ATTRIBUTION

tyleranderson.sessions

tyleranderson.events_all


tyleranderson.store_impressions ;


select * from tyleranderson.events_all where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';

select * from tyleranderson.store_impressions where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';

select * from tyleranderson.sessions where event_date between '2025-08-01' and '2025-08-31' and user_id = '1114297252';


select source_page, page, source, preference_type, count(1) cnt, count(distinct consumer_id) cc from iGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where iguazu_timestamp>= '2025-08-25' group by all;


select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' limit 100 ;

select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' and consumer_id = '1964336184' limit 100 ;
with base as (select consumer_id, entity_id, count(1) cnt 
from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where page = 'onboarding_preference_page' group by all)
select count(1), sum(case when cnt>1 then 1 else 0 end) as multiple_cnt
from base;
select * from IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE where consumer_id = '1965216620' and entity_id = 'chinese'  limit;

create or replace table proddb.fionafan.preference_entity_cnt_distribution as (
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
select * from second_base
);

proddb.fionafan.preference_entity_cnt_distribution 

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
iguazu_consumer.m_onboarding_end_promo_page_click_ice


