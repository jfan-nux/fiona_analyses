SET exp_name = 'should_pin_leaderboard_carousel';
SET start_date = '2025-08-21';
SET end_date = current_date; 
SET version = 1;

with base_Population as (
SELECT  ee.tag as experiment_group
              , ee.bucket_key
              , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = $exp_name
AND experiment_version = $version
and segment = 'Users'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY all
) select * from base_Population ;

create or replace table proddb.fionafan.leaderboard_m_card_view as
select a.*, b.experiment_group, b.first_exposure_time
from iguazu.consumer.m_card_view a
inner join METRICS_REPO.PUBLIC.should_pin_leaderboard_carousel__Users_exposures b
on a.consumer_id = b.bucket_key
AND a.TIMESTAMP>b.first_exposure_time
where a.TIMESTAMP>='2025-08-21'
and a.TIMESTAMP<='2025-09-04';

create or replace table proddb.fionafan.leaderboard_new_user_status as (
    select event_date, first_order_date, first_event_date,event_type, event_name, user_id, count(1) cnt
    from proddb.public.fact_unique_visitors_full_pt
    where TIMESTAMP>='2025-08-21'
    and TIMESTAMP<='2025-09-04'
    and is_bot=0
    and user_id in (select distinct consumer_id from proddb.fionafan.leaderboard_m_card_view)
);
with a as (
        select event_date, first_order_date,first_event, user_id, count(1) cnt
    from proddb.public.fact_unique_visitors_full_pt
    where event_date>='2025-08-21'
    and event_date<='2025-09-04'
    and is_bot=0
    and user_id in (select distinct consumer_id from proddb.fionafan.leaderboard_m_card_view)
    group by all
)
select * from a where cnt>1;
select count(distinct user_id) from proddb.public.fact_unique_visitors_full_pt
    where event_date>='2025-08-21'
    and event_date<='2025-09-04'
    and is_bot=0
    and user_id in (select distinct consumer_id from proddb.fionafan.leaderboard_m_card_view);
select count(distinct user_id) from proddb.fionafan.leaderboard_m_card_view;

select experiment_group, avg(card_position) card_position, avg(position), avg(vertical_position), avg(item_position), avg(item_card_position), avg(item_photo_position), avg(original_position), avg(store_card_position) 
from proddb.fionafan.leaderboard_m_card_view where badges ilike '%leader%' and card_position is not null group by all;

with rnk as (
    select container, count(1) cont_cnt 
    from proddb.fionafan.leaderboard_m_card_view 
    where container not like '%category%' 
    group by container
),
det as (
    select container, 
           case when store_id is not null then 1 else 0 end as store_type, 
           count(1) cnt 
    from proddb.fionafan.leaderboard_m_card_view 
    where container not like '%category%' 
    group by container, case when store_id is not null then 1 else 0 end
) 
select * from det 
left join rnk on det.container = rnk.container 
order by cont_cnt desc, store_type desc;

select distinct carousel_name from proddb.fionafan.leaderboard_m_card_view  limit 10;

proddb.public.fact_unique_visitors_full_pt
