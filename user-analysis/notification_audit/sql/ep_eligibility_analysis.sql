SELECT 
        CONVERT_TIMEZONE('UTC', 'US/Pacific', received_at)::DATE AS DTE, 
        case when program_name = 'ep_integrated_offers_v1_1_DTP' then 'IO_V1_1_DTP'
             when program_name = 'ep_integrated_offers_v1_1_FT' then 'IO_V1_1_FT'
             when program_name = 'ep_consumer_dtp_integrated_offers_churn_v1' then 'IO_V2_DTP'
            when program_name = 'ep_consumer_ft_integrated_offers_churn_v1' then 'IO_V2_FT'
            else null
            end as program_name,
       count(distinct consumer_id) AS CX_count
    FROM SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM

    WHERE program_name IN ('ep_integrated_offers_v1_1_DTP', 'ep_integrated_offers_v1_1_FT',     'ep_consumer_dtp_integrated_offers_churn_v1','ep_consumer_ft_integrated_offers_churn_v1')
    GROUP BY 1,2;


create or replace table proddb.fionafan.notif_new_user_table_dc as (
select 
user_id, created_at as join_time, created_date as day, id as consumer_id 
from dimension_consumer where created_at between '2025-07-23'::date-30 AND '2025-07-23'
 and experience = 'doordash' and email_domain not ilike '%doordash.com%'
);
create or replace table proddb.fionafan.notif_new_user_table_dc_with_lifestage as (
with base_with_lifestage as (
    select 
        n.user_id,
        n.consumer_id,
        n.join_time,
        n.day,
        l.lifestage,
        l.lifestage_bucket,
        l.signup_date,
        l.last_order_date,
        l.scd_start_date as lifestage_scd_start_date,
        l.scd_end_date as lifestage_scd_end_date
    from proddb.fionafan.notif_new_user_table_dc n
    left join edw.growth.consumer_growth_accounting_scd3 l
        on n.consumer_id::string = l.consumer_id::string
        and n.join_time >= l.scd_start_date
        and (l.scd_end_date is null or n.join_time <= l.scd_end_date)
),
push_by_device as (
    select 
        b.user_id,
        b.consumer_id,
        b.join_time,
        b.day,
        b.lifestage,
        b.lifestage_bucket,
        b.signup_date,
        b.last_order_date,
        b.lifestage_scd_start_date,
        b.lifestage_scd_end_date,
        p.device_id,
        case 
            when p.system_level_status = 'on' 
                or p.doordash_offers_status = 'on'
                or p.order_updates_status = 'on'
                or p.product_updates_news_status = 'on'
                or p.recommendations_status = 'on'
                or p.reminders_status = 'on'
                or p.store_offers_status = 'on'
            then 'on'
            else 'off'
        end as push_opt_in_device_level
    from base_with_lifestage b
    left join edw.consumer.dimension_consumer_device_push_settings_scd3 p
        on b.consumer_id::string = p.consumer_id::string
        and b.join_time + interval '24 h' between p.scd_start_date and coalesce(p.scd_end_date, '9999-12-31')
)
select 
    user_id,
    consumer_id,
    join_time,
    day,
    lifestage,
    lifestage_bucket,
    signup_date,
    last_order_date,
    lifestage_scd_start_date,
    lifestage_scd_end_date,
    max(case when push_opt_in_device_level = 'on' then 1 else 0 end) as push_opt_in
from push_by_device
group by all
);
create or replace table proddb.fionafan.notif_base_table_week_dc as (
SELECT
  n.*, u.join_time, u.day as join_day, 
  FLOOR(DATEDIFF(day, u.join_time, n.SENT_AT_DATE) / 7) + 1 AS LIFECYCLE_WEEK
FROM edw.consumer.fact_consumer_notification_engagement n

inner join proddb.fionafan.notif_new_user_table_dc u on n.consumer_id = u.consumer_id
WHERE 1=1
  AND n.SENT_AT_DATE >= '2025-07-23'::date-30
  AND n.SENT_AT_DATE < '2025-07-23'::date+30
  and n.sent_at_date > u.join_time
and n.sent_at_date <= DATEADD('day', 30, u.join_time)
  );

select case when b.consumer_id is null then 'not onboarded' else 'onboarded' end if_onboarded,lifestage , count(1) cnt from proddb.fionafan.notif_new_user_table_dc_with_lifestage a 
left join iguazu.consumer.m_onboarding_start_promo_page_view_ice b on a.consumer_id = b.consumer_id 
-- where a.experience = 'doordash' 
-- and a.is_guest = 0
group by all;

-- select * from dimension_consumer where user_id in 
-- (select distinct user_id from proddb.fionafan.notif_new_user_table_dc_with_lifestage a 
-- left join iguazu.consumer.m_onboarding_start_promo_page_view_ice b on a.consumer_id = b.consumer_id 
-- where b.consumer_id is null  
-- )
-- limit 100
-- ;

select count(1), count(distinct consumer_id) from proddb.fionafan.notif_base_table_week limit 10;
select * from SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM where sent_at >= '2025-06-01' and sent_at<='2025-07-23' and consumer_id = '820537733';


-- Create table with EP eligibility data joined with user lifestage and push opt-in
create or replace table proddb.fionafan.notif_ep_eligibility_with_user_info as (
with raw_data as (
    select 
        ep.sent_at,
        ep.applied,
        ep.program_id,
        ep.consumer_id,
        ep.user_id,
        ep.program_experiment_name,
        ep.program_name,
        ep.consumer_submarket_id,
        ep.program_experiment_variant,
        u.user_id as dimension_user_id,
        u.join_time,
        u.day as join_day,
        u.lifestage,
        u.lifestage_bucket,
        u.signup_date,
        u.last_order_date,
        u.push_opt_in,
        row_number() over (partition by ep.consumer_id, ep.user_id,program_experiment_variant, ep.sent_at order by ep.received_at desc) as rn
    from SEGMENT_EVENTS_RAW.CONSUMER_PRODUCTION.ENGAGEMENT_PROGRAM ep
    inner join proddb.fionafan.notif_new_user_table_dc_with_lifestage u
        on ep.consumer_id::string = u.consumer_id::string
    where ep.sent_at > u.join_time
)
select 
    sent_at,
    applied,
    program_id,
    consumer_id,
    user_id,
    program_experiment_name,
    program_name,
    consumer_submarket_id,
    program_experiment_variant,
    dimension_user_id,
    join_time,
    join_day,
    lifestage,
    lifestage_bucket,
    signup_date,
    last_order_date,
    push_opt_in
from raw_data
where rn = 1
);

select count(1), count(distinct consumer_id) from proddb.fionafan.notif_ep_eligibility_with_user_info;

select * from proddb.fionafan.notif_ep_eligibility_with_user_info where consumer_id = '1960173354';


select avg(push_opt_in) from proddb.fionafan.notif_new_user_table_dc_with_lifestage;