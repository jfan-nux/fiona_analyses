-- Original query
select consumer_id, dd_device_id, dd_session_id, min(timestamp) as min_timestamp, max(timestamp) as max_timestamp from IGUAZU.SERVER_EVENTS_PRODUCTION.ARGO_SEARCH_DISCOVERY_QUERY_LOGGER
where consumer_id = '1125900281188900' and date_trunc('day',timestamp) = '2025-07-28' group by all;

-- Unpivot search_result_ids array from search_result_summary JSON (OPTIMIZED)
SELECT 
    consumer_id,
    dd_device_id,
    dd_session_id, 
    search_result.value::STRING AS search_result_id,
    LISTAGG(DISTINCT use_case_id, ', ') AS use_case_ids
FROM IGUAZU.SERVER_EVENTS_PRODUCTION.ARGO_SEARCH_DISCOVERY_QUERY_LOGGER,
    LATERAL FLATTEN(input => PARSE_JSON(search_result_summary):search_result_ids) AS search_result
WHERE consumer_id = '1125900281188900' 
    AND dd_session_id = 'sx_BADCF723-6772-42C1-9C96-B44F5A33B4DE'
    AND convert_timezone('UTC','America/Los_Angeles',timestamp) >= '2025-07-28' 
    AND timestamp < '2025-07-29'  -- Better for partition pruning
    AND search_result_summary IS NOT NULL
GROUP BY ALL;



select * from iguazu.server_events_production.home_page_store_ranker_compact_event_ice
where consumer_id = '1125900281188900' and date_trunc('day',iguazu_sent_at) = '2025-07-28' limit 10;-- Unpivot store_ranker_payloads JSON array

-- OPTIMIZED: Remove DISTINCT if not needed, or add LIMIT for exploration
SELECT 
    consumer_id, 
    dd_device_id, 
    submarket_id, 
    district_id, 
    platform, 
    dd_session_id,
    store.value:store_id::INTEGER AS store_id
FROM iguazu.server_events_production.home_page_store_ranker_compact_event_ice,
    LATERAL FLATTEN(input => store_ranker_payloads) AS store
WHERE IGUAZU_PARTITION_DATE = '2025-07-28' 
  AND consumer_id = '1125900281188900' 
  AND dd_session_id = 'sx_BADCF723-6772-42C1-9C96-B44F5A33B4DE'
  AND ranking_type IN ('carousel','store_page')
  AND store_ranker_payloads IS NOT NULL
GROUP BY ALL;



SELECT 
    IGUAZU_PARTITION_DATE, 
    consumer_id, 
    dd_session_id, 
    facet_type, 
    facet_name, 
    store_name, 
    store_id, 
    MAX(store_price_range) AS store_price_range, 
    MAX(store_is_asap_available) AS store_is_asap_available, 
    MAX(store_asap_minutes) AS store_asap_minutes, 
    MAX(store_average_rating) AS store_average_rating,
    MAX(store_distance_from_consumer) AS store_distance_from_consumer, 
    MAX(store_delivery_fee_amount) AS store_delivery_fee_amount, 
    MAX(is_sponsored) AS is_sponsored
FROM IGUAZU.SERVER_EVENTS_PRODUCTION.CX_CROSS_VERTICAL_HOME_PAGE_FEED_ICE
WHERE IGUAZU_PARTITION_DATE = '2025-07-28' 
    AND consumer_id = '1125900281188900' 
    AND dd_session_id = 'sx_BADCF723-6772-42C1-9C96-B44F5A33B4DE'
GROUP BY ALL;



select    dd_session_id,
                dd_device_id,
                consumer_id, timestamp,
                store_id, store_name,
                carousel_name, container, page,
                card_position, badges, item_id, item_name,
                vertical_position
from iguazu.consumer.m_card_view
where timestamp >= '2025-07-28'
and timestamp < '2025-07-29'
and consumer_id = '1125900281188900'
and dd_session_id = 'sx_BADCF723-6772-42C1-9C96-B44F5A33B4DE'
-- and container = 'cluster' and page = 'explore_page'
-- and store_id is not null
group by all;

select * 
from tyleranderson.events_all
where event_date = '2025-07-28'
and USER_ID = '1125900281188900'
-- and dd_session_id = 'sx_BADCF723-6772-42C1-9C96-B44F5A33B4DE'
and session_num = 5
group by all;


iguazu.server_events_production.m_store_content_page_load
;


SELECT
  n.*,
  c.LIFESTAGE,
  FLOOR(DATEDIFF(day, c.first_order_date, n.SENT_AT_DATE) / 7) + 1 AS LIFECYCLE_WEEK
FROM edw.consumer.fact_consumer_notification_engagement n
JOIN edw.growth.consumer_growth_accounting_scd3 c
  ON n.CONSUMER_ID = c.CONSUMER_ID
  AND n.SENT_AT_DATE >= c.SCD_START_DATE
  AND (n.SENT_AT_DATE < c.SCD_END_DATE OR c.SCD_END_DATE IS NULL)
JOIN proddb.public.dimension_consumer dc
  ON n.CONSUMER_ID = dc.ID
WHERE 1=1
  AND c.LIFESTAGE ILIKE 'New Cx'
  AND n.SENT_AT_DATE >= '2025-07-01'
  AND n.SENT_AT_DATE < '2025-08-13'
  AND c.first_order_date >= '2025-07-01'
  AND c.first_order_date < '2025-07-15'
  AND dc.DEFAULT_COUNTRY = 'United States';

select count(1) from proddb.fionafan.all_user_sessions_with_events_features_gen where session_type = 'first_session';

where funnel_converted_bool is not null;


select distinct store_text_attribution, count(1) cnt
from proddb.fionafan.session_features_with_store_embeddings 
where store_similarity_attribution_prev > 0.999 group by all order by cnt desc;

select case when store_text_impression = 'no_store_info' then 0 else 1 end as has_impression, count(1) cnt
from proddb.fionafan.session_features_with_store_embeddings group by all;
-- where store_text_impression = 'no_store_info';


select cohort_type, case when store_tags_concat_impression <>'' then 1 else 0 end as has_impression
, case when funnel_had_any=1 then 1 else 0 end as has_funnel
, count(1) cnt, avg(funnel_converted_bool) as avg_funnel_converted_bool, median(session_duration_seconds) mediuan_session_length_seconds
from proddb.fionafan.all_user_sessions_with_events_features_gen group by all order by cohort_type, has_impression;


select cohort_type, case when store_tags_concat_attribution <>'' then 1 else 0 end as has_attribution
, case when funnel_had_any=1 then 1 else 0 end as has_funnel
, count(1) cnt
, count(1)/sum(count(1)) over (partition by cohort_type) as cnt_pct_of_cohort
, avg(funnel_converted_bool) as avg_funnel_converted_bool, median(session_duration_seconds) mediuan_session_length_seconds
from proddb.fionafan.all_user_sessions_with_events_features_gen 
where store_tags_concat_impression <>'' 
group by all order by cohort_type, has_attribution, has_funnel;

select cohort_type, session_type, case when funnel_had_any=1 then 1 else 0 end as has_funnel
, count(1) cnt, avg(funnel_converted_bool) as avg_funnel_converted_bool, median(session_duration_seconds) median_duration_seconds
from proddb.fionafan.all_user_sessions_with_events_features_gen group by all order by all;
select session_type, count(1) cnt from proddb.fionafan.all_user_sessions_with_events_features_gen  where store_tags_concat_impression = ''
and funnel_had_any = 0 group by all order by cnt desc;
select * from proddb.fionafan.all_user_sessions_with_events a
inner join (select distinct user_id, event_date, session_num, dd_device_id 
from proddb.fionafan.all_user_sessions_with_events_features_gen where store_tags_concat_impression = ''
and funnel_had_any = 0 group by all) b
on a.user_id = b.user_id and a.event_date = b.event_date and a.session_num = b.session_num and a.dd_device_id = b.dd_device_id
order by a.user_id, a.event_date, a.session_num, a.dd_device_id limit 1000;
select event, count(1) cnt, count(1) / sum(count(1)) over () as pct_of_total from proddb.fionafan.all_user_sessions_with_events a
inner join proddb.fionafan.all_user_empty_sessions b
on a.user_id = b.user_id and a.event_date = b.event_date and a.session_num = b.session_num and a.dd_device_id = b.dd_device_id
group by all
order by cnt desc ;


create or replace table proddb.fionafan.all_user_empty_sessions as (
select distinct user_id, event_date, session_num, dd_device_id 
from proddb.fionafan.all_user_sessions_with_events_features_gen where store_tags_concat_impression = ''
and funnel_had_any = 0 group by all
);



-- Activity pattern analysis: create pattern like "1011" for weekly activity
select 
    cohort_type,
    concat(
        case when sessions_day_0_7 > 0 then '1' else '0' end,
        case when sessions_day_8_14 > 0 then '1' else '0' end,
        case when sessions_day_15_21 > 0 then '1' else '0' end,
        case when sessions_day_22_28 > 0 then '1' else '0' end
    ) as activity_pattern,
    count(distinct consumer_id) as user_count,
    avg(sessions_day_0_7) as avg_sessions_week1,
    avg(sessions_day_8_14) as avg_sessions_week2,
    avg(sessions_day_15_21) as avg_sessions_week3,
    avg(sessions_day_22_28) as avg_sessions_week4,
    avg(sessions_day_0_7 + sessions_day_8_14 + sessions_day_15_21 + sessions_day_22_28) as avg_total_sessions
from proddb.fionafan.all_user_sessions_enriched

where sessions_day_0_7>0
group by all
order by cohort_type, user_count desc;

-- Activity pattern with order metrics (28d post-onboarding)
with user_activity_patterns as (
    select 
        consumer_id,
        cohort_type,
        onboarding_day,
        concat(
            case when sessions_day_0_7 > 0 then '1' else '0' end,
            case when sessions_day_8_14 > 0 then '1' else '0' end,
            case when sessions_day_15_21 > 0 then '1' else '0' end,
            case when sessions_day_22_28 > 0 then '1' else '0' end
        ) as activity_pattern,
        (sessions_day_0_7 + sessions_day_8_14 + sessions_day_15_21 + sessions_day_22_28) as total_sessions,
        -- Count active weeks
        (case when sessions_day_0_7 > 0 then 1 else 0 end +
         case when sessions_day_8_14 > 0 then 1 else 0 end +
         case when sessions_day_15_21 > 0 then 1 else 0 end +
         case when sessions_day_22_28 > 0 then 1 else 0 end) as active_weeks
    from proddb.fionafan.all_user_sessions_enriched
    where sessions_day_0_7 > 0
),
user_orders as (
    select 
        u.consumer_id,
        u.cohort_type,
        u.activity_pattern,
        u.total_sessions,
        u.active_weeks,
        count(distinct d.delivery_id) as orders_28d
    from user_activity_patterns u
    left join dimension_deliveries d
        on u.consumer_id = d.creator_id
        and d.actual_delivery_time >= u.onboarding_day
        and d.actual_delivery_time < dateadd(day, 28, u.onboarding_day)
        and d.is_filtered_core = 1
    group by all
)
select 
    cohort_type,
    activity_pattern,
    -- User counts
    count(distinct consumer_id) as user_count,
    count(distinct consumer_id) / sum(count(distinct consumer_id)) over (partition by cohort_type) as pct_of_cohort,
    
    -- Session metrics
    avg(total_sessions) as avg_total_sessions,
    
    -- Order metrics (28d post-onboarding)
    avg(orders_28d) as avg_orders_28d,
    median(orders_28d) as median_orders_28d,
    sum(orders_28d) as total_orders_28d,
    
    -- Order frequency (orders per active week)
    avg(orders_28d / nullif(active_weeks, 0)) as avg_orders_per_active_week,
    
    -- Order rate (% who ordered at least once)
    avg(case when orders_28d > 0 then 1 else 0 end) as order_rate_28d
    
from user_orders
group by all
order by cohort_type, user_count desc;

-- Create table for 1111 activity pattern users (active all 4 weeks)
create or replace table proddb.fionafan.users_activity_1111 as (

    select 
        consumer_id,
        cohort_type,
        onboarding_day,
        concat(
            case when sessions_day_0_7 > 0 then '1' else '0' end,
            case when sessions_day_8_14 > 0 then '1' else '0' end,
            case when sessions_day_15_21 > 0 then '1' else '0' end,
            case when sessions_day_22_28 > 0 then '1' else '0' end
        ) as activity_pattern
    from proddb.fionafan.all_user_sessions_enriched

    where concat(
            case when sessions_day_0_7 > 0 then '1' else '0' end,
            case when sessions_day_8_14 > 0 then '1' else '0' end,
            case when sessions_day_15_21 > 0 then '1' else '0' end,
            case when sessions_day_22_28 > 0 then '1' else '0' end
        ) = '1111'
);

-- ============================================================================
-- QUESTION: For 1111 cohort, what % never ordered in 28d after onboarding?
-- ============================================================================

with users_1111_with_orders as (
    select 
        u.consumer_id,
        u.cohort_type,
        u.onboarding_day,
        u.activity_pattern,
        count(distinct d.delivery_id) as orders_28d,
        case when count(distinct d.delivery_id) = 0 then 1 else 0 end as never_ordered
    from proddb.fionafan.users_activity_1111 u
    left join dimension_deliveries d
        on u.consumer_id = d.creator_id
        and d.actual_delivery_time > u.onboarding_day+1
        and d.actual_delivery_time < dateadd(day, 29, u.onboarding_day)
        and d.is_filtered_core = 1
    group by all
)
select 
    cohort_type,
    count(distinct consumer_id) as total_users_1111,
    sum(never_ordered) as users_never_ordered,
    round(100.0 * sum(never_ordered) / count(distinct consumer_id), 2) as pct_never_ordered,
    round(100.0 * sum(case when orders_28d > 0 then 1 else 0 end) / count(distinct consumer_id), 2) as pct_ordered,
    avg(orders_28d) as avg_orders_28d,
    median(orders_28d) as median_orders_28d
from users_1111_with_orders
group by all
order by cohort_type;

-- ============================================================================
-- QUESTION: 28d conversion rate across all cohort types
-- ============================================================================

with all_users_with_orders as (
    select 
        u.consumer_id,
        u.cohort_type,
        u.onboarding_day,
        count(distinct d.delivery_id) as orders_28d,
        case when count(distinct d.delivery_id) = 0 then 1 else 0 end as never_ordered,
        case when count(distinct d.delivery_id) > 0 then 1 else 0 end as converted
    from proddb.fionafan.all_user_sessions_enriched u
    left join dimension_deliveries d
        on u.consumer_id = d.creator_id
        and d.actual_delivery_time > u.onboarding_day + 1
        and d.actual_delivery_time < dateadd(day, 29, u.onboarding_day)
        and d.is_filtered_core = 1
    group by all
)
select 
    cohort_type,
    count(distinct consumer_id) as total_users,
    sum(converted) as users_converted,
    sum(never_ordered) as users_never_ordered,
    round(100.0 * sum(converted) / count(distinct consumer_id), 2) as conversion_rate_28d,
    round(100.0 * sum(never_ordered) / count(distinct consumer_id), 2) as pct_never_ordered,
    avg(orders_28d) as avg_orders_28d,
    median(orders_28d) as median_orders_28d,
    sum(orders_28d) as total_orders_28d
from all_users_with_orders
group by all
order by cohort_type;

create or replace table proddb.fionafan.users_activity_1110 as (
    select 
        consumer_id,
        cohort_type,
        onboarding_day,
        concat(
            case when sessions_day_0_7 > 0 then '1' else '0' end,
            case when sessions_day_8_14 > 0 then '1' else '0' end,
            case when sessions_day_15_21 > 0 then '1' else '0' end,
            case when sessions_day_22_28 > 0 then '1' else '0' end
        ) as activity_pattern
    from proddb.fionafan.all_user_sessions_enriched
    where concat(
            case when sessions_day_0_7 > 0 then '1' else '0' end,
            case when sessions_day_8_14 > 0 then '1' else '0' end,
            case when sessions_day_15_21 > 0 then '1' else '0' end,
            case when sessions_day_22_28 > 0 then '1' else '0' end
        ) = '1110'
);
-- Analyze first_session behavior: 1111 users vs others
select 
    case when u.consumer_id is not null then '1111_active' else 'other' end as user_group,
    s.cohort_type,
    
    -- Counts
    count(distinct s.user_id) as user_count,
    count(distinct s.user_id, s.event_date, s.session_num, s.dd_device_id) as session_count,
    
    -- Conversion
    avg(s.funnel_converted_bool) as conversion_rate,
    
    -- Session duration
    avg(s.session_duration_seconds) as avg_session_duration_seconds,
    median(s.session_duration_seconds) as median_session_duration_seconds,
    
    -- Impression metrics
    avg(s.impression_unique_stores) as avg_impression_unique_stores,
    avg(s.impression_unique_features) as avg_impression_unique_features,
    avg(s.impression_to_attribution_rate) as avg_impression_to_attribution_rate,
    
    -- Attribution metrics
    avg(s.attribution_event_count) as avg_attribution_event_count,
    avg(s.attribution_num_card_clicks) as avg_attribution_num_card_clicks,
    avg(s.attribution_unique_stores_clicked) as avg_attribution_unique_stores_clicked,
    avg(s.attribution_to_add_rate) as avg_attribution_to_add_rate,
    
    -- Funnel metrics
    avg(s.funnel_num_adds) as avg_funnel_num_adds,
    avg(s.funnel_num_store_page) as avg_funnel_num_store_page,
    avg(s.funnel_num_cart) as avg_funnel_num_cart,
    avg(s.funnel_num_checkout) as avg_funnel_num_checkout,
    avg(s.funnel_had_any) as pct_had_funnel,
    
    -- Action metrics
    avg(s.action_num_backgrounds) as avg_action_num_backgrounds,
    avg(s.action_num_foregrounds) as avg_action_num_foregrounds,
    avg(s.action_num_tab_switches) as avg_action_num_tab_switches,
    avg(s.action_had_address) as pct_had_address_action,
    avg(s.action_had_auth) as pct_had_auth_action,
    
    -- Attribution sources
    avg(s.attribution_cnt_traditional_carousel) as avg_attribution_carousel,
    avg(s.attribution_cnt_core_search) as avg_attribution_search,
    avg(s.attribution_cnt_pill_filter) as avg_attribution_pill_filter,
    avg(s.attribution_cnt_home_feed) as avg_attribution_home_feed,
    
    -- Timing metrics
    avg(s.timing_mean_inter_event_seconds) as avg_timing_mean_inter_event_seconds,
    avg(s.funnel_seconds_to_first_add) as avg_funnel_seconds_to_first_add,
    avg(s.attribution_seconds_to_first_card_click) as avg_attribution_seconds_to_first_card_click
    
from proddb.fionafan.all_user_sessions_with_events_features_gen s

left join proddb.fionafan.users_activity_1111 u
    on s.user_id = u.consumer_id
where s.session_type = 'first_session'
    and s.funnel_converted_bool is not null
group by all
order by user_group, cohort_type;

-- Analyze 1111 cohort timing consistency across weeks
create or replace table proddb.fionafan.cohort_1111_timing as (
with cohort_1111_timing as (
    select 
        consumer_id,
        cohort_type,
        onboarding_day,
        
        -- Week 1 (days 0-7) first session
        latest_session_day_0_7_ts as week1_first_ts,
        dayofweek(latest_session_day_0_7_ts) as week1_dow,
        case 
            when hour(latest_session_day_0_7_ts) between 0 and 5 then 'night'
            when hour(latest_session_day_0_7_ts) between 6 and 9 then 'breakfast'
            when hour(latest_session_day_0_7_ts) between 10 and 13 then 'lunch'
            when hour(latest_session_day_0_7_ts) between 14 and 16 then 'happy_hour'
            when hour(latest_session_day_0_7_ts) between 17 and 23 then 'dinner'
        end as week1_timeblock,
        
        -- Week 2 (days 8-14) first session
        latest_session_day_8_14_ts as week2_first_ts,
        dayofweek(latest_session_day_8_14_ts) as week2_dow,
        case 
            when hour(latest_session_day_8_14_ts) between 0 and 5 then 'night'
            when hour(latest_session_day_8_14_ts) between 6 and 9 then 'breakfast'
            when hour(latest_session_day_8_14_ts) between 10 and 13 then 'lunch'
            when hour(latest_session_day_8_14_ts) between 14 and 16 then 'happy_hour'
            when hour(latest_session_day_8_14_ts) between 17 and 23 then 'dinner'
        end as week2_timeblock,
        
        -- Week 3 (days 15-21) first session
        latest_session_day_15_21_ts as week3_first_ts,
        dayofweek(latest_session_day_15_21_ts) as week3_dow,
        case 
            when hour(latest_session_day_15_21_ts) between 0 and 5 then 'night'
            when hour(latest_session_day_15_21_ts) between 6 and 9 then 'breakfast'
            when hour(latest_session_day_15_21_ts) between 10 and 13 then 'lunch'
            when hour(latest_session_day_15_21_ts) between 14 and 16 then 'happy_hour'
            when hour(latest_session_day_15_21_ts) between 17 and 23 then 'dinner'
        end as week3_timeblock,
        
        -- Week 4 (days 22-28) first session
        latest_session_day_22_28_ts as week4_first_ts,
        dayofweek(latest_session_day_22_28_ts) as week4_dow,
        case 
            when hour(latest_session_day_22_28_ts) between 0 and 5 then 'night'
            when hour(latest_session_day_22_28_ts) between 6 and 9 then 'breakfast'
            when hour(latest_session_day_22_28_ts) between 10 and 13 then 'lunch'
            when hour(latest_session_day_22_28_ts) between 14 and 16 then 'happy_hour'
            when hour(latest_session_day_22_28_ts) between 17 and 23 then 'dinner'
        end as week4_timeblock
        
    from proddb.fionafan.all_user_sessions_enriched

    where consumer_id in (select consumer_id from proddb.fionafan.users_activity_1111)
),
timing_consistency as (
    select 
        *,
        -- Day of week consistency (current week vs previous week, allowing ±1 day)
        case when abs(week2_dow - week1_dow) <= 1 or abs(week2_dow - week1_dow) = 6 then 1 else 0 end as week2_dow_same_day,
        case when abs(week3_dow - week2_dow) <= 1 or abs(week3_dow - week2_dow) = 6 then 1 else 0 end as week3_dow_same_day,
        case when abs(week4_dow - week3_dow) <= 1 or abs(week4_dow - week3_dow) = 6 then 1 else 0 end as week4_dow_same_day,
        
        -- case when week2_dow = week1_dow then 1 else 0 end as week2_dow_same_day,
        -- case when week3_dow = week2_dow then 1 else 0 end as week3_dow_same_day,
        -- case when week4_dow = week3_dow then 1 else 0 end as week4_dow_same_day,
        -- Time block consistency (current week vs previous week)
        case when week2_timeblock = week1_timeblock then 1 else 0 end as week2_timeblock_same,
        case when week3_timeblock = week2_timeblock then 1 else 0 end as week3_timeblock_same,
        case when week4_timeblock = week3_timeblock then 1 else 0 end as week4_timeblock_same
    from cohort_1111_timing
)
select 
    consumer_id,
    cohort_type,
    
    -- Week timestamps
    week1_first_ts,
    week2_first_ts,
    week3_first_ts,
    week4_first_ts,
    
    -- Day of week for each week
    week1_dow,
    week2_dow,
    week3_dow,
    week4_dow,
    
    -- Time blocks for each week
    week1_timeblock,
    week2_timeblock,
    week3_timeblock,
    week4_timeblock,
    
    -- Binary flags for day of week consistency (±1 day)
    week2_dow_same_day,
    week3_dow_same_day,
    week4_dow_same_day,
    
    -- Binary flags for timeblock consistency
    week2_timeblock_same,
    week3_timeblock_same,
    week4_timeblock_same,
    
    -- Overall consistency scores
    (week2_dow_same_day + week3_dow_same_day + week4_dow_same_day) as total_dow_matches,
    (week2_timeblock_same + week3_timeblock_same + week4_timeblock_same) as total_timeblock_matches
    
from timing_consistency
order by cohort_type, consumer_id
);



select * from proddb.fionafan.cohort_1111_timing limit 1000;

-- Day of week consistency patterns
select 
    cohort_type,
    
    -- Day of week pattern (week2, week3, week4 vs previous)
    concat(
        week2_dow_same_day::varchar,
        week3_dow_same_day::varchar,
        week4_dow_same_day::varchar
    ) as dow_consistency_pattern,
    
    -- User counts
    count(distinct consumer_id) as user_count,
    count(distinct consumer_id) / sum(count(distinct consumer_id)) over (partition by cohort_type) as pct_of_cohort,
    
    -- Average matches
    avg(total_dow_matches) as avg_dow_matches
    
from proddb.fionafan.cohort_1111_timing
group by all
order by cohort_type, user_count desc;

-- Timeblock consistency patterns
select 
    cohort_type,
    
    -- Timeblock pattern (week2, week3, week4 vs previous)
    concat(
        week2_timeblock_same::varchar,
        week3_timeblock_same::varchar,
        week4_timeblock_same::varchar
    ) as timeblock_consistency_pattern,
    
    -- User counts
    count(distinct consumer_id) as user_count,
    count(distinct consumer_id) / sum(count(distinct consumer_id)) over (partition by cohort_type) as pct_of_cohort,
    
    -- Average matches
    avg(total_timeblock_matches) as avg_timeblock_matches
    
from proddb.fionafan.cohort_1111_timing
group by all
order by cohort_type, user_count desc;


select * from session_conversion_by_cohort_model_results where model_name = 'Pre-Funnel Model' order by all;

select * from proddb.fionafan.first_session_1111_cohort_prediction_results;

select * from proddb.fionafan.first_session_1111_xgboost_feature_importance;



select distinct session_type from proddb.fionafan.all_user_sessions_with_events_features_gen;


SELECT 
    f.USER_ID AS CONSUMER_ID,
    f.COHORT_TYPE,
    f.SESSION_TYPE,
    f.DD_DEVICE_ID,
    f.EVENT_DATE,
    f.SESSION_NUM,
    f.STORE_TAGS_CONCAT_IMPRESSION,
    f.STORE_CATEGORIES_CONCAT_IMPRESSION,
    f.STORE_NAMES_CONCAT_IMPRESSION,
    f.STORE_TAGS_CONCAT_ATTRIBUTION,
    f.STORE_CATEGORIES_CONCAT_ATTRIBUTION,
    f.STORE_NAMES_CONCAT_ATTRIBUTION,
    f.STORE_TAGS_CONCAT_FUNNEL_STORE,
    f.STORE_CATEGORIES_CONCAT_FUNNEL_STORE,
    f.STORE_NAMES_CONCAT_FUNNEL_STORE
FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
INNER JOIN proddb.fionafan.users_activity_1111 a
    ON f.USER_ID = a.CONSUMER_ID
WHERE 1=1
    AND f.SESSION_TYPE IN ('first_session', 'second_session', 'third_session', 
                           'latest_week_1', 'latest_week_2', 'latest_week_3', 'latest_week_4')
and STORE_TAGS_CONCAT_IMPRESSION <> ''
limit 100;


-- Session timing analysis: time differences from onboarding to first, first to second, second to third
WITH session_timestamps AS (
    SELECT 
        consumer_id,
        cohort_type,
        onboarding_day,
        -- Session timestamps
        latest_session_day_0_7_ts AS first_session_ts,
        latest_session_day_8_14_ts AS second_session_ts,
        latest_session_day_15_21_ts AS third_session_ts,
        -- Calculate time differences in hours for precision
        DATEDIFF(hour, onboarding_day, latest_session_day_0_7_ts) / 24.0 AS days_onboarding_to_first,
        DATEDIFF(hour, latest_session_day_0_7_ts, latest_session_day_8_14_ts) / 24.0 AS days_first_to_second,
        DATEDIFF(hour, latest_session_day_8_14_ts, latest_session_day_15_21_ts) / 24.0 AS days_second_to_third
    FROM proddb.fionafan.all_user_sessions_enriched
    WHERE 1=1
        AND onboarding_day IS NOT NULL
        AND latest_session_day_0_7_ts IS NOT NULL
        AND latest_session_day_8_14_ts IS NOT NULL
        AND latest_session_day_15_21_ts IS NOT NULL

)
SELECT 
    cohort_type,
    
    -- Count of users with all three sessions
    COUNT(DISTINCT consumer_id) AS user_count,
    
    -- Time from onboarding to first session (days)
    AVG(days_onboarding_to_first) AS avg_days_onboarding_to_first,
    MEDIAN(days_onboarding_to_first) AS median_days_onboarding_to_first,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_onboarding_to_first) AS p75_days_onboarding_to_first,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_onboarding_to_first) AS p90_days_onboarding_to_first,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY days_onboarding_to_first) AS p99_days_onboarding_to_first,
    
    -- Time between first and second session (days)
    AVG(days_first_to_second) AS avg_days_first_to_second,
    MEDIAN(days_first_to_second) AS median_days_first_to_second,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_first_to_second) AS p75_days_first_to_second,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_first_to_second) AS p90_days_first_to_second,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY days_first_to_second) AS p99_days_first_to_second,
    
    -- Time between second and third session (days)
    AVG(days_second_to_third) AS avg_days_second_to_third,
    MEDIAN(days_second_to_third) AS median_days_second_to_third,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY days_second_to_third) AS p75_days_second_to_third,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY days_second_to_third) AS p90_days_second_to_third,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY days_second_to_third) AS p99_days_second_to_third
    
FROM session_timestamps
GROUP BY cohort_type
ORDER BY cohort_type;


select cohort_type, count(1),
avg(case when session_type = 'first_session' then similarity_impression else null end) as avg_first_session_similarity_impression,
avg(case when session_type = 'second_session' then similarity_impression else null end) as avg_second_session_similarity_impression,
avg(case when session_type = 'third_session' then similarity_impression else null end) as avg_third_session_similarity_impression,
avg(case when session_type = 'latest_week_1' then similarity_impression else null end) as avg_latest_week_1_similarity_impression,
avg(case when session_type = 'latest_week_2' then similarity_impression else null end) as avg_latest_week_2_similarity_impression,
avg(case when session_type = 'latest_week_3' then similarity_impression else null end) as avg_latest_week_3_similarity_impression,
avg(case when session_type = 'latest_week_4' then similarity_impression else null end) as avg_latest_week_4_similarity_impression,
avg(case when session_type = 'first_session' then similarity_attribution else null end) as avg_first_session_similarity_attribution,
avg(case when session_type = 'second_session' then similarity_attribution else null end) as avg_second_session_similarity_attribution,
avg(case when session_type = 'third_session' then similarity_attribution else null end) as avg_third_session_similarity_attribution,
avg(case when session_type = 'latest_week_1' then similarity_attribution else null end) as avg_latest_week_1_similarity_attribution,
avg(case when session_type = 'latest_week_2' then similarity_attribution else null end) as avg_latest_week_2_similarity_attribution,
avg(case when session_type = 'latest_week_3' then similarity_attribution else null end) as avg_latest_week_3_similarity_attribution,
avg(case when session_type = 'latest_week_4' then similarity_attribution else null end) as avg_latest_week_4_similarity_attribution,
avg(case when session_type = 'first_session' then similarity_funnel_store else null end) as avg_first_session_similarity_funnel_store,
avg(case when session_type = 'second_session' then similarity_funnel_store else null end) as avg_second_session_similarity_funnel_store,
avg(case when session_type = 'third_session' then similarity_funnel_store else null end) as avg_third_session_similarity_funnel_store,
avg(case when session_type = 'latest_week_1' then similarity_funnel_store else null end) as avg_latest_week_1_similarity_funnel_store,
avg(case when session_type = 'latest_week_2' then similarity_funnel_store else null end) as avg_latest_week_2_similarity_funnel_store,
avg(case when session_type = 'latest_week_3' then similarity_funnel_store else null end) as avg_latest_week_3_similarity_funnel_store,
avg(case when session_type = 'latest_week_4' then similarity_funnel_store else null end) as avg_latest_week_4_similarity_funnel_store
from proddb.fionafan.feed_evolution_1111_cosine_similarity group by all ;


select * from proddb.fionafan.first_session_second_session_return_logit_results 
where covariate in ( 'ATTRIBUTION_CNT_BANNER', 'ATTRIBUTION_FIRST_CLICK_FROM_SEARCH')
limit 1000;



select cohort_type, avg(impression_had_any) as avg_impression_had_any, avg(attribution_had_any) as avg_attribution_had_any, avg(funnel_had_any) as avg_funnel_had_any, avg(funnel_reached_store_bool) as avg_funnel_reached_store_bool
, avg(funnel_reached_cart_bool) as avg_funnel_reached_cart_bool, avg(funnel_reached_checkout_bool) as avg_funnel_reached_checkout_bool, avg(funnel_converted_bool) as avg_funnel_converted_bool
FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
where 1=1
    AND f.SESSION_TYPE IN ('first_session')
group by all;


select cohort_type, avg(impression_had_any) as avg_impression_had_any, avg(attribution_had_any) as avg_attribution_had_any, avg(funnel_had_any) as avg_funnel_had_any, avg(funnel_reached_store_bool) as avg_funnel_reached_store_bool
, avg(funnel_reached_cart_bool) as avg_funnel_reached_cart_bool, avg(funnel_reached_checkout_bool) as avg_funnel_reached_checkout_bool, avg(funnel_converted_bool) as avg_funnel_converted_bool
FROM proddb.fionafan.all_user_sessions_with_events_features_gen f
where 1=1
    AND f.SESSION_TYPE not IN ('first_session')
group by all;


-- Conversion analysis: Users who didn't convert in first session but converted later
-- VERSION 1: Using funnel_converted_bool from features table
WITH first_session_users AS (
    -- Users who didn't convert in their first session
    SELECT DISTINCT
        cohort_type,
        user_id,
        dd_device_id,
        MAX(funnel_converted_bool) as first_session_converted
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen
    WHERE session_type = 'first_session'
    GROUP BY cohort_type, user_id, dd_device_id
    HAVING MAX(funnel_converted_bool) = 0  -- Did NOT convert in first session
),
later_session_conversions AS (
    -- Check if these users converted in any later session
    SELECT DISTINCT
        cohort_type,
        user_id,
        dd_device_id,
        MAX(funnel_converted_bool) as later_session_converted
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen

    WHERE session_type != 'first_session'
    GROUP BY cohort_type, user_id, dd_device_id
)
SELECT 
    f.cohort_type,
    COUNT(DISTINCT f.user_id) as users_no_first_session_conversion,
    COUNT(DISTINCT CASE WHEN l.later_session_converted = 1 THEN f.user_id END) as users_converted_later,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN l.later_session_converted = 1 THEN f.user_id END) / 
          NULLIF(COUNT(DISTINCT f.user_id), 0), 2) as pct_converted_later
FROM first_session_users f
LEFT JOIN later_session_conversions l
    ON f.user_id = l.user_id 
    AND f.dd_device_id = l.dd_device_id
    AND f.cohort_type = l.cohort_type
GROUP BY f.cohort_type
ORDER BY f.cohort_type;


-- VERSION 2: Using funnel_converted_bool for first session, dimension_deliveries for later conversions
WITH first_session_users AS (
    -- Get users who did NOT convert in their first session (based on funnel_converted_bool)
    SELECT DISTINCT
        cohort_type,
        user_id,
        dd_device_id,
        session_start_ts::date as session_start_date,
        MAX(funnel_converted_bool) as first_session_converted
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen

    WHERE session_type = 'first_session'
    GROUP BY cohort_type, user_id, dd_device_id, session_start_ts::date
    HAVING MAX(funnel_converted_bool) = 0  -- Did NOT convert in first session
),
later_deliveries AS (
    -- Check if these users had any deliveries within 28 days after their first session
    SELECT DISTINCT
        fs.cohort_type,
        fs.user_id,
        fs.dd_device_id,
        COUNT(DISTINCT d.delivery_id) as later_delivery_count
    FROM first_session_users fs
    LEFT JOIN proddb.public.dimension_deliveries d
        ON fs.user_id = d.creator_id
        AND fs.dd_device_id = d.dd_device_id
        AND d.created_at > fs.session_start_date  -- After first session date
        AND d.created_at <= fs.session_start_date + 28  -- Within 28 days
    GROUP BY fs.cohort_type, fs.user_id, fs.dd_device_id
)
SELECT 
    fs.cohort_type,
    COUNT(DISTINCT fs.user_id) as users_no_first_session_conversion,
    COUNT(DISTINCT CASE WHEN ld.later_delivery_count > 0 THEN fs.user_id END) as users_converted_later,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN ld.later_delivery_count > 0 THEN fs.user_id END) / 
          NULLIF(COUNT(DISTINCT fs.user_id), 0), 2) as pct_converted_later_28d,
    SUM(ld.later_delivery_count) as total_later_deliveries_28d
FROM first_session_users fs
LEFT JOIN later_deliveries ld
    ON fs.user_id = ld.user_id 
    AND fs.dd_device_id = ld.dd_device_id
    AND fs.cohort_type = ld.cohort_type
GROUP BY fs.cohort_type
ORDER BY fs.cohort_type;

