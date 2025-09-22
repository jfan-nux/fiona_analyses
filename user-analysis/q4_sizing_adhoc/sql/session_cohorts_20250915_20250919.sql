-- Session cohorts and in-session behavior flags for 2025-09-15 to 2025-09-19
-- Data sources: proddb.tyleranderson.events_all, proddb.tyleranderson.sessions, edw.growth.consumer_growth_accounting_scd3
-- Cohorts:
--   1) Lifestage at session start (from edw.growth.consumer_growth_accounting_scd3.lifestage, using session start date within SCD window)
--   2) Number of sessions per user during the time window
-- Metrics within session (0/1 flags):
--   - viewed_{cuisine|pill|search|grocery}
--   - clicked_{cuisine|pill|search|grocery}
--   - add_to_cart
--   - checkout_success

-- NOTE: Device normalization as per workspace rules
--   dd_device_id_filtered = replace(lower(CASE WHEN dd_device_id ILIKE 'dx_%' THEN dd_device_id ELSE 'dx_'||dd_device_id END), '-', '')

-- Materialize detailed session-level table
create or replace table proddb.fionafan.q4_sessions_flags_20250915_20250919 as
with
windowed_sessions as (
    select
        s.event_date::date as session_date,
        -- device normalization
        replace(lower(case when s.dd_device_id ilike 'dx_%' then s.dd_device_id else 'dx_'||s.dd_device_id end), '-', '') as dd_device_id_filtered,
        s.session_num,
        try_to_number(s.user_id) as consumer_id,
        s.platform,
        s.start_ts,
        s.end_ts
    from proddb.tyleranderson.sessions s
    where s.event_date::date between '2025-09-15' and '2025-09-19'
)
, windowed_events as (
    select
        e.event_date::date as event_date,
        replace(lower(case when e.dd_device_id ilike 'dx_%' then e.dd_device_id else 'dx_'||e.dd_device_id end), '-', '') as dd_device_id_filtered,
        e.session_num,
        try_to_number(e.user_id) as consumer_id,
        e.timestamp as event_ts,
        e.event_type,
        e.event,
        e.discovery_feature,
        e.discovery_surface
    from proddb.tyleranderson.events_all e
    where e.event_date::date between '2025-09-15' and '2025-09-19'
)
, sessions_in_window_by_user as (
    select
        ws.consumer_id,
        count(distinct ws.dd_device_id_filtered||'|'||ws.session_num||'|'||to_char(ws.session_date)) as sessions_in_window
    from windowed_sessions ws
    where ws.consumer_id is not null
    group by all
)
, lifestage_at_start as (
    select
        ws.consumer_id,
        ws.session_date,
        ws.dd_device_id_filtered,
        ws.session_num,
        max(s.lifestage) as lifestage
    from windowed_sessions ws
    left join edw.growth.consumer_growth_accounting_scd3 s
        on s.consumer_id = ws.consumer_id
       and ws.start_ts::date >= s.scd_start_date
       and (s.scd_end_date is null or ws.start_ts::date <= s.scd_end_date)
    group by all
)
, session_event_flags as (
    select
        ws.session_date,
        ws.dd_device_id_filtered,
        ws.session_num,
        ws.consumer_id,
        ws.platform,
        ws.start_ts,
        ws.end_ts,
        -- Views by discovery feature/surface from store impressions
        max(case when we.event_type = 'store_impression' and we.event = 'm_card_view' and we.discovery_feature = 'Cuisine Filter' then 1 else 0 end) as viewed_cuisine,
        max(case when we.event_type = 'store_impression' and we.event = 'm_card_view' and we.discovery_feature = 'Pill Filter' then 1 else 0 end) as viewed_pill,
        max(case when we.event_type = 'store_impression' and we.event = 'm_card_view' and coalesce(we.discovery_feature,'') in ('Core Search','Autocomplete','Vertical Search') then 1 else 0 end) as viewed_search,
        max(case when we.event_type = 'store_impression' and we.event = 'm_card_view' and (
                 coalesce(we.discovery_feature,'') ilike 'Grocery Tab - %' or coalesce(we.discovery_surface,'') = 'Grocery Tab') then 1 else 0 end) as viewed_grocery,
        -- Clicks by discovery feature from attribution
        max(case when we.event_type = 'attribution' and we.event = 'm_card_click' and we.discovery_feature = 'Cuisine Filter' then 1 else 0 end) as clicked_cuisine,
        max(case when we.event_type = 'attribution' and we.event = 'm_card_click' and we.discovery_feature = 'Pill Filter' then 1 else 0 end) as clicked_pill,
        max(case when we.event_type = 'attribution' and we.event = 'm_card_click' and coalesce(we.discovery_feature,'') in ('Core Search','Autocomplete','Vertical Search') then 1 else 0 end) as clicked_search,
        max(case when we.event_type = 'attribution' and we.event = 'm_card_click' and (
                 coalesce(we.discovery_feature,'') ilike 'Grocery Tab - %' or coalesce(we.discovery_surface,'') = 'Grocery Tab') then 1 else 0 end) as clicked_grocery,
        -- Add to cart and checkout success from funnel
        max(case when we.event_type = 'funnel' and coalesce(we.event,'') in ('action_add_item','action_quick_add_item') then 1 else 0 end) as add_to_cart,
        max(case when we.event_type = 'funnel' and coalesce(we.event,'') = 'system_checkout_success' then 1 else 0 end) as checkout_success
    from windowed_sessions ws
    left join windowed_events we
      on ws.dd_device_id_filtered = we.dd_device_id_filtered
     and ws.session_num = we.session_num
     and ws.session_date = we.event_date
    group by all
)
select
    sef.consumer_id,
    sef.session_date,
    sef.dd_device_id_filtered,
    sef.session_num,
    sef.platform,
    sef.start_ts,
    sef.end_ts,
    coalesce(la.lifestage, 'Unknown') as lifestage,
    coalesce(siw.sessions_in_window, 0) as sessions_in_window,
    viewed_cuisine,
    viewed_pill,
    viewed_search,
    viewed_grocery,
    clicked_cuisine,
    clicked_pill,
    clicked_search,
    clicked_grocery,
    add_to_cart,
    checkout_success,
    -- stable session identifier within window
    sef.dd_device_id_filtered||'|'||sef.session_num||'|'||to_char(sef.session_date) as session_id
from session_event_flags sef
left join lifestage_at_start la
  on la.consumer_id = sef.consumer_id
 and la.session_date = sef.session_date
 and la.dd_device_id_filtered = sef.dd_device_id_filtered
 and la.session_num = sef.session_num
left join sessions_in_window_by_user siw
  on siw.consumer_id = sef.consumer_id
;

-- Aggregated percentages by cohort
create or replace table proddb.fionafan.q4_sessions_percentages_20250915_20250919 as
select
    lifestage,
    sessions_in_window,
    count(distinct session_id) as sessions,
    avg(viewed_cuisine) as pct_view_cuisine,
    avg(clicked_cuisine) as pct_click_cuisine,
    avg(viewed_pill) as pct_view_pill,
    avg(clicked_pill) as pct_click_pill,
    avg(viewed_search) as pct_view_search,
    avg(clicked_search) as pct_click_search,
    avg(viewed_grocery) as pct_view_grocery,
    avg(clicked_grocery) as pct_click_grocery,
    avg(add_to_cart) as pct_add_to_cart,
    avg(checkout_success) as pct_checkout_success
from proddb.fionafan.q4_sessions_flags_20250915_20250919
group by all
order by lifestage, sessions_in_window;


-- select * from proddb.fionafan.q4_sessions_percentages_20250915_20250919;

select
    lifestage,
    -- sessions_in_window,
    count(distinct session_id) as sessions,
    avg(viewed_cuisine) as pct_view_cuisine,
    avg(clicked_cuisine) as pct_click_cuisine,
    avg(viewed_pill) as pct_view_pill,
    avg(clicked_pill) as pct_click_pill,
    avg(viewed_search) as pct_view_search,
    avg(clicked_search) as pct_click_search,
    avg(viewed_grocery) as pct_view_grocery,
    avg(clicked_grocery) as pct_click_grocery,
    avg(add_to_cart) as pct_add_to_cart,
    avg(checkout_success) as pct_checkout_success
from proddb.fionafan.q4_sessions_flags_20250915_20250919
group by all
order by all;