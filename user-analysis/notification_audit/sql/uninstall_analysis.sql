-- Uninstall pre-event indicators for a specific promo campaign
-- Source: proddb.fionafan.notif_base_table_w_braze_week (defined in campaign_analysis.sql)
-- Goal: Among rows with uninstall for the specified promo, check if prior to uninstall there exists
--       any open, unsubscribe, first session, and order events; also surface their latest timestamps before uninstall.

-- Parameters
set target_campaign = 'TRG-NPWS45d-YDTMRefresh-CR-HQCRM-Other-P-NA-EN-NEW40OFF';

-- Materialize results table (overwrites)
create or replace table proddb.fionafan.notif_uninstall_pre_event_indicators as (

with base as (
  select *
  from proddb.fionafan.notif_base_table_w_braze_week

  where 1=1
    and notification_source = 'Braze'
    and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and is_valid_send = 1
    and master_campaign_name = $target_campaign
),

-- Only rows where we observed an uninstall event
uninst as (
  select 
    consumer_id,
    device_id,
    join_time,
    deduped_message_id,
    sent_at,
    uninstalled_at
  from base
  where uninstalled_at is not null
),

-- For each uninstall row, pull latest prior events from the same consumer+device thread
events_before as (
  select 
    u.consumer_id,
    u.device_id,
    u.join_time,
    u.deduped_message_id,
    u.sent_at as uninstall_triggering_sent_at,
    u.uninstalled_at,

    -- Latest timestamps strictly before uninstall
    max(case when b.opened_at < u.uninstalled_at then b.opened_at end) as last_opened_before_uninstall,
    max(case when b.unsubscribed_at < u.uninstalled_at then b.unsubscribed_at end) as last_unsubscribed_before_uninstall,
    max(case when b.ordered_at < u.uninstalled_at then b.ordered_at end) as last_ordered_before_uninstall,

    -- First-session signal columns exist but are ids; treat existence of either id before uninstall on same thread as indicator
    max(case when (b.first_session_id_after_send is not null or b.first_session_id_after_open_or_click is not null)
              and b.sent_at < u.uninstalled_at then b.sent_at end) as any_first_session_signal_before_uninstall_sent_at
  from uninst u
  left join base b
    on b.consumer_id = u.consumer_id
   and b.device_id = u.device_id
   and b.sent_at <= u.uninstalled_at  -- include events at or before uninstall for timestamp comparisons
  group by all
),

final as (
  select 
    consumer_id,
    device_id,
    join_time,
    deduped_message_id,
    uninstalled_at,

    case when last_opened_before_uninstall is not null then 1 else 0 end as had_open_before_uninstall,
    last_opened_before_uninstall,

    case when last_unsubscribed_before_uninstall is not null then 1 else 0 end as had_unsubscribe_before_uninstall,
    last_unsubscribed_before_uninstall,

    case when any_first_session_signal_before_uninstall_sent_at is not null then 1 else 0 end as had_first_session_signal_before_uninstall,
    any_first_session_signal_before_uninstall_sent_at as first_session_signal_reference_time,

    case when last_ordered_before_uninstall is not null then 1 else 0 end as had_order_before_uninstall,
    last_ordered_before_uninstall
  from events_before
)

select * from final
);

-- Preview a small sample
select consumer_id, count(1) cnt 
from proddb.fionafan.notif_uninstall_pre_event_indicators group by all having cnt>1 limit 50;

select * from proddb.fionafan.notif_uninstall_pre_event_indicators where consumer_id = '1961703588' limit 50;

-- Consumer-level aggregation: one row per consumer who uninstalled
create or replace table proddb.fionafan.notif_uninstall_pre_event_indicators_consumer as (

with base as (
  select * from proddb.fionafan.notif_uninstall_pre_event_indicators
),
first_uninst as (
  select 
    consumer_id,
    min(uninstalled_at) as first_uninstalled_at,
    min(join_time) as join_time
  from base
  group by all
),
anchor as (
  -- Rows corresponding to the consumer's first uninstall timestamp
  select b.*
  from base b
  inner join first_uninst f
    on f.consumer_id = b.consumer_id
   and f.first_uninstalled_at = b.uninstalled_at
),
agg as (
  select 
    consumer_id,
    min(join_time) as join_time,
    min(uninstalled_at) as first_uninstalled_at,
    max(last_opened_before_uninstall) as open_ts,
    max(last_unsubscribed_before_uninstall) as unsub_ts,
    max(first_session_signal_reference_time) as first_session_signal_ts,
    max(last_ordered_before_uninstall) as order_ts
  from anchor
  group by all
)

select 
  consumer_id,
  join_time,
  first_uninstalled_at,

  -- Open
  case when open_ts is not null then 1 else 0 end as had_open_before_uninstall,
  DATEDIFF(day,   join_time, open_ts) as open_days_since_join,
  DATEDIFF(minute, open_ts, first_uninstalled_at) as open_minutes_before_uninstall,

  -- First session (signal)
  case when first_session_signal_ts is not null then 1 else 0 end as had_first_session_signal_before_uninstall,
  DATEDIFF(day,    join_time, first_session_signal_ts) as first_session_days_since_join,
  DATEDIFF(minute, first_session_signal_ts, first_uninstalled_at) as first_session_minutes_before_uninstall,

  -- Order
  case when order_ts is not null then 1 else 0 end as had_order_before_uninstall,
  DATEDIFF(day,    join_time, order_ts) as order_days_since_join,
  DATEDIFF(minute, order_ts, first_uninstalled_at) as order_minutes_before_uninstall,

  -- Unsubscribe
  case when unsub_ts is not null then 1 else 0 end as had_unsubscribe_before_uninstall,
  DATEDIFF(day,    join_time, unsub_ts) as unsubscribe_days_since_join,
  DATEDIFF(minute, unsub_ts, first_uninstalled_at) as unsubscribe_minutes_before_uninstall
from agg
);

-- Rates over all consumers who uninstalled (denominator)
select 
  count(1) as consumers_uninstalled,
  avg(had_open_before_uninstall) as open_before_uninstall_rate,
  avg(had_first_session_signal_before_uninstall) as first_session_before_uninstall_rate,
  avg(had_order_before_uninstall) as order_before_uninstall_rate,
  avg(had_unsubscribe_before_uninstall) as unsubscribe_before_uninstall_rate
from proddb.fionafan.notif_uninstall_pre_event_indicators_consumer;

select count(distinct case when uninstalled_at is not null then consumer_id end) 
from proddb.fionafan.notif_base_table_w_braze_week

where 1=1
    and notification_source = 'Braze'
    and notification_channel = 'PUSH'
    and notification_message_type_overall != 'TRANSACTIONAL'
    and is_valid_send = 1
    -- and has_offers_promos = 1
    -- and master_campaign_name = $target_campaign
    ;

select count(distinct consumer_id) from proddb.fionafan.notif_new_user_table ;


-- DATEDIFF(hour, sent_at, unsubscribed_at)
select case when uninstall_within_24h = 1 then 1 else 0 end,
count(1) cnt
, avg(DATEDIFF(hour, sent_at::timestamp, uninstalled_at::timestamp))
, max(DATEDIFF(hour, sent_at::timestamp, uninstalled_at::timestamp))
-- , count(distinct deduped_message_id||consumer_id||device_id)/ count(distinct consumer_id||device_id) 
from proddb.fionafan.notif_base_table_w_braze_week where uninstalled_at is not null group by all limit 50;

select 
  consumer_id, 
  deduped_message_id, 
  sent_at, 
  uninstalled_at, 
  coalesce(unsubscribed_at, device_unsubscribed_at) as unsubscribed_at_coalesced,
  DATEDIFF(hour, sent_at::timestamp, coalesce(unsubscribed_at, device_unsubscribed_at)::timestamp) as hours_sent_to_unsubscribe
from proddb.fionafan.notif_base_table_w_braze_week 
where uninstalled_at is not null 
  and uninstall_within_24h = 0 
limit 50;
