-- TODO:
    -- consider adding in iguazu.consumer.m_load_tab. could extract tab_name to see what is shown and horizontal_position.. Might just want to track this separately since it will add a ton of data and isn't too valuable
    -- add in DoubleDash impressions? they will bloat the data so leave out for now but will be important to add back in later. For now, I can detect doubledash through the funnel events
    -- in the events_all table, flag when events are post purchase (aka after the first m_checkout_page_system_checkout_success event). Mainly want this to better count foreground events that happen pre-purchase. Also would be nice for pre-purchase and post-purchase user flow analysis.


--Get Store Impressions
create or replace temporary table imps_datevar as
select pst(received_at) as event_date, context_device_type as platform, user_id, dd_device_id, pst_time(timestamp) timestamp, 'store_impression' as event_type ,event,

case
    when page like 'post_checkout%' then 'DoubleDash'
    when page in ('explore_page','homepage') then 'Home Page'
    when page = 'vertical_page' then 'Vertical Page'
    when page ilike ('grocery_tab%') then 'Grocery Tab'
    when page ilike ('retail_tab%') then 'Retail Tab'
    when page = 'vertical_search_page' then 'Vertical Search'
    when page in ('search_autocomplete','search_results') then 'Search Tab'
    when page = 'saved_stores_page' then 'Saved Stores Page'
    when page = 'pickup' then 'Pickup Tab'
    when page IN ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offers Tab'
    -- when container in('store','list') AND page in ('collection_page_list','collection_landing_page') AND tile_name IS NOT NULL then 'Landing Page'
    when page in ('collection_page_list','collection_landing_page') then 'Landing Page'
    when container = 'banner_carousel' then 'Home Page'
    when page ILIKE 'post_checkout%' then 'DoubleDash'
    when page = 'all_reviews' then 'All Reviews Page'
    when page = 'order_history' then 'Order History Tab' -- this is broken for iOS
    when page = 'browse_page' then 'Browse Tab'
    when page = 'open_carts_page' then 'Open Carts Page'
    when page = 'cuisine_see_all_page' then 'Cuisine See All Page'
    when page = 'checkout_aisle' then 'Checkout Aisle Page'
    -- when page = 'cuisine_filter_search_result' then 'Cuisine Search'
    -- when container = 'cluster' then 'Other Pages'
    else 'Other'--page
  end as discovery_surface,

  case
    when page ILIKE 'post_checkout%' then 'DoubleDash'
    when page = 'all_reviews' then 'Reviews'
    when container = 'collection' AND page in ('explore_page', 'vertical_page') AND tile_name IS NOT NULL then 'Collection Carousel Landing Page'
    when page = 'vertical_page' and container = 'cluster' then 'Traditional Carousel'
    when page = 'vertical_search_page' then 'Vertical Search'
    when page in ('search_autocomplete') then 'Autocomplete'
    when page in ('search_results') then 'Core Search'
    when page = 'saved_stores_page' then 'Saved Stores'
    when page = 'pickup' then 'Pickup'
    when page IN ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offers'
    when container in('store','list') AND page in ('collection_page_list','collection_landing_page') AND tile_name IS NOT NULL then 'Collection Carousel Landing Page'
    when container in('store','list') AND page in ('collection_page_list','collection_landing_page') then 'Traditional Carousel Landing Page'
    when container = 'cluster' then 'Traditional Carousel'
    when container = 'banner_carousel' then 'Banner'
    when container IN ('store', 'list', 'item:go-to-store') AND page IN ('explore_page', 'vertical_page','cuisine_see_all_page') AND list_filter ilike '%cuisine:%' then 'Cuisine Filter'
    when container in('store','list') AND page IN ('explore_page', 'vertical_page') AND list_filter IS NOT NULL then 'Pill Filter'
    when container in('store','list') AND page IN ('explore_page', 'vertical_page') then 'Home Feed'
    when container = 'announcement' AND page IN ('explore_page', 'vertical_page') then 'Announcement'
    when page = 'explore_page' AND container = 'carousel' AND lower(carousel_name) = 'cuisines' then 'Entry Point - Cuisine Filter'
    when page = 'explore_page' AND container = 'carousel' AND carousel_name = 'Verticals' then 'Entry Point - Vertical Nav'
    when page = 'order_history' then 'Order History'
    when container ilike 'collection_standard_%'then 'Traditional Carousel'
    when page in ('grocery_tab') and container = 'grid' then 'Grocery Tab - Grid'
    when page in ('retail_tab') and container = 'grid' then 'Retail Tab - Grid'
    when page in ('grocery_tab') and container IN ('store', 'list', 'item:go-to-store') then 'Grocery Tab - Feed'
    when page in ('retail_tab') and container IN ('store', 'list', 'item:go-to-store') then 'Retail Tab - Feed'
    when page in ('grocery_tab_see_all_page') then 'Grocery Tab - See All Page'
    when page in ('retail_tab_see_all_page') then 'Retail Tab - See All Page'
    when page = 'browse_page' and container in('store','list', 'item:go-to-store') and query is not null then 'Browse Tab - Search'
    when page = 'open_carts_page' then 'Open Carts Page'
    else 'Other'--container
  end as discovery_feature,

coalesce(list_filter, query, container_name) as detail, to_number(store_id) as store_id, store_name,  CONTEXT_TIMEZONE, CONTEXT_OS_VERSION, null as event_rank
 from iguazu.consumer.m_card_view
where pst(received_at) = current_date-1
  and store_id is not null
  and item_name is null
  and item_id is null
  and page != 'store' -- Explicitly exclude Store page as a catch-all
  -- and page not like 'post_checkout%' -- Explicitly exclude DbD pages as a catch-all
//  and context_device_type = 'ios'
and ((page != 'order_history' and context_device_type = 'ios') or context_device_type = 'android')-- since order_history impressions are broken on ios. We launch impressions after a order for some reason adn triple fire them.


  ;

  --Fix banner over counting on android
  create or replace temporary table imps_banner_fix_datevar as
  select *
  from imps_datevar
  where discovery_feature = 'Banner' and platform = 'android'
  qualify row_number() over (partition by event_date, platform, dd_device_id, user_id, store_id, detail order by timestamp) = 1
  ;

  delete from imps_datevar where discovery_feature = 'Banner'  and platform = 'android';
  insert into imps_datevar select * from imps_banner_fix_datevar;



--FUNNEL EVENTS:

  --STEP 1: grab all events
  -- 4min
create or replace temporary table funnel_events_1_datevar as
with funnel_events as (
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp, 'store_page_load' as event, dd_session_id,  user_id, store_id, 7 as event_rank, store_status, case when page ilike 'post_checkout%' OR lower(attr_src) like 'post_checkout%' OR is_postcheckout_bundle = true then 1 else 0 end as double_dash_flag, null as order_uuid, source
from segment_events_raw.consumer_production.m_store_page_load
 where pst(received_at) = current_date-1
union all

--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp, 'store_page_load' as event, dd_session_id,  user_id, store_id, 7 as event_rank, store_status, case when bundle_context = 'post_checkout' then 1 else 0 end as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.store_page_load
--  where pst(received_at) = current_date-1
-- union all

--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp, 'item_page_load' as event, dd_session_id,  user_id, store_id, 6 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.item_page_load
--  where pst(received_at) = current_date-1
-- union all


select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'item_page_load' as event, dd_session_id,  user_id, store_id, 6 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_item_page_load
 where pst(received_at) = current_date-1
union all

--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp, 'action_add_item' as event, dd_session_id,  user_id, store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.action_add_item ai
--  where pst(received_at) = current_date-1
-- union all


select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_add_item' as event, dd_session_id,  user_id, store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_item_page_action_add_item
 where pst(received_at) = current_date-1
union all


select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_quick_add_item' as event, dd_session_id,  user_id, store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_action_quick_add_item
 where pst(received_at) = current_date-1
union all
--added 1/2/2024 for NV add item events
select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_add_item' as event, dd_session_id,  user_id, try_to_number(store_id) as store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as source, null as order_uuid
from segment_events_raw.consumer_production.m_stepper_action
where pst(received_at) = current_date-1 and store_id is not null and (page NOT ilike '%post_checkout%' or page is null)
union all
select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp, 'action_add_item' as event, dd_session_id,  user_id, try_to_number(store_id) as store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as source, null as order_uuid
from segment_events_raw.consumer_production.stepper_action
where pst(received_at) = current_date-1 and store_id is not null and (page NOT ilike '%post_checkout%' or page is null) and (bundle_context <> 'post_checkout' or bundle_context is null)
union all
select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_add_item' as event, dd_session_id,  user_id, try_to_number(store_id) as store_id, 5 as event_rank, null as store_status, null as double_dash_flag, null as source, null as order_uuid
from segment_events_raw.consumer_production.m_savecart_add_click
where pst(received_at) = current_date-1 and store_id is not null
union all
--end added 1/2/2024

 select pst(iguazu_received_at) as event_date, dd_device_id, context_device_type, pst_time(iguazu_timestamp) as timestamp, 'order_cart_page_load' as event, dd_session_id,  iguazu_user_id as user_id, store_id, 4 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from iguazu.consumer.m_order_cart_page_load
where pst(iguazu_received_at) = current_date-1
union all

--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp, 'checkout_page_load' as event, dd_session_id,  user_id, store_id, 3 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.checkout_page_load
--  where pst(received_at) = current_date-1
-- union all


select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'checkout_page_load' as event, dd_session_id,  user_id, store_id, 3 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_checkout_page_load
 where pst(received_at) = current_date-1
union all


--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'action_place_order' as event, dd_session_id,  user_id, store_id, 2 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.action_place_order
--  where pst(received_at) = current_date-1
-- union all

select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_place_order' as event, dd_session_id,  user_id, store_id, 2 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_checkout_page_action_place_order
 where pst(received_at) = current_date-1
union all

select pst(received_at) as event_date, dd_device_id, context_device_type, pst_time(timestamp) as timestamp, 'action_place_order' as event, dd_session_id,  user_id, null as store_id, 2 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
from segment_events_raw.consumer_production.m_checkout_page_system_submit
 where pst(received_at) = current_date-1
union all

--web
-- select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'system_checkout_success' as event, dd_session_id,  user_id, store_id, 1 as event_rank, null as store_status, null as double_dash_flag, null as order_uuid, null as source
-- from segment_events_raw.consumer_production.system_checkout_success
--  where pst(received_at) = current_date-1
-- union all




select pst(received_at) as event_date, dd_device_id, context_Device_type, pst_time(timestamp) as timestamp,'system_checkout_success' as event, dd_session_id,  user_id, store_id, 1 as event_rank, null as store_status, null as double_dash_flag, order_uuid, null as source
from segment_events_raw.consumer_production.m_checkout_page_system_checkout_success
 where pst(received_at) = current_date-1

 -- select pst(iguazu_received_at) as event_date, dd_device_id, context_Device_type, pst_time(iguazu_timestamp) as timestamp,'system_checkout_success' as event, dd_session_id,  iguazu_user_id, store_id, 1 as event_rank, null as store_status, null as double_dash_flag, order_uuid, null as source
 -- from iguazu.consumer.m_checkout_page_system_checkout_success
 --  where pst(iguazu_received_at) = current_date-1

)
select * from funnel_events
//union all

;



  --STEP 2: Assign store_ids where they are null by using the most recent one before or after it. (This drops null events from 6.4% to 0.2%; Over 90% of the 0.2% is from Android order cart events when Cx start their session from their abandonded cart)
  --1.5min
create or replace temporary table funnel_events_2_datevar as
select *,
case when store_id is null then null else event_rank end event_rank_w_store,

last_value(store_id) IGNORE NULLS over (partition by dd_device_id, event_date order by timestamp rows between unbounded preceding and current row) as store_id_derived_prior,
last_value(event_rank_w_store) IGNORE NULLS over (partition by dd_device_id, event_date order by timestamp rows between unbounded preceding and current row) as event_rank_prior,

first_value(store_id) IGNORE NULLS over (partition by dd_device_id, event_date order by timestamp rows between current row and unbounded following) as store_id_derived_following,
first_value(event_rank_w_store) IGNORE NULLS over (partition by dd_device_id, event_date order by timestamp rows between current row and unbounded following) as event_rank_following,

case when store_id is not null then store_id::int
     when event_rank_prior >= event_rank then store_id_derived_prior::int
     when event_rank_following <= event_rank then store_id_derived_following::int
     else 0 end as store_id_final
from funnel_events_1_datevar
;

--ATTRIBUTION EVENTS for funnel
    -- 1.5min
create or replace temporary table attribution_events_datevar as
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp, 'm_card_click' as event, dd_session_id,  user_id, store_id, store_name,
case
    --Home Page & Vertical Page
    when page IN ('explore_page') then 'Home Page'
    when page in ('vertical_page') then 'Vertical Page'
    when page in ('grocery_tab') then 'Grocery Tab'
    when page in ('retail_tab') then 'Retail Tab'
    --Pickup Tab
    when tab = 'pickup' and page = 'pickup' and click_type = 'merchant_info' then 'Pickup Tab'
    when tab = 'pickup' and page = 'pickup' and click_type = 'item' then 'Pickup Tab'
    when tab = 'pickup' and page = 'pickup' and click_type = 'go_to_store' then 'Pickup Tab'
    -- Offer Tab
    when page in ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offers Tab'
    --Search Tab
    when page = 'search_autocomplete' AND container = 'list' then 'Search Tab'
    when page = 'search_results' /*and container = 'item' */ then 'Search Tab'
    when page = 'hybrid_search' then 'Search Tab'
    --Vertical Search
    when page = 'vertical_search_page' /*and container = 'item' */ then 'Vertical Search'
    --DashMart
    when container = 'carousel' and carousel_name = 'Verticals' and vertical_name = 'DashMart' and page in ('explore_page') then 'DashMart'
    --Notifications Hub
    when page = 'notification_hub' then 'Notification Hub'
    --DoubleDash (NOTE: use m_store_page_load directly for attribution)
    end as discovery_surface,
case
    --Home Page & Vertical Page
    when container = 'announcement' and page IN ('explore_page', 'vertical_page') then 'Announcement'
    when container = 'banner_carousel' then 'Banner'
    when container in ('list', 'store', 'item:go-to-store')  and list_filter ilike 'cuisine:%' and page in ('explore_page', 'vertical_page') then 'Cuisine Filter'
    when container in ('cluster')  and list_filter ilike 'cuisine:%' and page in ('cuisine_filter_search_result') then 'Cuisine Filter'
    when container = 'list'  and list_filter not ilike 'cuisine:%' and page in ('explore_page', 'vertical_page') then 'Pill Filter'
    when container IN('cluster', 'store_item_logo_cluster') and page in('explore_page', 'vertical_page') then 'Traditional Carousel'
    when container = 'list' and page = 'collection_page_list' and tile_name is not null then 'Collection Carousel Landing Page'
    when container = 'collection' and page in ('explore_page', 'vertical_page') and tile_name is not null then 'Collection Landing Page Click'
    when container = 'list' and page = 'collection_page_list' then 'Traditional Carousel Landing Page'
    when container = 'list' and list_filter is null and page in ('explore_page', 'vertical_page') then 'Home Feed'
    --Pickup Tab
    when tab = 'pickup' and page = 'pickup' and click_type = 'merchant_info' then 'Store Card Mx Name'
    when tab = 'pickup' and page = 'pickup' and click_type = 'item' then 'Store Card Menu Item'
    when tab = 'pickup' and page = 'pickup' and click_type = 'go_to_store' then 'Store Card Button (“Go to Store”, “Delivery”, “Pickup”)'
    -- Offer Tab
    when page in ('deals_hub_list', 'offers_hub', 'offers_list') then 'Offer Tab'
    --Search Tab
    when page = 'search_autocomplete' AND container = 'list' then 'Autocomplete'
    when page = 'search_results' /*and container = 'item' */ then 'Core Search'
    when page = 'hybrid_search' then 'Hybrid Search'
    --Vertical Search
    when page = 'vertical_search_page' /*and container = 'item' */ then 'Vertical Search'
    --DashMart
    when container = 'carousel' and carousel_name = 'Verticals' and vertical_name = 'DashMart' and page in ('explore_page') then 'DashMart Vertical Entrypoint'
    --Notifications Hub
    when page = 'notification_hub' then 'Notification Hub'
    --DoubleDash (NOTE: use m_store_page_load directly for attribution)
    end as discovery_feature
from iguazu.consumer.m_card_click
 where pst(received_at) = current_date-1
and discovery_feature is not null

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_cms_banner' as event, dd_session_id,  user_id, store_id, null as store_name,
case when page = 'explore_page' then 'Homepage' when page = 'vertical_page' then 'Vertical Page' end as discovery_surface,
'Banner' as discovery_feture
from segment_events_raw.consumer_production.m_cms_banner
 where pst(received_at) = current_date-1
and event_type = 'click'

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_order_history_store_click' as event, dd_session_id,  user_id, store_id, null as store_name,
'Account' as discovery_surface,
'Reorder' as discovery_feture
from segment_events_raw.consumer_production.m_order_history_store_click
 where pst(received_at) = current_date-1

;



--ACTIONS/ MICRO CONVERSION / Other Events
    --3min
create or replace temporary table actions_datevar as
--This takes 2 min to run by itself
select pst(iguazu_received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(iguazu_timestamp) as timestamp,'m_store_content_page_load' as event, dd_session_id,  iguazu_user_id as user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from iguazu.server_events_production.m_store_content_page_load
where pst(iguazu_received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_select_tab - ' || name as event, dd_session_id,  user_id, name as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//,name as tab
from segment_events_raw.consumer_production.m_select_tab
 where pst(received_at) = current_date-1

union all
--NOT TRACKED IN SNOWFLAKE YET
//select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'m_open_carts_page_load' as event, dd_session_id,  user_id, num_cart::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_open_carts_page_load
// where pst(received_at) = current_date-1
//
//union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_notif_hub_page_view' as event, dd_session_id,  user_id, num_items::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_notif_hub_page_view
 where pst(received_at) = current_date-1

union all
--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'m_notif_hub_entry_point_click' as event, dd_session_id,  user_id, num_unread_items::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_notif_hub_entry_point_click
// where pst(received_at) = current_date-1
//
//union all


select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_benefit_reminder_should_display' as event, dd_session_id,  user_id, credits_amount::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_benefit_reminder_should_display
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_benefit_reminder_click' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_benefit_reminder_click
 where pst(received_at) = current_date-1

union all

--could be redundant since the impressions will be attributed back to the banner
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'banner_carousel' as event, dd_session_id,  user_id, container_name as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from iguazu.consumer.m_card_click
 where pst(received_at) = current_date-1
and container = 'banner_carousel'

union all

select pst(received_at) as event_date, dd_device_id, CONTEXT_DEVICE_TYPE as platform, pst_time(timestamp) as timestamp,'m_error_appear - ' || message as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_error_appear
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_enter_address_page_view' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_enter_address_page_view
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_default_address_tap' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_default_address_tap
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_enter_address_page_action_save_success' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_enter_address_page_action_save_success
 where pst(received_at) = current_date-1

union all

--START: Account page clicks

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_subscription_mgmt_page_view' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_subscription_mgmt_page_view
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_account_tap_bookmarks - saved stores' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_account_tap_bookmarks
 where pst(received_at) = current_date-1


union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_account_tap_bookmarks - saved stores' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_account_tap_bookmarks
 where pst(received_at) = current_date-1

union all
--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_bookmarks_page_load - saved stores' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_bookmarks_page_load
// where pst(received_at) = current_date-1
//
//union all

select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_account_tap_dine_in_vouchers' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_account_tap_dine_in_vouchers
 where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_account_saved_groups' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_account_saved_groups
// where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_support_chat_get_help_screen_viewed_deeplink' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_support_chat_get_help_screen_viewed_deeplink
 where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_gift_card_landing_page_view' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_gift_card_landing_page_view
// where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_gift_card_landing_page_view' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_gift_card_entry_point_click
// where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_buy_gift_card' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_buy_gift_card
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_redeem_gift' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_redeem_gift
 where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'click_account_page_care_dash' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.click_account_page_care_dash
// where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_referral_entry_point_click' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_referral_entry_point_click
 where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_view_name' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_view_name
// where pst(received_at) = current_date-1

union all
--new address pageview event (m_enter_address_page_view is the old one)
select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_address_page_view' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_address_page_view
 where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, dd_platform as platform, pst_time(timestamp) as timestamp,'m_accounts_privacy_tap' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_accounts_privacy_tap
// where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_account_tap_notifications' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_account_tap_notifications
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_logout' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_logout
 where pst(received_at) = current_date-1

--END: Account page clicks

--START: Login
union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_onboarding_page_load' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_onboarding_page_load
 where pst(received_at) = current_date-1

union all
-- Android version of m_onboarding_page_load
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_onboarding_page_load' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_intro_page_loaded
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_sign_in_success' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_sign_in_success
 where pst(received_at) = current_date-1

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_registration_success' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_registration_success
 where pst(received_at) = current_date-1


--Other Login success events to consider that I think are for web but confirm with Heming
//login_page_system_signup_successful
//login_page_system_login_successful
//login_page_system_google_signup_successful
//login_page_system_fb_signup_successful
//login_page_system_apple_signup_successful
//login_page_system_google_login_successful
//login_page_system_fb_login_successful
//login_page_system_apple_login_successful
//--m_sign_in_success
//--m_registration_success
//
//doordash_signup_success
//doordash_login_success
//
//select pst(received_at) as event_date, coalesce(original_webview_device_id,dd_device_id) as dd_device_id,coalesce(case when webview_platform = 'web' then null else webview_platform end, platform) as platform, timestamp,'signup_success' as event_type, user_id, 'doordash' as experience
//from segment_events_raw.consumer_production.login_page_system_signup_successful
//where pst(received_at) = current_date-1
//
//union all
//
//select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'doordash_login_success' as event, NULL AS dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.doordash_login_success
// where pst(received_at) = current_date-1

--END: Login




--Promo applied found in matts f-fresh-login-pathing (Not tracked for iOS)
//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'m_consumer_promotion_apply_to_order_cart_success' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_consumer_promotion_apply_to_order_cart_success
// where pst(received_at) = current_date-1

//union all
//--NOT TRACKED
//select pst(received_at) as event_date, dd_device_id, platform, pst_time(timestamp) as timestamp,'m_add_promo_click' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
//from segment_events_raw.consumer_production.m_add_promo_click
// where pst(received_at) = current_date-1


--Payment
union all
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_add_payment_method_result' as event, dd_session_id,  user_id, is_successful::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_add_payment_method_result
 where pst(received_at) = current_date-1
 and (coalesce(payment_method,payment_method_type) is null or coalesce(payment_method,payment_method_type) <> 'ApplePay')

 union all
 select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_change_payment_card_success' as event, dd_session_id,  user_id, NEW_DEFAULT_PAYMENT_METHOD::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
 from segment_events_raw.consumer_production.m_change_payment_card_success
  where pst(received_at) = current_date-1

union all
select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_payment_page_load' as event, dd_session_id,  user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_payment_page_load
 where pst(received_at) = current_date-1


-- Other

union all

select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_launch_appear' as event, dd_session_id, user_id, badge_count as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
from segment_events_raw.consumer_production.m_launch_appear
where pst(received_at) = current_date-1

union all

select pst(iguazu_received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(iguazu_timestamp) as timestamp,'m_app_foreground' as event, dd_session_id, iguazu_user_id as user_id, null as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
 from iguazu.consumer.m_app_foreground
  where pst(iguazu_received_at) = current_date-1

  union all

  select pst(received_at) as event_date, dd_device_id, context_device_type as platform, pst_time(timestamp) as timestamp,'m_checkout_page_action_change_tip' as event, dd_session_id, user_id, amount::varchar as detail, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION
  from segment_events_raw.consumer_production.m_checkout_page_action_change_tip
  where pst(received_at) = current_date-1
;







--COMBINE ALL EVENTS
    --1min

create or replace temporary table events_all_datevar as

with setup1 as (
select * from imps_datevar
where event_date = timestamp::date

union all

select event_date, platform, user_id, dd_device_id, timestamp, 'funnel' as event_type, event, case when double_dash_flag =1 then 'DoubleDash' when source = 'order_history' then 'Order History' else null end as discovery_surface, case when double_dash_flag =1 then 'DoubleDash' when source = 'order_history' then 'Order History' else null end as discovery_feature, null as detail, to_number(store_id_final) as store_id, null as store_name, null as  CONTEXT_TIMEZONE, null as CONTEXT_OS_VERSION, event_rank
from funnel_events_2_datevar
where event_date = timestamp::date
 --event_rank
  -- i eventually group repeats as a cleaning step..
  --might need to make them varchars to union later

union all

select event_date, platform, user_id, dd_device_id, timestamp, 'attribution' as event_type, event, discovery_surface, discovery_feature, null as detail, to_number(store_id), store_name, null as  CONTEXT_TIMEZONE, null as CONTEXT_OS_VERSION, null as event_rank
from attribution_events_datevar
where event_date = timestamp::date

union all

select event_date, platform, user_id, dd_device_id, timestamp,
case
    when event in ('m_onboarding_page_load','m_sign_in_success', 'm_registration_success') then 'action - login'
    when event in ('m_subscription_mgmt_page_view', 'm_account_tap_bookmarks - saved stores','m_account_tap_dine_in_vouchers', 'm_buy_gift_card', 'm_support_chat_get_help_screen_viewed_deeplink', 'm_redeem_gift', 'm_referral_entry_point_click', 'm_account_tap_notifications', 'm_logout') then 'action - account'
    when event in('m_enter_address_page_view', 'm_default_address_tap', 'm_enter_address_page_action_save_success', 'm_address_page_view') then 'action - address'
    when event in ('m_benefit_reminder_should_display', 'm_benefit_reminder_click') then 'action - credits'
    when event in ('m_payment_page_load', 'm_add_payment_method_result') then 'action - payment'
    when event ilike '%error%' then 'error'
    when event ilike '%m_select_tab%' then 'action - tab'
    else 'action'end as event_type,
event, null as discovery_surface, null as discovery_feature, detail, null as store_id, null as store_name, CONTEXT_TIMEZONE, CONTEXT_OS_VERSION, null as event_rank
from actions_datevar
where event_date = timestamp::date
)

, setup2 as (
select *,
case when event_type in('store_impression','funnel') then discovery_surface end discovery_surface_imp_setup, case when event_type in('store_impression','funnel') then discovery_feature end discovery_feature_imp_setup,
case when event_type in('attribution','funnel') then discovery_surface end discovery_surface_click_setup, case when event_type in('attribution','funnel') then discovery_feature end discovery_feature_click_setup,

case when event_type = 'funnel' then last_value(discovery_surface_imp_setup) IGNORE NULLS over (partition by dd_device_id, event_date, store_id order by timestamp rows between unbounded preceding and current row) end as discovery_surface_imp_attr,
case when event_type = 'funnel' then last_value(discovery_feature_imp_setup) IGNORE NULLS over (partition by dd_device_id, event_date, store_id order by timestamp rows between unbounded preceding and current row) end as discovery_feature_imp_attr,

case when event_type = 'funnel' then last_value(discovery_surface_click_setup) IGNORE NULLS over (partition by dd_device_id, event_date, store_id order by timestamp rows between unbounded preceding and current row) end as discovery_surface_click_attr,
case when event_type = 'funnel' then last_value(discovery_feature_click_setup) IGNORE NULLS over (partition by dd_device_id, event_date, store_id order by timestamp rows between unbounded preceding and current row) end as discovery_feature_click_attr,

case when event_type = 'funnel' then coalesce(discovery_surface,discovery_surface_click_attr,discovery_surface_imp_attr, 'Unknown') else coalesce(discovery_surface, event_type) end as discovery_surface_final,
case when event_type = 'funnel' then coalesce(discovery_feature,discovery_feature_click_attr,discovery_feature_imp_attr, 'Unknown') else coalesce(discovery_feature, event) end as discovery_feature_final, --'event' messes up user flow since it will group by a new feature name

lag(timestamp) over (partition by dd_device_id, event_date order by timestamp) as prior_ts,
case when prior_ts is null or datediff('minute',prior_ts, timestamp) >= 30 then timestamp else null end as first_session_group_ts_setup

from setup1
where platform in ('ios','android')

)

, setup3 as (
  select *, max(first_session_group_ts_setup) over (partition by dd_device_id, event_date, timestamp) as first_session_group_ts -- for impressions that fire at the same exact time, put them in the same group unless it wasn't the first impression

  from setup2
  )

, setup4 as (
select *, last_value(first_session_group_ts) IGNORE NULLS over (partition by dd_device_id, event_date order by timestamp rows between unbounded preceding and current row) as session_group_ts
from setup3
)

select event_date, dense_rank() over (partition by dd_device_id, event_date order by session_group_ts) as session_num,
platform, dd_device_id, user_id, timestamp, event_type, event, discovery_surface_final as discovery_surface, discovery_feature_final as discovery_feature, detail, a.store_id, ds.name as store_name,
 CONTEXT_TIMEZONE,CONTEXT_OS_VERSION, event_rank, discovery_surface_click_attr, discovery_feature_click_attr, discovery_surface_imp_attr, discovery_feature_imp_attr,
 1 as pre_purchase_flag, null as l91d_orders, null as last_order_date
from setup4 a
left join dimension_store ds on a.store_id = ds.store_id

;


create or replace temporary table pre_purchase_datevar as
SELECT event_date, session_num, dd_device_id, min(case when event = 'system_checkout_success' then timestamp else null end) as min_purchase_ts
from events_all_datevar
group by 1,2,3
;

UPDATE events_all_datevar e
SET e.pre_purchase_flag = 0
from pre_purchase_datevar p
WHERE e.dd_device_id = p.dd_device_id and e.event_date = p.event_date and e.session_num = p.session_num
and e.timestamp > min_purchase_ts
;


create or replace temporary table l91d_orders_datevar as (
  select try_to_number(creator_id) creator_id, try_to_number(store_id) store_id, count(*) as l91d_orders, max(active_date) last_order_date
from dimension_deliveries dd
where is_filtered_core
and active_date between dateadd(day,-91,current_date-1) and dateadd(day,-1,current_date-1)
group by 1,2
)
;



UPDATE events_all_datevar e
SET e.l91d_orders = p.l91d_orders,
    e.last_order_date = p.last_order_date
from l91d_orders_datevar p
WHERE try_to_number(e.user_id) = p.creator_id and e.store_id = p.store_id
;




delete from tyleranderson.events_all where event_date = current_date-1;
insert into tyleranderson.events_all select * from events_all_datevar;


delete from tyleranderson.events_all_agg where event_date = current_date-1;
insert into tyleranderson.events_all_agg
  select event_date, platform, event_type, event, discovery_surface, discovery_feature, count(*) cnt
  from tyleranderson.events_all
  where event_date = current_date-1
  group by 1,2,3,4,5,6
  ;



--Create Sessions table


  create or replace temporary table sessions_datevar as
  with setup1 as (
  select e.*, nv.vertical_name,
  last_value(user_id) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp) as max_user_id,
  last_value(CONTEXT_TIMEZONE) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp) as timezone,
  first_value(CONTEXT_OS_VERSION) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp) as os_version,
  first_value(lower(platform)) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp) as platform_,
  case when event_type = 'funnel' then timestamp end as funnel_ts,
  case when event_type = 'error' then timestamp end as error_ts,
  case when event_type in ('error','funnel','store_impression') or event in ('m_store_content_page_load', 'm_onboarding_page_load') then timestamp end as landing_page_ts


  from tyleranderson.events_all e
  left join edw.cng.dimension_new_vertical_store_tags nv on nv.store_id = e.store_id and nv.is_filtered_mp_vertical = 1
  where event_date = current_date-1
  --would need to join on store_id to get vertical here if I wanted to add a did_visit_nv cut
  )

  select event_date, dd_device_id, session_num, max_user_id as user_id, platform_ as platform, timezone, os_version,
  case when min_by(event,landing_page_ts) = 'm_card_view' then 'm_store_content_page_load' when min_by(event_type,landing_page_ts) = 'error' then 'Error' else min_by(event,landing_page_ts) end as landing_page,


  --funnel
  1 as visitor,
  max(case when event in ('m_store_content_page_load', 'store_page_load', 'item_page_load', 'action_add_item', 'action_quick_add_item', 'order_cart_page_load', 'checkout_page_load', 'action_place_order','system_checkout_success') or event_type = 'store_impression' then 1 else 0 end) as core_visitor,
  max(case when event in ('store_page_load') then 1 else 0 end) as store_page_visitor,
  max(case when event in ('item_page_load') then 1 else 0 end) as item_page_visitor,
  max(case when event in ('action_add_item','action_quick_add_item') then 1 else 0 end) as item_add_visitor,
  max(case when event in ('order_cart_page_load') then 1 else 0 end) as order_cart_page_visitor,
  max(case when event in ('checkout_page_load') then 1 else 0 end) as checkout_page_visitor,
  max(case when event in ('action_place_order') then 1 else 0 end) as action_place_order_visitor,
  max(case when event = 'system_checkout_success' then 1 else 0 end) as purchaser,
  min_by(event,event_rank) as max_funnel_step,
  min_by(event,funnel_ts) as first_funnel_step,

  --# of imptessions and stores visited
  count(distinct case when event_type = 'store_impression' then store_id end) as unique_store_impressions,
  count(distinct case when event_type = 'funnel' then store_id end) as unique_store_visits,

  count(distinct case when event_type = 'store_impression' and discovery_surface = 'Home Page' then store_id end) as home_page_impressions,
  count(distinct case when event_type = 'store_impression' and discovery_surface = 'Search Tab' then store_id end) as search_impressions,
  count(distinct case when event_type = 'store_impression' and discovery_surface = 'Vertical Page' then store_id end) as vertical_page_impressions,
  count(distinct case when event_type = 'store_impression' and discovery_surface not in('Vertical Page', 'Search Tab', 'Home Page','DoubleDash') then store_id end) as other_impressions,

  count(distinct case when event_type = 'funnel' and discovery_surface = 'Home Page' then store_id end) as home_page_store_visits,
  count(distinct case when event_type = 'funnel' and discovery_surface = 'Search Tab' then store_id end) as search_store_visits,
  count(distinct case when event_type = 'funnel' and discovery_surface = 'Vertical Page' then store_id end) as vertical_page_store_visits,
  count(distinct case when event_type = 'funnel' and discovery_surface not in('Vertical Page', 'Search Tab', 'Home Page') then store_id end) as other_store_visits, -- split out doubledash later if needed

  --micro conversions
  max(case when event in ('m_notif_hub_page_view', 'm_account_tap_notifications') then 1 else 0 end) as notifications_tab_visitor,
  max(case when event_type = 'attribution' and event = 'm_card_click' and discovery_feature = 'Notification Hub' then 1 else 0 end) notifications_tab_click_visitor,
  max(case when event in ('m_add_payment_method_result', 'm_payment_page_load','m_change_payment_card_success') then 1 else 0 end) as payment_page_visitor,
  max(case when event in ('m_add_payment_method_result') then 1 else 0 end) as payment_add_visitor,
  max(case when event in ('m_add_payment_method_result') and detail = 'true' then 1 else 0 end) as payment_add_success_visitor,
  max(case when event in ('m_change_payment_card_success') then 1 else 0 end) as payment_change_visitor,
  max(case when event_type = 'action - address' then 1 else 0 end) as address_visitor,
  max(case when event in ('m_default_address_tap', 'm_enter_address_page_action_save_success') then 1 else 0 end) as address_update_visitor,
  max(case when event in ('m_onboarding_page_load') then 1 else 0 end) as onboarding_page_visitor,
  max(case when event in ('m_sign_in_success', 'm_registration_success') then 1 else 0 end) as sign_in_success_visitor,
  max(case when event_type = 'attribution' and event = 'm_cms_banner' then 1 else 0 end) banner_click_visitor,
  --Add Review impression and review submission

  --account page
  max(case when event in ('m_redeem_gift') then 1 else 0 end) as redeem_gift_visitor,
  max(case when event in ('m_buy_gift_card') then 1 else 0 end) as buy_gift_visitor,
  max(case when event in ('m_support_chat_get_help_screen_viewed_deeplink') then 1 else 0 end) as support_visitor,
  max(case when event in ('m_referral_entry_point_click') then 1 else 0 end) as referral_visitor,
  max(case when event in ('m_subscription_mgmt_page_view') then 1 else 0 end) as manage_sub_visitor,
  max(case when event in ('m_logout') then 1 else 0 end) as log_out_visitor,

  --other
  max(case when event in ('m_benefit_reminder_should_display') then try_to_number(detail) else 0 end)/100.00 as credits_available,
  min_by(event, error_ts) as first_error,
  max_by(event, error_ts) as last_error,




  min(timestamp) as start_ts, max(timestamp) as end_ts, datediff('seconds',start_ts, end_ts) as duration_seconds,
  min_by(event,timestamp) as first_event, max_by(event,timestamp) as last_event, count(*) as events --event_count
  , count(distinct event) as distinct_events,

  coalesce(lag(purchaser) over (partition by dd_device_id, event_date order by session_num),0) as is_prior_session_purchaser,
  max(case when event in ('m_launch_appear') then detail else 0 end) as badge_count,
  sum(case when event in ('m_app_foreground') and pre_purchase_flag = 1 then 1 else 0 end) as foreground_count,

  max(case when event in('m_select_tab - grocery') or discovery_surface ilike 'Grocery Tab%' then 1 else 0 end) as grocery_tab_visitor,
  max(case when event in('m_select_tab - retail') or discovery_surface ilike 'Retail Tab%' then 1 else 0 end) as retail_tab_visitor,
  max(case when event in('m_select_tab - pickup') or discovery_surface ilike 'Pickup Tab%' then 1 else 0 end) as pickup_tab_visitor,
  max(case when event in('m_select_tab - browse') or discovery_surface ilike 'Browse Tab%' then 1 else 0 end) as browse_tab_visitor,
  max(case when event in('m_select_tab - orders') or discovery_surface ilike 'Order History' then 1 else 0 end) as orders_tab_visitor,
  'Bounce' as first_action,
  'Bounce' as first_action_detail,
  null as IS_DASHPASS,
  null as country_name,
  null as user_type,
  null as media_type,
  null as ghost_push_ts,
  count(distinct case when event_type = 'funnel' and l91d_orders is not null then store_id end) as past_purchase_store_visits,--prior_purchase_unique_store_visits,

  count(distinct case when event_type = 'store_impression' and pre_purchase_flag = 1 and discovery_surface <> 'DoubleDash' then store_id end) as unique_store_impressions_pp, --pp = pre purchase
  count(distinct case when event_type = 'funnel' and pre_purchase_flag = 1 and discovery_surface <> 'DoubleDash' then store_id end) as unique_store_visits_pp,--pp = pre purchase
  count(distinct case when event_type = 'funnel' and l91d_orders is not null and pre_purchase_flag = 1 and discovery_surface <> 'DoubleDash' then store_id end) as past_purchase_store_visits_pp,--pp = pre purchase
  '5. Unknown' as sensitivity_type,
  null as sensitivity_cohort,
  max(case when event = 'm_checkout_page_action_change_tip' then 1 else 0 end) as change_tip_visitor,
  min(case when event = 'system_checkout_success' then timestamp end) as purchase_ts,
  max(case when vertical_name is not null and event_type = 'funnel' then 1 else 0 end) as nv_visitor,
  count(distinct case when event_type = 'funnel' and vertical_name is not null then store_id end) as nv_store_visits,
  count(distinct case when event_type = 'funnel' and PRE_PURCHASE_FLAG = 1 then store_id end) pre_purchase_store_visits,
  count(distinct case when event_type = 'funnel' and PRE_PURCHASE_FLAG = 0 then store_id end) post_purchase_store_visits,
  count(distinct case when event_type = 'funnel' and PRE_PURCHASE_FLAG = 1 and vertical_name is not null then store_id end) pre_purchase_nv_store_visits,
  count(distinct case when event_type = 'funnel' and PRE_PURCHASE_FLAG = 0 and vertical_name is not null then store_id end) post_purchase_nv_store_visits,

  max(case when (discovery_surface = 'Search' or discovery_feature ilike '%search%') and event_type = 'store_impression' then 1 else 0 end) as search_visitor,
  max(case when discovery_feature ilike '%filter%' and event_type = 'store_impression' then 1 else 0 end) as filter_visitor,
  max(case when discovery_surface ilike 'Offer%' or event = 'm_select_tab - Deals Hub' then 1 else 0 end) as offers_hub_visitor,
  array_agg(distinct case when discovery_surface ilike 'search%' then detail end) as search_array,
  array_agg(distinct case when discovery_feature ilike '%filter%' and event_type = 'store_impression' then detail end) as filter_array,
  array_agg(distinct case when event_type = 'funnel' then store_name end) as store_visit_array,
  min_by(case when event = 'system_checkout_success' then store_id end,case when event = 'system_checkout_success' then timestamp end) as purchase_store_id,
  min_by(case when event = 'system_checkout_success' then store_name end,case when event = 'system_checkout_success' then timestamp end) as purchase_store_name,
  max(case when event = 'system_checkout_success' and l91d_orders is not null then 1 else 0 end) as purchased_from_prior_store,
  count(distinct case when event_type in ('store_impression','funnel') and discovery_surface = 'DoubleDash' then store_id end) as doubledash_impressions




  --add session_id? event_date || ' - ' || dd_device_id || ' - ' ||session_num
  -- add daily_purchaser... probably easiest if I set as null then update with the table itself using max(purchaser) group by dd_device_id, event_date

  from setup1
  group by 1,2,3,4,5,6,7
  ;

  delete from tyleranderson.sessions where event_date = current_date-1;
  insert into tyleranderson.sessions select * from sessions_datevar;

  ----------------------------
  --First Action
  ----------------------------
  create or replace temporary table  event_group_setup_datevar as
  with setup1 as (
  select event_date, platform, dd_device_id, session_num, user_id, timestamp, event_type, event, discovery_surface, discovery_feature, detail, store_id, store_name, context_timezone, context_os_version, to_number(event_rank) event_rank,

  lag(discovery_surface) over (partition by dd_device_id, event_date, session_num order by timestamp) as prior_discovery_surface,
  lag(DISCOVERY_FEATURE) over (partition by dd_device_id, event_date, session_num  order by timestamp) as prior_DISCOVERY_FEATURE,
  lag(store_id) over (partition by dd_device_id, event_date, session_num  order by timestamp) as prior_store_id,
  lag(event_type) over (partition by dd_device_id, event_date, session_num  order by timestamp) as prior_event_type,

  case when event = 'system_checkout_success' then timestamp else null end as order_ts,

  case when prior_event_type is null or prior_event_type <> event_type then timestamp
       when event_type = 'funnel' and store_id <> prior_store_id then timestamp
       when event_type = 'store_impression' and (discovery_surface <> prior_discovery_surface or discovery_feature <> prior_discovery_feature) then timestamp
       else null end as first_intent_group_ts,

  case when prior_event_type is null then timestamp
       when event_type in('store_impression','funnel') and (discovery_surface <> prior_discovery_surface or discovery_feature <> prior_discovery_feature) then timestamp
       when event_type not in('store_impression','funnel') and prior_event_type <> event_type then timestamp
       else null end as first_feature_group_ts,

  case when prior_event_type is null or coalesce(discovery_surface,event_type) <> coalesce(prior_discovery_surface,prior_event_type) then timestamp
  //     when event_type = 'funnel' and store_id <> prior_store_id then timestamp
       else null end as first_surface_group_ts

  from tyleranderson.events_all e
  where (event_type in ('funnel', 'action - login', 'action - address', 'action - account', 'action - tab'/*, 'action - payment', 'error'*/)
          or (event_type = 'store_impression' and discovery_feature <> 'Banner')
          or event in ('m_cms_banner', 'm_notif_hub_page_view', 'm_notif_hub_entry_point_click'/*, 'm_app_foreground'*/)) -- cut errors and payment for now. Can add to the session itself and keep this more discovery related. cut foreground since it would always be the first action for continued sessions.
  and event_date = current_date-1

  )



  select *,
  last_value(first_intent_group_ts) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp rows between unbounded preceding and current row) as intent_group_ts,
  //last_value(first_feature_group_ts) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp rows between unbounded preceding and current row) as feature_group_ts,--add back in for user flow
  //last_value(first_surface_group_ts) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp rows between unbounded preceding and current row) as surface_group_ts,--add back in for user flow
  last_value(order_ts) IGNORE NULLS over (partition by dd_device_id, event_date, session_num order by timestamp rows between unbounded preceding and 1 preceding) as first_purchase_ts,
  case when first_purchase_ts is not null then 1 else 0 end as post_purchase_flag

  from setup1
    ;



create or replace temporary table first_action_datevar as

  with setup0 as (
  select event_date, platform, dd_device_id, session_num, intent_group_ts, discovery_surface, discovery_feature, count(distinct case when event_type = 'store_impression' then store_id end) store_impressions, count(distinct case when event_type = 'funnel' then store_id end) store_visits,
    min_by(event,event_rank) as max_funnel_step ,
    min_by(event,timestamp) first_funnel_step,
    max(post_purchase_flag) as post_purchase_flag

  from event_group_setup_datevar
  //where platform = 'ios'
  group by 1,2,3,4,5,6,7

  )

  ,setup1 as (
  select *,
  lead(discovery_surface) over (partition by dd_device_id, event_date, session_num order by intent_group_ts) as next_discovery_surface,
  lead(DISCOVERY_FEATURE) over (partition by dd_device_id, event_date, session_num order by intent_group_ts) as next_DISCOVERY_FEATURE,
  lead(store_impressions) over (partition by dd_device_id, event_date, session_num order by intent_group_ts) as next_store_impressions,
  lead(store_visits) over (partition by dd_device_id, event_date, session_num order by intent_group_ts) as next_store_visits,
  lead(first_funnel_step) over (partition by dd_device_id, event_date, session_num order by intent_group_ts) as next_first_funnel_step,
  max(post_purchase_flag) over (partition by dd_device_id, event_date, session_num) as did_order
  from setup0
  )

      --get first event
  , setup2 as (
  select *
  from setup1
  qualify row_number() over (partition by dd_device_id, event_date, session_num order by intent_group_ts) =1
  )

, setup3 as (
  select dd_device_id, event_date, session_num,
  case
      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel' and store_impressions between 3 and 9 then 'Home Page Scroll 3-9'
      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel'  and store_impressions >=10 then 'Home Page Scroll 10+'
      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel' and store_impressions <=2 and next_store_visits > 0 and next_first_funnel_step = 'order_cart_page_load' then 'Continue Abandoned Cart'

      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel' and store_impressions <=2 and next_discovery_surface = 'Home Page' and next_discovery_feature = 'Traditional Carousel' and next_store_visits > 0 then 'Storepage Visit'
      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel' and store_impressions <=2  and next_discovery_surface is null then 'Bounce'
      when discovery_surface = 'Home Page' and discovery_feature = 'Traditional Carousel' and store_impressions <=2 then next_discovery_surface || ' - ' || next_discovery_feature
      else discovery_surface || ' - ' || discovery_feature end as first_action_detail

  from setup2 s
)

select dd_device_id, event_date, session_num,
--banner clicks are probably next. They already make sense to do for andorid ('attribution - Banner', 'Home Page - Banner')
case when first_action_detail in ('Search Tab - Autocomplete', 'Search Tab - Core Search', 'action - tab - m_select_tab - search') then 'Search'
     when first_action_detail in ('Home Page - Cuisine Filter', 'Home Page - Pill Filter') then 'Home Page - Filter'
     when first_action_detail in('action - tab - m_select_tab - orders','Order History - Order History') then 'Nav - Orders Tab'
     when first_action_detail in('action - m_notif_hub_page_view') then 'Nav - Notifications Hub'
     when first_action_detail in('action - tab - m_select_tab - explore','action - tab - m_select_tab - delivery') then 'Nav - Home Page'
     when first_action_detail in('action - address - m_enter_address_page_view','action - address - m_address_page_view','action - address - m_default_address_tap','action - address - m_enter_address_page_action_save_success') then 'Nav - Address'
     when first_action_detail in('action - tab - m_select_tab - browse')  or first_action_detail ilike 'Browse Tab%' then 'Nav - Browse Tab'
     when first_action_detail in('action - tab - m_select_tab - account','action - account - m_subscription_mgmt_page_view') or first_action_detail ilike 'action - account -%' then 'Nav - Account'
     when first_action_detail in('action - login - m_onboarding_page_load','action - login - m_sign_in_success','action - login - m_registration_success') then 'Login'
     when first_action_detail ilike 'Vertical Page%' then 'Vertical Page'
     when first_action_detail in('Open Carts Page - Open Carts Page') then 'Nav - Open Carts Page'
     when first_action_detail in('action - tab - m_select_tab - pickup','Pickup Tab - Pickup') or first_action_detail ilike 'Pickup Tab%' then 'Nav - Pickup'
     when first_action_detail in('action - tab - m_select_tab - grocery') or first_action_detail ilike 'Grocery Tab%' then 'Nav - Grocery'
     when first_action_detail in('action - tab - m_select_tab - retail') or first_action_detail ilike 'Retail Tab%' then 'Nav - Retail'
     when first_action_detail ilike 'Offer%' or first_action_detail in('action - tab - m_select_tab - Deals Hub') then 'Offers Tab'
     when first_action_detail in('Home Page Scroll 3-9', 'Home Page Scroll 10+','Bounce','Continue Abandoned Cart','Storepage Visit') then first_action_detail
     else 'Other' end as first_action,
first_action_detail
from setup3
  ;

delete from tyleranderson.first_action where event_date = current_date-1;
insert into tyleranderson.first_action select * from first_action_datevar;

----------------------------
--Update Sessions Table
----------------------------

UPDATE tyleranderson.sessions s
 SET s.first_action = f.first_action,
     s.first_action_detail = f.first_action_detail
 from tyleranderson.first_action f
 WHERE f.dd_device_id = s.dd_device_id and f.event_date = s.event_date and f.session_num = s.session_num
 and f.event_date = current_date-1 and s.event_date = current_date-1
;


UPDATE tyleranderson.sessions s
SET s.is_dashpass = uv.is_dashpass,
    s.media_type = uv.media_type,
    s.country_name = uv.country_name,
    s.user_type = case when uv.first_order_date is null and uv.user_id is not null then 'New'
                       when uv.first_order_date is null and uv.user_id is null then 'Unrecognized'
                       when datediff('day',uv.first_order_date, uv.event_date) between 1 and 28 then 'P0'
                       when datediff('day',uv.last_order_date, uv.event_date) between 29 and 89 then 'Dormant'
                       when datediff('day',uv.last_order_date, uv.event_date) >= 90 then 'Churned'
                       when datediff('day',uv.last_order_date, uv.event_date) <= 28 then 'Active'
                           end
from mattheitz.fact_unique_visitors_full uv
WHERE uv.dd_device_id = s.dd_device_id and uv.event_date = s.event_date
 and uv.event_date = current_date-1 and s.event_date = current_date-1
 ;

 --ghost push logic
 create or replace temporary table gp_base_datevar as (
   select pst(received_at) as event_date, dd_device_id,  pst_time(timestamp) as event_timestamp, pst_time(received_at) as received_timestamp,pst_time(sent_at) as sent_timestamp,pst_time(original_timestamp) as original_timestamp,
   PARSE_JSON(PARSE_JSON(other_properties):itbl):isGhostPush as ghost_push_flag,
   PARSE_JSON(PARSE_JSON(other_properties):aps):"content-available" as ca_flag,
    PARSE_JSON(PARSE_JSON(other_properties):"content_available") as ca2_flag,
   push_event_id, type, CONTEXT_DEVICE_TYPE
   from iguazu.consumer.m_push_notification_received
   where pst(received_at) = current_date-1 and (contains(other_properties,'content-available') or contains(other_properties,'content_available'))
 )
 ;
 create or replace temporary table gp_datevar as (
 select event_date, dd_device_id, received_timestamp as ghost_ts
 from gp_base_datevar
 where ghost_push_flag = 'true' or (push_event_id is not null and type = 'consumer_push' and (ca_flag = 1 or ca2_flag = 'true') )
 )
 ;

 UPDATE tyleranderson.sessions s
 SET s.ghost_push_ts = g.ghost_ts,
     s.core_visitor = 0 --update so these Cx are not considered a core visitor
 from gp_datevar g
 WHERE g.dd_device_id = s.dd_device_id  and g.event_date = s.event_date and abs(datediff(minute,ghost_ts,start_ts)) <= 5
 and s.event_date = current_date-1;


 UPDATE tyleranderson.sessions s
 SET s.sensitivity_type = case when cs.cohort = 'p84d_active_very_insensitive'
  then '1. Very Insensitive'
  when cs.cohort in ('p84d_active_very_sensitive','p84d_churned')
  then '2. Very Sensitive Or Churned'
  when cs.cohort in ('p84d_active_middle','p84d_active_insensitive','p84d_active_sensitive')
  then '3. Middle Sensitivity'
 when cs.cohort like 'dp_active%' then '4. DashPass'
 when cs.cohort is null then '5. Unknown'
  end,
  s.sensitivity_cohort = cs.cohort
 from cx_sensitivity_v2 cs
 WHERE s.user_id = cs.consumer_id::VARCHAR(16777216) and s.event_date = cs.PREDICTION_DATETIME_EST
and s.event_date = current_date-1 and cs.PREDICTION_DATETIME_EST = current_date-1;

----------------------------
--Sessions Agg
----------------------------
delete from tyleranderson.session_agg where event_date = current_date-1;
insert into tyleranderson.session_agg
//create or replace table tyleranderson.session_agg as
select s.event_date, s.platform,


case when s.landing_page in ('m_store_content_page_load','m_onboarding_page_load','store_page_load', 'order_cart_page_load','Error') then s.landing_page else 'Other' end as landing_page,


is_prior_session_purchaser, case when s.session_num in (1,2) then s.session_num::varchar else '3+' end as session_num_group,
first_funnel_step, coalesce(first_action,'Bounce') first_action ,core_visitor as core_visitor_flag,

case when media_type in ('App Direct','CRM - Push','CRM - Email','ASO','Web Display') then media_type else 'Other' end media_type,



case when hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts)) between 0 and 4 then '1. Early Morning'
     when  hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts))  between 5 and 10 then '2. Breakfast'
     when  hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts))  between 11 and 13 then '3. Lunch'
     when  hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts))  between 14 and 16 then '4. Snack'
     when  hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts))  between 17 and 20 then '5. Dinner'
     when  hour(convert_timezone('America/Los_Angeles',case when s.timezone = 'GMT+08:00' then 'America/Los_Angeles' else s.timezone end,start_ts))  between 21 and 23 then '6. Late Night'
     else 'Unknown'
   end daypart,

is_dashpass, country_name,user_type, sensitivity_type,

case when credits_available > 0 then 'Has Credits' else 'No Credits' end as credits_flag,

case when duration_seconds = 0 then '1. 0 seconds'
     when duration_seconds <= 10 then '2. 1-10 seconds'
     when duration_seconds <= 60 then '3. 11-60 seconds'
     when duration_seconds <= 60*5 then '4. 1-5 minutes'
     when duration_seconds <= 60*15 then '5. 5-15 minutes'
     when duration_seconds <= 60*30 then '6. 15-30 minutes'
     else '7. 30+ minutes' end as session_duration_bucket,

case when unique_store_visits_pp = 1 and past_purchase_store_visits_pp = 1 then '2. 1 Mx - Prior purchase'
     when unique_store_visits_pp = 1 then '3. 1 Mx - No prior purchase'
     when unique_store_visits_pp >=2 and past_purchase_store_visits_pp >= 1 then '4. 2+ Mx - Prior purchase'
     when unique_store_visits_pp >=2 then '5. 2+ Mx - No prior purchase'
     when unique_store_visits_pp = 0 then '1. 0 Mx'
     else 'Other' end as session_summary,


--imp bucketing (do the splits I have here: 0, 1-2, 3-9, 10-20, 21+)
case when unique_store_impressions_pp = 0 then '1. 0 Impressions'
    when unique_store_impressions_pp between 1 and 2 then '2. 1-2 Impressions'
    when unique_store_impressions_pp between 3 and 9 then '3. 3-9 Impressions'
    when unique_store_impressions_pp between 10 and 20 then '4. 10-20 Impressions'
    else '5. 21+ Impressions' end as store_impression_bucket,
    nv_visitor,



--funnel
sum(s.visitor) as visitors,
sum(s.core_visitor) as core_visitors,
sum(s.store_page_visitor) store_page_visitors,
sum(s.item_page_visitor) as item_page_visitors,
sum(s.item_add_visitor) as item_add_visitors,
sum(s.order_cart_page_visitor) as order_cart_page_visitors,
sum(s.checkout_page_visitor) as checkout_page_visitors,
sum(s.action_place_order_visitor) as action_place_order_visitors,
sum(s.purchaser) as purchasers,

--impressions
sum(s.unique_store_impressions) unique_store_impressions,
sum(s.unique_store_visits) unique_store_visits,
sum(nv_store_visits) as nv_store_visits,
sum(pre_purchase_store_visits) as pre_purchase_store_visits,
sum(post_purchase_store_visits) as post_purchase_store_visits,
sum(pre_purchase_nv_store_visits) as pre_purchase_nv_store_visits,
sum(post_purchase_nv_store_visits) as post_purchase_nv_store_visits,

sum(home_page_impressions) home_page_impressions,
sum(search_impressions) search_impressions,
sum(vertical_page_impressions) vertical_page_impressions,
sum(other_impressions) other_impressions,


---micro conversions
sum(notifications_tab_visitor) notifications_tab_visitor,
sum(notifications_tab_click_visitor) notifications_tab_click_visitor,
sum(payment_page_visitor) payment_page_visitor,
sum(payment_add_success_visitor) payment_add_success_visitor,
sum(payment_change_visitor) payment_change_visitor,
sum(address_visitor) address_visitor,
sum(address_update_visitor) address_update_visitor,
sum(s.onboarding_page_visitor) onboarding_page_visitor,
sum(sign_in_success_visitor) sign_in_success_visitor,
sum(banner_click_visitor) banner_click_visitor,

sum(grocery_tab_visitor) grocery_tab_visitor,
sum(retail_tab_visitor) retail_tab_visitor,
sum(pickup_tab_visitor) pickup_tab_visitor,
sum(browse_tab_visitor) browse_tab_visitor,
sum(orders_tab_visitor) orders_tab_visitor,

--Add Review impression and review submission

--account page
sum(redeem_gift_visitor) redeem_gift_visitor,
sum(buy_gift_visitor) buy_gift_visitor,
sum(support_visitor) support_visitor,
sum(referral_visitor) referral_visitor,
sum(manage_sub_visitor) manage_sub_visitor,
sum(log_out_visitor) log_out_visitor,

--other
sum(credits_available) credits_available,




sum(duration_seconds/60) as session_duration_minutes,
sum(case when duration_seconds = 0 then 1 else 0 end) as session_duration_0_seconds,
sum(case when duration_seconds <= 10 then 1 else 0 end) as session_duration_under_10_seconds,
sum(case when badge_count > 0 then 1 else 0 end) as has_badge,
sum(case when foreground_count > 0 then 1 else 0 end) as has_foreground, -- this might have to be pre-purchase... would need to add a field to events_all that flags when an event is pre or post purchase (big window function, do this 1d at a time)
sum(change_tip_visitor) as change_tip_visitor,
sum(search_visitor) as search_visitor,
sum(filter_visitor) as filter_visitor,
sum(offers_hub_visitor) as offers_hub_visitor,

--day conversion... will need to do window function max(checkout success)


sum(events) event_count,
sum(distinct_events) distinct_events


from tyleranderson.sessions s
//where s.event_date between '2023-03-01' and current_date -1
where s.event_date = current_date-1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
;
