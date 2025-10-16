# Consumer Tracking Events - J-Fresh Sessions Table

## Overview
This document catalogs all consumer tracking events captured in the j-fresh-sessions table, organized by event type and purpose. **Each event includes the specific SQL filters used to extract it.**

---

## Event Categories

### 1. Store Impression Events
**Event Type:** `store_impression`  
**Source Table:** `iguazu.consumer.m_card_view`  
**Description:** Captures when a store card is viewed/impressed on the consumer app

#### SQL Filters:
```sql
WHERE pst(received_at) = current_date-1
  AND store_id IS NOT NULL
  AND item_name IS NULL
  AND item_id IS NULL
  AND page != 'store'  -- Explicitly exclude Store page as a catch-all
  AND ((page != 'order_history' AND context_device_type = 'ios') 
       OR context_device_type = 'android')
```

#### Filter Logic Explanation:
- **Date Filter:** Only include events from previous day
- **Store ID Required:** Must have a store_id (store impressions only, not item impressions)
- **Item Exclusion:** Exclude item-level impressions (item_name and item_id must be null)
- **Store Page Exclusion:** Exclude the store detail page itself (page != 'store')
- **Platform-Specific Logic:**
  - **iOS:** Exclude order_history page (broken tracking - fires after order and triple-fires)
  - **Android:** Include all pages including order_history

#### Additional Processing - Banner Deduplication (Android only):
Banner impressions on Android are deduplicated using:
```sql
WHERE discovery_feature = 'Banner' AND platform = 'android'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY event_date, platform, dd_device_id, user_id, store_id, detail 
  ORDER BY timestamp
) = 1
```

#### Discovery Surfaces:
- Home Page
- Vertical Page
- Grocery Tab
- Retail Tab
- Vertical Search
- Search Tab
- Saved Stores Page
- Pickup Tab
- Offers Tab
- Landing Page
- DoubleDash
- All Reviews Page
- Order History Tab
- Browse Tab
- Open Carts Page
- Cuisine See All Page
- Checkout Aisle Page
- Other

#### Discovery Features:
- Traditional Carousel
- Collection Carousel Landing Page
- Banner
- Cuisine Filter
- Pill Filter
- Home Feed
- Announcement
- Autocomplete
- Core Search
- Saved Stores
- Pickup
- Offers
- Vertical Search
- Order History
- Grocery Tab - Grid/Feed
- Retail Tab - Grid/Feed
- Browse Tab - Search
- Open Carts Page
- Other

---

### 2. Funnel Events

#### A. Store Page Load
**Event Name:** `store_page_load`  
**Source Table:** `segment_events_raw.consumer_production.m_store_page_load`  
**Event Rank:** 7  
**Description:** Fired when a consumer lands on a store page

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Attributes:**
- store_id
- store_status
- double_dash_flag: `CASE WHEN page ILIKE 'post_checkout%' OR LOWER(attr_src) LIKE 'post_checkout%' OR is_postcheckout_bundle = true THEN 1 ELSE 0 END`
- source

---

#### B. Item Page Load
**Event Name:** `item_page_load`  
**Source Table:** `segment_events_raw.consumer_production.m_item_page_load`  
**Event Rank:** 6  
**Description:** Fired when a consumer views an item detail page

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### C. Add to Cart Events

**Event Name:** `action_add_item`  
**Event Rank:** 5  
**Description:** Fired when a consumer adds an item to their cart

**Source Tables & Filters:**

1. **From m_item_page_action_add_item:**
```sql
FROM segment_events_raw.consumer_production.m_item_page_action_add_item
WHERE pst(received_at) = current_date-1
```

2. **From m_stepper_action (mobile NV):**
```sql
FROM segment_events_raw.consumer_production.m_stepper_action
WHERE pst(received_at) = current_date-1 
  AND store_id IS NOT NULL 
  AND (page NOT ILIKE '%post_checkout%' OR page IS NULL)
```
*Purpose: Exclude DoubleDash/post-checkout adds*

3. **From stepper_action (web NV):**
```sql
FROM segment_events_raw.consumer_production.stepper_action
WHERE pst(received_at) = current_date-1 
  AND store_id IS NOT NULL 
  AND (page NOT ILIKE '%post_checkout%' OR page IS NULL)
  AND (bundle_context <> 'post_checkout' OR bundle_context IS NULL)
```
*Purpose: Exclude DoubleDash/post-checkout adds (web uses bundle_context field)*

4. **From m_savecart_add_click:**
```sql
FROM segment_events_raw.consumer_production.m_savecart_add_click
WHERE pst(received_at) = current_date-1 
  AND store_id IS NOT NULL
```

---

**Event Name:** `action_quick_add_item`  
**Source Table:** `segment_events_raw.consumer_production.m_action_quick_add_item`  
**Event Rank:** 5  
**Description:** Fired when a consumer uses quick-add functionality

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### D. Order Cart Page Load
**Event Name:** `order_cart_page_load`  
**Source Table:** `iguazu.consumer.m_order_cart_page_load`  
**Event Rank:** 4  
**Description:** Fired when consumer views their cart

**SQL Filters:**
```sql
WHERE pst(iguazu_received_at) = current_date-1
```

---

#### E. Checkout Page Load
**Event Name:** `checkout_page_load`  
**Source Table:** `segment_events_raw.consumer_production.m_checkout_page_load`  
**Event Rank:** 3  
**Description:** Fired when consumer lands on checkout page

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### F. Place Order
**Event Name:** `action_place_order`  
**Event Rank:** 2  
**Description:** Fired when consumer clicks place order button

**Source Tables & Filters:**

1. **From m_checkout_page_action_place_order:**
```sql
FROM segment_events_raw.consumer_production.m_checkout_page_action_place_order
WHERE pst(received_at) = current_date-1
```

2. **From m_checkout_page_system_submit:**
```sql
FROM segment_events_raw.consumer_production.m_checkout_page_system_submit
WHERE pst(received_at) = current_date-1
```

---

#### G. Checkout Success
**Event Name:** `system_checkout_success`  
**Source Table:** `segment_events_raw.consumer_production.m_checkout_page_system_checkout_success`  
**Event Rank:** 1  
**Description:** Fired when order is successfully placed

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Attributes:**
- order_uuid

---

### 3. Attribution Events

#### A. Card Click
**Event Name:** `m_card_click`  
**Source Table:** `iguazu.consumer.m_card_click`  
**Description:** Fired when consumer clicks on a store card

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
  AND discovery_feature IS NOT NULL
```

**Filter Logic Explanation:**
- Only include events from previous day
- Must have a valid discovery_feature (filters out unmapped clicks)
- Discovery feature is derived from page, container, click_type, and other attributes

**Discovery Feature Mapping Examples:**
```sql
-- Banner clicks
WHEN container = 'banner_carousel' THEN 'Banner'

-- Cuisine filter clicks
WHEN container IN ('list', 'store', 'item:go-to-store') 
  AND list_filter ILIKE 'cuisine:%' 
  AND page IN ('explore_page', 'vertical_page') 
THEN 'Cuisine Filter'

-- Pill filter clicks
WHEN container = 'list'  
  AND list_filter NOT ILIKE 'cuisine:%' 
  AND page IN ('explore_page', 'vertical_page') 
THEN 'Pill Filter'

-- Home feed clicks
WHEN container = 'list' 
  AND list_filter IS NULL 
  AND page IN ('explore_page', 'vertical_page') 
THEN 'Home Feed'

-- Traditional carousel
WHEN container IN('cluster', 'store_item_logo_cluster') 
  AND page IN('explore_page', 'vertical_page') 
THEN 'Traditional Carousel'

-- Pickup tab clicks (Store Card components)
WHEN tab = 'pickup' AND page = 'pickup' AND click_type = 'merchant_info' 
THEN 'Store Card Mx Name'

WHEN tab = 'pickup' AND page = 'pickup' AND click_type = 'item' 
THEN 'Store Card Menu Item'

WHEN tab = 'pickup' AND page = 'pickup' AND click_type = 'go_to_store' 
THEN 'Store Card Button ("Go to Store", "Delivery", "Pickup")'

-- Search clicks
WHEN page = 'search_autocomplete' AND container = 'list' 
THEN 'Autocomplete'

WHEN page = 'search_results' 
THEN 'Core Search'

WHEN page = 'hybrid_search' 
THEN 'Hybrid Search'

-- Vertical search
WHEN page = 'vertical_search_page' 
THEN 'Vertical Search'
```

**Discovery Surfaces Tracked:**
- Home Page, Vertical Page, Grocery Tab, Retail Tab
- Pickup Tab, Offers Tab, Search Tab, Vertical Search
- DashMart, Notification Hub

---

#### B. CMS Banner Click
**Event Name:** `m_cms_banner`  
**Source Table:** `segment_events_raw.consumer_production.m_cms_banner`  
**Description:** Fired when consumer clicks on a CMS banner

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
  AND event_type = 'click'
```

**Filter Logic:**
- Only 'click' events (excludes impressions)
- Discovery Surface: `CASE WHEN page = 'explore_page' THEN 'Homepage' WHEN page = 'vertical_page' THEN 'Vertical Page' END`
- Discovery Feature: Always 'Banner'

---

#### C. Order History Store Click
**Event Name:** `m_order_history_store_click`  
**Source Table:** `segment_events_raw.consumer_production.m_order_history_store_click`  
**Description:** Fired when consumer clicks on a store from order history (reorder)

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Attributes:**
- Discovery Surface: 'Account'
- Discovery Feature: 'Reorder'

---

#### D. Banner Carousel Click
**Event Name:** `banner_carousel`  
**Source Table:** `iguazu.consumer.m_card_click`  
**Description:** Fired when consumer clicks on banner carousel item

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
  AND container = 'banner_carousel'
```

**Details:** container_name

---

### 4. Action Events - Navigation & Browsing

#### A. Store Content Page Load
**Event Name:** `m_store_content_page_load`  
**Source Table:** `iguazu.server_events_production.m_store_content_page_load`  
**Description:** Server-side event for store content page load

**SQL Filters:**
```sql
WHERE pst(iguazu_received_at) = current_date-1
```

---

#### B. Tab Selection
**Event Name:** `m_select_tab - [tab_name]`  
**Source Table:** `segment_events_raw.consumer_production.m_select_tab`  
**Description:** Fired when consumer switches between tabs

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Tab Names:** grocery, retail, pickup, browse, orders, explore, delivery, account, Deals Hub, search

---

#### C. Notification Hub
**Event Name:** `m_notif_hub_page_view`  
**Source Table:** `segment_events_raw.consumer_production.m_notif_hub_page_view`  
**Description:** Fired when consumer views notifications hub

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** num_items (number of notifications)

---

### 5. Action Events - Credits & Benefits

#### A. Benefit Reminder Display
**Event Name:** `m_benefit_reminder_should_display`  
**Source Table:** `segment_events_raw.consumer_production.m_benefit_reminder_should_display`  
**Description:** Fired when benefit reminder is displayed to consumer

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** credits_amount

---

#### B. Benefit Reminder Click
**Event Name:** `m_benefit_reminder_click`  
**Source Table:** `segment_events_raw.consumer_production.m_benefit_reminder_click`  
**Description:** Fired when consumer clicks on benefit reminder

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

### 6. Action Events - Errors

**Event Name:** `m_error_appear - [error_message]`  
**Source Table:** `segment_events_raw.consumer_production.m_error_appear`  
**Description:** Fired when an error message appears to consumer

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** Error message text

---

### 7. Action Events - Address Management

#### A. Enter Address Page View
**Event Name:** `m_enter_address_page_view`  
**Source Table:** `segment_events_raw.consumer_production.m_enter_address_page_view`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### B. Address Page View (New)
**Event Name:** `m_address_page_view`  
**Source Table:** `segment_events_raw.consumer_production.m_address_page_view`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### C. Default Address Tap
**Event Name:** `m_default_address_tap`  
**Source Table:** `segment_events_raw.consumer_production.m_default_address_tap`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### D. Save Address Success
**Event Name:** `m_enter_address_page_action_save_success`  
**Source Table:** `segment_events_raw.consumer_production.m_enter_address_page_action_save_success`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

### 8. Action Events - Account Page

#### A. Subscription Management
**Event Name:** `m_subscription_mgmt_page_view`  
**Source Table:** `segment_events_raw.consumer_production.m_subscription_mgmt_page_view`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### B. Saved Stores (Bookmarks)
**Event Name:** `m_account_tap_bookmarks - saved stores`  
**Source Table:** `segment_events_raw.consumer_production.m_account_tap_bookmarks`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### C. Dine-In Vouchers
**Event Name:** `m_account_tap_dine_in_vouchers`  
**Source Table:** `segment_events_raw.consumer_production.m_account_tap_dine_in_vouchers`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### D. Support Chat
**Event Name:** `m_support_chat_get_help_screen_viewed_deeplink`  
**Source Table:** `segment_events_raw.consumer_production.m_support_chat_get_help_screen_viewed_deeplink`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### E. Gift Card - Buy
**Event Name:** `m_buy_gift_card`  
**Source Table:** `segment_events_raw.consumer_production.m_buy_gift_card`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### F. Gift Card - Redeem
**Event Name:** `m_redeem_gift`  
**Source Table:** `segment_events_raw.consumer_production.m_redeem_gift`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### G. Referral Entry Point
**Event Name:** `m_referral_entry_point_click`  
**Source Table:** `segment_events_raw.consumer_production.m_referral_entry_point_click`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### H. Notification Settings
**Event Name:** `m_account_tap_notifications`  
**Source Table:** `segment_events_raw.consumer_production.m_account_tap_notifications`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### I. Logout
**Event Name:** `m_logout`  
**Source Table:** `segment_events_raw.consumer_production.m_logout`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

### 9. Action Events - Login & Registration

#### A. Onboarding Page Load (iOS)
**Event Name:** `m_onboarding_page_load`  
**Source Table:** `segment_events_raw.consumer_production.m_onboarding_page_load`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### B. Intro Page Load (Android)
**Event Name:** `m_intro_page_loaded`  
**Source Table:** `segment_events_raw.consumer_production.m_intro_page_loaded`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### C. Sign In Success
**Event Name:** `m_sign_in_success`  
**Source Table:** `segment_events_raw.consumer_production.m_sign_in_success`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

#### D. Registration Success
**Event Name:** `m_registration_success`  
**Source Table:** `segment_events_raw.consumer_production.m_registration_success`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

### 10. Action Events - Payment

#### A. Add Payment Method
**Event Name:** `m_add_payment_method_result`  
**Source Table:** `segment_events_raw.consumer_production.m_add_payment_method_result`  
**Description:** Fired when consumer attempts to add payment method

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
  AND (COALESCE(payment_method, payment_method_type) IS NULL 
       OR COALESCE(payment_method, payment_method_type) <> 'ApplePay')
```

**Filter Logic:**
- Exclude ApplePay additions (tracked separately)

**Details:** is_successful (true/false)

---

#### B. Change Payment Card
**Event Name:** `m_change_payment_card_success`  
**Source Table:** `segment_events_raw.consumer_production.m_change_payment_card_success`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** NEW_DEFAULT_PAYMENT_METHOD

---

#### C. Payment Page Load
**Event Name:** `m_payment_page_load`  
**Source Table:** `segment_events_raw.consumer_production.m_payment_page_load`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

---

### 11. App Lifecycle Events

#### A. Launch Appear
**Event Name:** `m_launch_appear`  
**Source Table:** `segment_events_raw.consumer_production.m_launch_appear`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** badge_count (notification badge count)

---

#### B. App Foreground
**Event Name:** `m_app_foreground`  
**Source Table:** `iguazu.consumer.m_app_foreground`

**SQL Filters:**
```sql
WHERE pst(iguazu_received_at) = current_date-1
```

---

### 12. Checkout Actions

**Event Name:** `m_checkout_page_action_change_tip`  
**Source Table:** `segment_events_raw.consumer_production.m_checkout_page_action_change_tip`

**SQL Filters:**
```sql
WHERE pst(received_at) = current_date-1
```

**Details:** amount

---

## Event Type Classifications

The query classifies events into the following categories:

1. **store_impression** - Store card impressions
2. **funnel** - Core conversion funnel events
3. **attribution** - Click attribution events
4. **action - login** - Login and registration events
5. **action - account** - Account page interactions
6. **action - address** - Address management events
7. **action - credits** - Credit/benefit related events
8. **action - payment** - Payment method management
9. **action - tab** - Tab navigation events
10. **error** - Error events
11. **action** - Other general actions

---

## Data Processing Details

### Store ID Backfilling Logic

For funnel events where store_id is null (6.4% initially, reduced to 0.2%), the query backfills using temporal logic:

```sql
CASE 
  WHEN store_id IS NOT NULL THEN store_id
  WHEN event_rank_prior >= event_rank THEN store_id_derived_prior
  WHEN event_rank_following <= event_rank THEN store_id_derived_following
  ELSE 0 
END AS store_id_final
```

**Algorithm:**
1. Use actual store_id if present
2. Look backward: If prior event had same or higher funnel rank, use its store_id
3. Look forward: If following event had same or lower funnel rank, use its store_id
4. Default to 0 if no match

**Rationale:** Over 90% of the 0.2% remaining nulls are Android order cart events when consumers start sessions from abandoned carts.

---

### Ghost Push Detection & Filtering

**Source Table:** `iguazu.consumer.m_push_notification_received`

**Detection SQL:**
```sql
WHERE pst(received_at) = current_date-1 
  AND (CONTAINS(other_properties, 'content-available') 
       OR CONTAINS(other_properties, 'content_available'))
```

**Ghost Push Criteria:**
```sql
WHERE ghost_push_flag = 'true' 
  OR (push_event_id IS NOT NULL 
      AND type = 'consumer_push' 
      AND (ca_flag = 1 OR ca2_flag = 'true'))
```

**Session Impact:**
- Sessions starting within **5 minutes** of ghost push are flagged
- These sessions are marked as `core_visitor = 0` (excluded from core visitor metrics)
- Ghost push timestamp stored in `ghost_push_ts` field

**Update Logic:**
```sql
UPDATE sessions
SET ghost_push_ts = g.ghost_ts,
    core_visitor = 0
WHERE dd_device_id = g.dd_device_id 
  AND event_date = g.event_date 
  AND ABS(DATEDIFF(minute, ghost_ts, start_ts)) <= 5
```

---

### Platform Filtering

All events are filtered by platform at the final stage:
```sql
WHERE platform IN ('ios', 'android')
```

**Excludes:** Web events (though some web event tables are commented out in the query)

---

## Key Attributes Tracked

### Discovery Attribution
- **discovery_surface**: High-level page/section where event occurred
- **discovery_feature**: Specific UI component/feature that triggered event
- **detail**: Additional context (filters, queries, container names)

### Store Information
- **store_id**: Unique store identifier
- **store_name**: Store name

### Session Information
- **dd_device_id**: Device identifier
- **user_id**: User identifier
- **dd_session_id**: Session identifier
- **platform**: ios or android
- **timestamp**: Event timestamp
- **event_date**: Date in PST

### Context
- **CONTEXT_TIMEZONE**: User's timezone
- **CONTEXT_OS_VERSION**: Operating system version
- **event_rank**: Funnel position ranking (1=highest, 7=lowest)
- **pre_purchase_flag**: Whether event occurred before first purchase in session
- **l91d_orders**: Last 91 days order count for user-store pair
- **last_order_date**: Most recent order date for user-store pair

---

## Session-Level Metrics

The sessions table aggregates events into sessions (30-minute inactivity threshold) and calculates:

### Funnel Metrics
- visitor, core_visitor
- store_page_visitor, item_page_visitor, item_add_visitor
- order_cart_page_visitor, checkout_page_visitor
- action_place_order_visitor, purchaser

### Discovery Metrics
- unique_store_impressions, unique_store_visits
- home_page_impressions, search_impressions, vertical_page_impressions
- home_page_store_visits, search_store_visits, vertical_page_store_visits
- nv_visitor (New Verticals), nv_store_visits
- past_purchase_store_visits (visits to stores with prior orders)

### Micro Conversions
- notifications_tab_visitor, banner_click_visitor
- payment_page_visitor, payment_add_success_visitor, payment_change_visitor
- address_visitor, address_update_visitor
- onboarding_page_visitor, sign_in_success_visitor
- Account page: redeem_gift, buy_gift, support, referral, manage_sub, log_out
- Tab visitors: grocery_tab, retail_tab, pickup_tab, browse_tab, orders_tab
- search_visitor, filter_visitor, offers_hub_visitor
- change_tip_visitor, purchased_from_prior_store

### Session Attributes
- landing_page: First event type in session
- first_action, first_action_detail: First consumer action
- session_num: Session number within day
- duration_seconds: Session length
- badge_count, foreground_count: App engagement metrics
- credits_available: DashPass credits amount

### User Attributes (Joined from other tables)
- is_dashpass: DashPass subscriber flag
- user_type: New, P0, Active, Dormant, Churned, Unrecognized
- media_type: Acquisition channel
- country_name: User country
- sensitivity_type: Price sensitivity cohort
- ghost_push_ts: Ghost push notification timestamp

---

## Data Refresh
Table updated daily for previous day (current_date - 1)

---

## Notes
- Events are deduplicated where appropriate (e.g., banner impressions on Android)
- Store IDs are backfilled using temporal logic when null
- Sessions defined by 30-minute inactivity threshold
- Pre/post purchase flags enable pre-purchase funnel analysis
- Ghost push sessions are flagged and excluded from core_visitor metrics

---

*Generated from j-fresh-sessions.sql query analysis*  
*Last Updated: October 15, 2025*

