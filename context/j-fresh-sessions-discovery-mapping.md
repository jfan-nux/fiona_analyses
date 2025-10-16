# J-Fresh Sessions - Discovery Surface & Feature Mapping Logic

## Overview
This document provides the exact CASE statement logic that determines how events are classified into Discovery Surfaces and Discovery Features in the j-fresh-sessions table.

---

## 1. STORE IMPRESSIONS (m_card_view)

### Discovery Surface Mapping

| Discovery Surface | Condition Logic |
|---|---|
| **DoubleDash** | `page LIKE 'post_checkout%'` |
| **Home Page** | `page IN ('explore_page','homepage')` OR `container = 'banner_carousel'` |
| **Vertical Page** | `page = 'vertical_page'` |
| **Grocery Tab** | `page ILIKE 'grocery_tab%'` |
| **Retail Tab** | `page ILIKE 'retail_tab%'` |
| **Vertical Search** | `page = 'vertical_search_page'` |
| **Search Tab** | `page IN ('search_autocomplete','search_results')` |
| **Saved Stores Page** | `page = 'saved_stores_page'` |
| **Pickup Tab** | `page = 'pickup'` |
| **Offers Tab** | `page IN ('deals_hub_list', 'offers_hub', 'offers_list')` |
| **Landing Page** | `page IN ('collection_page_list','collection_landing_page')` |
| **All Reviews Page** | `page = 'all_reviews'` |
| **Order History Tab** | `page = 'order_history'` |
| **Browse Tab** | `page = 'browse_page'` |
| **Open Carts Page** | `page = 'open_carts_page'` |
| **Cuisine See All Page** | `page = 'cuisine_see_all_page'` |
| **Checkout Aisle Page** | `page = 'checkout_aisle'` |
| **Other** | All other cases |

### Discovery Feature Mapping

| Discovery Feature | Condition Logic |
|---|---|
| **DoubleDash** | `page ILIKE 'post_checkout%'` |
| **Reviews** | `page = 'all_reviews'` |
| **Collection Carousel Landing Page** | `container = 'collection' AND page IN ('explore_page', 'vertical_page') AND tile_name IS NOT NULL`<br>OR<br>`container IN('store','list') AND page IN ('collection_page_list','collection_landing_page') AND tile_name IS NOT NULL` |
| **Traditional Carousel** | `page = 'vertical_page' AND container = 'cluster'`<br>OR<br>`container = 'cluster'`<br>OR<br>`container ILIKE 'collection_standard_%'` |
| **Traditional Carousel Landing Page** | `container IN('store','list') AND page IN ('collection_page_list','collection_landing_page')` (without tile_name) |
| **Vertical Search** | `page = 'vertical_search_page'` |
| **Autocomplete** | `page = 'search_autocomplete'` |
| **Core Search** | `page = 'search_results'` |
| **Saved Stores** | `page = 'saved_stores_page'` |
| **Pickup** | `page = 'pickup'` |
| **Offers** | `page IN ('deals_hub_list', 'offers_hub', 'offers_list')` |
| **Banner** | `container = 'banner_carousel'` |
| **Cuisine Filter** | `container IN ('store', 'list', 'item:go-to-store') AND page IN ('explore_page', 'vertical_page','cuisine_see_all_page') AND list_filter ILIKE '%cuisine:%'` |
| **Pill Filter** | `container IN('store','list') AND page IN ('explore_page', 'vertical_page') AND list_filter IS NOT NULL` (excluding cuisine filters) |
| **Home Feed** | `container IN('store','list') AND page IN ('explore_page', 'vertical_page')` (without list_filter) |
| **Announcement** | `container = 'announcement' AND page IN ('explore_page', 'vertical_page')` |
| **Entry Point - Cuisine Filter** | `page = 'explore_page' AND container = 'carousel' AND LOWER(carousel_name) = 'cuisines'` |
| **Entry Point - Vertical Nav** | `page = 'explore_page' AND container = 'carousel' AND carousel_name = 'Verticals'` |
| **Order History** | `page = 'order_history'` |
| **Grocery Tab - Grid** | `page = 'grocery_tab' AND container = 'grid'` |
| **Retail Tab - Grid** | `page = 'retail_tab' AND container = 'grid'` |
| **Grocery Tab - Feed** | `page = 'grocery_tab' AND container IN ('store', 'list', 'item:go-to-store')` |
| **Retail Tab - Feed** | `page = 'retail_tab' AND container IN ('store', 'list', 'item:go-to-store')` |
| **Grocery Tab - See All Page** | `page = 'grocery_tab_see_all_page'` |
| **Retail Tab - See All Page** | `page = 'retail_tab_see_all_page'` |
| **Browse Tab - Search** | `page = 'browse_page' AND container IN('store','list', 'item:go-to-store') AND query IS NOT NULL` |
| **Open Carts Page** | `page = 'open_carts_page'` |
| **Other** | All other cases |

---

## 2. CARD CLICKS (m_card_click)

### Discovery Surface Mapping

| Discovery Surface | Condition Logic |
|---|---|
| **Home Page** | `page = 'explore_page'` |
| **Vertical Page** | `page = 'vertical_page'` |
| **Grocery Tab** | `page = 'grocery_tab'` |
| **Retail Tab** | `page = 'retail_tab'` |
| **Pickup Tab** | `tab = 'pickup' AND page = 'pickup' AND click_type IN ('merchant_info', 'item', 'go_to_store')` |
| **Offers Tab** | `page IN ('deals_hub_list', 'offers_hub', 'offers_list')` |
| **Search Tab** | `page = 'search_autocomplete' AND container = 'list'`<br>OR<br>`page = 'search_results'`<br>OR<br>`page = 'hybrid_search'` |
| **Vertical Search** | `page = 'vertical_search_page'` |
| **DashMart** | `container = 'carousel' AND carousel_name = 'Verticals' AND vertical_name = 'DashMart' AND page = 'explore_page'` |
| **Notification Hub** | `page = 'notification_hub'` |

### Discovery Feature Mapping

| Discovery Feature | Condition Logic |
|---|---|
| **Announcement** | `container = 'announcement' AND page IN ('explore_page', 'vertical_page')` |
| **Banner** | `container = 'banner_carousel'` |
| **Cuisine Filter** | `container IN ('list', 'store', 'item:go-to-store') AND list_filter ILIKE 'cuisine:%' AND page IN ('explore_page', 'vertical_page')`<br>OR<br>`container = 'cluster' AND list_filter ILIKE 'cuisine:%' AND page = 'cuisine_filter_search_result'` |
| **Pill Filter** | `container = 'list' AND list_filter NOT ILIKE 'cuisine:%' AND page IN ('explore_page', 'vertical_page')` |
| **Traditional Carousel** | `container IN('cluster', 'store_item_logo_cluster') AND page IN('explore_page', 'vertical_page')` |
| **Collection Carousel Landing Page** | `container = 'list' AND page = 'collection_page_list' AND tile_name IS NOT NULL` |
| **Collection Landing Page Click** | `container = 'collection' AND page IN ('explore_page', 'vertical_page') AND tile_name IS NOT NULL` |
| **Traditional Carousel Landing Page** | `container = 'list' AND page = 'collection_page_list'` (without tile_name) |
| **Home Feed** | `container = 'list' AND list_filter IS NULL AND page IN ('explore_page', 'vertical_page')` |
| **Store Card Mx Name** | `tab = 'pickup' AND page = 'pickup' AND click_type = 'merchant_info'` |
| **Store Card Menu Item** | `tab = 'pickup' AND page = 'pickup' AND click_type = 'item'` |
| **Store Card Button** | `tab = 'pickup' AND page = 'pickup' AND click_type = 'go_to_store'` |
| **Offer Tab** | `page IN ('deals_hub_list', 'offers_hub', 'offers_list')` |
| **Autocomplete** | `page = 'search_autocomplete' AND container = 'list'` |
| **Core Search** | `page = 'search_results'` |
| **Hybrid Search** | `page = 'hybrid_search'` |
| **Vertical Search** | `page = 'vertical_search_page'` |
| **DashMart Vertical Entrypoint** | `container = 'carousel' AND carousel_name = 'Verticals' AND vertical_name = 'DashMart' AND page = 'explore_page'` |
| **Notification Hub** | `page = 'notification_hub'` |

---

## 3. CMS BANNER CLICKS (m_cms_banner)

### Discovery Surface Mapping

| Discovery Surface | Condition Logic |
|---|---|
| **Homepage** | `page = 'explore_page'` |
| **Vertical Page** | `page = 'vertical_page'` |

### Discovery Feature Mapping

| Discovery Feature | Condition Logic |
|---|---|
| **Banner** | Always (all events are banner clicks) |

**Additional Filter:** `event_type = 'click'` (excludes impressions)

---

## 4. ORDER HISTORY CLICKS (m_order_history_store_click)

### Discovery Surface Mapping

| Discovery Surface | Condition Logic |
|---|---|
| **Account** | Always (all events are from account) |

### Discovery Feature Mapping

| Discovery Feature | Condition Logic |
|---|---|
| **Reorder** | Always (all events are reorders) |

---

## 5. BANNER CAROUSEL CLICKS (from m_card_click)

### Filter Logic

**Event:** `banner_carousel`

**SQL Filter:** `container = 'banner_carousel'`

**Detail Field:** `container_name`

---

## KEY FIELD DEFINITIONS

### Primary Fields Used in Mapping

| Field | Description | Example Values |
|---|---|---|
| **page** | The page/screen where event occurred | `explore_page`, `vertical_page`, `search_results`, `pickup` |
| **container** | The UI container/component type | `list`, `cluster`, `banner_carousel`, `announcement`, `collection` |
| **list_filter** | Filter applied to list views | `cuisine:italian`, `offers:true` |
| **tile_name** | Name of collection tile | Collection name string |
| **tab** | Tab identifier for Pickup tab | `pickup` |
| **click_type** | Type of click on Pickup tab | `merchant_info`, `item`, `go_to_store` |
| **carousel_name** | Name of carousel component | `Verticals`, `cuisines` |
| **vertical_name** | Specific vertical identifier | `DashMart` |
| **query** | Search query text | User's search term |
| **container_name** | Name of container (for banners) | Banner campaign name |

---

## DETAIL FIELD POPULATION

The `detail` field is populated with additional context:

```sql
COALESCE(list_filter, query, container_name) AS detail
```

**Priority Order:**
1. `list_filter` - For filtered views (cuisine, pills)
2. `query` - For search queries
3. `container_name` - For banner/carousel names

---

## LOGIC HIERARCHY

### For Impressions (m_card_view)

**Order of evaluation matters!** The CASE statement evaluates top-to-bottom:

1. **DoubleDash** checks first (post_checkout pages)
2. **Specific pages** (explore_page, vertical_page, etc.)
3. **Special containers** (banner_carousel, announcement)
4. **Container-based logic** (cluster, list, store) with additional filters
5. **Other** as catch-all

### For Clicks (m_card_click)

**Order of evaluation:**

1. **Special components** (announcement, banner_carousel)
2. **Filtered views** (cuisine, pill filters)
3. **Container types** (cluster, list, collection)
4. **Page-specific** (search, pickup, offers)
5. **Null as default** (filtered out by WHERE discovery_feature IS NOT NULL)

---

## IMPORTANT NOTES

### Multi-Condition Features

Some discovery features require **multiple conditions** to be true:

**Example: Cuisine Filter**
- `container` must be IN ('list', 'store', 'item:go-to-store')
- `list_filter` must contain 'cuisine:'
- `page` must be IN ('explore_page', 'vertical_page')

**Example: Home Feed**
- `container` must be IN('store','list')
- `page` must be IN ('explore_page', 'vertical_page')
- `list_filter` must be NULL (no filters applied)

### Platform Differences

**iOS vs Android:**
- Order History impressions are **excluded on iOS** due to broken tracking
- Android includes all order_history impressions
- Banner impressions are **deduplicated on Android**

### Attribution Filtering

**Card clicks** require `discovery_feature IS NOT NULL` to be included in attribution events. This filters out clicks that don't map to any defined discovery feature.

---

*Generated from j-fresh-sessions.sql*  
*Last Updated: October 15, 2025*

