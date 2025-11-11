
select top 10 * from dimension_consumer;
CREATE OR REPLACE TABLE proddb.fionafan.all_user_sessions_with_events_features_gen AS


WITH raw AS (
  SELECT
    e.COHORT_TYPE,
    e.EVENT_DATE,
    e.SESSION_NUM,
    e.PLATFORM,
    e.DD_DEVICE_ID,
    e.USER_ID,
    e.timestamp AS raw_timestamp,
    e.timestamp AS event_ts,
    e.EVENT_TYPE,
    e.EVENT,
    e.DISCOVERY_SURFACE,
    e.DISCOVERY_FEATURE,
    e.DETAIL,
    e.STORE_ID,
    e.STORE_NAME,
    e.CONTEXT_TIMEZONE,
    e.CONTEXT_OS_VERSION,
    e.EVENT_RANK,
    e.DISCOVERY_SURFACE_CLICK_ATTR,
    e.DISCOVERY_FEATURE_CLICK_ATTR,
    e.DISCOVERY_SURFACE_IMP_ATTR,
    e.DISCOVERY_FEATURE_IMP_ATTR,
    e.PRE_PURCHASE_FLAG,
    e.L91D_ORDERS,
    e.LAST_ORDER_DATE,
    e.SESSION_TYPE,
    e.SESSION_TS_UTC,
    e.SESSION_DATE_PST,
    e.ONBOARDING_DAY,
    e.EXPOSURE_TIME,
    e.LIFESTAGE,
    e.ONBOARDING_TYPE,
    e.PROMO_TITLE,
    -- Store dimension attributes
    ds.NV_ORG,
    ds.NV_VERTICAL_NAME,
    ds.NV_BUSINESS_LINE,
    ds.NV_BUSINESS_SUB_TYPE,
    ds.TIER_LEVEL,
    ds.PRIMARY_TAG_NAME,
    ds.PRIMARY_CATEGORY_NAME,
    ds.SUBMARKET_ID,
    ds.SUBMARKET_NAME,
    ds.NAME as store_name_from_dim
  FROM proddb.fionafan.all_user_sessions_with_events e
  LEFT JOIN edw.merchant.dimension_store ds
    ON e.STORE_ID = ds.store_ID

),

events AS (
  -- Normalized event-level flags
  SELECT
    COALESCE(COHORT_TYPE, 'UNKNOWN')                        AS cohort_type,
    EVENT_DATE,
    SESSION_NUM,
    PLATFORM,
    DD_DEVICE_ID,
    USER_ID,
    event_ts,
    EVENT_TYPE,
    COALESCE(EVENT_TYPE, 'UNKNOWN')                         AS event_type_norm,
    COALESCE(EVENT, 'UNKNOWN')                              AS event_norm,
    COALESCE(DISCOVERY_SURFACE, 'UNKNOWN')                  AS discovery_surface_norm,
    COALESCE(DISCOVERY_FEATURE, 'UNKNOWN')                  AS discovery_feature_norm,
    DETAIL,
    STORE_ID,
    STORE_NAME,
    CONTEXT_TIMEZONE,
    CONTEXT_OS_VERSION,
    EVENT_RANK,
    DISCOVERY_SURFACE_CLICK_ATTR,
    DISCOVERY_FEATURE_CLICK_ATTR,
    DISCOVERY_SURFACE_IMP_ATTR,
    DISCOVERY_FEATURE_IMP_ATTR,
    PRE_PURCHASE_FLAG,
    L91D_ORDERS,
    LAST_ORDER_DATE,
    SESSION_TYPE,
    SESSION_TS_UTC,
    SESSION_DATE_PST,
    ONBOARDING_DAY,
    EXPOSURE_TIME,
    LIFESTAGE,
    ONBOARDING_TYPE,
    PROMO_TITLE,
    -- Store dimension fields
    NV_ORG,
    NV_VERTICAL_NAME,
    NV_BUSINESS_LINE,
    NV_BUSINESS_SUB_TYPE,
    TIER_LEVEL,
    PRIMARY_TAG_NAME,
    PRIMARY_CATEGORY_NAME,
    SUBMARKET_ID,
    SUBMARKET_NAME,
    store_name_from_dim,
    -- NV flag
    CASE WHEN NV_ORG IS NOT NULL OR NV_VERTICAL_NAME IS NOT NULL OR NV_BUSINESS_LINE IS NOT NULL OR NV_BUSINESS_SUB_TYPE IS NOT NULL THEN 1 ELSE 0 END AS is_nv_store,

    -- Funnel flags (event_type = 'funnel')
    CASE WHEN EVENT_TYPE = 'funnel' AND (LOWER(event_norm) LIKE '%add_item%' OR event_norm IN ('action_add_item','action_quick_add_item')) THEN 1 ELSE 0 END AS funnel_is_add,
    CASE WHEN EVENT_TYPE = 'funnel' AND (LOWER(event_norm) IN ('action_place_order','system_checkout_success')) THEN 1 ELSE 0 END AS funnel_is_order,
    CASE WHEN EVENT_TYPE = 'funnel' AND LOWER(event_norm) LIKE '%store_page_load%' THEN 1 ELSE 0 END AS reached_store_page,
    CASE WHEN EVENT_TYPE = 'funnel' AND LOWER(event_norm) LIKE '%order_cart_page_load%' THEN 1 ELSE 0 END AS reached_cart,
    CASE WHEN EVENT_TYPE = 'funnel' AND LOWER(event_norm) LIKE '%checkout_page_load%' THEN 1 ELSE 0 END AS reached_checkout,
    CASE WHEN EVENT_TYPE = 'funnel' AND LOWER(event_norm) LIKE '%system_checkout_success%' THEN 1 ELSE 0 END AS reached_success,
    
    -- Attribution flags (event_type = 'attribution', m_card_click)
    CASE WHEN EVENT_TYPE = 'attribution' AND event_norm = 'm_card_click' THEN 1 ELSE 0 END AS attribution_is_card_click,
    
    -- Action flags (event_type ILIKE '%action%')  
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND event_norm = 'm_app_background' THEN 1 ELSE 0 END AS action_is_background,
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND event_norm = 'm_app_foreground' THEN 1 ELSE 0 END AS action_is_foreground,
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND (event_norm LIKE 'm_select_tab%' OR event_norm LIKE '%select_tab%') THEN 1 ELSE 0 END AS action_is_tab_switch,
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND event_norm LIKE '%address%' THEN 1 ELSE 0 END AS action_is_address,
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND event_norm LIKE '%payment%' THEN 1 ELSE 0 END AS action_is_payment,
    CASE WHEN EVENT_TYPE ILIKE '%action%' AND (event_norm LIKE '%login%' OR event_norm LIKE '%registration%' OR event_norm LIKE '%sign_in%') THEN 1 ELSE 0 END AS action_is_auth

  FROM raw
),

events_ordered AS (
  -- Add sequence number and next/prev timestamps per session
  SELECT
    e.*,
    ROW_NUMBER() OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts) AS event_sequence,
    LEAD(event_ts) OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts) AS next_event_ts,
    LAG(event_ts)  OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts) AS prev_event_ts
  FROM events e
),

events_deltas AS (
  SELECT
    *,
    CASE WHEN next_event_ts IS NOT NULL THEN DATEDIFF(second, event_ts, next_event_ts) ELSE NULL END AS seconds_to_next,
    CASE WHEN prev_event_ts IS NOT NULL THEN DATEDIFF(second, prev_event_ts, event_ts) ELSE NULL END AS seconds_from_prev,
    -- Add session start time as a window function (for use in aggregates later)
    MIN(event_ts) OVER (PARTITION BY user_id, event_date, session_num, dd_device_id) AS session_start_ts
  FROM events_ordered
),

-- Basic session-level aggregates - separated by event type
session_basic AS (
  SELECT
    user_id,
    event_date,
    session_num,
    platform,
    dd_device_id,

    MIN(cohort_type) AS cohort_type,
    MIN(session_ts_utc) AS session_ts_utc,
    MIN(session_date_pst) AS session_date_pst,
    MIN(onboarding_day) AS onboarding_day,
    MIN(exposure_time) AS exposure_time,
    MIN(lifestage) AS lifestage,
    MIN(onboarding_type) AS onboarding_type,
    MIN(promo_title) AS promo_title,
    MIN(session_type) AS session_type,

    -- Overall counts
    COUNT(*) AS total_events,
    
    -- Store Impression features (event_type = 'store_impression')
    SUM(CASE WHEN event_type = 'store_impression' THEN 1 ELSE 0 END) AS impression_event_count,
    COUNT(DISTINCT CASE WHEN event_type = 'store_impression' THEN STORE_ID END) AS impression_unique_stores,
    COUNT(DISTINCT CASE WHEN event_type = 'store_impression' THEN discovery_feature_norm END) AS impression_unique_features,
    
    -- Action features (event_type ILIKE '%action%')
    SUM(CASE WHEN event_type ILIKE '%action%' THEN 1 ELSE 0 END) AS action_event_count,
    SUM(action_is_background) AS action_num_backgrounds,
    SUM(action_is_foreground) AS action_num_foregrounds,
    SUM(action_is_tab_switch) AS action_num_tab_switches,
    SUM(action_is_address) AS action_num_address_actions,
    SUM(action_is_payment) AS action_num_payment_actions,
    SUM(action_is_auth) AS action_num_auth_actions,
    
    -- Attribution features (event_type = 'attribution', m_card_click)
    SUM(CASE WHEN event_type = 'attribution' THEN 1 ELSE 0 END) AS attribution_event_count,
    SUM(attribution_is_card_click) AS attribution_num_card_clicks,
    COUNT(DISTINCT CASE WHEN event_type = 'attribution' THEN STORE_ID END) AS attribution_unique_stores_clicked,
    
    -- Error features (event_type = 'error')
    SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS error_event_count,
    
    -- Funnel features (event_type = 'funnel')
    SUM(CASE WHEN event_type = 'funnel' THEN 1 ELSE 0 END) AS funnel_event_count,
    SUM(funnel_is_add) AS funnel_num_adds,
    SUM(funnel_is_order) AS funnel_num_orders,
    SUM(reached_store_page) AS funnel_num_store_page,
    SUM(reached_cart) AS funnel_num_cart,
    SUM(reached_checkout) AS funnel_num_checkout,
    SUM(reached_success) AS funnel_num_success,

    MIN(event_ts) AS session_start_ts,
    MAX(event_ts) AS session_end_ts,

    COUNT(DISTINCT discovery_feature_norm) AS num_unique_discovery_features,
    COUNT(DISTINCT discovery_surface_norm) AS num_unique_discovery_surfaces,
    COUNT(DISTINCT STORE_ID) AS num_unique_stores,

    AVG(event_rank) AS avg_event_rank,
    MIN(event_rank) AS min_event_rank,
    MAX(event_rank) AS max_event_rank,
    
    -- Store features: NV detection
    MAX(is_nv_store) AS store_had_nv,
    MAX(CASE WHEN event_type = 'store_impression' AND is_nv_store = 1 THEN 1 ELSE 0 END) AS store_nv_impression_occurred,
    
    -- Store features: Tier levels
    LISTAGG(DISTINCT TIER_LEVEL, '|') AS store_tier_levels_concat,
    
    -- Store features: Tags, Categories, Names for embedding (by event type)
    -- Store Impression events (browsing)
    LISTAGG(DISTINCT CASE WHEN event_type = 'store_impression' THEN PRIMARY_TAG_NAME END, '|') AS store_tags_concat_impression,
    LISTAGG(DISTINCT CASE WHEN event_type = 'store_impression' THEN PRIMARY_CATEGORY_NAME END, '|') AS store_categories_concat_impression,
    LISTAGG(DISTINCT CASE WHEN event_type = 'store_impression' THEN store_name_from_dim END, '|') AS store_names_concat_impression,
    
    -- Attribution events (clicking)
    LISTAGG(DISTINCT CASE WHEN event_type = 'attribution' THEN PRIMARY_TAG_NAME END, '|') AS store_tags_concat_attribution,
    LISTAGG(DISTINCT CASE WHEN event_type = 'attribution' THEN PRIMARY_CATEGORY_NAME END, '|') AS store_categories_concat_attribution,
    LISTAGG(DISTINCT CASE WHEN event_type = 'attribution' THEN store_name_from_dim END, '|') AS store_names_concat_attribution,
    
    -- Funnel store page load events (visiting)
    LISTAGG(DISTINCT CASE WHEN event_type = 'funnel' AND LOWER(event_norm) LIKE '%store_page_load%' THEN PRIMARY_TAG_NAME END, '|') AS store_tags_concat_funnel_store,
    LISTAGG(DISTINCT CASE WHEN event_type = 'funnel' AND LOWER(event_norm) LIKE '%store_page_load%' THEN PRIMARY_CATEGORY_NAME END, '|') AS store_categories_concat_funnel_store,
    LISTAGG(DISTINCT CASE WHEN event_type = 'funnel' AND LOWER(event_norm) LIKE '%store_page_load%' THEN store_name_from_dim END, '|') AS store_names_concat_funnel_store,
    
    -- Store features: Submarket
    LISTAGG(DISTINCT CONCAT(COALESCE(SUBMARKET_ID::VARCHAR, ''), ':', COALESCE(SUBMARKET_NAME, '')), '|') AS store_submarkets_concat

  FROM events_deltas
  GROUP BY user_id, event_date, session_num, platform, dd_device_id
),

session_time_metrics AS (
  SELECT
    sb.*,
    DATEDIFF(second, session_start_ts, session_end_ts) AS session_duration_seconds,
    CASE
      WHEN DATEDIFF(second, session_start_ts, session_end_ts) <= 0 THEN NULL
      ELSE sb.total_events * 60.0 / NULLIF(DATEDIFF(second, session_start_ts, session_end_ts), 0)
    END AS events_per_minute
  FROM session_basic sb
),

-- Time-to-first actions per session and early engagement windows
session_firsts AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    -- Funnel timing (these are the conversion funnel events)
    MIN(CASE WHEN funnel_is_add = 1 THEN event_ts ELSE NULL END) AS funnel_first_add_ts,
    MIN(CASE WHEN funnel_is_order = 1 THEN event_ts ELSE NULL END) AS funnel_first_order_ts,
    MIN(CASE WHEN reached_store_page = 1 THEN event_ts ELSE NULL END) AS funnel_first_store_page_ts,
    MIN(CASE WHEN reached_cart = 1 THEN event_ts ELSE NULL END) AS funnel_first_cart_ts,
    MIN(CASE WHEN reached_checkout = 1 THEN event_ts ELSE NULL END) AS funnel_first_checkout_ts,
    MIN(CASE WHEN reached_success = 1 THEN event_ts ELSE NULL END) AS funnel_first_success_ts,
    -- Attribution timing (m_card_click events)
    MIN(CASE WHEN attribution_is_card_click = 1 THEN event_ts ELSE NULL END) AS attribution_first_card_click_ts,
    -- First attribution click source (is it from search?)
    MAX_BY(
      CASE WHEN discovery_surface_norm ILIKE '%search%' OR discovery_feature_norm ILIKE '%search%' THEN 1 ELSE 0 END,
      CASE WHEN attribution_is_card_click = 1 THEN event_ts ELSE NULL END
    ) AS attribution_first_click_from_search,
    -- Action timing
    MIN(CASE WHEN EVENT_TYPE ILIKE '%action%' AND action_is_tab_switch = 1 THEN event_ts ELSE NULL END) AS action_first_tab_switch_ts,
    MIN(CASE WHEN EVENT_TYPE ILIKE '%action%' AND action_is_foreground = 1 THEN event_ts ELSE NULL END) AS action_first_foreground_ts,
    -- Early engagement (all event types)
    SUM(CASE WHEN DATEDIFF(second, session_start_ts, event_ts) <= 30 THEN 1 ELSE 0 END) AS timing_events_first_30s,
    SUM(CASE WHEN DATEDIFF(second, session_start_ts, event_ts) <= 120 THEN 1 ELSE 0 END) AS timing_events_first_2min
  FROM events_deltas
  GROUP BY user_id, event_date, session_num, dd_device_id
),

-- NV impression position tracking - calculate position among impressions first
impression_positions AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    is_nv_store,
    ROW_NUMBER() OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts) AS impression_position
  FROM events_deltas
  WHERE event_type = 'store_impression'
),

session_nv_impression_position AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    MIN(CASE WHEN is_nv_store = 1 THEN impression_position ELSE NULL END) AS store_nv_impression_position
  FROM impression_positions
  GROUP BY user_id, event_date, session_num, dd_device_id
),

-- discovery feature counts per session
session_feature_counts AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    discovery_feature_norm,
    discovery_surface_norm,
    COUNT(*) AS cnt_by_feature
  FROM events_deltas
  GROUP BY user_id, event_date, session_num, dd_device_id, discovery_feature_norm, discovery_surface_norm
),

-- pivot top-K discovery features into columns (adjust the list as needed)
session_topk AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    SUM(CASE WHEN discovery_feature_norm = 'Traditional Carousel' THEN cnt_by_feature ELSE 0 END) AS cnt_traditional_carousel,
    SUM(CASE WHEN (discovery_feature_norm = 'Core Search' OR discovery_surface_norm = 'Search Tab') THEN cnt_by_feature ELSE 0 END) AS cnt_core_search,
    SUM(CASE WHEN discovery_feature_norm = 'Pill Filter' THEN cnt_by_feature ELSE 0 END) AS cnt_pill_filter,
    SUM(CASE WHEN discovery_feature_norm = 'Cuisine Filter' THEN cnt_by_feature ELSE 0 END) AS cnt_cuisine_filter,
    SUM(CASE WHEN discovery_feature_norm = 'DoubleDash' THEN cnt_by_feature ELSE 0 END) AS cnt_doubledash,
    SUM(CASE WHEN discovery_feature_norm = 'Home Feed' THEN cnt_by_feature ELSE 0 END) AS cnt_home_feed,
    SUM(CASE WHEN discovery_feature_norm = 'Banner' THEN cnt_by_feature ELSE 0 END) AS cnt_banner,
    SUM(CASE WHEN discovery_feature_norm = 'Offers' THEN cnt_by_feature ELSE 0 END) AS cnt_offers,
    SUM(CASE WHEN discovery_feature_norm = 'm_select_tab - explore' THEN cnt_by_feature ELSE 0 END) AS cnt_tab_explore,
    SUM(CASE WHEN discovery_feature_norm = 'm_select_tab - me' THEN cnt_by_feature ELSE 0 END) AS cnt_tab_me,
    SUM(cnt_by_feature) AS total_feature_events
  FROM session_feature_counts
  GROUP BY user_id, event_date, session_num, dd_device_id
),

-- dominant feature + entropy calculation
session_feature_stats AS (
  -- dominant feature (ties arbitrarily resolved)
  SELECT
    sfc.user_id,
    sfc.event_date,
    sfc.session_num,
    sfc.dd_device_id,
    MAX_BY(sfc.discovery_feature_norm, sfc.cnt_by_feature) AS dominant_discovery_feature,
    MAX(sfc.cnt_by_feature) AS top_feature_count,
    SUM(sfc.cnt_by_feature) AS total_feature_events
  FROM session_feature_counts sfc
  GROUP BY sfc.user_id, sfc.event_date, sfc.session_num, sfc.dd_device_id
),

session_feature_entropy AS (
  SELECT
    sfs.user_id,
    sfs.event_date,
    sfs.session_num,
    sfs.dd_device_id,
    sfs.dominant_discovery_feature,
    sfs.top_feature_count,
    sfs.total_feature_events,
    CASE
      WHEN sfs.total_feature_events = 0 THEN 0
      ELSE - SUM( (sfc.cnt_by_feature / sfs.total_feature_events) * LN( NULLIF( (sfc.cnt_by_feature / sfs.total_feature_events), 0) ) )
    END AS entropy_discovery_feature
  FROM session_feature_stats sfs
  LEFT JOIN session_feature_counts sfc
    ON sfc.user_id = sfs.user_id AND sfc.event_date = sfs.event_date AND sfc.session_num = sfs.session_num AND sfc.dd_device_id = sfs.dd_device_id
  GROUP BY sfs.user_id, sfs.event_date, sfs.session_num, sfs.dd_device_id, sfs.dominant_discovery_feature, sfs.top_feature_count, sfs.total_feature_events
),

-- Conversion ratios and funnel booleans - organized by category
session_ratios AS (
  SELECT
    stm.user_id,
    stm.event_date,
    stm.session_num,
    stm.dd_device_id,
    
    -- Action flags
    CASE WHEN stm.action_num_tab_switches > 0 THEN 1 ELSE 0 END AS action_had_tab_switch,
    CASE WHEN stm.action_num_backgrounds > 0 THEN 1 ELSE 0 END AS action_had_background,
    CASE WHEN stm.action_num_foregrounds > 0 THEN 1 ELSE 0 END AS action_had_foreground,
    CASE WHEN stm.action_num_address_actions > 0 THEN 1 ELSE 0 END AS action_had_address,
    CASE WHEN stm.action_num_payment_actions > 0 THEN 1 ELSE 0 END AS action_had_payment,
    CASE WHEN stm.action_num_auth_actions > 0 THEN 1 ELSE 0 END AS action_had_auth,
    
    -- Impression flags
    CASE WHEN stm.impression_event_count > 0 THEN 1 ELSE 0 END AS impression_had_any,
    
    -- Attribution flags
    CASE WHEN stm.attribution_event_count > 0 THEN 1 ELSE 0 END AS attribution_had_any,
    
    -- Error flags
    CASE WHEN stm.error_event_count > 0 THEN 1 ELSE 0 END AS error_had_any,
    
    -- Funnel flags
    CASE WHEN stm.funnel_event_count > 0 THEN 1 ELSE 0 END AS funnel_had_any,
    CASE WHEN stm.funnel_num_adds > 0 THEN 1 ELSE 0 END AS funnel_had_add,
    CASE WHEN stm.funnel_num_orders > 0 THEN 1 ELSE 0 END AS funnel_had_order,
    CASE WHEN stm.funnel_num_store_page > 0 THEN 1 ELSE 0 END AS funnel_reached_store_bool,
    CASE WHEN stm.funnel_num_cart > 0 THEN 1 ELSE 0 END AS funnel_reached_cart_bool,
    CASE WHEN stm.funnel_num_checkout > 0 THEN 1 ELSE 0 END AS funnel_reached_checkout_bool,
    CASE WHEN stm.funnel_num_success > 0 THEN 1 ELSE 0 END AS funnel_converted_bool,
    
    -- Conversion rates
    CASE WHEN stm.attribution_num_card_clicks = 0 THEN NULL ELSE (stm.funnel_num_adds::DOUBLE / stm.attribution_num_card_clicks) END AS funnel_add_per_attribution_click,
    CASE WHEN stm.funnel_num_adds = 0 THEN NULL ELSE (stm.funnel_num_orders::DOUBLE / stm.funnel_num_adds) END AS funnel_order_per_add,
    CASE WHEN stm.impression_event_count = 0 THEN NULL ELSE (stm.attribution_event_count::DOUBLE / stm.impression_event_count) END AS impression_to_attribution_rate,
    CASE WHEN stm.attribution_event_count = 0 THEN NULL ELSE (stm.funnel_num_adds::DOUBLE / stm.attribution_event_count) END AS attribution_to_add_rate
  FROM session_time_metrics stm
),

-- bigrams: event -> next_event text and counts; then pivot a few
event_bigrams AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    CONCAT(event_norm, '->', LEAD(event_norm) OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts)) AS event_bigram
  FROM events_deltas
  QUALIFY LEAD(event_norm) OVER (PARTITION BY user_id, event_date, session_num, dd_device_id ORDER BY event_ts) IS NOT NULL
),

session_bigrams_pivot AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    SUM(CASE WHEN event_bigram = 'm_card_view->m_card_click' THEN 1 ELSE 0 END) AS bigram_impression_to_click,
    SUM(CASE WHEN event_bigram LIKE '%m_card_click->store_page_load%' THEN 1 ELSE 0 END) AS bigram_click_to_store,
    SUM(CASE WHEN event_bigram LIKE '%store_page_load->action_add_item%' THEN 1 ELSE 0 END) AS bigram_store_to_add
  FROM event_bigrams
  GROUP BY user_id, event_date, session_num, dd_device_id
),

-- inter-event time stats
session_interevent AS (
  SELECT
    user_id,
    event_date,
    session_num,
    dd_device_id,
    AVG(seconds_from_prev) AS mean_inter_event_seconds,
    MIN(seconds_from_prev) AS min_inter_event_seconds,
    MAX(seconds_from_prev) AS max_inter_event_seconds,
    STDDEV_POP(seconds_from_prev) AS std_inter_event_seconds,
    MEDIAN(seconds_from_prev) AS median_inter_event_seconds
  FROM events_deltas
  WHERE seconds_from_prev IS NOT NULL
  GROUP BY user_id, event_date, session_num, dd_device_id
),

-- Assembled time-to-first metrics with proper prefixes
session_times_assembled AS (
  SELECT
    sf.user_id,
    sf.event_date,
    sf.session_num,
    sf.dd_device_id,
    -- Funnel timing
    CASE WHEN sf.funnel_first_add_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_add_ts) END AS funnel_seconds_to_first_add,
    CASE WHEN sf.funnel_first_order_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_order_ts) END AS funnel_seconds_to_first_order,
    CASE WHEN sf.funnel_first_store_page_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_store_page_ts) END AS funnel_seconds_to_store_page,
    CASE WHEN sf.funnel_first_cart_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_cart_ts) END AS funnel_seconds_to_cart,
    CASE WHEN sf.funnel_first_checkout_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_checkout_ts) END AS funnel_seconds_to_checkout,
    CASE WHEN sf.funnel_first_success_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.funnel_first_success_ts) END AS funnel_seconds_to_success,
    -- Attribution timing (m_card_click)
    CASE WHEN sf.attribution_first_card_click_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.attribution_first_card_click_ts) END AS attribution_seconds_to_first_card_click,
    sf.attribution_first_click_from_search,
    -- Action timing
    CASE WHEN sf.action_first_tab_switch_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.action_first_tab_switch_ts) END AS action_seconds_to_first_tab_switch,
    CASE WHEN sf.action_first_foreground_ts IS NULL THEN NULL ELSE DATEDIFF(second, stm.session_start_ts, sf.action_first_foreground_ts) END AS action_seconds_to_first_foreground,
    -- Early engagement
    sf.timing_events_first_30s,
    sf.timing_events_first_2min
  FROM session_firsts sf
  LEFT JOIN session_time_metrics stm
    ON sf.user_id = stm.user_id AND sf.event_date = stm.event_date AND sf.session_num = stm.session_num AND sf.dd_device_id = stm.dd_device_id
),

-- Final assembly
final_sessions AS (
  SELECT
    stm.user_id,
    stm.event_date,
    stm.session_num,
    stm.platform,
    stm.dd_device_id,

    -- preserved metadata
    stm.cohort_type,
    stm.session_ts_utc,
    stm.session_date_pst,
    stm.onboarding_day,
    stm.exposure_time,
    stm.lifestage,
    stm.onboarding_type,
    stm.promo_title,
    stm.session_type,
    
    -- temporal features
    DATEDIFF('day', stm.onboarding_day, stm.session_date_pst) AS days_since_onboarding,
    MOD(DATEDIFF('day', stm.onboarding_day, stm.session_date_pst), 7) AS days_since_onboarding_mod7,
    DAYOFWEEK(stm.session_date_pst) AS session_day_of_week,
    DAYOFWEEK(stm.onboarding_day) AS onboarding_day_of_week,

    -- overall session metrics
    stm.total_events,
    stm.session_start_ts,
    stm.session_end_ts,
    stm.session_duration_seconds,
    stm.events_per_minute,
    stm.num_unique_discovery_features,
    stm.num_unique_discovery_surfaces,
    stm.num_unique_stores,
    
    -- impression features
    stm.impression_event_count,
    stm.impression_unique_stores,
    stm.impression_unique_features,
    sr.impression_had_any,
    sr.impression_to_attribution_rate,
    
    -- action features
    stm.action_event_count,
    stm.action_num_backgrounds,
    stm.action_num_foregrounds,
    stm.action_num_tab_switches,
    stm.action_num_address_actions,
    stm.action_num_payment_actions,
    stm.action_num_auth_actions,
    sr.action_had_background,
    sr.action_had_foreground,
    sr.action_had_tab_switch,
    sr.action_had_address,
    sr.action_had_payment,
    sr.action_had_auth,
    
    -- attribution features (m_card_click and discovery surface/feature usage)
    stm.attribution_event_count,
    stm.attribution_num_card_clicks,
    stm.attribution_unique_stores_clicked,
    sr.attribution_had_any,
    sr.attribution_to_add_rate,
    COALESCE(tk.cnt_traditional_carousel, 0) AS attribution_cnt_traditional_carousel,
    COALESCE(tk.cnt_core_search, 0) AS attribution_cnt_core_search,
    COALESCE(tk.cnt_pill_filter, 0) AS attribution_cnt_pill_filter,
    COALESCE(tk.cnt_cuisine_filter, 0) AS attribution_cnt_cuisine_filter,
    COALESCE(tk.cnt_doubledash, 0) AS attribution_cnt_doubledash,
    COALESCE(tk.cnt_home_feed, 0) AS attribution_cnt_home_feed,
    COALESCE(tk.cnt_banner, 0) AS attribution_cnt_banner,
    COALESCE(tk.cnt_offers, 0) AS attribution_cnt_offers,
    COALESCE(tk.cnt_tab_explore, 0) AS attribution_cnt_tab_explore,
    COALESCE(tk.cnt_tab_me, 0) AS attribution_cnt_tab_me,
    COALESCE(tk.total_feature_events, 0) AS attribution_total_feature_events,
    sfe.dominant_discovery_feature AS attribution_dominant_feature,
    sfe.top_feature_count AS attribution_dominant_count,
    CASE WHEN sfe.total_feature_events = 0 THEN 0 ELSE sfe.top_feature_count::DOUBLE / sfe.total_feature_events END AS attribution_pct_from_dominant,
    sfe.entropy_discovery_feature AS attribution_entropy,
    
    -- error features
    stm.error_event_count,
    sr.error_had_any,
    
    -- funnel features
    stm.funnel_event_count,
    stm.funnel_num_adds,
    stm.funnel_num_orders,
    stm.funnel_num_store_page,
    stm.funnel_num_cart,
    stm.funnel_num_checkout,
    stm.funnel_num_success,
    sr.funnel_had_any,
    sr.funnel_had_add,
    sr.funnel_had_order,
    sr.funnel_reached_store_bool,
    sr.funnel_reached_cart_bool,
    sr.funnel_reached_checkout_bool,
    sr.funnel_converted_bool,
    sr.funnel_add_per_attribution_click,
    sr.funnel_order_per_add,

    -- event rank stats
    stm.avg_event_rank,
    stm.min_event_rank,
    stm.max_event_rank,

    -- timing features - inter-event intervals
    si.mean_inter_event_seconds AS timing_mean_inter_event_seconds,
    si.median_inter_event_seconds AS timing_median_inter_event_seconds,
    si.std_inter_event_seconds AS timing_std_inter_event_seconds,
    si.min_inter_event_seconds AS timing_min_inter_event_seconds,
    si.max_inter_event_seconds AS timing_max_inter_event_seconds,

    -- timing features - time to first funnel/attribution/action
    sta.funnel_seconds_to_first_add,
    sta.funnel_seconds_to_first_order,
    sta.funnel_seconds_to_store_page,
    sta.funnel_seconds_to_cart,
    sta.funnel_seconds_to_checkout,
    sta.funnel_seconds_to_success,
    sta.attribution_seconds_to_first_card_click,
    sta.attribution_first_click_from_search,
    sta.action_seconds_to_first_tab_switch,
    sta.action_seconds_to_first_foreground,
    sta.timing_events_first_30s,
    sta.timing_events_first_2min,

    -- behavioral sequence features (bigrams)
    COALESCE(sbp.bigram_impression_to_click, 0) AS sequence_impression_to_attribution,
    COALESCE(sbp.bigram_click_to_store, 0) AS sequence_attribution_to_funnel_store,
    COALESCE(sbp.bigram_store_to_add, 0) AS sequence_funnel_store_to_action_add,
    
    -- store features
    stm.store_had_nv,
    stm.store_nv_impression_occurred,
    nvpos.store_nv_impression_position,
    stm.store_tier_levels_concat,
    -- Store embeddings by event type
    stm.store_tags_concat_impression,
    stm.store_categories_concat_impression,
    stm.store_names_concat_impression,
    stm.store_tags_concat_attribution,
    stm.store_categories_concat_attribution,
    stm.store_names_concat_attribution,
    stm.store_tags_concat_funnel_store,
    stm.store_categories_concat_funnel_store,
    stm.store_names_concat_funnel_store,
    stm.store_submarkets_concat

  FROM session_time_metrics stm
  LEFT JOIN session_topk tk
    ON stm.user_id = tk.user_id AND stm.event_date = tk.event_date AND stm.session_num = tk.session_num AND stm.dd_device_id = tk.dd_device_id
  LEFT JOIN session_feature_entropy sfe
    ON stm.user_id = sfe.user_id AND stm.event_date = sfe.event_date AND stm.session_num = sfe.session_num AND stm.dd_device_id = sfe.dd_device_id
  LEFT JOIN session_ratios sr
    ON stm.user_id = sr.user_id AND stm.event_date = sr.event_date AND stm.session_num = sr.session_num AND stm.dd_device_id = sr.dd_device_id
  LEFT JOIN session_interevent si
    ON stm.user_id = si.user_id AND stm.event_date = si.event_date AND stm.session_num = si.session_num AND stm.dd_device_id = si.dd_device_id
  LEFT JOIN session_times_assembled sta
    ON stm.user_id = sta.user_id AND stm.event_date = sta.event_date AND stm.session_num = sta.session_num AND stm.dd_device_id = sta.dd_device_id
  LEFT JOIN session_bigrams_pivot sbp
    ON stm.user_id = sbp.user_id AND stm.event_date = sbp.event_date AND stm.session_num = sbp.session_num AND stm.dd_device_id = sbp.dd_device_id
  LEFT JOIN session_nv_impression_position nvpos
    ON stm.user_id = nvpos.user_id AND stm.event_date = nvpos.event_date AND stm.session_num = nvpos.session_num AND stm.dd_device_id = nvpos.dd_device_id
)
select * from final_sessions;


-- ============================================================================
-- VALIDATION QUERIES FOR proddb.fionafan.all_user_sessions_with_events_features_gen
-- ============================================================================

-- 1. Check for duplicates at session grain (user_id + dd_device_id + event_date + session_num)
SELECT 
  'Duplicate Check' as validation_type,
  COUNT(*) as total_rows,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as unique_sessions,
  COUNT(*) - COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id, '|', event_date, '|', session_num)) as duplicate_count
FROM proddb.fionafan.all_user_sessions_with_events_features_gen;


-- 2. Show duplicate sessions if any exist
SELECT 
  user_id,
  dd_device_id,
  event_date,
  session_num,
  COUNT(*) as row_count,
  LISTAGG(DISTINCT funnel_had_order, ',') as different_funnel_had_order_values,
  LISTAGG(DISTINCT funnel_converted_bool, ',') as different_funnel_converted_values
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY user_id, dd_device_id, event_date, session_num
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 20;


-- 3. Row count validation by cohort
SELECT 
  cohort_type,
  COUNT(*) as total_sessions,
  COUNT(DISTINCT user_id) as unique_users,
  COUNT(DISTINCT dd_device_id) as unique_devices,
  COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id)) as unique_user_devices,
  ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT user_id), 2) as avg_sessions_per_user,
  ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT CONCAT(user_id, '|', dd_device_id)), 2) as avg_sessions_per_user_device
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 4. Session type distribution by cohort
SELECT 
  cohort_type,
  session_type,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_count DESC;


-- 5. Null check for key features
SELECT 
  'total_events' as feature,
  SUM(CASE WHEN total_events IS NULL THEN 1 ELSE 0 END) as null_count,
  ROUND(100.0 * SUM(CASE WHEN total_events IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
UNION ALL
SELECT 
  'session_duration_seconds',
  SUM(CASE WHEN session_duration_seconds IS NULL THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN session_duration_seconds IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
UNION ALL
SELECT 
  'avg_event_rank',
  SUM(CASE WHEN avg_event_rank IS NULL THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN avg_event_rank IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM proddb.fionafan.all_user_sessions_with_events_features_gen;


-- 6. Feature distribution statistics
SELECT 
  cohort_type,
  -- Event counts by type
  ROUND(AVG(total_events), 2) as avg_total_events,
  ROUND(MEDIAN(total_events), 2) as median_total_events,
  ROUND(AVG(impression_event_count), 2) as avg_impression_events,
  ROUND(AVG(action_event_count), 2) as avg_action_events,
  ROUND(AVG(attribution_event_count), 2) as avg_attribution_events,
  ROUND(AVG(funnel_event_count), 2) as avg_funnel_events,
  ROUND(AVG(error_event_count), 2) as avg_error_events,
  
  -- Session duration
  ROUND(AVG(session_duration_seconds), 2) as avg_duration_sec,
  ROUND(MEDIAN(session_duration_seconds), 2) as median_duration_sec,
  
  -- Action flags
  ROUND(100.0 * SUM(action_had_tab_switch) / COUNT(*), 2) as pct_had_tab_switch,
  ROUND(100.0 * SUM(action_had_foreground) / COUNT(*), 2) as pct_had_foreground,
  ROUND(100.0 * SUM(action_had_address) / COUNT(*), 2) as pct_had_address,
  
  -- Funnel progression
  ROUND(100.0 * SUM(funnel_had_add) / COUNT(*), 2) as pct_funnel_had_add,
  ROUND(100.0 * SUM(funnel_had_order) / COUNT(*), 2) as pct_funnel_had_order,
  ROUND(100.0 * SUM(funnel_reached_cart_bool) / COUNT(*), 2) as pct_funnel_reached_cart,
  ROUND(100.0 * SUM(funnel_converted_bool) / COUNT(*), 2) as pct_funnel_converted
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;

select sum(action_had_click) from proddb.fionafan.all_user_sessions_with_events_features_gen ;

-- 7. Conversion rates validation
SELECT 
  cohort_type,
  COUNT(*) as total_sessions,
  -- Impression to attribution (CTR)
  ROUND(AVG(impression_to_attribution_rate), 4) as avg_impression_to_attribution_rate,
  ROUND(MEDIAN(impression_to_attribution_rate), 4) as median_impression_to_attribution_rate,
  -- Funnel add rate (attribution clicks to adds)
  ROUND(AVG(funnel_add_per_attribution_click), 4) as avg_funnel_add_per_click,
  ROUND(MEDIAN(funnel_add_per_attribution_click), 4) as median_funnel_add_per_click,
  -- Funnel purchase rate
  ROUND(AVG(funnel_order_per_add), 4) as avg_funnel_order_per_add,
  ROUND(MEDIAN(funnel_order_per_add), 4) as median_funnel_order_per_add,
  -- Attribution to add
  ROUND(AVG(attribution_to_add_rate), 4) as avg_attribution_to_add_rate,
  ROUND(MEDIAN(attribution_to_add_rate), 4) as median_attribution_to_add_rate
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 8. Discovery feature usage (Attribution features)
SELECT 
  cohort_type,
  attribution_dominant_feature,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort,
  ROUND(AVG(attribution_pct_from_dominant), 2) as avg_pct_from_dominant,
  ROUND(AVG(attribution_entropy), 3) as avg_entropy
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE attribution_dominant_feature IS NOT NULL
GROUP BY cohort_type, attribution_dominant_feature
QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_type ORDER BY COUNT(*) DESC) <= 5
ORDER BY cohort_type, session_count DESC;


-- 9. Timing features validation
SELECT 
  cohort_type,
  -- Funnel timing
  ROUND(AVG(funnel_seconds_to_first_add), 2) as avg_funnel_seconds_to_add,
  ROUND(MEDIAN(funnel_seconds_to_first_add), 2) as median_funnel_seconds_to_add,
  ROUND(AVG(funnel_seconds_to_first_order), 2) as avg_funnel_seconds_to_order,
  ROUND(AVG(funnel_seconds_to_cart), 2) as avg_funnel_seconds_to_cart,
  ROUND(AVG(funnel_seconds_to_checkout), 2) as avg_funnel_seconds_to_checkout,
  ROUND(AVG(funnel_seconds_to_success), 2) as avg_funnel_seconds_to_success,
  
  -- Attribution timing
  ROUND(AVG(attribution_seconds_to_first_card_click), 2) as avg_attribution_seconds_to_card_click,
  ROUND(MEDIAN(attribution_seconds_to_first_card_click), 2) as median_attribution_seconds_to_card_click,
  
  -- Action timing
  ROUND(AVG(action_seconds_to_first_tab_switch), 2) as avg_action_seconds_to_tab_switch,
  ROUND(AVG(action_seconds_to_first_foreground), 2) as avg_action_seconds_to_foreground,
  
  -- Early engagement
  ROUND(AVG(timing_events_first_30s), 2) as avg_events_first_30s,
  ROUND(AVG(timing_events_first_2min), 2) as avg_events_first_2min,
  
  -- Inter-event timing
  ROUND(AVG(timing_mean_inter_event_seconds), 2) as avg_mean_inter_event_seconds,
  ROUND(AVG(timing_median_inter_event_seconds), 2) as avg_median_inter_event_seconds
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 10. Sample sessions for manual inspection
SELECT TOP 10 
  cohort_type,
  user_id,
  dd_device_id,
  event_date,
  session_num,
  session_type,
  days_since_onboarding,
  days_since_onboarding_mod7,
  total_events,
  impression_event_count,
  action_event_count,
  attribution_event_count,
  funnel_event_count,
  error_event_count,
  session_duration_seconds,
  action_had_tab_switch,
  funnel_had_add,
  funnel_had_order,
  funnel_converted_bool,
  attribution_dominant_feature,
  attribution_num_card_clicks
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
ORDER BY RANDOM();


-- 11. Temporal features validation
SELECT 
  cohort_type,
  ROUND(AVG(days_since_onboarding), 2) as avg_days_since_onboarding,
  MIN(days_since_onboarding) as min_days_since_onboarding,
  MAX(days_since_onboarding) as max_days_since_onboarding,
  COUNT(DISTINCT days_since_onboarding_mod7) as unique_mod7_values,
  COUNT(DISTINCT session_day_of_week) as unique_session_dow,
  COUNT(DISTINCT onboarding_day_of_week) as unique_onboarding_dow
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 12. Store features validation - NV presence
SELECT 
  cohort_type, session_type,
  COUNT(*) as total_sessions,
  SUM(store_had_nv) as sessions_with_nv,
  SUM(store_nv_impression_occurred) as sessions_with_nv_impression,
  ROUND(100.0 * SUM(store_had_nv) / COUNT(*), 2) as pct_sessions_with_nv,
  ROUND(100.0 * SUM(store_nv_impression_occurred) / COUNT(*), 2) as pct_sessions_with_nv_impression,
  ROUND(AVG(CASE WHEN store_nv_impression_position IS NOT NULL THEN store_nv_impression_position ELSE NULL END), 2) as avg_nv_impression_position,
  ROUND(MEDIAN(CASE WHEN store_nv_impression_position IS NOT NULL THEN store_nv_impression_position ELSE NULL END), 2) as median_nv_impression_position
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY all
ORDER BY all;


-- 13. Store tier distribution
SELECT 
  cohort_type,
  store_tier_levels_concat,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type), 2) as pct_of_cohort
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE store_tier_levels_concat IS NOT NULL
GROUP BY cohort_type, store_tier_levels_concat
QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort_type ORDER BY COUNT(*) DESC) <= 10
ORDER BY cohort_type, session_count DESC;


-- 14. Store tags and categories - check for nulls and sample values (by event type)
SELECT 
  cohort_type,
  COUNT(*) as total_sessions,
  -- Impression (browsing)
  SUM(CASE WHEN store_tags_concat_impression IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_impression_tags,
  SUM(CASE WHEN store_categories_concat_impression IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_impression_categories,
  SUM(CASE WHEN store_names_concat_impression IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_impression_names,
  -- Attribution (clicking)
  SUM(CASE WHEN store_tags_concat_attribution IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_attribution_tags,
  SUM(CASE WHEN store_categories_concat_attribution IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_attribution_categories,
  SUM(CASE WHEN store_names_concat_attribution IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_attribution_names,
  -- Funnel store (visiting)
  SUM(CASE WHEN store_tags_concat_funnel_store IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_funnel_store_tags,
  SUM(CASE WHEN store_categories_concat_funnel_store IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_funnel_store_categories,
  SUM(CASE WHEN store_names_concat_funnel_store IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_funnel_store_names
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 15. Sample store embedding strings (for review before creating embeddings)
SELECT TOP 20
  cohort_type,
  user_id,
  session_type,
  days_since_onboarding,
  store_had_nv,
  store_nv_impression_position,
  store_tier_levels_concat,
  -- Impression embeddings
  LEFT(store_tags_concat_impression, 100) as impression_tags_sample,
  LEFT(store_categories_concat_impression, 100) as impression_categories_sample,
  LEFT(store_names_concat_impression, 100) as impression_names_sample,
  -- Attribution embeddings
  LEFT(store_tags_concat_attribution, 100) as attribution_tags_sample,
  LEFT(store_categories_concat_attribution, 100) as attribution_categories_sample,
  LEFT(store_names_concat_attribution, 100) as attribution_names_sample,
  -- Funnel store embeddings
  LEFT(store_tags_concat_funnel_store, 100) as funnel_store_tags_sample,
  LEFT(store_categories_concat_funnel_store, 100) as funnel_store_categories_sample,
  LEFT(store_names_concat_funnel_store, 100) as funnel_store_names_sample
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE store_tags_concat_impression IS NOT NULL 
  OR store_tags_concat_attribution IS NOT NULL
  OR store_tags_concat_funnel_store IS NOT NULL
ORDER BY RANDOM();


-- 16. Attribution step funnel validation
SELECT 
  cohort_type,
  COUNT(*) as total_sessions,
  -- Impression -> Attribution
  SUM(impression_had_any) as step1_had_impression,
  SUM(attribution_had_any) as step2_had_attribution,
  ROUND(100.0 * SUM(attribution_had_any) / NULLIF(SUM(impression_had_any), 0), 2) as step1_to_step2_rate,
  -- Attribution -> Funnel Add
  SUM(CASE WHEN attribution_had_any = 1 AND funnel_had_add = 1 THEN 1 ELSE 0 END) as step3_attribution_to_add,
  ROUND(100.0 * SUM(CASE WHEN attribution_had_any = 1 AND funnel_had_add = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(attribution_had_any), 0), 2) as step2_to_step3_rate,
  -- Add -> Order
  SUM(CASE WHEN funnel_had_add = 1 AND funnel_had_order = 1 THEN 1 ELSE 0 END) as step4_add_to_order,
  ROUND(100.0 * SUM(CASE WHEN funnel_had_add = 1 AND funnel_had_order = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(funnel_had_add), 0), 2) as step3_to_step4_rate,
  -- Overall conversion
  ROUND(100.0 * SUM(funnel_converted_bool) / COUNT(*), 2) as overall_conversion_rate
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
GROUP BY cohort_type
ORDER BY cohort_type;


-- 17. Click rate and first click source validation
SELECT 
  cohort_type,
  session_type,
  COUNT(*) as total_sessions,
  SUM(attribution_had_any) as sessions_with_click,
  ROUND(100.0 * SUM(attribution_had_any) / COUNT(*), 2) as pct_sessions_with_click,
  SUM(CASE WHEN attribution_had_any = 1 AND attribution_first_click_from_search = 1 THEN 1 ELSE 0 END) as sessions_first_click_from_search,
  ROUND(100.0 * SUM(CASE WHEN attribution_had_any = 1 AND attribution_first_click_from_search = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(attribution_had_any), 0), 2) as pct_first_click_from_search_among_clickers
FROM proddb.fionafan.all_user_sessions_with_events_features_gen

GROUP BY cohort_type, session_type
ORDER BY cohort_type, session_type;


-- 18. First attribution click source detailed breakdown (Search vs Non-Search)
SELECT 
  cohort_type,
  session_type,
  attribution_first_click_from_search,
  COUNT(*) as session_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY cohort_type, session_type), 2) as pct_of_sessions,
  -- Conversion rates by source
  ROUND(100.0 * SUM(funnel_had_add) / COUNT(*), 2) as pct_with_add,
  ROUND(100.0 * SUM(funnel_had_order) / COUNT(*), 2) as pct_with_order,
  ROUND(100.0 * SUM(funnel_converted_bool) / COUNT(*), 2) as pct_converted,
  -- Time to add
  ROUND(AVG(funnel_seconds_to_first_add - attribution_seconds_to_first_card_click), 2) as avg_seconds_click_to_add,
  ROUND(MEDIAN(funnel_seconds_to_first_add - attribution_seconds_to_first_card_click), 2) as median_seconds_click_to_add
FROM proddb.fionafan.all_user_sessions_with_events_features_gen
WHERE attribution_had_any = 1
GROUP BY cohort_type, session_type, attribution_first_click_from_search
ORDER BY cohort_type, session_type, attribution_first_click_from_search;



select distinct discovery_surface, count(1) cnt from proddb.fionafan.all_user_sessions_with_events 
-- where discovery_surface = 'Other'
where event = 'm_card_view'
group by all order by cnt desc;

select top 10 * from tyleranderson.events_all 
-- where discovery_surface = 'Other'
wherd