-- Store Anomaly Analysis Queries
-- Querying source table proddb.fionafan.onboarded_users_events directly
-- Based on the logic from stores_visited_by_order_outcome table creation
-- Author: Fiona Fan

-- =============================================================================
-- QUERY 1: Sessions with place order but no viewed stores
-- =============================================================================
-- This identifies sessions where users placed orders but viewed 0 stores
-- Based on the same logic used to create stores_visited_by_order_outcome

WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
),
session_orders AS (
    SELECT 
        consumer_id,
        event_date,
        session_num,
        MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
    FROM dedupe_events
    GROUP BY consumer_id, event_date, session_num
),
session_stores AS (
    SELECT 
        de.consumer_id,
        de.event_date,
        de.session_num,
        so.has_order,
        -- Loaded stores: store_page_load OR m_card_click with store_id
        COUNT(DISTINCT CASE WHEN (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
                                  OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
                            THEN de.store_id END) AS loaded_store_count,
        -- Viewed stores: any event with store_id (includes m_card_view)
        COUNT(DISTINCT de.store_id) AS viewed_stores_count
    FROM dedupe_events de
    INNER JOIN session_orders so 
        ON de.consumer_id = so.consumer_id 
        AND de.event_date = so.event_date 
        AND de.session_num = so.session_num
    WHERE (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_view')
    GROUP BY de.consumer_id, de.event_date, de.session_num, so.has_order
)

-- Sessions with place order but no viewed stores
-- FIXED: Added parentheses to ensure correct operator precedence
-- Now correctly identifies: sessions WITH orders AND (no viewed stores OR no loaded stores)
SELECT
    'Sessions with orders but 0 viewed stores' AS anomaly_type,
    consumer_id,
    event_date,
    session_num,
    loaded_store_count,
    viewed_stores_count
FROM session_stores
WHERE has_order = 1
  AND (viewed_stores_count = 0 OR loaded_store_count = 0)
ORDER BY consumer_id, event_date, session_num limit 10;

select * from proddb.fionafan.onboarded_users_events where consumer_id = '100066409' and event_date = '2025-07-21' and session_num = 1;
-- =============================================================================
-- QUERY 2: Sessions where viewed stores < loaded stores
-- =============================================================================
-- This identifies data inconsistency where loaded stores exceeds viewed stores
-- Logically impossible since you can't load more stores than you viewed

WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
),
session_orders AS (
    SELECT 
        consumer_id,
        event_date,
        session_num,
        MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
    FROM dedupe_events
    GROUP BY consumer_id, event_date, session_num
),
session_stores AS (
    SELECT 
        de.consumer_id,
        de.event_date,
        de.session_num,
        so.has_order,
        -- Loaded stores: store_page_load OR m_card_click with store_id
        COUNT(DISTINCT CASE WHEN (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
                                  OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
                            THEN de.store_id END) AS loaded_store_count,
        -- Viewed stores: any event with store_id (includes m_card_view)
        COUNT(DISTINCT de.store_id) AS viewed_stores_count
    FROM dedupe_events de
    INNER JOIN session_orders so 
        ON de.consumer_id = so.consumer_id 
        AND de.event_date = so.event_date 
        AND de.session_num = so.session_num
    WHERE (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_view')
    GROUP BY de.consumer_id, de.event_date, de.session_num, so.has_order
)

-- Sessions where loaded stores > viewed stores (data inconsistency)
SELECT 
    'Loaded > Viewed stores (data inconsistency)' AS anomaly_type,
    consumer_id,
    event_date,
    session_num,
    CASE WHEN has_order = 1 THEN 'With Order' ELSE 'Without Order' END AS session_type,
    loaded_store_count,
    viewed_stores_count,
    (loaded_store_count - viewed_stores_count) AS store_count_diff
FROM session_stores
WHERE loaded_store_count > viewed_stores_count
ORDER BY (loaded_store_count - viewed_stores_count) DESC, consumer_id, event_date, session_num;

-- =============================================================================
-- QUERY 3: Detailed Event Investigation for Anomalous Sessions
-- =============================================================================
-- Look at the actual events for sessions that show anomalies to understand root cause

-- Get specific events for sessions with orders but no viewed stores
WITH anomalous_sessions AS (
    -- First, identify the anomalous sessions using same logic
    WITH dedupe_events AS (
        SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
    ),
    session_orders AS (
        SELECT 
            consumer_id,
            event_date,
            session_num,
            MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
        FROM dedupe_events
        GROUP BY consumer_id, event_date, session_num
    ),
    session_stores AS (
        SELECT 
            de.consumer_id,
            de.event_date,
            de.session_num,
            so.has_order,
            COUNT(DISTINCT CASE WHEN (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
                                      OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
                                THEN de.store_id END) AS loaded_store_count,
            COUNT(DISTINCT de.store_id) AS viewed_stores_count
        FROM dedupe_events de
        INNER JOIN session_orders so 
            ON de.consumer_id = so.consumer_id 
            AND de.event_date = so.event_date 
            AND de.session_num = so.session_num
        WHERE (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
           OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
           OR (de.event = 'm_card_view')
        GROUP BY de.consumer_id, de.event_date, de.session_num, so.has_order
    )
    SELECT consumer_id, event_date, session_num
    FROM session_stores
    WHERE has_order = 1 AND viewed_stores_count = 0
)

-- Show all events for these anomalous sessions
SELECT 
    de.consumer_id,
    de.event_date,
    de.session_num,
    de.event,
    de.store_id,
    de.event_timestamp,
    'Order with 0 viewed stores' AS anomaly_context
FROM proddb.fionafan.onboarded_users_events de
INNER JOIN anomalous_sessions a 
    ON de.consumer_id = a.consumer_id 
    AND de.event_date = a.event_date 
    AND de.session_num = a.session_num
ORDER BY de.consumer_id, de.event_date, de.session_num, de.event_timestamp
LIMIT 100;

-- =============================================================================
-- QUERY 4: Summary Statistics for Both Anomalies
-- =============================================================================

WITH dedupe_events AS (
    SELECT DISTINCT * FROM proddb.fionafan.onboarded_users_events
),
session_orders AS (
    SELECT 
        consumer_id,
        event_date,
        session_num,
        MAX(CASE WHEN event = 'action_place_order' THEN 1 ELSE 0 END) AS has_order
    FROM dedupe_events
    GROUP BY consumer_id, event_date, session_num
),
session_stores AS (
    SELECT 
        de.consumer_id,
        de.event_date,
        de.session_num,
        so.has_order,
        COUNT(DISTINCT CASE WHEN (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
                                  OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
                            THEN de.store_id END) AS loaded_store_count,
        COUNT(DISTINCT de.store_id) AS viewed_stores_count
    FROM dedupe_events de
    INNER JOIN session_orders so 
        ON de.consumer_id = so.consumer_id 
        AND de.event_date = so.event_date 
        AND de.session_num = so.session_num
    WHERE (de.event = 'store_page_load' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_click' AND de.store_id IS NOT NULL) 
       OR (de.event = 'm_card_view')
    GROUP BY de.consumer_id, de.event_date, de.session_num, so.has_order
),
anomaly_counts AS (
    SELECT 
        COUNT(*) AS total_sessions,
        COUNT(CASE WHEN has_order = 1 THEN 1 END) AS total_order_sessions,
        COUNT(CASE WHEN has_order = 1 AND viewed_stores_count = 0 THEN 1 END) AS orders_zero_viewed,
        COUNT(CASE WHEN loaded_store_count > viewed_stores_count THEN 1 END) AS loaded_gt_viewed,
        COUNT(DISTINCT consumer_id) AS unique_consumers
    FROM session_stores
)

SELECT 
    total_sessions,
    total_order_sessions,
    unique_consumers,
    orders_zero_viewed,
    loaded_gt_viewed,
    ROUND(orders_zero_viewed * 100.0 / total_order_sessions, 3) AS pct_orders_zero_viewed,
    ROUND(loaded_gt_viewed * 100.0 / total_sessions, 3) AS pct_loaded_gt_viewed,
    CASE 
        WHEN orders_zero_viewed > 0 THEN 'ANOMALY DETECTED: Orders without store views'
        ELSE 'No orders without store views'
    END AS orders_anomaly_status,
    CASE 
        WHEN loaded_gt_viewed > 0 THEN 'DATA ISSUE: Loaded > Viewed inconsistency'
        ELSE 'No loaded > viewed inconsistency'
    END AS data_consistency_status
FROM anomaly_counts;
