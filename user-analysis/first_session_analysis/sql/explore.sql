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
  AND dc.DEFAULT_COUNTRY = 'United States'
