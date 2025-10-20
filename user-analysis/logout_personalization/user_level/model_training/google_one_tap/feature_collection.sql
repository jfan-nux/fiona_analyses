--#QUERY# generate_logged_out_personalization_historical_web_device_id
WITH vars AS (
    SELECT 
        CURRENT_DATE()::DATE - INTERVAL '1 DAY' AS observed_experiment_start_date,
        CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '180 DAYS' AS long_term_start_date,
        CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '1 DAYS' AS long_term_end_date,
        CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '28 DAYS' AS short_term_start_date,
        CURRENT_DATE()::DATE - INTERVAL '1 DAY' - INTERVAL '1 DAYS' AS short_term_end_date
),
raw_sessions_data AS (
    SELECT *
    FROM (
        SELECT
            platform,
            dd_device_id,
            dd_session_id,
            consumer_id,
            iguazu_timestamp AS timestamp,
            'Store' AS page,
            NULLIF(context_campaign_name, '') AS utm_campaign,
            NULLIF(context_campaign_source, '') AS utm_source,
            NULLIF(context_campaign_medium, '') AS utm_medium,
            NULLIF(context_page_referrer, '') AS referrer
        FROM iguazu.server_events_production.store_page_load_consumer
        WHERE context_user_agent NOT ILIKE '%bot%'
            AND context_user_agent NOT ILIKE '%prerender%'
            AND context_user_agent NOT ILIKE '%read-aloud%'
            AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

        UNION ALL

        SELECT
            platform,
            dd_device_id,
            dd_session_id,
            consumer_id,
            iguazu_timestamp AS timestamp,
            'Home' AS page,
            NULLIF(context_campaign_name, '') AS utm_campaign,
            NULLIF(context_campaign_source, '') AS utm_source,
            NULLIF(context_campaign_medium, '') AS utm_medium,
            NULLIF(context_page_referrer, '') AS referrer
        FROM iguazu.consumer.home_page_view
        WHERE context_user_agent NOT ILIKE '%bot%'
            AND context_user_agent NOT ILIKE '%prerender%'
            AND context_user_agent NOT ILIKE '%read-aloud%'
            AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

        UNION ALL

        SELECT
            platform,
            dd_device_id,
            dd_session_id,
            consumer_id,
            iguazu_timestamp AS timestamp,
            'Explore' AS page,
            NULLIF(context_campaign_name, '') AS utm_campaign,
            NULLIF(context_campaign_source, '') AS utm_source,
            NULLIF(context_campaign_medium, '') AS utm_medium,
            NULLIF(context_page_referrer, '') AS referrer
        FROM iguazu.server_events_production.store_content_page_load
        WHERE context_user_agent NOT ILIKE '%bot%'
            AND context_user_agent NOT ILIKE '%prerender%'
            AND context_user_agent NOT ILIKE '%read-aloud%'
            AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

        UNION ALL

        SELECT
            platform,
            dd_device_id,
            dd_session_id,
            consumer_id,
            iguazu_timestamp AS timestamp,
            'Business' AS page,
            NULLIF(context_campaign_name, '') AS utm_campaign,
            NULLIF(context_campaign_source, '') AS utm_source,
            NULLIF(context_campaign_medium, '') AS utm_medium,
            NULLIF(context_page_referrer, '') AS referrer
        FROM iguazu.consumer.business_menu_page_load
        WHERE context_user_agent NOT ILIKE '%bot%'
            AND context_user_agent NOT ILIKE '%prerender%'
            AND context_user_agent NOT ILIKE '%read-aloud%'
            AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)

        UNION ALL

        SELECT
            platform,
            dd_device_id,
            dd_session_id,
            consumer_id,
            iguazu_timestamp AS timestamp,
            'Product' AS page,
            NULLIF(context_campaign_name, '') AS utm_campaign,
            NULLIF(context_campaign_source, '') AS utm_source,
            NULLIF(context_campaign_medium, '') AS utm_medium,
            NULLIF(context_page_referrer, '') AS referrer
        FROM iguazu.server_events_production.product_display_page_view
        WHERE context_user_agent NOT ILIKE '%bot%'
            AND context_user_agent NOT ILIKE '%prerender%'
            AND context_user_agent NOT ILIKE '%read-aloud%'
            AND iguazu_timestamp::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_session_id, dd_device_id ORDER BY timestamp ASC) = 1
),

raw_unique_web_device_id AS (
    WITH device_id AS (
        SELECT
            dd_device_id,
            platform,
            MAX(consumer_id) AS consumer_id_logged_in_status
        FROM raw_sessions_data
        GROUP BY 1, 2
    ),

    joined AS (
        SELECT
            a.dd_device_id,
            a.platform,
            a.consumer_id_logged_in_status,
            dc.consumer_id AS consumer_id_checked_in_log_in_records
        FROM device_id a
        LEFT JOIN (
            SELECT
                dd_device_id,
                user_id,
                ts
            FROM proddb.public.fact_consumer_frontend_login_and_signup_events
            WHERE ts::DATE BETWEEN (SELECT long_term_start_date FROM vars) - INTERVAL '181 DAYS' AND (SELECT long_term_end_date FROM vars)
            QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY ts DESC) = 1
        ) b ON a.dd_device_id::VARCHAR = b.dd_device_id::VARCHAR
        LEFT JOIN edw.consumer.dimension_consumers dc
            ON dc.user_id::VARCHAR = b.user_id::VARCHAR
    )

    SELECT
        dd_device_id,
        platform,
        CASE
            WHEN consumer_id_logged_in_status IS NOT NULL THEN consumer_id_logged_in_status
            ELSE consumer_id_checked_in_log_in_records
        END AS consumer_id,
        CASE
            WHEN consumer_id IS NOT NULL THEN True
            ELSE False
        END AS has_associated_consumer_id
    FROM joined
),

raw_unique_web_device_id_step1 AS (
    SELECT
        a.*,
        MAX(CASE WHEN b.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_is_active_recent_28_days,
        COUNT(DISTINCT CASE WHEN b.dd_device_id IS NULL THEN NULL ELSE IFNULL(b.dd_session_id, 'Null') END) AS device_id_sessions_recent_28_days,
        COUNT(DISTINCT dd.delivery_id) AS device_id_orders_recent_28_days,
        MAX(CASE WHEN c.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_out_address_entry_recent_28_days,
        MAX(CASE WHEN e.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_has_logged_in_recent_28_days
    FROM raw_unique_web_device_id a
    LEFT JOIN raw_sessions_data b
        ON a.dd_device_id = b.dd_device_id
        AND b.timestamp BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars)
    LEFT JOIN edw.finance.dimension_deliveries dd
        ON dd.is_caviar != 1
        AND dd.is_filtered_core = 1
        AND dd.dd_device_id = a.dd_device_id
        AND dd.created_at::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars)
        AND b.dd_device_id IS NOT NULL
    LEFT JOIN iguazu.server_events_production.debug_address_create c
        ON c.is_guest = True
        AND c.dd_device_id = a.dd_device_id
        AND c.iguazu_timestamp::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars)
        AND b.dd_device_id IS NOT NULL
    LEFT JOIN proddb.public.fact_consumer_frontend_login_and_signup_events e
        ON e.dd_device_id = a.dd_device_id
        AND e.ts::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars)
    GROUP BY 1, 2, 3, 4
),

raw_unique_web_device_id_step2 AS (
    WITH most_recent_sessions AS (
        SELECT *
        FROM raw_sessions_data
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dd_device_id ORDER BY timestamp DESC) = 1
    )

    SELECT
        a.*,
        b.dd_session_id AS device_id_session_id_most_recent_session,
        CASE
            WHEN b.referrer LIKE '%doordash.%' THEN 'Direct'
            WHEN NULLIF(b.utm_medium, '') IS NULL
                AND NULLIF(b.utm_source, '') IS NULL
                AND NULLIF(b.utm_campaign, '') IS NULL
                AND NULLIF(b.referrer, '') IS NULL THEN 'Direct'
            WHEN NULLIF(b.utm_medium, '') IS NULL
                AND NULLIF(b.utm_source, '') IS NULL
                AND NULLIF(b.utm_campaign, '') IS NULL
                AND (NULLIF(b.referrer, '') LIKE '%google.%'
                    OR NULLIF(b.referrer, '') LIKE '%bing.%'
                    OR NULLIF(b.referrer, '') LIKE '%search.yahoo.%') THEN 'Organic Search'
            WHEN NULLIF(b.utm_campaign, '') = 'gpa' THEN 'Organic Search'
            WHEN NULLIF(b.utm_medium, '') = 'Paid_Social' THEN 'Paid Social'
            WHEN NULLIF(b.utm_medium, '') = 'SEMb' THEN 'Paid Media'
            WHEN NULLIF(b.utm_medium, '') = 'SEMu' THEN 'Paid Media'
            WHEN NULLIF(b.utm_medium, '') = 'SEMc' THEN 'Paid Media'
            WHEN NULLIF(b.utm_medium, '') = 'PLA' THEN 'Paid Media'
            WHEN LOWER(NULLIF(b.utm_medium, '')) = 'email' THEN 'Email'
            WHEN LOWER(NULLIF(b.utm_medium, '')) LIKE '%enterprise%'
                OR LOWER(NULLIF(b.utm_source, '')) IN ('partner-link') THEN 'Partners'
            WHEN LOWER(NULLIF(b.utm_medium, '')) IN ('affiliate') THEN 'Affiliate'
            WHEN LOWER(NULLIF(b.utm_medium, '')) IN ('web_display') THEN 'Paid Media'
            WHEN LOWER(NULLIF(b.utm_medium, '')) IN ('video') THEN 'Paid Media'
            ELSE 'Other'
        END AS device_id_general_traffic_type_most_recent_session,
        DATEDIFF('DAYS', b.timestamp, (SELECT observed_experiment_start_date FROM vars) - INTERVAL '1 DAY') AS device_id_recency_most_recent_session,
        MAX(CASE WHEN dd.dd_device_id IS NOT NULL THEN True ELSE False END) AS device_id_placed_order_most_recent_session,
        MAX(CASE WHEN b.consumer_id IS NOT NULL THEN True ELSE False END) AS device_id_is_auto_logged_in_most_recent_session
    FROM raw_unique_web_device_id_step1 a
    LEFT JOIN most_recent_sessions b
        ON a.dd_device_id = b.dd_device_id
    LEFT JOIN edw.finance.dimension_deliveries dd
        ON dd.is_caviar != 1
        AND dd.is_filtered_core = 1
        AND dd.dd_device_id = a.dd_device_id
        AND dd.created_at BETWEEN b.timestamp AND b.timestamp + INTERVAL '24 HOURS'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

SELECT a.*, (SELECT observed_experiment_start_date FROM vars) AS date_summarized,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('android', 'ios') THEN True ELSE False END) AS consumer_id_has_in_app_orders_recent_28_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('desktop', 'desktop-web') THEN True ELSE False END) AS consumer_id_has_desktop_orders_recent_28_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN True ELSE False END) AS consumer_id_has_mobile_orders_recent_28_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_start_date FROM vars) AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN True ELSE False END) AS consumer_id_has_orders_recent_28_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('android', 'ios') THEN True ELSE False END) AS consumer_id_has_in_app_orders_recent_90_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('desktop', 'desktop-web') THEN True ELSE False END) AS consumer_id_has_desktop_orders_recent_90_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN True ELSE False END) AS consumer_id_has_mobile_orders_recent_90_days,
    MAX(CASE WHEN dd.created_at::DATE BETWEEN (SELECT short_term_end_date FROM vars) - INTERVAL '90 DAYS' AND (SELECT short_term_end_date FROM vars) AND LOWER(dd.submit_platform) IN ('mobile', 'mobile-web') THEN True ELSE False END) AS consumer_id_has_orders_recent_90_days,
    MAX(CASE WHEN dd0.delivery_id IS NOT NULL THEN True ELSE False END) AS consumer_id_has_placed_order_lifetime,
    MAX(CASE WHEN dd0.delivery_id IS NOT NULL THEN dd0.created_at ELSE NULL END) AS consumer_id_first_order_timestamp_lifetime,
    DATEDIFF('DAYS', consumer_id_first_order_timestamp_lifetime, (SELECT short_term_end_date FROM vars)) AS consumer_id_tenancy_lifetime
FROM raw_unique_web_device_id_step2 a
LEFT JOIN edw.finance.dimension_deliveries dd
    ON dd.is_caviar != 1
    AND dd.is_filtered_core = 1
    AND dd.creator_id::VARCHAR = a.consumer_id::VARCHAR
    AND dd.created_at::DATE BETWEEN (SELECT long_term_start_date FROM vars) AND (SELECT long_term_end_date FROM vars)
LEFT JOIN edw.finance.dimension_deliveries dd0
    ON dd0.is_first_ordercart_dd = 1
    AND dd0.is_caviar != 1
    AND dd0.is_filtered_core = 1
    AND dd0.creator_id::VARCHAR = a.consumer_id::VARCHAR
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

--#QUERY# generate_logged_out_personalization_historical_web_device_id_for_labelling
SELECT
    dd_device_id,
    CASE WHEN dd_device_id IS NOT NULL THEN True ELSE False END AS device_id_has_records_last_180_days,
    IFNULL(has_associated_consumer_id, False) AS has_associated_consumer_id,
    IFNULL(device_id_is_active_recent_28_days, False) AS device_id_is_active_recent_28_days,
    CASE WHEN device_id_has_logged_in_recent_28_days > 0 THEN True ELSE False END AS device_id_placed_order_most_recent_28_days,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Direct' THEN True ELSE False END AS device_id_channel_is_direct_most_recent_session,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Organic Search' THEN True ELSE False END AS device_id_channel_is_organic_search_most_recent_session,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Paid Media' THEN True ELSE False END AS device_id_channel_is_paid_media_most_recent_session,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Email' THEN True ELSE False END AS device_id_channel_is_email_most_recent_session,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Partners' THEN True ELSE False END AS device_id_channel_is_partners_most_recent_session,
    CASE WHEN device_id_general_traffic_type_most_recent_session = 'Affiliate' THEN True ELSE False END AS device_id_channel_is_affiliate_most_recent_session,
    IFNULL(device_id_placed_order_most_recent_session, False) AS device_id_placed_order_most_recent_session,
    IFNULL(device_id_is_auto_logged_in_most_recent_session, False) AS device_id_is_auto_logged_in_most_recent_session,
    IFNULL(consumer_id_has_in_app_orders_recent_28_days, False) AS consumer_id_has_in_app_orders_recent_28_days,
    IFNULL(consumer_id_has_mobile_orders_recent_28_days, False) AS consumer_id_has_mobile_orders_recent_28_days,
    IFNULL(consumer_id_has_desktop_orders_recent_28_days, False) AS consumer_id_has_desktop_orders_recent_28_days,
    IFNULL(consumer_id_has_orders_recent_28_days, False) AS consumer_id_has_orders_recent_28_days,
    IFNULL(consumer_id_has_in_app_orders_recent_90_days, False) AS consumer_id_has_in_app_orders_recent_90_days,
    IFNULL(consumer_id_has_mobile_orders_recent_90_days, False) AS consumer_id_has_mobile_orders_recent_90_days,
    IFNULL(consumer_id_has_desktop_orders_recent_90_days, False) AS consumer_id_has_desktop_orders_recent_90_days,
    IFNULL(consumer_id_has_orders_recent_90_days, False) AS consumer_id_has_orders_recent_90_days,
    IFNULL(consumer_id_has_placed_order_lifetime, False) AS consumer_id_has_placed_order_lifetime,
    CASE WHEN consumer_id_tenancy_lifetime <= 28 THEN True ELSE False END AS consumer_id_tenancy_0_28_lifetime,
    CASE WHEN consumer_id_tenancy_lifetime >= 29 AND consumer_id_tenancy_lifetime <= 90 THEN True ELSE False END AS consumer_id_tenancy_29_to_90_lifetime,
    CASE WHEN consumer_id_tenancy_lifetime >= 91 AND consumer_id_tenancy_lifetime <= 180 THEN True ELSE False END AS consumer_id_tenancy_91_to_180_lifetime,
    CASE WHEN consumer_id_tenancy_lifetime >= 181 THEN True ELSE False END AS consumer_id_tenancy_181_or_more_lifetime,
    ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn,
    COUNT(*) OVER () AS total_rows,
    FLOOR((rn - 1)::FLOAT / total_rows * 10, 0) + 1 AS decile_1_to_10
FROM seo.public.logged_out_personalization_historical_web_device_id

--#QUERY# delete_logged_out_personalization_historical_web_device_id
DELETE FROM seo.public.logged_out_personalization_historical_web_device_id

--#QUERY# delete_logged_out_personalization_historical_web_device_id_for_labelling
DELETE FROM seo.public.logged_out_personalization_historical_web_device_id_for_labelling

--#QUERY# generate_personalization_table
SELECT
    dd_device_id,
    IFF(uplift_Direct > 0.01, True, False) AS direct,
    IFF(uplift_Organic_Search > 0.01, True, False) AS organic_search,
    IFF(uplift_Paid_Media > 0.01, True, False) AS paid_media,
    -- IFF(uplift_Email > 0.05, True, False) AS email,
    false AS email,
    IFF(uplift_Partners > 0.01, True, False) AS partners,
    -- IFF(uplift_Affiliate > 0.05, True, False) AS affiliate,
    false AS affiliate,
    IFF(uplift_Null > 0.01, True, False) AS null_channel,
    CURRENT_TIMESTAMP AS updated_at
FROM seo.public.personalization_data_v1
QUALIFY
  ROW_NUMBER()
    OVER (PARTITION BY dd_device_id ORDER BY dd_device_id ) = 1

--#QUERY# get_personalization_data
SELECT *
FROM seo.public.logged_out_personalization
WHERE updated_at > DATEADD(hour, -24, CURRENT_TIMESTAMP())