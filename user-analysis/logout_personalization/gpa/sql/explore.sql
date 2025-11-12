select * from proddb.markwu.gpa_metadata_mx_model_training_data limit 10;

select * from seo.public.personalization_data_v1 limit 10;

select * from seo.public.logged_out_personalization_historical_web_device_id_for_labelling limit 10;


select count(1) from seo.public.personalization_data_v1;


WITH core_dd AS (
    SELECT
        store_id
        , business_id
        , store_name
        , delivery_fee
        , is_caviar
        , delivery_id
        , ntile(4) over (partition by store_id order by delivery_fee) as df_quartile
    FROM edw.finance.dimension_deliveries
    WHERE is_filtered_core = true
        AND active_date >= current_date - 29
        AND is_subscription_discount_applied = False
        AND is_consumer_pickup = false
        AND fulfillment_type = 'dasher'
),

delivery_fee_dup AS (
    SELECT
        store_id,
        store_name,
        is_caviar,
        CASE WHEN AVG(delivery_fee) = 0
            THEN 0
            ELSE (ceil(avg(delivery_fee), -2) - 1)::int
        END AS average_delivery_fee,
        MIN(case when df_quartile = 4 then delivery_fee end) AS seventyfive_percentile_delivery_fee,
        MAX(delivery_fee) AS max_delivery_fee,
        COUNT(delivery_id) AS delivery_fee_last_month_amount,
        IFF(seventyfive_percentile_delivery_fee = 0 OR seventyfive_percentile_delivery_fee < average_delivery_fee, average_delivery_fee, seventyfive_percentile_delivery_fee) AS final_delivery_fee,
        IFF(final_delivery_fee > 300, 0, final_delivery_fee) AS final_delivery_fee_or_zero,
    FROM core_dd
    GROUP BY 1, 2, 3
),

delivery_fee_dup_row_number AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY store_id, is_caviar ORDER BY average_delivery_fee) AS row_num
  FROM delivery_fee_dup
),

delivery_fee AS (
  SELECT
    store_id,
    MAX(CASE WHEN is_caviar != TRUE THEN final_delivery_fee ELSE NULL END) AS final_delivery_fee_doordash,
    MAX(CASE WHEN is_caviar != TRUE THEN final_delivery_fee_or_zero ELSE NULL END) AS final_delivery_fee_or_zero_doordash,
    MAX(CASE WHEN is_caviar = TRUE THEN final_delivery_fee ELSE NULL END) AS final_delivery_fee_caviar,
    MAX(CASE WHEN is_caviar = TRUE THEN final_delivery_fee_or_zero ELSE NULL END) AS final_delivery_fee_or_zero_caviar,
  FROM delivery_fee_dup_row_number
  WHERE row_num = 1
  GROUP BY store_id
),

static_eta_3d AS (
    SELECT
        store_id,
        ROUND(AVG(FINAL_PICKUP_ETA_SECONDS)) AS pickup_eta,
        ROUND(MIN(FINAL_PICKUP_ETA_SECONDS)) AS pickup_eta_min,
        ROUND(AVG(FINAL_DELIVERY_ETA_SECONDS)) AS delivery_eta,
        ROUND(MIN(FINAL_DELIVERY_ETA_SECONDS)) AS delivery_eta_min,
    FROM IGUAZU.SERVER_EVENTS_PRODUCTION.STATIC_ETA_EVENT_ICE
    WHERE iguazu_partition_date >= TO_CHAR(current_date - 3, 'yyyy-mm-dd')
    AND store_id REGEXP '^[0-9]+$'
    GROUP BY store_id
),

static_eta_28d_overall AS (
    SELECT
        store_id,
        ROUND(APPROX_PERCENTILE(FINAL_PICKUP_ETA_SECONDS, 0.5)) AS pickup_eta_median_28d,
        ROUND(APPROX_PERCENTILE(FINAL_DELIVERY_ETA_SECONDS, 0.5)) AS delivery_eta_median_28d
    FROM IGUAZU.SERVER_EVENTS_PRODUCTION.STATIC_ETA_EVENT_ICE SAMPLE SYSTEM (15)
    WHERE iguazu_partition_date >= TO_CHAR(current_date - 28, 'yyyy-mm-dd')
    AND store_id REGEXP '^[0-9]+$'
    AND FINAL_PICKUP_ETA_SECONDS IS NOT NULL
    AND FINAL_DELIVERY_ETA_SECONDS IS NOT NULL
    GROUP BY store_id
    HAVING COUNT(*) >= 3 -- Only stores with sufficient sample size
),

static_eta_same_dow AS (
    SELECT
        store_id,
        ROUND(APPROX_PERCENTILE(FINAL_PICKUP_ETA_SECONDS, 0.25)) AS pickup_eta_p25_same_dow_28d,
        ROUND(APPROX_PERCENTILE(FINAL_DELIVERY_ETA_SECONDS, 0.25)) AS delivery_eta_p25_same_dow_28d,
        ROUND(APPROX_PERCENTILE(FINAL_PICKUP_ETA_SECONDS, 0.75)) AS pickup_eta_p75_same_dow_28d,
        ROUND(APPROX_PERCENTILE(FINAL_DELIVERY_ETA_SECONDS, 0.75)) AS delivery_eta_p75_same_dow_28d
    FROM IGUAZU.SERVER_EVENTS_PRODUCTION.STATIC_ETA_EVENT_ICE SAMPLE SYSTEM (30)
    WHERE iguazu_partition_date >= TO_CHAR(current_date - 28, 'yyyy-mm-dd')
    AND EXTRACT(DAYOFWEEK FROM TO_DATE(iguazu_partition_date, 'yyyy-mm-dd')) = EXTRACT(DAYOFWEEK FROM CURRENT_DATE)
    AND store_id REGEXP '^[0-9]+$'
    AND FINAL_PICKUP_ETA_SECONDS IS NOT NULL
    AND FINAL_DELIVERY_ETA_SECONDS IS NOT NULL
    GROUP BY store_id
    HAVING COUNT(*) >= 2 -- Lower threshold since same-dow has less data
),

static_eta AS (
    SELECT
        COALESCE(COALESCE(eta_3d.store_id, overall.store_id), dow.store_id) AS store_id,
        eta_3d.pickup_eta,
        eta_3d.pickup_eta_min,
        eta_3d.delivery_eta,
        eta_3d.delivery_eta_min,
        overall.pickup_eta_median_28d,
        overall.delivery_eta_median_28d,
        dow.pickup_eta_p25_same_dow_28d,
        dow.delivery_eta_p25_same_dow_28d,
        dow.pickup_eta_p75_same_dow_28d,
        dow.delivery_eta_p75_same_dow_28d
    FROM static_eta_3d eta_3d
    FULL OUTER JOIN static_eta_28d_overall overall ON eta_3d.store_id = overall.store_id
    FULL OUTER JOIN static_eta_same_dow dow ON COALESCE(eta_3d.store_id, overall.store_id) = dow.store_id
),

store_categories AS (
    SELECT
        store_id,
        TO_VARCHAR(tags[0]['label']) AS category
    FROM PUBLIC.FACT_FOOD_CATALOG_V2
    WHERE type = 'store'
    AND tag_category = 'cuisine_types'
    AND concept_scheme_version = '1.0'
    AND model_version = '1.0'
),

exclude AS (
    SELECT
        id,
        IFF(db.business_id IS NULL AND e.id_type = 'Business ID', 'Store ID', id_type) AS final_id_type,
        MAX(IFF(experience IS NULL, 'DoorDash, Caviar', experience)) AS experience
    FROM proddb.public.FACT_MX_EXCLUSION_GSHEET e
    LEFT JOIN edw.merchant.dimension_business db ON e.id = db.business_id AND e.id_type = 'Business ID'
    GROUP BY id, final_id_type
),

q3_experiment AS (
    SELECT
        store_id,
        experiment_group
    FROM proddb.markwu.valid_gpa_store_candidates_2025q3 q3
    UNION ALL
    SELECT
        store_id,
        'Treatment' AS experiment_group
    FROM proddb.markwu.gpa_mx_model_test_roll_out_control
),

store_overrides AS (
    SELECT
        store_id,
        CASE
            WHEN "Correct Rx Name on Place Card?" = False
                 AND "If No - Rx Name Shown on Google Maps" IS NOT NULL
            THEN "If No - Rx Name Shown on Google Maps"
            ELSE NULL
        END AS override_name,
        CASE
            WHEN "Address Matches on Place Card?" = False
                 AND "If No - Address Shown" IS NOT NULL
            THEN "If No - Address Shown"
            ELSE NULL
        END AS override_address,
        CASE
            WHEN "Phone Matches Place Card?" = 'No'
                 AND "If No - Phone Number Shown on Google Maps" IS NOT NULL
            THEN "If No - Phone Number Shown on Google Maps"
            ELSE NULL
        END AS override_phone
    FROM seo.public.gpa_unmatched_stores
    WHERE ("Correct Rx Name on Place Card?" = False AND "If No - Rx Name Shown on Google Maps" IS NOT NULL)
       OR ("Address Matches on Place Card?" = False AND "If No - Address Shown" IS NOT NULL)
       OR ("Phone Matches Place Card?" = 'No' AND "If No - Phone Number Shown on Google Maps" IS NOT NULL)
),

active_stores AS (
    SELECT
        store.store_id,
        COALESCE(overrides.override_name, store.name) AS name,
        store.business_id,
        COALESCE(overrides.override_name, store.business_name) AS business_name,
        IFF (
            COALESCE(overrides.override_phone, store.phone_number) like '+%',
            REGEXP_REPLACE(COALESCE(overrides.override_phone, store.phone_number), '[^0-9+]', ''),
            CASE
                WHEN store.country_shortname = 'US' THEN '+1'
                WHEN store.country_shortname = 'CA' THEN '+1'
                WHEN store.country_shortname = 'NZ' THEN '+64'
                WHEN store.country_shortname = 'MX' THEN '+52'
                WHEN store.country_shortname = 'JP' THEN '+81'
                WHEN store.country_shortname = 'AU' THEN '+61'
                ELSE '+1' -- Default country code
            END || REGEXP_REPLACE(COALESCE(overrides.override_phone, store.phone_number), '[^0-9+]', '')
        ) AS phone_number,
        COALESCE(overrides.override_address, store.store_address) AS store_address,
        store.primary_category_name,
        store.offers_delivery,
        store.offers_pickup,
        store.is_restaurant,
        store.is_cng,
        store.experience,
        IFF (store.is_restaurant = true OR vertical.vertical_name like '%restaurant%', 'store', 'convenience/store') as url_path,
        address.EXT_POINT_LAT as latitude,
        address.EXT_POINT_LONG as longitude,
        address.street_address as street,
        IFF (address.locality IS NULL OR address.locality = '', address.neighborhood, address.locality) as city,
        address.administrative_area_level_1 as state,
        address.country,
        address.country_shortname,
        address.postal_code as zip_code,
        CASE
            WHEN q3_experiment.experiment_group = 'Treatment' OR q3_experiment.experiment_group = 'Treatment 1' OR q3_experiment.experiment_group = 'Treatment 3' OR q3_experiment.experiment_group = 'Treatment 4' OR q3_experiment.experiment_group = 'Treatment 5' OR q3_experiment.experiment_group = 'Treatment 6' THEN delivery_fee.final_delivery_fee_doordash
            WHEN q3_experiment.experiment_group = 'Treatment 7' THEN 0
            ELSE delivery_fee.final_delivery_fee_or_zero_doordash
        END AS final_delivery_fee_doordash,
        CASE
            WHEN q3_experiment.experiment_group = 'Treatment' OR q3_experiment.experiment_group = 'Treatment 1' OR q3_experiment.experiment_group = 'Treatment 3' OR q3_experiment.experiment_group = 'Treatment 4' OR q3_experiment.experiment_group = 'Treatment 5' OR q3_experiment.experiment_group = 'Treatment 6' THEN final_delivery_fee_caviar
            WHEN q3_experiment.experiment_group = 'Treatment 7' THEN 0
            ELSE IFF(delivery_fee.final_delivery_fee_or_zero_caviar IS NULL OR delivery_fee.final_delivery_fee_or_zero_caviar <= 0, delivery_fee.final_delivery_fee_or_zero_doordash, delivery_fee.final_delivery_fee_or_zero_caviar)
        END AS final_delivery_fee_caviar,
        eta.delivery_eta,
        eta.pickup_eta,
        FLOOR((delivery_eta + delivery_eta_min) / 2) AS express_delivery_eta,
        FLOOR((pickup_eta + pickup_eta_min) / 2) AS express_pickup_eta,
        eta.pickup_eta_median_28d,
        eta.delivery_eta_median_28d,
        eta.pickup_eta_p25_same_dow_28d,
        eta.delivery_eta_p25_same_dow_28d,
        eta.pickup_eta_p75_same_dow_28d,
        eta.delivery_eta_p75_same_dow_28d,
        q3_experiment.experiment_group,
        exclude.experience AS excluded_experience,
        IFF (is_restaurant, 'restaurant', 'convenience') AS category
    FROM proddb.public.dimension_store store
    LEFT JOIN store_overrides overrides ON store.store_id = overrides.store_id
    LEFT JOIN edw.geo.address address
    ON store.address_id = address.id
    LEFT JOIN delivery_fee
    ON delivery_fee.store_id = store.store_id
    LEFT JOIN static_eta eta
    ON TO_VARCHAR(eta.store_id) = TO_VARCHAR(store.store_id)
    LEFT JOIN edw.merchant.dimension_business_vertical vertical
    ON store.business_vertical_id = vertical.business_vertical_id
    LEFT JOIN exclude
    ON (exclude.id = store.store_id AND exclude.final_id_type = 'Store ID') OR (exclude.id = store.business_id AND exclude.final_id_type = 'Business ID')
    LEFT JOIN q3_experiment ON q3_experiment.store_id = store.store_id
    LEFT JOIN store_categories
    ON store_categories.store_id = store.store_id
    WHERE
        is_active = true
        AND is_partner = true
        AND is_test = false
        -- AND address.is_active_merchant = true
        AND (excluded_experience IS NULL OR excluded_experience != 'DoorDash, Caviar')
)

SELECT DISTINCT
    store_id,
    IFF(
        business_name IS NULL OR business_name = '' OR city IS NULL OR city = '',
        'https://www.doordash.com/' || url_path || '/' || store_id || '/',
        LOWER('https://www.doordash.com/' || url_path || '/' || REGEXP_REPLACE(business_name, '[ /]', '-') || '-' || REPLACE(city, ' ', '-') || '-' || store_id || '/')
    ) AS merchant_id,
    COALESCE(NULLIF(business_name, ''), NULLIF(name, '')) AS name,
    -- E.164 phone validation: must start with '+', 7-15 total digits, not all zeros
    CASE
        WHEN phone_number IS NULL OR phone_number = '' THEN NULL
        WHEN NOT (phone_number LIKE '+%') THEN NULL  -- Must start with '+'
        WHEN LENGTH(phone_number) < 8 OR LENGTH(phone_number) > 16 THEN NULL  -- E.164: 7-15 digits
        WHEN REGEXP_REPLACE(SUBSTRING(phone_number, 2), '0', '') = '' THEN NULL  -- Not all zeros
        -- Country-specific validation (length + NSN check combined)
        WHEN phone_number LIKE '+1%' AND (LENGTH(phone_number) NOT IN (11, 12) OR LENGTH(SUBSTRING(phone_number, 3)) < 6) THEN NULL  -- US/CA
        WHEN phone_number LIKE '+61%' AND (LENGTH(phone_number) NOT BETWEEN 11 AND 13 OR LENGTH(SUBSTRING(phone_number, 4)) < 6) THEN NULL  -- AU
        WHEN phone_number LIKE '+64%' AND (LENGTH(phone_number) NOT BETWEEN 11 AND 12 OR LENGTH(SUBSTRING(phone_number, 4)) < 6) THEN NULL  -- NZ
        WHEN phone_number LIKE '+52%' AND (LENGTH(phone_number) NOT BETWEEN 12 AND 13 OR LENGTH(SUBSTRING(phone_number, 4)) < 6) THEN NULL  -- MX
        WHEN phone_number LIKE '+81%' AND (LENGTH(phone_number) NOT BETWEEN 12 AND 13 OR LENGTH(SUBSTRING(phone_number, 4)) < 6) THEN NULL  -- JP
        ELSE phone_number
    END AS telephone,
    latitude,
    longitude,
    IFF(store_address IS NULL OR store_address = '', NULL, store_address) AS unstructured_address,
    country_shortname AS country,
    city AS locality,
    zip_code AS postal_code,
    state AS region,
    street AS street_address,
    ARRAY_CONSTRUCT_COMPACT(
        IFF(excluded_experience IS NULL OR excluded_experience != 'DoorDash', 'doordash', NULL),
        IFF(experience LIKE '%caviar%' AND (excluded_experience IS NULL OR excluded_experience != 'Caviar'), 'caviar', NULL)
    ) AS experience_array,
    -- Delivery fee logic (platform-aware)
    'absolute' AS delivery_fee_type,
    CASE
        WHEN ARRAY_CONTAINS('caviar'::VARIANT, experience_array) AND final_delivery_fee_caviar IS NOT NULL AND final_delivery_fee_caviar >= 0 THEN final_delivery_fee_caviar
        WHEN ARRAY_CONTAINS('doordash'::VARIANT, experience_array) AND final_delivery_fee_doordash IS NOT NULL AND final_delivery_fee_doordash >= 0 THEN final_delivery_fee_doordash
        ELSE NULL
    END AS delivery_fee_fixed_amount,
    NULL AS delivery_fee_max_amount,
    NULL AS delivery_fee_min_amount,
    -- Service fee logic
    CASE
        WHEN experiment_group = 'Treatment 5' THEN NULL
        ELSE 'percentage'
    END AS service_fee_type,
    CASE
        WHEN experiment_group = 'Treatment 5' THEN NULL
        WHEN experiment_group = 'Treatment 4' THEN NULL
        ELSE 5.0
    END AS service_fee_min_amount,
    CASE
        WHEN experiment_group = 'Treatment 5' THEN NULL
        ELSE 15.0
    END AS service_fee_max_amount,
    NULL AS service_fee_fixed_amount,
    -- Currency
    'USD' AS currency,
    -- ETA logic
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN
            CASE
                WHEN delivery_eta_p25_same_dow_28d IS NOT NULL AND (experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN delivery_eta_p25_same_dow_28d
                WHEN express_delivery_eta IS NOT NULL THEN express_delivery_eta
                ELSE NULL
            END
        ELSE NULL
    END AS delivery_eta_min_amount,
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN
            CASE
                WHEN delivery_eta_p75_same_dow_28d IS NOT NULL AND (experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN delivery_eta_p75_same_dow_28d
                WHEN delivery_eta IS NOT NULL THEN delivery_eta
                ELSE NULL
            END
        ELSE NULL
    END AS delivery_eta_max_amount,
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN NULL
        WHEN delivery_eta IS NOT NULL AND (experiment_group IN ('Treatment', 'Treatment 1')) THEN delivery_eta
        WHEN delivery_eta_median_28d IS NOT NULL AND experiment_group = 'Treatment 6' THEN delivery_eta_median_28d
        WHEN experiment_group = 'Control' AND delivery_eta IS NOT NULL AND express_delivery_eta IS NOT NULL THEN FLOOR((delivery_eta + express_delivery_eta) / 2)
        ELSE NULL
    END AS delivery_eta_fixed_amount,
    -- Pickup ETA (normalized)
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN
            CASE
                WHEN pickup_eta_p25_same_dow_28d IS NOT NULL AND (experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN pickup_eta_p25_same_dow_28d
                WHEN express_pickup_eta IS NOT NULL THEN express_pickup_eta
                ELSE NULL
            END
        ELSE NULL
    END AS pickup_eta_min_amount,
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN
            CASE
                WHEN pickup_eta_p75_same_dow_28d IS NOT NULL AND (experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN pickup_eta_p75_same_dow_28d
                WHEN pickup_eta IS NOT NULL THEN pickup_eta
                ELSE NULL
            END
        ELSE NULL
    END AS pickup_eta_max_amount,
    CASE
        WHEN express_delivery_eta > 0 AND express_pickup_eta < pickup_eta AND (experiment_group IS NULL OR experiment_group IN ('Treatment 2', 'Treatment 3', 'Treatment 4', 'Treatment 5', 'Treatment 7')) THEN NULL
        WHEN pickup_eta IS NOT NULL AND (experiment_group IN ('Treatment', 'Treatment 1')) THEN pickup_eta
        WHEN pickup_eta_median_28d IS NOT NULL AND experiment_group = 'Treatment 6' THEN pickup_eta_median_28d
        WHEN experiment_group = 'Control' AND pickup_eta IS NOT NULL AND express_pickup_eta IS NOT NULL THEN FLOOR((pickup_eta + express_pickup_eta) / 2)
        ELSE NULL
    END AS pickup_eta_fixed_amount,
    offers_delivery,
    offers_pickup,
    url_path,
    is_restaurant,
    country_shortname,
    CURRENT_TIMESTAMP AS updated_at
FROM
    active_stores store
WHERE offers_delivery = true OR offers_pickup = true

;


select distinct query_text from proddb.public.fact_snowflake_query_history where start_time>='2025-06-15' and lower(role_name) = 'markwu'
and query_text ilike '%DINNER_17_21_ORDERS_ATTRIBUTED_TO_GPA_SAME_DAY%';


select distinct query_text from proddb.public.fact_snowflake_query_history where start_time>='2025-06-24' and start_time<='2025-07-01' and lower(role_name) = 'markwu'
and query_text ilike '%markwu.causal_inference_baseline_output%' ;

select distinct query_text, start_time from proddb.public.fact_snowflake_query_history where start_time>='2025-06-24' and start_time<='2025-07-01' start_time>='2025-06-15' and lower(role_name) = 'markwu'
and query_text ilike '%markwu.gpa_mx360_table_output%' ;




CREATE OR REPLACE TABLE proddb.markwu.gpa_mx360_table_output AS 

WITH dimension_store_data AS (
SELECT 
store_id 
, CASE WHEN nv_vertical_name IS NULL THEN 'Restaurant' ELSE nv_vertical_name END AS dimension_store_vertical_name 
, management_type_grouped AS dimension_store_management_type_grouped 
, is_partner AS dimension_store_is_partner 
, is_consumer_subscription_eligible AS dimension_store_is_consumer_subscription_eligible
, is_caviar_enabled AS dimension_store_is_caviar_enabled 
, is_storefront_enabled AS dimension_store_is_storefront_enabled 
, is_food_truck AS dimension_store_is_food_truck 
, is_virtual_brand  AS dimension_store_is_virtual_brand 
, is_national_business AS dimension_store_is_national_business
, is_pickup_enabled AS dimension_store_is_pickup_enabled 
, is_delivery_enabled AS dimension_store_is_delivery_enabled 
, last_30_day_delivs AS dimension_store_last_30_day_delivs 
, primary_tag_name  AS dimension_store_primary_tag_name 
, primary_category_name AS dimension_store_primary_category_name 
, dietary_preference AS dimension_store_dietary_preference 
, SPLIT_PART(cuisine_type, ',', 1) AS dimension_store_1st_cuisine_type 
, price_range AS dimension_store_price_range 
, custom_delivery_fee AS dimension_store_custom_delivery_fee 
, commission_rate AS dimension_store_commission_rate 
, pickup_commission_rate AS dimension_store_pickup_commission_rate 
, service_rate AS dimension_store_service_rate 
, timezone AS dimension_store_timezone 
, delivery_radius_tier AS dimension_store_delivery_radius_tier 
, consumer_count AS dimension_store_consumer_count 
, avg_dist AS dimension_store_avg_dist 
, starting_point_id AS dimension_store_starting_point_id 
FROM edw.merchant.dimension_store 
WHERE experience LIKE '%doordash%'
    AND is_active_store = 1 
)

, num_stores_by_starting_point AS (
SELECT 
dimension_store_starting_point_id
, COUNT(DISTINCT store_id) AS num_stores_within_the_same_starting_point
FROM dimension_store_data 
GROUP BY 1 
)

, enhanced_dimension_store_data AS (
SELECT 
a.* 
, b.num_stores_within_the_same_starting_point
FROM dimension_store_data a
JOIN num_stores_by_starting_point b 
    ON a.dimension_store_starting_point_id = b.dimension_store_starting_point_id 
)

, gpa_performance_raw AS (
SELECT 
context_page_path 
, store_id::VARCHAR AS store_id 
, dd_device_id 
, platform 
, iguazu_timestamp 
, dd_session_id 
, LOWER(context_locale) AS locale 
, context_page_url 
FROM iguazu.server_events_production.store_page_load_consumer 
WHERE iguazu_timestamp::DATE >= $pre_treatment_start_date - INTERVAL '91 DAYS'
    AND iguazu_timestamp::DATE <= $pre_treatment_start_date - INTERVAL '2 DAYS'
    AND (
        referrer NOT LIKE '%doordash%'
        )
    AND NULLIF(utm_campaign, '') = 'gpa'
    AND context_user_agent NOT ILIKE '%bot%'
    AND context_user_agent NOT ILIKE '%prerender%'
    AND context_user_agent NOT ILIKE '%read-aloud%'   
UNION ALL 
SELECT 
CASE WHEN ENDSWITH(PARSE_URL(deep_link_url, 1):path::VARCHAR, '/') THEN PARSE_URL(deep_link_url, 1):path::VARCHAR ELSE PARSE_URL(deep_link_url, 1):path::VARCHAR||'/' END AS context_page_path_c
, SPLIT_PART(SPLIT_PART(context_page_path_c, '/', -2), '-', -1)::VARCHAR AS store_id
, iguazu_other_properties:dd_device_id::VARCHAR AS dd_device_id 
, context_device_type AS platform 
, iguazu_timestamp 
, iguazu_other_properties:dd_session_id ::VARCHAR AS dd_session_id 
, LOWER(context_locale) AS locale 
, deep_link_url AS context_page_url 
FROM iguazu.server_events_production.m_deep_link
WHERE iguazu_timestamp::DATE >= $pre_treatment_start_date - INTERVAL '91 DAYS' 
    AND iguazu_timestamp::DATE <= $pre_treatment_start_date  - INTERVAL '2 DAYS' 
    AND NULLIF(utm_campaign, '') = 'gpa'
    AND deep_link_url LIKE '%/store/%'
    AND deep_link_url NOT LIKE '%/search/%'
    AND deep_link_url NOT LIKE '%/product%' 
    AND deep_link_url LIKE '%http%doordash%'
)

, gpa_performance_orders_raw AS (
SELECT 
a.store_id 
, a.platform 
, dd.created_at 
, dd.gov 
, dd.variable_profit 
, dd.is_subscribed_consumer 
, dd.delivery_id 
, dd.creator_id AS consumer_id 
, dd.is_asap 
, DATEDIFF('MINUTES', dd.created_at, dd.actual_delivery_time) AS delivery_time
, dd.actual_delivery_time_local 
FROM gpa_performance_raw a 
JOIN edw.finance.dimension_deliveries dd 
  ON dd.is_caviar != 1 
  AND dd.is_filtered_core = 1 
  AND dd.dd_device_id::VARCHAR = a.dd_device_id::VARCHAR 
  AND dd.created_at::DATE = a.iguazu_timestamp::DATE 
  AND dd.created_at::DATE >= $pre_treatment_start_date - INTERVAL '29 DAYS'
  AND dd.created_at::DATE <= $pre_treatment_start_date - INTERVAL '2 DAYS'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11 
)

, gpa_performance AS (
SELECT 
store_id 
, COUNT(DISTINCT delivery_id) AS orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN platform IN ('mobile', 'desktop') THEN delivery_id ELSE NULL END) AS web_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN platform IN ('android', 'ios') THEN delivery_id ELSE NULL END) AS in_app_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN is_subscribed_consumer = True THEN delivery_id ELSE NULL END) AS dashpass_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN is_subscribed_consumer = False THEN delivery_id ELSE NULL END) AS classic_orders_attributed_to_gpa_same_day
, AVG(gov::FLOAT / 100) AS aov_attributed_to_gpa_same_day
, AVG(variable_profit::FLOAT / 100) AS vp_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN is_asap = True THEN delivery_id ELSE NULL END) AS asap_orders_attributed_to_gpa_same_day
, AVG(CASE WHEN is_asap = True THEN delivery_time ELSE NULL END) AS asap_orders_avg_delivery_time_attributed_to_gpa_same_day
, MEDIAN(CASE WHEN is_asap = True THEN delivery_time ELSE NULL END) AS asap_orders_median_delivery_time_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN is_asap = False THEN delivery_id ELSE NULL END) AS scheduled_orders_attributed_to_gpa_same_day 
, AVG(CASE WHEN is_asap = False THEN delivery_time ELSE NULL END) AS scheduled_orders_avg_delivery_time_attributed_to_gpa_same_day
, MEDIAN(CASE WHEN is_asap = False THEN delivery_time ELSE NULL END) AS scheduled_orders_median_delivery_time_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 1 THEN delivery_id ELSE NULL END) AS monday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 2 THEN delivery_id ELSE NULL END) AS tuesday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 3 THEN delivery_id ELSE NULL END) AS wednesday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 4 THEN delivery_id ELSE NULL END) AS thursday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 5 THEN delivery_id ELSE NULL END) AS friday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 6 THEN delivery_id ELSE NULL END) AS saturday_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN DAYOFWEEK(actual_delivery_time_local::DATE) = 7 OR DAYOFWEEK(actual_delivery_time_local::DATE) = 0 THEN delivery_id ELSE NULL END) AS sunday_orders_attributed_to_gpa_same_day 
, COUNT(DISTINCT CASE WHEN HOUR(actual_delivery_time_local) BETWEEN 6 AND 10 THEN delivery_id ELSE NULL END) AS breakfast_5_10_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN HOUR(actual_delivery_time_local) BETWEEN 11 AND 15 THEN delivery_id ELSE NULL END) AS lunch_11_15_orders_attributed_to_gpa_same_day
, COUNT(DISTINCT CASE WHEN HOUR(actual_delivery_time_local) BETWEEN 17 AND 21 THEN delivery_id ELSE NULL END) AS dinner_17_21_orders_attributed_to_gpa_same_day
, breakfast_5_10_orders_attributed_to_gpa_same_day / NULLIF(breakfast_5_10_orders_attributed_to_gpa_same_day + lunch_11_15_orders_attributed_to_gpa_same_day + dinner_17_21_orders_attributed_to_gpa_same_day,0) AS breakfast_5_10_order_ratio_attributed_to_gpa_same_day
, lunch_11_15_orders_attributed_to_gpa_same_day / NULLIF(breakfast_5_10_orders_attributed_to_gpa_same_day + lunch_11_15_orders_attributed_to_gpa_same_day + dinner_17_21_orders_attributed_to_gpa_same_day,0) AS lunch_11_15_order_ratio_attributed_to_gpa_same_day
, dinner_17_21_orders_attributed_to_gpa_same_day / NULLIF(breakfast_5_10_orders_attributed_to_gpa_same_day + lunch_11_15_orders_attributed_to_gpa_same_day + dinner_17_21_orders_attributed_to_gpa_same_day,0) AS dinner_17_21_order_ratio_attributed_to_gpa_same_day
FROM gpa_performance_orders_raw 
WHERE store_id IS NOT NULL 
    AND store_id != ''
GROUP BY 1 
)

SELECT 
a.* 
, b.orders_attributed_to_gpa_same_day
, b.web_orders_attributed_to_gpa_same_day
, b.in_app_orders_attributed_to_gpa_same_day
, b.dashpass_orders_attributed_to_gpa_same_day
, b.classic_orders_attributed_to_gpa_same_day
, b.aov_attributed_to_gpa_same_day
, b.vp_attributed_to_gpa_same_day
, b.asap_orders_attributed_to_gpa_same_day
, b.asap_orders_avg_delivery_time_attributed_to_gpa_same_day
, b.asap_orders_median_delivery_time_attributed_to_gpa_same_day
, b.scheduled_orders_attributed_to_gpa_same_day
, b.scheduled_orders_avg_delivery_time_attributed_to_gpa_same_day
, b.scheduled_orders_median_delivery_time_attributed_to_gpa_same_day
, b.monday_orders_attributed_to_gpa_same_day
, b.tuesday_orders_attributed_to_gpa_same_day
, b.wednesday_orders_attributed_to_gpa_same_day
, b.thursday_orders_attributed_to_gpa_same_day
, b.friday_orders_attributed_to_gpa_same_day
, b.saturday_orders_attributed_to_gpa_same_day
, b.sunday_orders_attributed_to_gpa_same_day
, b.breakfast_5_10_orders_attributed_to_gpa_same_day
, b.lunch_11_15_orders_attributed_to_gpa_same_day
, b.dinner_17_21_orders_attributed_to_gpa_same_day
, b.breakfast_5_10_order_ratio_attributed_to_gpa_same_day
, b.lunch_11_15_order_ratio_attributed_to_gpa_same_day
, b.dinner_17_21_order_ratio_attributed_to_gpa_same_day
FROM enhanced_dimension_store_data a 
LEFT JOIN gpa_performance b 
    ON a.store_id::VARCHAR = b.store_id::VARCHAR 
JOIN (
    SELECT 
    DISTINCT store_id 
    FROM proddb.public.gpa_store_feed
     ) c 
    ON a.store_id::VARCHAR = c.store_id::VARCHAR 

;


CREATE OR REPLACE TABLE proddb.markwu.causal_inference_baseline_output AS (

SELECT 
DISTINCT store_id 
FROM proddb.public.gpa_store_feed 

)

;

CREATE OR REPLACE TABLE proddb.markwu.gpa_metadata_mx_model_training_data_output AS 

SELECT 
a.* 
, b.dimension_store_vertical_name
, b.dimension_store_management_type_grouped
, b.dimension_store_is_partner
, b.dimension_store_is_consumer_subscription_eligible
, b.dimension_store_is_caviar_enabled
, b.dimension_store_is_storefront_enabled
, b.dimension_store_is_food_truck
, b.dimension_store_is_virtual_brand
, b.dimension_store_is_national_business
, b.dimension_store_is_pickup_enabled
, b.dimension_store_is_delivery_enabled
, b.dimension_store_last_30_day_delivs
, b.dimension_store_primary_tag_name
, b.dimension_store_primary_category_name
, b.dimension_store_dietary_preference
, b.dimension_store_1st_cuisine_type
, b.dimension_store_price_range
, b.dimension_store_custom_delivery_fee
, b.dimension_store_commission_rate
, b.dimension_store_pickup_commission_rate
, b.dimension_store_service_rate
, b.dimension_store_timezone
, b.dimension_store_delivery_radius_tier
, b.dimension_store_consumer_count
, b.dimension_store_avg_dist
, b.num_stores_within_the_same_starting_point
, b.orders_attributed_to_gpa_same_day
, b.web_orders_attributed_to_gpa_same_day
, b.in_app_orders_attributed_to_gpa_same_day
, b.dashpass_orders_attributed_to_gpa_same_day
, b.classic_orders_attributed_to_gpa_same_day
, b.aov_attributed_to_gpa_same_day
, b.vp_attributed_to_gpa_same_day
, b.asap_orders_attributed_to_gpa_same_day
, b.asap_orders_avg_delivery_time_attributed_to_gpa_same_day
, b.asap_orders_median_delivery_time_attributed_to_gpa_same_day
, b.scheduled_orders_attributed_to_gpa_same_day
, b.scheduled_orders_avg_delivery_time_attributed_to_gpa_same_day
, b.scheduled_orders_median_delivery_time_attributed_to_gpa_same_day
, b.monday_orders_attributed_to_gpa_same_day
, b.tuesday_orders_attributed_to_gpa_same_day
, b.wednesday_orders_attributed_to_gpa_same_day
, b.thursday_orders_attributed_to_gpa_same_day
, b.friday_orders_attributed_to_gpa_same_day
, b.saturday_orders_attributed_to_gpa_same_day
, b.sunday_orders_attributed_to_gpa_same_day
, b.breakfast_5_10_orders_attributed_to_gpa_same_day
, b.lunch_11_15_orders_attributed_to_gpa_same_day
, b.dinner_17_21_orders_attributed_to_gpa_same_day
, b.breakfast_5_10_order_ratio_attributed_to_gpa_same_day
, b.lunch_11_15_order_ratio_attributed_to_gpa_same_day
, b.dinner_17_21_order_ratio_attributed_to_gpa_same_day
FROM proddb.markwu.causal_inference_baseline_output a 
JOIN proddb.markwu.gpa_mx360_table_output b 
    ON a.store_id = b.store_id 

;