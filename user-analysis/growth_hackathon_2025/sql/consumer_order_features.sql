-- Extract consumer order features from last 90 days
-- Based on reonboading_cx_features_nux consumers

create or replace table proddb.public.onboarding_hackathon_nux_features as (

WITH consumer_list AS (
    SELECT DISTINCT consumer_id, email_consumer_map
    FROM proddb.public.reonboading_cx_features_nux
),

order_history AS (
    SELECT 
        dd.creator_id AS consumer_id,
        dd.delivery_id,
        dd.created_at AS created_at_utc,
        dd.timezone,
        -- Convert UTC to local timezone
        CONVERT_TIMEZONE(dd.timezone, dd.created_at) AS created_at_local,
        dd.subtotal,
        ds.CUISINE_TYPE AS cuisine_type,
        dd.store_id,
        ds.name as store_name
    FROM dimension_deliveries dd
    INNER JOIN consumer_list cl ON dd.creator_id = cl.consumer_id
    LEFT JOIN edw.merchant.dimension_store ds ON dd.store_id = ds.store_id
    WHERE dd.created_at >= DATEADD(day, -90, CURRENT_DATE())
        AND dd.is_filtered_core = 1
        AND dd.country_id = 1
),

time_blocks AS (
    SELECT 
        *,
        EXTRACT(HOUR FROM created_at_local) AS hour_of_day,
        DAYNAME(created_at_local) AS day_of_week,
        CASE 
            WHEN EXTRACT(HOUR FROM created_at_local) >= 5 AND EXTRACT(HOUR FROM created_at_local) < 11 THEN 'breakfast'
            WHEN EXTRACT(HOUR FROM created_at_local) >= 11 AND EXTRACT(HOUR FROM created_at_local) < 14 THEN 'lunch'
            WHEN EXTRACT(HOUR FROM created_at_local) >= 14 AND EXTRACT(HOUR FROM created_at_local) < 17 THEN 'happy-hour'
            WHEN EXTRACT(HOUR FROM created_at_local) >= 17 AND EXTRACT(HOUR FROM created_at_local) < 22 THEN 'dinner'
            ELSE 'midnight'
        END AS meal_time_block
    FROM order_history
),

consumer_order_features AS (
    SELECT 
        consumer_id,
        -- Last order from order data
        MAX(created_at_utc) AS last_order,
        
        -- Order metrics
        COUNT(DISTINCT delivery_id) AS orders_last_90d,
        AVG(subtotal) AS avg_order_value,
        
        -- Most common day of week
        MODE(day_of_week) AS common_day_of_week,
        
        -- Most common meal time block
        MODE(meal_time_block) AS common_meal_time_block
    FROM time_blocks
    GROUP BY consumer_id
),

-- Top cuisines per consumer (more detailed)
top_cuisines AS (
    SELECT 
        consumer_id,
        cuisine_type,
        COUNT(*) AS cuisine_order_count,
        ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY COUNT(*) DESC) AS cuisine_rank
    FROM time_blocks
    WHERE cuisine_type IS NOT NULL
    GROUP BY consumer_id, cuisine_type
),

favorite_cuisines_array AS (
    SELECT 
        consumer_id,
        ARRAY_AGG(cuisine_type) WITHIN GROUP (ORDER BY cuisine_order_count DESC) AS favorite_cuisines
    FROM top_cuisines
    WHERE cuisine_rank <= 3
    GROUP BY consumer_id
),

-- Top stores per consumer (more detailed)
top_stores AS (
    SELECT 
        consumer_id,
        store_id,
        store_name,
        COUNT(*) AS store_order_count,
        ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY COUNT(*) DESC) AS store_rank
    FROM time_blocks
    WHERE store_id IS NOT NULL
    GROUP BY consumer_id, store_id, store_name
),

favorite_stores_array AS (
    SELECT 
        ts.consumer_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'store_id', ts.store_id,
                'store_name', ts.store_name,
                'order_count', ts.store_order_count,
                'store_cover_img_url', COALESCE(si.store_cover_img_url, ''),
                'business_cover_img', COALESCE(si.business_cover_img, ''),
                'business_cover_square_img', COALESCE(si.business_cover_square_img, ''),
                'business_header_image', COALESCE(si.business_header_image, ''),
                'primary_image_url', COALESCE(si.primary_image_url, '')
            )
        ) WITHIN GROUP (ORDER BY ts.store_order_count DESC) AS favorite_stores
    FROM top_stores ts
    LEFT JOIN proddb.public.onboarding_hackathon_store_images si 
        ON ts.store_id = si.STORE_ID
    WHERE ts.store_rank <= 10
    GROUP BY ts.consumer_id
),

-- Get consumer tenure info from dimension_consumer
consumer_tenure AS (
    SELECT 
        consumer_id,
        created_date AS consumer_created_date,
        DATEDIFF(day, created_date, CURRENT_DATE()) AS days_since_signup,
        1 AS has_dashpass
    FROM edw.consumer.dimension_consumers
    WHERE consumer_id IN (SELECT consumer_id FROM consumer_list)
)

-- Final output
SELECT 
    cof.consumer_id,
    
    -- First visit and last order
    ct.consumer_created_date AS first_visit,
    cof.last_order,
    
    -- Order history features
    cof.orders_last_90d,
    cof.avg_order_value,
    cof.common_day_of_week,
    cof.common_meal_time_block,
    
    -- Favorite cuisines and stores
    fca.favorite_cuisines,
    fsa.favorite_stores,
    
    -- Tenure features from dimension_consumer
    ct.days_since_signup,
    ct.has_dashpass,
    
    -- From original table (reonboading_cx_features_nux)
    nux.country_id,
    nux.daf_p365d_order_cnt,
    nux.is_paid_active_dp,
    nux.is_trial_active_dp,
    nux.is_dashpass_active,
    nux.v3_sens_ptile_all_cx,
    nux.v3_sensitivity_cohort,
    
    -- Parsed from genai_cx_profile JSON
    PARSE_JSON(nux.genai_cx_profile):overall_profile:cuisine_preferences::STRING AS cuisine_preferences,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:food_preferences::STRING AS food_preferences,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:taste_preference::STRING AS taste_preference,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:reordering_tendency::STRING AS reordering_tendency,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:vertical_orientation::STRING AS vertical_orientation,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:price_sensitivity::STRING AS price_sensitivity,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:exploration_index:cuisine_variety::STRING AS exploration_cuisine_variety,
    PARSE_JSON(nux.genai_cx_profile):overall_profile:exploration_index:store_reliance::STRING AS exploration_store_reliance,
    
    -- Fake promo tags for demo purposes
    CASE 
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 0 THEN '20% off $15+'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 1 THEN '$0 delivery fee'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 2 THEN '15% off first order'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 3 THEN 'Buy 1 Get 1 Free'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 4 THEN '$5 off $25+'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 5 THEN 'Free dessert with order'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 6 THEN NULL
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 7 THEN '30% off pickup orders'
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 8 THEN NULL
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) = 9 THEN '$10 off $30+'
    END AS promo_tags,
    
    -- Promo eligible based on whether promo_tags exists
    CASE 
        WHEN MOD(ABS(HASH(cof.consumer_id)), 10) IN (6, 8) THEN 0
        ELSE 1
    END AS promo_eligible,
    
    -- Fake traffic source for demo purposes
    CASE 
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 0 THEN 'deep_link'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 1 THEN 'google_placed_ad'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 2 THEN 'meta'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 3 THEN 'organic_search'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 4 THEN 'instagram_ad'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 5 THEN 'email_campaign'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 6 THEN 'referral'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 7 THEN 'tiktok_ad'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 8 THEN 'direct'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 9 THEN 'youtube_ad'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 10 THEN 'sms_campaign'
        WHEN MOD(ABS(HASH(cof.consumer_id || 'traffic')), 12) = 11 THEN 'app_store'
    END AS traffic_source,
    
    nux.email_consumer_map

FROM consumer_order_features cof
LEFT JOIN favorite_cuisines_array fca ON cof.consumer_id = fca.consumer_id
LEFT JOIN favorite_stores_array fsa ON cof.consumer_id = fsa.consumer_id
LEFT JOIN consumer_tenure ct ON cof.consumer_id = ct.consumer_id
LEFT JOIN proddb.public.reonboading_cx_features_nux nux ON cof.consumer_id = nux.consumer_id
ORDER BY cof.consumer_id
);




select * from proddb.public.onboarding_hackathon_nux_features where consumer_id = '1125900281188900';

-- select count(1) from  proddb.public.onboarding_hackathon_store_images;
create or replace table proddb.public.onboarding_hackathon_store_images as (


WITH store_images AS (
  SELECT
    s.STORE_ID,
    s.NAME AS store_name,
    s.BUSINESS_ID,
    s.COVER_IMG_URL,
    s.UPDATED_AT,
    s.CREATED_AT
  FROM edw.merchant.dimension_store s
  WHERE
    s.IS_RESTAURANT = 1
    AND s.IS_MP_ENABLED = 1
    -- AND COALESCE(s.UPDATED_AT, s.CREATED_AT) >= DATEADD(day, -7, CURRENT_DATE)
),
business_images AS (
  SELECT
    b.BUSINESS_ID,
    b.COVER_IMG,
    b.COVER_SQUARE_IMG,
    b.HEADER_IMAGE,
    b.UPDATED_AT AS business_updated_at
  FROM edw.merchant.dimension_business b
--   WHERE
--     b.UPDATED_AT >= DATEADD(day, -7, CURRENT_DATE)
)
SELECT
  si.STORE_ID,
  si.store_name,
  -- Keep all image URLs
  NULLIF(si.COVER_IMG_URL, '') AS store_cover_img_url,
  CASE WHEN bi.COVER_IMG ILIKE 'http%' THEN bi.COVER_IMG END AS business_cover_img,
  CASE WHEN bi.COVER_SQUARE_IMG ILIKE 'http%' THEN bi.COVER_SQUARE_IMG END AS business_cover_square_img,
  CASE WHEN bi.HEADER_IMAGE ILIKE 'http%' THEN bi.HEADER_IMAGE END AS business_header_image,
  -- Primary image URL (first non-null)
  COALESCE(
    NULLIF(si.COVER_IMG_URL, ''),
    CASE WHEN bi.COVER_IMG ILIKE 'http%' THEN bi.COVER_IMG END,
    CASE WHEN bi.COVER_SQUARE_IMG ILIKE 'http%' THEN bi.COVER_SQUARE_IMG END,
    CASE WHEN bi.HEADER_IMAGE ILIKE 'http%' THEN bi.HEADER_IMAGE END
  ) AS primary_image_url
FROM store_images si
LEFT JOIN business_images bi
  ON bi.BUSINESS_ID = si.BUSINESS_ID
WHERE
  COALESCE(
    NULLIF(si.COVER_IMG_URL, ''),
    CASE WHEN bi.COVER_IMG ILIKE 'http%' THEN bi.COVER_IMG END,
    CASE WHEN bi.COVER_SQUARE_IMG ILIKE 'http%' THEN bi.COVER_SQUARE_IMG END,
    CASE WHEN bi.HEADER_IMAGE ILIKE 'http%' THEN bi.HEADER_IMAGE END
  ) IS NOT NULL
ORDER BY si.STORE_ID
);

-- Extract consumer_id, store_id, and image_url from favorite_stores
SELECT 
    consumer_id,
    store.value:store_id::VARCHAR AS store_id,
    store.value:store_name::VARCHAR AS store_name,
    COALESCE(
        NULLIF(store.value:store_cover_img_url::VARCHAR, ''),
        NULLIF(store.value:business_cover_img::VARCHAR, ''),
        NULLIF(store.value:business_cover_square_img::VARCHAR, ''),
        NULLIF(store.value:business_header_image::VARCHAR, '')
    ) AS image_url
FROM proddb.public.onboarding_hackathon_nux_features,
LATERAL FLATTEN(input => favorite_stores) AS store;
select * from proddb.public.onboarding_hackathon_nux_features where email_consumer_map in ('shivani.poddar@doordash.com', 'danni.liang@doordash.com','saur.vasil@doordash.com','connor.haskins@doordash.com', 'omung.goyal@doordash.com','carissa.sun@doordash.com');

Predominantly plant‐based bowls, salads, sandwiches, and rice dishes, with occasional pizzas and sides

Frequent orders include lunch bowls (burrito bowls, poke bowls), pizza slices, customizable sandwiches, salads and rice/ noodle bowls with varied proteins