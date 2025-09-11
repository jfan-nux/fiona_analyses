SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-08-11';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

-- Get experiment exposure data
CREATE OR REPLACE TABLE proddb.fionafan.REONBOARDING_TENURE_STATUS AS (
WITH exposure AS (
    SELECT  
        ee.tag,
        ee.result,
        ee.bucket_key,
        REPLACE(LOWER(CASE WHEN bucket_key LIKE 'dx_%' THEN bucket_key
                     ELSE 'dx_'||bucket_key END), '-') AS dd_device_id_filtered,
        segment,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS exposure_day,
        MIN(CONVERT_TIMEZONE('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)) AS exposure_time,
        max(ee.custom_attributes:userId) as user_id
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
    AND experiment_version::INT = $version
    AND segment = $segment
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY 1,2,3,4,5
),

-- Get actual order history from dimension_deliveries
actual_orders AS (
    SELECT 
        e.dd_device_id_filtered,
        e.user_id,
        e.exposure_day,
        MAX(CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date) AS actual_last_order_date
    FROM exposure e
    LEFT JOIN dimension_deliveries dd
        ON e.dd_device_id_filtered = REPLACE(LOWER(CASE WHEN dd.dd_device_id LIKE 'dx_%' THEN dd.dd_device_id
                                                  ELSE 'dx_'||dd.dd_device_id END), '-')
        AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date < e.exposure_day
        AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', dd.actual_order_place_time)::date >= DATEADD('year', -2, e.exposure_day)
        AND dd.is_filtered_core = TRUE
    GROUP BY 1, 2, 3
),

-- Get user growth accounting data based on exposure date
user_growth_data AS (
    SELECT 
        e.user_id,
        e.exposure_day,
        ugd.consumer_id,
        ugd.signup_date,
        CASE 
            WHEN ugd.last_order_date = '9999-12-31' THEN NULL
            ELSE ugd.last_order_date
        END AS scd_last_order_date,
        ao.actual_last_order_date,
        GREATEST(
            CASE WHEN ugd.last_order_date = '9999-12-31' THEN NULL ELSE ugd.last_order_date END,
            ao.actual_last_order_date
        ) AS final_last_order_date,
        ugd.lifestage,
        ugd.lifestage_bucket,
        ugd.scd_start_date,
        ugd.scd_end_date,
        DATEDIFF('day', ugd.signup_date, e.exposure_day) AS tenure_days,
        CASE 
            WHEN GREATEST(
                CASE WHEN ugd.last_order_date = '9999-12-31' THEN NULL ELSE ugd.last_order_date END,
                ao.actual_last_order_date
            ) IS NULL THEN NULL
            ELSE DATEDIFF('day', GREATEST(
                CASE WHEN ugd.last_order_date = '9999-12-31' THEN NULL ELSE ugd.last_order_date END,
                ao.actual_last_order_date
            ), e.exposure_day)
        END AS days_since_last_order,
        ROW_NUMBER() OVER (PARTITION BY e.user_id ORDER BY ugd.scd_start_date DESC) AS row_rank
    FROM exposure e
    LEFT JOIN actual_orders ao
        ON e.dd_device_id_filtered = ao.dd_device_id_filtered
    LEFT JOIN edw.growth.consumer_growth_accounting_scd3 ugd
        ON e.user_id::string = ugd.consumer_id::string
        AND e.exposure_day BETWEEN ugd.scd_start_date AND ugd.scd_end_date
),

-- Join exposure data with growth accounting data
final_analysis AS (
    SELECT 
        e.tag,
        e.result,
        e.dd_device_id_filtered,
        e.exposure_day,
        e.user_id,
        ugd.consumer_id,
        ugd.signup_date,
        ugd.scd_last_order_date,
        ugd.actual_last_order_date,
        ugd.final_last_order_date,
        ugd.tenure_days,
        ugd.days_since_last_order,
        COALESCE(ugd.lifestage, 'Unknown') AS lifestage,
        COALESCE(ugd.lifestage_bucket, 'Unknown') AS lifestage_bucket,
        CASE 
            WHEN ugd.consumer_id IS NULL THEN 'No Growth Accounting Record'
            WHEN ugd.days_since_last_order IS NULL THEN 'No Orders Ever'
            WHEN ugd.days_since_last_order = 0 THEN 'Today'
            WHEN ugd.days_since_last_order BETWEEN 1 AND 7 THEN '1-7 days ago'
            WHEN ugd.days_since_last_order BETWEEN 8 AND 30 THEN '8-30 days ago'
            WHEN ugd.days_since_last_order BETWEEN 31 AND 90 THEN '31-90 days ago'
            WHEN ugd.days_since_last_order BETWEEN 91 AND 180 THEN '91-180 days ago'
            WHEN ugd.days_since_last_order BETWEEN 181 AND 365 THEN '181-365 days ago'
            WHEN ugd.days_since_last_order > 365 THEN 'More than 365 days ago'
            ELSE 'Other'
        END AS recency_bucket
    FROM exposure e
    LEFT JOIN user_growth_data ugd 
        ON e.user_id::string = ugd.user_id::string
        AND ugd.row_rank = 1
)
select * from final_analysis
);


    
-- Summary by treatment/control and recency buckets
SELECT 
    -- tag,
    -- result,
    recency_bucket,
    -- lifestage,
    -- lifestage_bucket,
    COUNT(*) AS user_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS user_count_pct,
    -- COUNT(DISTINCT dd_device_id_filtered) AS unique_devices,
    AVG(days_since_last_order) AS avg_days_since_last_order,
    MEDIAN(days_since_last_order) AS median_days_since_last_order,
    AVG(tenure_days) AS avg_tenure_days,
    MEDIAN(tenure_days) AS median_tenure_days
FROM proddb.fionafan.REONBOARDING_TENURE_STATUS 
GROUP BY all
ORDER BY 
    CASE recency_bucket
        WHEN 'Today' THEN 1
        WHEN '1-7 days ago' THEN 2
        WHEN '8-30 days ago' THEN 3
        WHEN '31-90 days ago' THEN 4
        WHEN '91-180 days ago' THEN 5
        WHEN '181-365 days ago' THEN 6
        WHEN 'More than 365 days ago' THEN 7
        WHEN 'No Orders Ever' THEN 8
        ELSE 9
    END;



select 
    lifestage, 
    count(1) as cnt,
    count(1) * 100.0 / sum(count(1)) over () as percentage
from proddb.fionafan.REONBOARDING_TENURE_STATUS 
group by all
ORDER BY 
    CASE lifestage
        WHEN 'Active' THEN 1
        WHEN 'Churned' THEN 2
        WHEN 'Dormant' THEN 3
        WHEN 'New Cx' THEN 4
        WHEN 'Non-Purchaser' THEN 5
        WHEN 'Resurrected' THEN 6
        WHEN 'Unknown' THEN 7
        WHEN 'Very Churned' THEN 8
        ELSE 9
    END;

select 
    recency_bucket, 
    count(1) as cnt,
    count(1) * 100.0 / sum(count(1)) over () as percentage
from proddb.fionafan.REONBOARDING_TENURE_STATUS 
group by all
ORDER BY 
    CASE recency_bucket
        WHEN 'Today' THEN 1
        WHEN '1-7 days ago' THEN 2
        WHEN '8-30 days ago' THEN 3
        WHEN '31-90 days ago' THEN 4
        WHEN '91-180 days ago' THEN 5
        WHEN '181-365 days ago' THEN 6
        WHEN 'More than 365 days ago' THEN 7
        WHEN 'No Orders Ever' THEN 8
        ELSE 9
    END;


select 
    recency_bucket, 
    count(1) as cnt,
    count(1) * 100.0 / sum(count(1)) over () as percentage
from proddb.fionafan.REONBOARDING_TENURE_STATUS 
group by all
ORDER BY 
    CASE recency_bucket
        WHEN 'Today' THEN 1
        WHEN '1-7 days ago' THEN 2
        WHEN '8-30 days ago' THEN 3
        WHEN '31-90 days ago' THEN 4
        WHEN '91-180 days ago' THEN 5
        WHEN '181-365 days ago' THEN 6
        WHEN 'More than 365 days ago' THEN 7
        WHEN 'No Orders Ever' THEN 8
        ELSE 9
    END;


select * from proddb.fionafan.REONBOARDING_TENURE_STATUS where recency_bucket = '1-7 days ago';