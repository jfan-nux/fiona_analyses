SET exp_name = 'cx_ios_reonboarding';
SET start_date = '2025-08-11';
SET end_date = CURRENT_DATE;
SET version = 1;
SET segment = 'Users';

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
        MAX(ee.custom_attributes:userId) AS user_id
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
    AND experiment_version::INT = $version
    AND segment = $segment
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY 1,2,3,4,5
)

SELECT 
    e.tag,
    e.result,
    e.dd_device_id_filtered,
    e.user_id,
    e.exposure_day,
    COUNT(*) AS missing_user_count
FROM exposure e
LEFT JOIN edw.core.dimension_users du 
    ON e.user_id = du.user_id
WHERE du.user_id IS NULL
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3;

select * from edw.growth.consumer_growth_accounting_scd3 where user_id = '1234567890';