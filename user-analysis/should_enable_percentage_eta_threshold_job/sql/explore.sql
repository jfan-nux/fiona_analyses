

SET exp_name = 'should_enable_percentage_eta_threshold_job';
SET start_date = '2025-09-10';
SET end_date = current_date; 
SET version = 1;

with base_Population as (
SELECT  ee.tag as experiment_group
              , ee.bucket_key
              , MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS day
FROM proddb.public.fact_dedup_experiment_exposure ee
WHERE experiment_name = $exp_name
AND experiment_version = $version
and segment = 'Users'
AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
GROUP BY all
) select * from base_Population ;


select distinct experiment_version, segment, tag, count(1) 
from proddb.public.fact_dedup_experiment_exposure
where experiment_name = $exp_name
and convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
group by all;
