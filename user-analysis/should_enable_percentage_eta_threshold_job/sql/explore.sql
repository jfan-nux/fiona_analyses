

SET exp_name = 'should_enable_percentage_eta_threshold_job';
SET start_date = '2025-09-10';
SET end_date = current_date; 
SET version = 1;

create or replace table proddb.fionafan.should_enable_percentage_eta_threshold_job_experiment_exposures as (
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
) select * from base_Population 
);


select distinct experiment_version, segment, tag, count(1) 
from proddb.public.fact_dedup_experiment_exposure
where experiment_name = $exp_name
and convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
group by all;



select count(1) from proddb.henryliao.asap_elasticity_score_2025 a
inner join proddb.fionafan.should_enable_percentage_eta_threshold_job_experiment_exposures b
on a.creator_id = b.bucket_key


limit 10;


now I want to export the result for fny_ms2_ios_july_25 from curie to google sheet. I want thje following metrics:
consumers_mau
dsmp_order_rate
gov_per_order_curie
hqdr_ratio,
mx_takehome_pay_7d,
