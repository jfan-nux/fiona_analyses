select * from segment_events_raw.consumer_production.order_cart_submit_received
where timestamp>='2025-08-20' and consumer_id is not null limit 10;


CREATE OR REPLACE TABLE proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v6 AS
WITH exposure AS (
  SELECT
    ee.bucket_key::NUMBER AS consumer_id,
    ee.experiment_name,
    ee.experiment_version,
    ee.exposure_time,
    DATE(ee.exposure_time) AS exposure_day_utc,
    custom_attributes
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = 'should_pin_leaderboard_carousel'
    AND ee.experiment_version = 6
    AND ee.tag <>'overridden'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time) BETWEEN '2025-10-03'::date-1 AND CURRENT_DATE
)
SELECT
  e.consumer_id,
  e.experiment_name,
  e.experiment_version,
  e.exposure_time,
  e.exposure_day_utc,
  dc.created_date AS created_at,
  custom_attributes,
  s.lifestage,
  s.lifestage_bucket,
  last_order_date
FROM exposure e
LEFT JOIN edw.growth.consumer_growth_accounting_scd3 s
  ON s.consumer_id = e.consumer_id
 AND e.exposure_day_utc >= s.scd_start_date
 AND (s.scd_end_date IS NULL OR e.exposure_day_utc <= s.scd_end_date)
LEFT JOIN proddb.public.dimension_consumer dc
  ON dc.id = e.consumer_id;
-- GROUP BY 1,2,3

select * from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v6;
select 
-- custom_attributes:deployment, 

--   left(custom_attributes:hostname, length(custom_attributes:hostname) - length(split_part(custom_attributes:hostname, '-', -1)) - 1) AS hostname_part1,
--   -- Take everything before the second-to-last hyphen
--   left(custom_attributes:hostname, length(custom_attributes:hostname) - length(split_part(custom_attributes:hostname, '-', -2)) - length(split_part(custom_attributes:hostname, '-', -1)) - 2) AS hostname_part2,

case when datediff('day',last_order_date, exposure_day_utc)>1 then 'ordered_before_joining'
when  datediff('day',created_at, exposure_day_utc)>1 then 'created_after_joining'
else lifestage end as stages,
avg(case when datediff('day',last_order_date, exposure_day_utc)>0 then datediff('day',last_order_date, exposure_day_utc)
when  datediff('day',created_at, exposure_day_utc)>1 then datediff('day',created_at, exposure_day_utc)
else null end) days_before_joining,
 count(1) cnt
from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v6 group by all order by all;

select *
from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v6
where   datediff('day',last_order_date, exposure_day_utc)>0 
-- datediff('day',last_order_date, exposure_day_utc)=10
group by all
limit 10;


  SELECT

    left(custom_attributes:hostname, length(custom_attributes:hostname) - length(split_part(custom_attributes:hostname, '-', -1)) - 1) AS hostname_part1,
    count(1)
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = 'should_pin_leaderboard_carousel'
    AND ee.experiment_version = 5
    AND ee.tag <>'overridden'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time) >='2025-10-01 11:03:27.534000000'
    group by all
    ;

    select max(CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time)) 
    from proddb.public.fact_dedup_experiment_exposure ee where ee.exposure_time>='2025-09-29' and ee.experiment_name = 'should_pin_leaderboard_carousel';