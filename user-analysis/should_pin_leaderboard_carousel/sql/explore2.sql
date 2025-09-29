select * from segment_events_raw.consumer_production.order_cart_submit_received
where timestamp>='2025-08-20' and consumer_id is not null limit 10;


CREATE OR REPLACE TABLE proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v6 AS
WITH exposure AS (
  SELECT
    ee.bucket_key::NUMBER AS consumer_id,
    ee.experiment_name,
    ee.experiment_version,
    ee.exposure_time,
    DATE(ee.exposure_time) AS exposure_day_utc
  FROM proddb.public.fact_dedup_experiment_exposure ee
  WHERE ee.experiment_name = 'should_pin_leaderboard_carousel'
    AND ee.experiment_version = 6
    AND ee.tag <>'overridden'
    AND CONVERT_TIMEZONE('UTC','America/Los_Angeles', ee.exposure_time) BETWEEN '2025-09-29' AND CURRENT_DATE
)
SELECT
  e.consumer_id,
  e.experiment_name,
  e.experiment_version,
  e.exposure_time,
  e.exposure_day_utc,
  dc.created_date AS created_at,
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

select count(1) from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage;
select case when datediff('day',last_order_date, exposure_day_utc)>0 then 'ordered_before_joining'
when  datediff('day',created_at, exposure_day_utc)>1 then 'created_after_joining'
else lifestage end as stages,
avg(case when datediff('day',last_order_date, exposure_day_utc)>0 then datediff('day',last_order_date, exposure_day_utc)
when  datediff('day',created_at, exposure_day_utc)>1 then datediff('day',created_at, exposure_day_utc)
else null end) days_before_joining,
 count(1)
from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage group by all limit 10;

select exposure_day_utc, count(1) 
from proddb.fionafan.should_pin_leaderboard_carousel_exposures_lifestage_v2 
where   datediff('day',last_order_date, exposure_day_utc)=10 
group by all
limit 10;