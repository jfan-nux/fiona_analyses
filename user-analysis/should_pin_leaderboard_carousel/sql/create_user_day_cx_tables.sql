-- Experiment-Specific CX Funnel Analysis Template
-- This script creates an aggregated funnel view for a specific experiment
-- Only includes post-exposure events for experiment participants

-- INPUT VARIABLES (modify these for each experiment)
SET exp_name = 'should_pin_leaderboard_carousel';
SET start_date = '2025-08-21';
SET end_date = current_date; 
SET version = 1;

  create or replace table proddb.fionafan.should_pin_leaderboard_carousel_experiment_exposures as (
      WITH experiment_exposures AS (
    SELECT  
      ee.tag AS experiment_group,
      ee.bucket_key AS user_id,  -- bucket_key = consumer_id
      MIN(convert_timezone('UTC','America/Los_Angeles',ee.EXPOSURE_TIME)::date) AS first_exposure_date
    FROM proddb.public.fact_dedup_experiment_exposure ee
    WHERE experiment_name = $exp_name
    AND experiment_version = $version
    AND segment = 'Users'
    AND convert_timezone('UTC','America/Los_Angeles',EXPOSURE_TIME) BETWEEN $start_date AND $end_date
    GROUP BY 1, 2
  )
  select * from experiment_exposures

  );

--  select distinct pt.first_event
--  from proddb.public.fact_unique_visitors_full_pt pt

--   INNER JOIN proddb.fionafan.should_pin_leaderboard_carousel_experiment_exposures exp
--     ON pt.user_id = exp.user_id
--     AND pt.event_date >= exp.first_exposure_date  -- Only post-exposure events 
--     group by all;

  -- Create experiment-specific funnel analysis table
CREATE OR REPLACE VIEW proddb.fionafan.cx_funnel_base AS (
  SELECT
    $exp_name AS experiment_name,
    $version AS experiment_version,
    exp.experiment_group,
    pt.event_date,
    DATEDIFF('day', exp.first_exposure_date, pt.event_date) AS days_since_exposure,
    IFF(DATEDIFF('seconds', pt.first_timestamp, pt.last_timestamp) < 10, 1, 0) AS is_bounce,
    pt.urban_type,
    pt.region_name,
    pt.unique_purchaser AS is_purchaser,
    CASE 
      WHEN pt.first_order_date IS NULL THEN 'Not yet ordered'
      WHEN pt.event_date = pt.first_order_date THEN 'Same Day Ordered'
      WHEN pt.first_order_date IS NOT NULL THEN 'Previously Ordered'
      ELSE NULL 
    END AS customer_type,
    pt.is_dashpass,
    pt.platform,
    pt.channel,
    dl.lifestage,
    
    COUNT(DISTINCT pt.dd_device_id) AS app_open,
    COUNT(DISTINCT IFF(pt.unique_store_content_page_visitor = 1, pt.dd_device_id, NULL)) AS store_content_page,
    COUNT(DISTINCT IFF(pt.unique_store_page_visitor = 1, pt.dd_device_id, NULL)) AS store_page_visit,
    COUNT(DISTINCT IFF(pt.action_add_item_visitor = 1, pt.dd_device_id, NULL)) AS add_to_cart,
    COUNT(DISTINCT IFF(pt.unique_order_cart_page_visitor = 1, pt.dd_device_id, NULL)) AS order_cart_page,
    COUNT(DISTINCT IFF(pt.unique_checkout_page_visitor = 1, pt.dd_device_id, NULL)) AS checkout_page,
    COUNT(DISTINCT IFF(pt.action_place_order_visitor = 1, pt.dd_device_id, NULL)) AS placed_order,
    COUNT(DISTINCT IFF(pt.unique_purchaser = 1, pt.dd_device_id, NULL)) AS successful_checkout,
    COUNT(DISTINCT IFF(pt.unique_purchaser_7d = 1, pt.dd_device_id, NULL)) AS successful_checkout_7d,
    COUNT(DISTINCT IFF(pt.event_date = pt.first_order_date, pt.dd_device_id, NULL)) AS new_purchasers,
    COUNT(DISTINCT IFF(pt.event_date >= current_date - 28, pt.dd_device_id, NULL)) AS new_visitors,
    COUNT(DISTINCT IFF(pt.is_dashpass = 1, pt.dd_device_id, NULL)) AS dashpass_users
      
  FROM proddb.public.fact_unique_visitors_full_pt pt
  INNER JOIN proddb.fionafan.should_pin_leaderboard_carousel_experiment_exposures exp
    ON pt.user_id = exp.user_id
    AND pt.event_date >= exp.first_exposure_date
  LEFT JOIN EDW.GROWTH.CX360_MODEL_SNAPSHOT_DLCOPY dl 
    ON dl.consumer_id = pt.user_id
    AND dl.snapshot_date = DATE_TRUNC('week', pt.event_date) - 1
  WHERE pt.event_date BETWEEN $start_date AND $end_date
    AND pt.unique_core_visitor = 1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
);

CREATE OR REPLACE TABLE proddb.fionafan.cx_funnel_experiment_analysis AS (
  SELECT 
    base.*,
    ctrl.app_open AS control_app_open,
    ctrl.store_content_page AS control_store_content_page,
    ctrl.store_page_visit AS control_store_page_visit,
    ctrl.add_to_cart AS control_add_to_cart,
    ctrl.order_cart_page AS control_order_cart_page,
    ctrl.checkout_page AS control_checkout_page,
    ctrl.placed_order AS control_placed_order,
    ctrl.successful_checkout AS control_successful_checkout,
    ctrl.successful_checkout_7d AS control_successful_checkout_7d,
    ctrl.new_purchasers AS control_new_purchasers,
    ctrl.new_visitors AS control_new_visitors,
    ctrl.dashpass_users AS control_dashpass_users
    
  FROM cx_funnel_base base
  LEFT JOIN (SELECT * FROM cx_funnel_base WHERE experiment_group = 'control') ctrl
    ON base.event_date = ctrl.event_date
    AND base.days_since_exposure = ctrl.days_since_exposure
    AND base.is_bounce = ctrl.is_bounce
    AND base.urban_type = ctrl.urban_type
    AND base.region_name = ctrl.region_name
    AND base.is_purchaser = ctrl.is_purchaser
    AND base.customer_type = ctrl.customer_type
    AND base.is_dashpass = ctrl.is_dashpass
    AND base.platform = ctrl.platform
    AND base.channel = ctrl.channel
    AND COALESCE(base.lifestage, 'NULL') = COALESCE(ctrl.lifestage, 'NULL')
    -- where base.experiment_group <>'control'
);

grant select on proddb.fionafan.cx_funnel_experiment_analysis to read_only_users;

select * from proddb.fionafan.cx_funnel_experiment_analysis limit 10;