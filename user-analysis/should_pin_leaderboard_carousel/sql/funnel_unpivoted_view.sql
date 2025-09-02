-- Unpivoted Funnel View - Convert wide format to long format
-- Each funnel stage becomes a separate row for easier visualization and analysis

CREATE OR REPLACE VIEW proddb.fionafan.cx_funnel_experiment_unpivoted AS (
  
  SELECT 
    experiment_name,
    experiment_version,
    experiment_group,
    event_date,
    days_since_exposure,
    is_bounce,
    urban_type,
    region_name,
    customer_type,
    is_dashpass,
    platform,
    channel,
    lifestage,
    
    'app_open' AS funnel_stage,
    1 AS stage_order,
    app_open AS stage_count,
    control_app_open AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'store_content_page' AS funnel_stage,
    2 AS stage_order,
    store_content_page AS stage_count,
    control_store_content_page AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'store_page_visit' AS funnel_stage,
    3 AS stage_order,
    store_page_visit AS stage_count,
    control_store_page_visit AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'add_to_cart' AS funnel_stage,
    4 AS stage_order,
    add_to_cart AS stage_count,
    control_add_to_cart AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'order_cart_page' AS funnel_stage,
    5 AS stage_order,
    order_cart_page AS stage_count,
    control_order_cart_page AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'checkout_page' AS funnel_stage,
    6 AS stage_order,
    checkout_page AS stage_count,
    control_checkout_page AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'placed_order' AS funnel_stage,
    7 AS stage_order,
    placed_order AS stage_count,
    control_placed_order AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'successful_checkout' AS funnel_stage,
    8 AS stage_order,
    successful_checkout AS stage_count,
    control_successful_checkout AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis
  
  UNION ALL
  
  SELECT 
    experiment_name, experiment_version, experiment_group, event_date, days_since_exposure,
    is_bounce, urban_type, region_name, customer_type, is_dashpass, platform, channel, lifestage,
    'successful_checkout_7d' AS funnel_stage,
    9 AS stage_order,
    successful_checkout_7d AS stage_count,
    control_successful_checkout_7d AS control_stage_count
  FROM proddb.fionafan.cx_funnel_experiment_analysis



);

-- Grant permissions
GRANT SELECT ON proddb.fionafan.cx_funnel_experiment_unpivoted TO read_only_users;

-- Sample queries to use the unpivoted view with control_stage_count column

-- 1. Side-by-side comparison of all users vs control-only for each stage
/*
SELECT 
  experiment_group,
  funnel_stage,
  stage_order,
  SUM(stage_count) AS total_users,
  SUM(control_stage_count) AS control_users,
  SUM(stage_count) - SUM(control_stage_count) AS treatment_users,
  DIV0(SUM(stage_count) - SUM(control_stage_count), SUM(control_stage_count)) AS treatment_lift
FROM proddb.fionafan.cx_funnel_experiment_unpivoted
GROUP BY 1,2,3
ORDER BY experiment_group, stage_order;
*/

-- 2. Control funnel progression and conversion rates
/*
SELECT 
  funnel_stage,
  stage_order,
  SUM(control_stage_count) AS control_users,
  LAG(SUM(control_stage_count)) OVER (ORDER BY stage_order) AS previous_stage_control_users,
  DIV0(SUM(control_stage_count), previous_stage_control_users) AS control_stage_conversion_rate
FROM proddb.fionafan.cx_funnel_experiment_unpivoted
GROUP BY 1,2
ORDER BY stage_order;
*/

-- 3. Treatment impact analysis by day
/*
SELECT 
  event_date,
  funnel_stage,
  SUM(stage_count) AS total_users,
  SUM(control_stage_count) AS control_users,
  SUM(stage_count) - SUM(control_stage_count) AS treatment_impact,
  DIV0(treatment_impact, control_users) AS daily_lift
FROM proddb.fionafan.cx_funnel_experiment_unpivoted
WHERE funnel_stage = 'successful_checkout'
GROUP BY 1,2
ORDER BY event_date;
*/

-- 4. Time-based analysis with days since exposure
/*
SELECT 
  days_since_exposure,
  funnel_stage,
  SUM(stage_count) AS total_users,
  SUM(control_stage_count) AS control_users,
  DIV0(SUM(stage_count), SUM(control_stage_count)) AS total_vs_control_ratio
FROM proddb.fionafan.cx_funnel_experiment_unpivoted
WHERE days_since_exposure BETWEEN 0 AND 7
  AND funnel_stage IN ('app_open', 'successful_checkout')
GROUP BY 1,2
ORDER BY days_since_exposure, stage_order;
*/
