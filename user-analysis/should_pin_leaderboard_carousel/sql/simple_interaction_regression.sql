-- Simple Interaction Effect Regression Analysis
-- Focus on users included in both experiments

WITH regression_data AS (
    SELECT 
        consumer_id,
        
        -- Binary treatment indicators (0/1 for regression)
        CASE WHEN exp1_status = 'treatment' THEN 1 ELSE 0 END AS exp1_treat,
        CASE WHEN exp2_status = 'treatment' THEN 1 ELSE 0 END AS exp2_treat,
        
        -- Interaction term (exp1_treat * exp2_treat)
        CASE WHEN exp1_status = 'treatment' AND exp2_status = 'treatment' THEN 1 ELSE 0 END AS interaction,
        
        -- Outcome variables
        orders_count,
        total_variable_profit,
        total_gov,
        ordered,
        explored_after_exposure,
        viewed_store_after_exposure
        
    FROM proddb.fionafan.two_experiment_analysis
    WHERE exp1_status IN ('treatment', 'control')
      AND exp2_status IN ('treatment', 'control')  -- Both experiments
)

-- Regression coefficients using group means
, regression_coefficients AS (
    SELECT 
        'orders_count' AS metric,
        
        -- Intercept (both control)
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS intercept,
        
        -- Main effect of exp1 (exp1_treat coefficient)
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp1_effect,
        
        -- Main effect of exp2 (exp2_treat coefficient)  
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) -
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp2_effect,
        
        -- Interaction effect (interaction coefficient)
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 1) -
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) +
        (SELECT AVG(orders_count) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS interaction_effect
        
    UNION ALL
    
    SELECT 
        'ordered_rate' AS metric,
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS intercept,
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp1_effect,
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) -
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp2_effect,
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 1) -
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) +
        (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS interaction_effect
        
    UNION ALL
    
    SELECT 
        'variable_profit' AS metric,
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS intercept,
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp1_effect,
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) -
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp2_effect,
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 1) -
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) +
        (SELECT AVG(total_variable_profit) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS interaction_effect
        
    UNION ALL
    
    SELECT 
        'explore_rate' AS metric,
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS intercept,
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp1_effect,
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) -
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS exp2_effect,
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 1) -
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 1 AND exp2_treat = 0) -
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 1) +
        (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE exp1_treat = 0 AND exp2_treat = 0) AS interaction_effect
)

-- Display results in regression format
SELECT 
    metric,
    ROUND(intercept, 4) AS baseline_control,
    ROUND(exp1_effect, 4) AS exp1_main_effect,
    ROUND(exp2_effect, 4) AS exp2_main_effect,
    ROUND(interaction_effect, 4) AS interaction_effect,
    
    -- Predicted value for both treatments (intercept + exp1_effect + exp2_effect + interaction_effect)
    ROUND(intercept + exp1_effect + exp2_effect + interaction_effect, 4) AS both_treatment_predicted
FROM regression_coefficients
ORDER BY metric;

-- Summary statistics by treatment group
SELECT 
    '--- SUMMARY BY TREATMENT GROUP ---' AS summary_section,
    NULL AS exp1_treat,
    NULL AS exp2_treat,
    NULL AS sample_size,
    NULL AS avg_orders,
    NULL AS order_rate,
    NULL AS avg_profit,
    NULL AS explore_rate

UNION ALL

SELECT 
    CASE 
        WHEN exp1_treat = 0 AND exp2_treat = 0 THEN 'Both Control'
        WHEN exp1_treat = 1 AND exp2_treat = 0 THEN 'Exp1 Treatment Only'
        WHEN exp1_treat = 0 AND exp2_treat = 1 THEN 'Exp2 Treatment Only'
        WHEN exp1_treat = 1 AND exp2_treat = 1 THEN 'Both Treatment'
    END AS summary_section,
    exp1_treat,
    exp2_treat,
    COUNT(*) AS sample_size,
    ROUND(AVG(orders_count), 4) AS avg_orders,
    ROUND(AVG(ordered::FLOAT), 4) AS order_rate,
    ROUND(AVG(total_variable_profit), 2) AS avg_profit,
    ROUND(AVG(explored_after_exposure::FLOAT), 4) AS explore_rate
FROM regression_data
GROUP BY exp1_treat, exp2_treat
ORDER BY exp1_treat, exp2_treat;
