-- Interaction Effect Analysis: Regression approach for users in both experiments
-- Looking at the interaction effect of being in treatment for both experiments

-- First, create regression variables and analyze interaction effects
WITH regression_data AS (
    SELECT 
        consumer_id,
        exp1_status,
        exp2_status,
        
        -- Create binary treatment indicators
        CASE WHEN exp1_status = 'treatment' THEN 1 ELSE 0 END AS exp1_treatment,
        CASE WHEN exp2_status = 'treatment' THEN 1 ELSE 0 END AS exp2_treatment,
        
        -- Create interaction term
        CASE WHEN exp1_status = 'treatment' AND exp2_status = 'treatment' THEN 1 ELSE 0 END AS interaction_term,
        
        -- Treatment combination groups
        CASE 
            WHEN exp1_status = 'control' AND exp2_status = 'control' THEN 'both_control'
            WHEN exp1_status = 'treatment' AND exp2_status = 'control' THEN 'exp1_only'
            WHEN exp1_status = 'control' AND exp2_status = 'treatment' THEN 'exp2_only'
            WHEN exp1_status = 'treatment' AND exp2_status = 'treatment' THEN 'both_treatment'
            ELSE 'other'
        END AS treatment_group,
        
        -- Outcome variables
        orders_count,
        total_variable_profit,
        total_gov,
        ordered,
        new_customer,
        is_mau,
        explored_after_exposure,
        viewed_store_after_exposure,
        viewed_cart_after_exposure,
        viewed_checkout_after_exposure
        
    FROM proddb.fionafan.two_experiment_analysis
    WHERE exp1_status IN ('treatment', 'control')
      AND exp2_status IN ('treatment', 'control')  -- Only users in both experiments
)

-- Regression Analysis: Average Treatment Effects by Group
SELECT 
    '=== INTERACTION EFFECT REGRESSION ANALYSIS ===' AS analysis_section,
    NULL::FLOAT AS treatment_group,
    NULL::FLOAT AS sample_size,
    NULL::FLOAT AS orders_count_avg,
    NULL::FLOAT AS ordered_rate,
    NULL::FLOAT AS variable_profit_avg,
    NULL::FLOAT AS gov_avg,
    NULL::FLOAT AS explore_rate,
    NULL::FLOAT AS store_conversion_rate

UNION ALL

-- Group-level averages (this gives us the regression coefficients)
SELECT 
    treatment_group AS analysis_section,
    COUNT(*) AS sample_size,
    AVG(orders_count) AS orders_count_avg,
    AVG(ordered::FLOAT) AS ordered_rate,
    AVG(total_variable_profit) AS variable_profit_avg,
    AVG(total_gov) AS gov_avg,
    AVG(explored_after_exposure::FLOAT) AS explore_rate,
    CASE 
        WHEN SUM(explored_after_exposure) > 0 
        THEN AVG(viewed_store_after_exposure::FLOAT) / AVG(explored_after_exposure::FLOAT)
        ELSE 0 
    END AS store_conversion_rate
FROM regression_data
GROUP BY treatment_group

UNION ALL

-- Calculate interaction effects manually (difference-in-differences approach)
SELECT 
    '=== INTERACTION EFFECTS (Difference-in-Differences) ===' AS analysis_section,
    NULL AS sample_size,
    NULL AS orders_count_avg,
    NULL AS ordered_rate,
    NULL AS variable_profit_avg,
    NULL AS gov_avg,
    NULL AS explore_rate,
    NULL AS store_conversion_rate

UNION ALL

SELECT 
    'Orders Count - Interaction Effect' AS analysis_section,
    NULL AS sample_size,
    -- Interaction effect: (Both_treatment - Exp1_only) - (Exp2_only - Both_control)
    (SELECT AVG(orders_count) FROM regression_data WHERE treatment_group = 'both_treatment') -
    (SELECT AVG(orders_count) FROM regression_data WHERE treatment_group = 'exp1_only') -
    (SELECT AVG(orders_count) FROM regression_data WHERE treatment_group = 'exp2_only') +
    (SELECT AVG(orders_count) FROM regression_data WHERE treatment_group = 'both_control') AS orders_count_avg,
    
    (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_treatment') -
    (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp1_only') -
    (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp2_only') +
    (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_control') AS ordered_rate,
    
    (SELECT AVG(total_variable_profit) FROM regression_data WHERE treatment_group = 'both_treatment') -
    (SELECT AVG(total_variable_profit) FROM regression_data WHERE treatment_group = 'exp1_only') -
    (SELECT AVG(total_variable_profit) FROM regression_data WHERE treatment_group = 'exp2_only') +
    (SELECT AVG(total_variable_profit) FROM regression_data WHERE treatment_group = 'both_control') AS variable_profit_avg,
    
    (SELECT AVG(total_gov) FROM regression_data WHERE treatment_group = 'both_treatment') -
    (SELECT AVG(total_gov) FROM regression_data WHERE treatment_group = 'exp1_only') -
    (SELECT AVG(total_gov) FROM regression_data WHERE treatment_group = 'exp2_only') +
    (SELECT AVG(total_gov) FROM regression_data WHERE treatment_group = 'both_control') AS gov_avg,
    
    (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE treatment_group = 'both_treatment') -
    (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE treatment_group = 'exp1_only') -
    (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE treatment_group = 'exp2_only') +
    (SELECT AVG(explored_after_exposure::FLOAT) FROM regression_data WHERE treatment_group = 'both_control') AS explore_rate,
    
    NULL AS store_conversion_rate

UNION ALL

-- Statistical significance test using t-test approximation
SELECT 
    '=== STATISTICAL SIGNIFICANCE (t-test approximation) ===' AS analysis_section,
    NULL AS sample_size,
    NULL AS orders_count_avg,
    NULL AS ordered_rate,
    NULL AS variable_profit_avg,
    NULL AS gov_avg,
    NULL AS explore_rate,
    NULL AS store_conversion_rate

UNION ALL

-- T-statistics for ordered rate
SELECT 
    'Ordered Rate - t-statistic' AS analysis_section,
    NULL AS sample_size,
    NULL AS orders_count_avg,
    
    -- t-statistic calculation for ordered rate
    CASE 
        WHEN (
            (SELECT STDDEV(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_treatment') +
            (SELECT STDDEV(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp1_only') +
            (SELECT STDDEV(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp2_only') +
            (SELECT STDDEV(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_control')
        ) > 0 THEN
            (
                (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_treatment') -
                (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp1_only') -
                (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp2_only') +
                (SELECT AVG(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_control')
            ) / SQRT(
                (SELECT VAR_POP(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_treatment') / (SELECT COUNT(*) FROM regression_data WHERE treatment_group = 'both_treatment') +
                (SELECT VAR_POP(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp1_only') / (SELECT COUNT(*) FROM regression_data WHERE treatment_group = 'exp1_only') +
                (SELECT VAR_POP(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'exp2_only') / (SELECT COUNT(*) FROM regression_data WHERE treatment_group = 'exp2_only') +
                (SELECT VAR_POP(ordered::FLOAT) FROM regression_data WHERE treatment_group = 'both_control') / (SELECT COUNT(*) FROM regression_data WHERE treatment_group = 'both_control')
            )
        ELSE NULL
    END AS ordered_rate,
    
    NULL AS variable_profit_avg,
    NULL AS gov_avg,
    NULL AS explore_rate,
    NULL AS store_conversion_rate

ORDER BY 
    CASE 
        WHEN analysis_section = '=== INTERACTION EFFECT REGRESSION ANALYSIS ===' THEN 1
        WHEN analysis_section = 'both_control' THEN 2
        WHEN analysis_section = 'exp1_only' THEN 3
        WHEN analysis_section = 'exp2_only' THEN 4
        WHEN analysis_section = 'both_treatment' THEN 5
        WHEN analysis_section = '=== INTERACTION EFFECTS (Difference-in-Differences) ===' THEN 6
        WHEN analysis_section = 'Orders Count - Interaction Effect' THEN 7
        WHEN analysis_section = '=== STATISTICAL SIGNIFICANCE (t-test approximation) ===' THEN 8
        WHEN analysis_section = 'Ordered Rate - t-statistic' THEN 9
        ELSE 10
    END;
