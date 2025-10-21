-- Analysis: did_reonboarding lift by lifestage
-- Comparing treatment (result = 'true') vs control (result = 'false') groups
-- Shows the percentage of users who saw the reonboarding notification by lifestage

WITH reonboarding_by_lifestage AS (
    SELECT
        lifestage,
        result,
        COUNT(DISTINCT user_id) AS total_users,
        SUM(did_reonboarding) AS users_reonboarded,
        AVG(did_reonboarding) * 100.0 AS did_reonboarding_pct
    FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
    WHERE lifestage IS NOT NULL
    GROUP BY ALL
),

lift_calculation AS (
    SELECT
        lifestage,
        MAX(CASE WHEN result = 'false' THEN total_users END) AS control_users,
        MAX(CASE WHEN result = 'false' THEN users_reonboarded END) AS control_reonboarded,
        MAX(CASE WHEN result = 'false' THEN did_reonboarding_pct END) AS control_pct,
        MAX(CASE WHEN result = 'true' THEN total_users END) AS treatment_users,
        MAX(CASE WHEN result = 'true' THEN users_reonboarded END) AS treatment_reonboarded,
        MAX(CASE WHEN result = 'true' THEN did_reonboarding_pct END) AS treatment_pct
    FROM reonboarding_by_lifestage
    GROUP BY lifestage
)

SELECT
    lifestage,
    control_users,
    control_reonboarded,
    ROUND(control_pct, 2) AS control_pct,
    treatment_users,
    treatment_reonboarded,
    ROUND(treatment_pct, 2) AS treatment_pct,
    ROUND(treatment_pct - control_pct, 2) AS absolute_lift_pp
FROM lift_calculation
WHERE control_pct IS NOT NULL 
    AND treatment_pct IS NOT NULL
    AND lifestage NOT IN ('New Cx', 'Active')
ORDER BY treatment_pct DESC;

-- Overall summary (excluding New Cx and Active)
SELECT
    'Overall (excl New Cx & Active)' AS lifestage,
    COUNT(DISTINCT CASE WHEN result = 'false' THEN user_id END) AS control_users,
    SUM(CASE WHEN result = 'false' THEN did_reonboarding END) AS control_reonboarded,
    ROUND(AVG(CASE WHEN result = 'false' THEN did_reonboarding END) * 100, 2) AS control_pct,
    COUNT(DISTINCT CASE WHEN result = 'true' THEN user_id END) AS treatment_users,
    SUM(CASE WHEN result = 'true' THEN did_reonboarding END) AS treatment_reonboarded,
    ROUND(AVG(CASE WHEN result = 'true' THEN did_reonboarding END) * 100, 2) AS treatment_pct,
    ROUND(AVG(CASE WHEN result = 'true' THEN did_reonboarding END) * 100 - 
          AVG(CASE WHEN result = 'false' THEN did_reonboarding END) * 100, 2) AS absolute_lift_pp
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
WHERE lifestage IS NOT NULL
    AND lifestage NOT IN ('New Cx', 'Active');

