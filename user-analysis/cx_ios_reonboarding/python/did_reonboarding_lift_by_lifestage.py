"""
Analysis: did_reonboarding lift by lifestage
Shows treatment vs control lift in reonboarding completion rates
Excludes: New Cx and Active lifestages
"""

import pandas as pd
import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Query the data
query = """
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
ORDER BY treatment_pct DESC
"""

# Execute query
hook = SnowflakeHook()
df = hook.query_snowflake(query, method='pandas')
hook.close()

print("=" * 100)
print("DID_REONBOARDING LIFT BY LIFESTAGE (Excluding New Cx & Active)")
print("=" * 100)
print(df.to_string(index=False))
print("\n")

# Calculate overall stats
total_control = df['control_users'].sum()
total_control_reonboarded = df['control_reonboarded'].sum()
total_treatment = df['treatment_users'].sum()
total_treatment_reonboarded = df['treatment_reonboarded'].sum()

overall_control_pct = (total_control_reonboarded / total_control * 100) if total_control > 0 else 0
overall_treatment_pct = (total_treatment_reonboarded / total_treatment * 100) if total_treatment > 0 else 0
overall_lift = overall_treatment_pct - overall_control_pct

print("=" * 100)
print("OVERALL SUMMARY (Excluding New Cx & Active)")
print("=" * 100)
print(f"Control: {total_control:,} users, {total_control_reonboarded:,} reonboarded ({overall_control_pct:.2f}%)")
print(f"Treatment: {total_treatment:,} users, {total_treatment_reonboarded:,} reonboarded ({overall_treatment_pct:.2f}%)")
print(f"Lift: +{overall_lift:.2f}pp")
print("\n")

# Save to file
output_path = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "cx_ios_reonboarding/outputs/user_id_level/"
    "did_reonboarding_lift_by_lifestage.txt"
)
with open(output_path, 'w') as f:
    f.write("=" * 100 + "\n")
    f.write("DID_REONBOARDING LIFT BY LIFESTAGE (Excluding New Cx & Active)\n")
    f.write("=" * 100 + "\n\n")
    f.write(df.to_string(index=False))
    f.write("\n\n")
    f.write("=" * 100 + "\n")
    f.write("OVERALL SUMMARY (Excluding New Cx & Active)\n")
    f.write("=" * 100 + "\n")
    f.write(f"Control: {total_control:,} users, {total_control_reonboarded:,} reonboarded ({overall_control_pct:.2f}%)\n")
    f.write(f"Treatment: {total_treatment:,} users, {total_treatment_reonboarded:,} reonboarded ({overall_treatment_pct:.2f}%)\n")
    f.write(f"Lift: +{overall_lift:.2f}pp\n")

print(f"Results saved to: {output_path}")
