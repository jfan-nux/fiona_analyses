"""
Plot: did_reonboarding lift by lifestage
Bar: Treatment volume (users reonboarded)
Line: Lift (percentage points)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Set style
sns.set_style("whitegrid")

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
ORDER BY treatment_reonboarded DESC
"""

# Execute query
hook = SnowflakeHook()
df = hook.query_snowflake(query, method='pandas')
hook.close()

print("Data retrieved:")
print(df)

# Create figure with dual axis
fig, ax1 = plt.subplots(figsize=(12, 7))

# Bar chart: Treatment volume (left y-axis)
x_pos = range(len(df))
bars = ax1.bar(x_pos, df['treatment_reonboarded'], 
               color='steelblue', alpha=0.7, label='Treatment Volume (Users Reonboarded)')
ax1.set_xlabel('Lifestage', fontsize=13, fontweight='bold')
ax1.set_ylabel('Treatment Volume (Users Reonboarded)', fontsize=12, fontweight='bold', color='steelblue')
ax1.tick_params(axis='y', labelcolor='steelblue')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(df['lifestage'], rotation=45, ha='right')

# Add value labels on bars
for i, (bar, val) in enumerate(zip(bars, df['treatment_reonboarded'])):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width() / 2., height,
             f'{val:,.0f}',
             ha='center', va='bottom', fontsize=9, color='steelblue', fontweight='bold')

# Line chart: Treatment reonboarding rate (right y-axis)
ax2 = ax1.twinx()
line = ax2.plot(x_pos, df['treatment_pct'], 
                color='orangered', marker='o', linewidth=3, markersize=10,
                label='Treatment Reonboarding Rate')
ax2.set_ylabel('Treatment Reonboarding Rate (%)', fontsize=12, fontweight='bold', color='orangered')
ax2.tick_params(axis='y', labelcolor='orangered')

# Add value labels on line points
for i, (x, y) in enumerate(zip(x_pos, df['treatment_pct'])):
    ax2.text(x, y + 1, f'{y:.1f}%',
             ha='center', va='bottom', fontsize=9, color='orangered', fontweight='bold')

# Title
plt.title('Did Reonboarding: Treatment Volume & Rate by Lifestage\n(Excluding New Cx & Active)',
          fontsize=14, fontweight='bold', pad=20)

# Legends
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=10)

# Grid
ax1.grid(True, alpha=0.3)
ax1.set_axisbelow(True)

plt.tight_layout()

# Save plot
plot_path = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "cx_ios_reonboarding/plots/user_id_level/"
    "did_reonboarding_lift_by_lifestage.png"
)
plt.savefig(plot_path, dpi=300, bbox_inches='tight')
print(f"\nPlot saved to: {plot_path}")

plt.show()

