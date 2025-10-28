"""
Plot propensity score quartile distribution by days since onboarding
Creates 4 graphs (one per quartile) showing percentage within cohort over time
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from utils.snowflake_connection import SnowflakeHook

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (20, 12)

# Query to get the data
query = """
SELECT 
    cohort_type, 
    score_quartile,
    DATEDIFF(day, onboarding_day, active_date) AS days_since_onboarding,
    COUNT(1) AS cnt,
    ROUND(100.0 * COUNT(1) / SUM(COUNT(1)) OVER (PARTITION BY cohort_type, DATEDIFF(day, onboarding_day, active_date)), 2) AS pct_within_cohort_day
FROM proddb.fionafan.all_july_cohort_with_scores 
WHERE active_date >= '2025-06-20'
GROUP BY cohort_type, score_quartile, days_since_onboarding
HAVING days_since_onboarding BETWEEN 0 AND 28
ORDER BY cohort_type, days_since_onboarding, score_quartile
"""

print("Executing query...")
hook = SnowflakeHook()
df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")
print(f"Columns: {df.columns.tolist()}")

# Convert column names to uppercase for consistency
df.columns = df.columns.str.upper()

print(f"Cohort types: {df['COHORT_TYPE'].unique()}")
print(f"Quartiles: {sorted(df['SCORE_QUARTILE'].unique())}")

# Create 4 subplots (one for each quartile)
fig, axes = plt.subplots(2, 2, figsize=(20, 12))
axes = axes.flatten()

# Define color palette for cohorts
cohort_types = sorted(df['COHORT_TYPE'].unique())
colors = sns.color_palette("husl", len(cohort_types))
color_map = dict(zip(cohort_types, colors))

# Plot each quartile
for idx, quartile in enumerate(sorted(df['SCORE_QUARTILE'].unique())):
    ax = axes[idx]
    
    # Filter data for this quartile
    quartile_data = df[df['SCORE_QUARTILE'] == quartile].copy()
    
    # Plot each cohort type
    for cohort in cohort_types:
        cohort_data = quartile_data[quartile_data['COHORT_TYPE'] == cohort].copy()
        
        if len(cohort_data) > 0:
            ax.plot(
                cohort_data['DAYS_SINCE_ONBOARDING'],
                cohort_data['PCT_WITHIN_COHORT_DAY'],
                marker='o',
                label=cohort,
                color=color_map[cohort],
                linewidth=2,
                markersize=6,
                alpha=0.7
            )
    
    # Formatting
    ax.set_xlabel('Days Since Onboarding', fontsize=12, fontweight='bold')
    ax.set_ylabel('% of Cohort in This Quartile (by Day)', fontsize=12, fontweight='bold')
    ax.set_title(f'Quartile {int(quartile)} - Propensity Score Distribution', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.legend(title='Cohort Type', loc='best', frameon=True, shadow=True)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 100)
    ax.set_xlim(0, 28)

plt.tight_layout()

# Save plot
output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/notification_audit/plots'
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, 'cohort_quartile_by_days_since_onboarding.png')
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"\nPlot saved to: {output_path}")

# Also save the data for reference
data_output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/notification_audit/outputs'
os.makedirs(data_output_dir, exist_ok=True)
data_output_path = os.path.join(data_output_dir, 'cohort_quartile_by_days_data.csv')
df.to_csv(data_output_path, index=False)
print(f"Data saved to: {data_output_path}")

# Show summary statistics
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)
for quartile in sorted(df['SCORE_QUARTILE'].unique()):
    print(f"\nQuartile {int(quartile)}:")
    quartile_data = df[df['SCORE_QUARTILE'] == quartile]
    for cohort in sorted(df['COHORT_TYPE'].unique()):
        cohort_data = quartile_data[quartile_data['COHORT_TYPE'] == cohort]
        if len(cohort_data) > 0:
            print(f"  {cohort}:")
            print(f"    Days range: {cohort_data['DAYS_SINCE_ONBOARDING'].min():.0f} - {cohort_data['DAYS_SINCE_ONBOARDING'].max():.0f}")
            print(f"    Total count: {cohort_data['CNT'].sum():.0f}")

plt.show()

