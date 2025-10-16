import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/lift_by_speed_type_pct.sql', 'r') as f:
    query = f.read()

# Execute query using SnowflakeHook
print("Connecting to Snowflake and executing query...")
with SnowflakeHook() as hook:
    df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")

if len(df) == 0:
    print("ERROR: Query returned 0 rows. Check your filters and date range.")
    exit(1)

# Convert numeric columns to float
numeric_cols = ['speed_type_pct_rounded', 'treatment_deliveries', 'control_deliveries',
                'delivery_lift_pct', 'treatment_consumers', 'control_consumers',
                'consumer_lift_pct']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Sort by speed_type_pct
df = df.sort_values('speed_type_pct_rounded')

# Filter to cap x-axis at < 0.3
df = df[df['speed_type_pct_rounded'] < 0.3].copy()

# Calculate p-values for each point
def calc_count_pvalue(treatment_count, control_count):
    """Calculate p-value for count difference using Poisson approximation"""
    if pd.isna(treatment_count) or pd.isna(control_count):
        return np.nan
    pooled_rate = (treatment_count + control_count) / 2
    se = np.sqrt(pooled_rate / 1 + pooled_rate / 1)
    z = abs(treatment_count - control_count) / se if se > 0 else 0
    p_value = 2 * (1 - stats.norm.cdf(z))
    return p_value

df['delivery_pvalue'] = df.apply(
    lambda row: calc_count_pvalue(row['treatment_deliveries'], row['control_deliveries']), 
    axis=1
)

# Print summary table
print("\n=== Delivery Count Lift by Speed Type Percentage ===")
print("(Speed Type % = Percentage of time store was tagged as FASTEST)")
print("(Rounded to 2 decimal places: 0.01, 0.02, 0.03, etc.)")
print("\nData Summary:")
print(df[['speed_type_pct_rounded', 'treatment_deliveries', 'control_deliveries', 
          'delivery_lift_pct']].to_string(index=False))

# Calculate weighted average lift
total_treatment = df['treatment_deliveries'].sum()
total_control = df['control_deliveries'].sum()
overall_lift = ((total_treatment - total_control) / total_control * 100)
print(f"\n\nOverall Delivery Lift (Weighted): {overall_lift:.2f}%")
print(f"Total Treatment Deliveries: {total_treatment:,.0f}")
print(f"Total Control Deliveries: {total_control:,.0f}")

# Create visualization
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
fig.suptitle('Speed Store Card Experiment: Order Lift by Speed Type Percentage\n(Stores Tagged as FASTEST - Rounded to 2 decimal places)',
             fontsize=16, fontweight='bold')

# Plot 1: Delivery Count Lift Line Plot
ax1.plot(df['speed_type_pct_rounded'], df['delivery_lift_pct'], 
         marker='o', linewidth=2.5, markersize=8, color='#2ca02c', label='Delivery Lift %')

# Mark significant points
significant_points = df[df['delivery_pvalue'] < 0.05]
if len(significant_points) > 0:
    ax1.scatter(significant_points['speed_type_pct_rounded'], 
               significant_points['delivery_lift_pct'],
               s=200, color='#1f77b4', marker='o', 
               label='Significant (p<0.05)', zorder=5, alpha=0.7)

# Add horizontal line at 0
ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.7)

# Add overall lift line
ax1.axhline(y=overall_lift, color='red', linestyle=':', linewidth=2, 
           label=f'Overall Lift: {overall_lift:.2f}%', alpha=0.7)

ax1.set_xlabel('Speed Type Percentage (% of time tagged as FASTEST)', fontsize=12)
ax1.set_ylabel('Delivery Count Lift %', fontsize=12)
ax1.set_title('Delivery Count Lift by Speed Type Percentage (< 30%)', fontsize=13, fontweight='bold')
ax1.legend(loc='best', fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.set_xlim(-0.01, 0.30)

# Add value labels for all points
for i, (idx, row) in enumerate(df.iterrows()):
    ax1.annotate(f"{row['delivery_lift_pct']:.2f}%",
                xy=(row['speed_type_pct_rounded'], row['delivery_lift_pct']),
                xytext=(0, 10), textcoords='offset points',
                ha='center', fontsize=8, alpha=0.8, fontweight='bold')

# Plot 2: Volume by Speed Type Percentage
total_deliveries = df['treatment_deliveries'] + df['control_deliveries']
ax2.bar(df['speed_type_pct_rounded'], total_deliveries / 1000, 
       width=0.008, color='#9467bd', alpha=0.7, edgecolor='black')

ax2.set_xlabel('Speed Type Percentage (% of time tagged as FASTEST)', fontsize=12)
ax2.set_ylabel('Total Deliveries (Thousands)', fontsize=12)
ax2.set_title('Experiment Volume by Speed Type Percentage', fontsize=13, fontweight='bold')
ax2.grid(True, alpha=0.3, axis='y')

# Add volume annotation
total_volume = total_deliveries.sum()
ax2.text(0.98, 0.95, f'Total Volume: {total_volume/1e6:.2f}M deliveries',
        transform=ax2.transAxes, ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
        fontsize=10)

plt.tight_layout()
plt.savefig('../output/lift_by_speed_type_pct_plot.png', dpi=300, bbox_inches='tight')
print("\n\nPlot saved to ../output/lift_by_speed_type_pct_plot.png")

# Calculate correlation
correlation = df['speed_type_pct_rounded'].corr(df['delivery_lift_pct'])
print(f"\n=== Correlation Analysis ===")
print(f"Correlation between Speed Type % and Delivery Lift: {correlation:.3f}")

if correlation > 0.3:
    print("→ Positive correlation: Higher speed % shows higher lift")
elif correlation < -0.3:
    print("→ Negative correlation: Higher speed % shows lower lift")
else:
    print("→ Weak correlation: Lift relatively consistent across speed %")

# Print stats summary
print("\n=== Summary by Speed Type Percentage ===")
print(f"Total bins: {len(df)}")
print(f"Min speed %: {df['speed_type_pct_rounded'].min():.1f}")
print(f"Max speed %: {df['speed_type_pct_rounded'].max():.1f}")
print(f"\nTop 3 bins by volume:")
top_volume = df.nlargest(3, 'treatment_deliveries')[['speed_type_pct_rounded', 'treatment_deliveries', 
                                                      'control_deliveries', 'delivery_lift_pct']]
print(top_volume.to_string(index=False))

# plt.show()  # Commented out to avoid blocking

