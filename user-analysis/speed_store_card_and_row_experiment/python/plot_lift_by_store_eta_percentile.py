import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/lift_by_store_eta_percentile.sql', 'r') as f:
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
numeric_cols = ['eta_percentile_decile', 'avg_store_eta_minutes', 
                'treatment_deliveries', 'control_deliveries',
                'delivery_diff', 'delivery_lift_pct',
                'treatment_consumers', 'control_consumers',
                'consumer_diff', 'consumer_lift_pct']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Calculate p-values for each percentile bucket
def calc_count_pvalue(treatment_count, control_count):
    """Calculate p-value for count difference using Poisson approximation"""
    if pd.isna(treatment_count) or pd.isna(control_count):
        return np.nan
    # Using normal approximation for Poisson
    pooled_rate = (treatment_count + control_count) / 2
    se = np.sqrt(pooled_rate / 1 + pooled_rate / 1)  # Simplified SE
    z = abs(treatment_count - control_count) / se if se > 0 else 0
    p_value = 2 * (1 - stats.norm.cdf(z))
    return p_value

# Calculate p-values for each row
df['delivery_pvalue'] = df.apply(
    lambda row: calc_count_pvalue(row['treatment_deliveries'], row['control_deliveries']), 
    axis=1
)
df['consumer_pvalue'] = df.apply(
    lambda row: calc_count_pvalue(row['treatment_consumers'], row['control_consumers']), 
    axis=1
)

# Helper function to format significance
def format_sig(p_value):
    if np.isnan(p_value):
        return ""
    elif p_value < 0.001:
        return "***"
    elif p_value < 0.01:
        return "**"
    elif p_value < 0.05:
        return "*"
    else:
        return ""

# Add significance markers
df['delivery_sig_marker'] = df['delivery_pvalue'].apply(format_sig)
df['consumer_sig_marker'] = df['consumer_pvalue'].apply(format_sig)

# Print summary table
print("\n=== Lift by Store Historical ETA Percentile ===")
print("\nNote: Percentiles based on pre-experiment historical ETA (Aug 22 - Sep 22)")
print("\nDelivery Count Lift:")
print(df[['eta_percentile_bucket', 'eta_percentile_decile', 'avg_store_eta_minutes',
          'treatment_deliveries', 'control_deliveries', 'delivery_lift_pct', 
          'delivery_sig_marker']].to_string(index=False))

print("\n\nConsumer Count Lift:")
print(df[['eta_percentile_bucket', 'eta_percentile_decile', 'avg_store_eta_minutes',
          'treatment_consumers', 'control_consumers', 'consumer_lift_pct',
          'consumer_sig_marker']].to_string(index=False))

# Create visualization
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
fig.suptitle('Speed Store Card Experiment: Lift by Store Historical ETA Percentile\n(Treatment vs Control)',
             fontsize=16, fontweight='bold')

# Sort by decile for plotting
df_sorted = df.sort_values('eta_percentile_decile')

# First plot: Delivery count lift
x_pos = df_sorted['eta_percentile_decile']
delivery_lift = df_sorted['delivery_lift_pct']

bars1 = ax1.bar(x_pos, delivery_lift, color='#2ca02c', alpha=0.7, edgecolor='black', linewidth=1)

# Color bars by significance
for i, (idx, row) in enumerate(df_sorted.iterrows()):
    if row['delivery_pvalue'] < 0.05:
        bars1[i].set_color('#1f77b4')  # Blue for significant
        bars1[i].set_alpha(0.8)
    
# Add value labels on bars
for i, (idx, row) in enumerate(df_sorted.iterrows()):
    height = row['delivery_lift_pct']
    label = f"{height:.2f}%{row['delivery_sig_marker']}"
    ax1.text(row['eta_percentile_decile'], height, label,
            ha='center', va='bottom' if height >= 0 else 'top',
            fontsize=9, fontweight='bold')

ax1.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax1.set_xlabel('Store ETA Percentile Decile (1=Fastest, 10=Slowest)', fontsize=12)
ax1.set_ylabel('Delivery Count Lift %', fontsize=12)
ax1.set_title('Delivery Count Lift by Store Historical ETA Performance', fontsize=13)
ax1.grid(True, alpha=0.3, axis='y')
ax1.set_xticks(range(1, 11))

# Add legend
from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='#1f77b4', alpha=0.8, label='Significant (p<0.05)'),
    Patch(facecolor='#2ca02c', alpha=0.7, label='Not Significant')
]
ax1.legend(handles=legend_elements, loc='best')

# Second plot: Consumer count lift
consumer_lift = df_sorted['consumer_lift_pct']

bars2 = ax2.bar(x_pos, consumer_lift, color='#e377c2', alpha=0.7, edgecolor='black', linewidth=1)

# Color bars by significance
for i, (idx, row) in enumerate(df_sorted.iterrows()):
    if row['consumer_pvalue'] < 0.05:
        bars2[i].set_color('#ff7f0e')  # Orange for significant
        bars2[i].set_alpha(0.8)

# Add value labels on bars
for i, (idx, row) in enumerate(df_sorted.iterrows()):
    height = row['consumer_lift_pct']
    label = f"{height:.2f}%{row['consumer_sig_marker']}"
    ax2.text(row['eta_percentile_decile'], height, label,
            ha='center', va='bottom' if height >= 0 else 'top',
            fontsize=9, fontweight='bold')

ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax2.set_xlabel('Store ETA Percentile Decile (1=Fastest, 10=Slowest)', fontsize=12)
ax2.set_ylabel('Consumer Count Lift %', fontsize=12)
ax2.set_title('Consumer Count Lift by Store Historical ETA Performance', fontsize=13)
ax2.grid(True, alpha=0.3, axis='y')
ax2.set_xticks(range(1, 11))

# Add legend
legend_elements2 = [
    Patch(facecolor='#ff7f0e', alpha=0.8, label='Significant (p<0.05)'),
    Patch(facecolor='#e377c2', alpha=0.7, label='Not Significant')
]
ax2.legend(handles=legend_elements2, loc='best')

# Add text annotation with avg ETA ranges
eta_range_text = f"Decile 1 (Fastest): ~{df_sorted.iloc[0]['avg_store_eta_minutes']:.1f} min avg ETA\n"
eta_range_text += f"Decile 10 (Slowest): ~{df_sorted.iloc[-1]['avg_store_eta_minutes']:.1f} min avg ETA"
fig.text(0.5, 0.02, eta_range_text, ha='center', fontsize=10, style='italic')

plt.tight_layout(rect=[0, 0.03, 1, 0.97])
plt.savefig('../output/lift_by_store_eta_percentile_plot.png', dpi=300, bbox_inches='tight')
print("\n\nPlot saved to ../output/lift_by_store_eta_percentile_plot.png")

# Calculate overall correlation
print("\n=== Correlation Analysis ===")
corr_delivery = df['eta_percentile_decile'].corr(df['delivery_lift_pct'])
corr_consumer = df['eta_percentile_decile'].corr(df['consumer_lift_pct'])

print(f"Correlation between ETA percentile and Delivery Lift: {corr_delivery:.3f}")
print(f"Correlation between ETA percentile and Consumer Lift: {corr_consumer:.3f}")

if corr_delivery > 0.3:
    print("→ Positive correlation: Slower stores (higher percentile) show higher lift")
elif corr_delivery < -0.3:
    print("→ Negative correlation: Faster stores (lower percentile) show higher lift")
else:
    print("→ Weak correlation: Lift relatively consistent across ETA performance")

# plt.show()  # Commented out to avoid blocking

