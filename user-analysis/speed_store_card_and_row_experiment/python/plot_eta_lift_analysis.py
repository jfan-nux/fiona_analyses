import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
from scipy import stats
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/eta_analysis.sql', 'r') as f:
    query = f.read()

# Execute query using SnowflakeHook
print("Connecting to Snowflake and executing query...")
with SnowflakeHook() as hook:
    df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")

if len(df) == 0:
    print("ERROR: Query returned 0 rows. Check your filters and date range.")
    exit(1)

# Convert numeric columns to float to avoid Decimal type issues
numeric_cols = ['delivery_count_lift_pct', 'consumer_count_lift_pct',
                'quoted_minutes_lift_pct', 'qdt_min_minutes_lift_pct',
                'qdt_max_minutes_lift_pct', 'actual_minutes_lift_pct', 'treatment_deliveries',
                'control_deliveries', 'treatment_consumers', 'control_consumers',
                'quoted_significant', 'qdt_min_significant', 'qdt_max_significant',
                'actual_significant']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Split into daily and overall datasets
df_daily = df[df['metric_type'] == 'daily'].copy()
df_overall = df[df['metric_type'] == 'overall'].iloc[0]

# Convert date for daily data
df_daily['order_date'] = pd.to_datetime(df_daily['order_date'])

# Get overall statistics from the overall dataset
overall_delivery_lift = df_overall['delivery_count_lift_pct']
overall_consumer_lift = df_overall['consumer_count_lift_pct']
overall_quoted_lift = df_overall['quoted_minutes_lift_pct']
overall_qdt_min_lift = df_overall['qdt_min_minutes_lift_pct']
overall_qdt_max_lift = df_overall['qdt_max_minutes_lift_pct']
overall_actual_lift = df_overall['actual_minutes_lift_pct']

# Calculate p-values for overall metrics
treatment_orders = int(df_overall['treatment_deliveries'])
control_orders = int(df_overall['control_deliveries'])
treatment_consumers = int(df_overall['treatment_consumers'])
control_consumers = int(df_overall['control_consumers'])

# Delivery count p-value (Poisson rate test for counts)
# Using normal approximation with Poisson variance assumption (variance = mean)
treatment_rate = treatment_orders / treatment_consumers
control_rate = control_orders / control_consumers

# For Poisson rates, variance = rate/n
var_treatment = treatment_rate / treatment_consumers
var_control = control_rate / control_consumers
se_delivery = np.sqrt(var_treatment + var_control)
z_delivery = (treatment_rate - control_rate) / se_delivery
p_delivery = 2 * (1 - stats.norm.cdf(abs(z_delivery)))

# Time metrics p-values (using conservative std = 30% of mean)
def calc_time_pvalue(treatment_time, control_time, n_treatment, n_control):
    std_treatment = treatment_time * 0.3
    std_control = control_time * 0.3
    se = np.sqrt((std_treatment**2 / n_treatment) + (std_control**2 / n_control))
    z = (treatment_time - control_time) / se
    return 2 * (1 - stats.norm.cdf(abs(z)))

p_quoted = calc_time_pvalue(float(df_overall['treatment_quoted_minutes']), 
                             float(df_overall['control_quoted_minutes']),
                             treatment_orders, control_orders)
p_qdt_min = calc_time_pvalue(float(df_overall['treatment_qdt_min_minutes']), 
                              float(df_overall['control_qdt_min_minutes']),
                              treatment_orders, control_orders)
p_qdt_max = calc_time_pvalue(float(df_overall['treatment_qdt_max_minutes']), 
                              float(df_overall['control_qdt_max_minutes']),
                              treatment_orders, control_orders)
p_actual = calc_time_pvalue(float(df_overall['treatment_actual_minutes']), 
                             float(df_overall['control_actual_minutes']),
                             treatment_orders, control_orders)

# Helper function to format significance
def format_sig(p_value):
    if np.isnan(p_value):
        return "not sig: p=N/A"
    elif p_value < 0.001:
        return f"sig: p<0.001***"
    elif p_value < 0.01:
        return f"sig: p={p_value:.3f}**"
    elif p_value < 0.05:
        return f"sig: p={p_value:.3f}*"
    elif p_value < 0.10:
        return f"marg sig: p={p_value:.3f}"
    else:
        return f"not sig: p={p_value:.3f}"

# Count significant days from daily data
quoted_sig_days = int(df_daily['quoted_significant'].sum())
qdt_min_sig_days = int(df_daily['qdt_min_significant'].sum())
qdt_max_sig_days = int(df_daily['qdt_max_significant'].sum())
actual_sig_days = int(df_daily['actual_significant'].sum())

# Use daily data for plotting
df = df_daily

# Create figure with single plot showing all lift metrics
fig, ax = plt.subplots(1, 1, figsize=(16, 9))
fig.suptitle('Speed Store Card Experiment: Lift Analysis (Treatment vs Control)', fontsize=16, fontweight='bold')

# Plot lift percentages with p-values in legend
ax.plot(df['order_date'], df['delivery_count_lift_pct'], marker='o', 
        label=f'Delivery Count: {overall_delivery_lift:.2f}% ({format_sig(p_delivery)})', 
        linewidth=2, color='#2ca02c')
ax.plot(df['order_date'], df['consumer_count_lift_pct'], marker='*', markersize=10, 
        label=f'Daily Consumers: {overall_consumer_lift:.2f}%', 
        linewidth=2.5, color='#e377c2')
ax.plot(df['order_date'], df['quoted_minutes_lift_pct'], marker='x', 
        label=f'Quoted Time: {overall_quoted_lift:.2f}% ({format_sig(p_quoted)})', 
        linewidth=2, color='#d62728')
ax.plot(df['order_date'], df['qdt_min_minutes_lift_pct'], marker='s', 
        label=f'QDT Min: {overall_qdt_min_lift:.2f}% ({format_sig(p_qdt_min)})', 
        linewidth=2, color='#1f77b4')
ax.plot(df['order_date'], df['qdt_max_minutes_lift_pct'], marker='D', 
        label=f'QDT Max: {overall_qdt_max_lift:.2f}% ({format_sig(p_qdt_max)})', 
        linewidth=2, color='#9467bd')
ax.plot(df['order_date'], df['actual_minutes_lift_pct'], marker='^', 
        label=f'Actual Time: {overall_actual_lift:.2f}% ({format_sig(p_actual)})', 
        linewidth=2, color='#ff7f0e')

# Add horizontal line at 0
ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)

# Mark statistically significant days for quoted time
sig_quoted = df[df['quoted_significant'] == 1]
for _, row in sig_quoted.iterrows():
    y_pos = row['quoted_minutes_lift_pct']
    ax.text(row['order_date'], y_pos + 0.3, '*', fontsize=16, ha='center', va='bottom', color='#d62728')

# Mark statistically significant days for QDT min
sig_qdt_min = df[df['qdt_min_significant'] == 1]
for _, row in sig_qdt_min.iterrows():
    y_pos = row['qdt_min_minutes_lift_pct']
    ax.text(row['order_date'], y_pos + 0.3, '*', fontsize=16, ha='center', va='bottom', color='#1f77b4')

# Mark statistically significant days for QDT max
sig_qdt_max = df[df['qdt_max_significant'] == 1]
for _, row in sig_qdt_max.iterrows():
    y_pos = row['qdt_max_minutes_lift_pct']
    ax.text(row['order_date'], y_pos + 0.3, '*', fontsize=16, ha='center', va='bottom', color='#9467bd')

# Mark statistically significant days for actual time
sig_actual = df[df['actual_significant'] == 1]
for _, row in sig_actual.iterrows():
    y_pos = row['actual_minutes_lift_pct']
    ax.text(row['order_date'], y_pos - 0.3, '*', fontsize=16, ha='center', va='top', color='#ff7f0e')

ax.set_xlabel('Order Date', fontsize=12)
ax.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax.set_title('Percentage Lift Over Time (Negative = Treatment Faster/Better)', fontsize=14)
ax.legend(loc='best')
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('../output/eta_lift_analysis_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/eta_lift_analysis_plot.png")

# Print summary statistics using overall data
print("\n=== Summary Statistics (Overall) ===")
print(f"\nDelivery Count Lift:")
print(f"  Overall: {overall_delivery_lift:.2f}%")
print(f"  Total Treatment Deliveries: {df_overall['treatment_deliveries']:,.0f}")
print(f"  Total Control Deliveries: {df_overall['control_deliveries']:,.0f}")

print(f"\nDaily Consumer Count Lift:")
print(f"  Overall: {overall_consumer_lift:.2f}%")
print(f"  Total Treatment Consumers: {df_overall['treatment_consumers']:,.0f}")
print(f"  Total Control Consumers: {df_overall['control_consumers']:,.0f}")

print(f"\nQuoted Time Lift:")
print(f"  Overall: {overall_quoted_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_quoted_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_quoted_minutes']:.2f} min")
print(f"  Days with significant difference: {quoted_sig_days} out of {len(df)}")

print(f"\nQDT Min Lift:")
print(f"  Overall: {overall_qdt_min_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_qdt_min_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_qdt_min_minutes']:.2f} min")
print(f"  Days with significant difference: {qdt_min_sig_days} out of {len(df)}")

print(f"\nQDT Max Lift:")
print(f"  Overall: {overall_qdt_max_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_qdt_max_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_qdt_max_minutes']:.2f} min")
print(f"  Days with significant difference: {qdt_max_sig_days} out of {len(df)}")

print(f"\nActual Delivery Time Lift:")
print(f"  Overall: {overall_actual_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_actual_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_actual_minutes']:.2f} min")
print(f"  Days with significant difference: {actual_sig_days} out of {len(df)}")

# plt.show()  # Commented out to avoid blocking
