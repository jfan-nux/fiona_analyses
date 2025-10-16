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
                'qdt_max_minutes_lift_pct', 'actual_minutes_lift_pct',
                'r2c_minutes_lift_pct', 'd2c_minutes_lift_pct',
                'create_to_dash_minutes_lift_pct', 'create_to_restaurant_minutes_lift_pct',
                'treatment_deliveries', 'control_deliveries', 
                'treatment_consumers', 'control_consumers',
                'quoted_significant', 'qdt_min_significant', 'qdt_max_significant',
                'actual_significant', 'r2c_significant', 'd2c_significant']
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
overall_r2c_lift = df_overall['r2c_minutes_lift_pct']
overall_d2c_lift = df_overall['d2c_minutes_lift_pct']
overall_create_to_dash_lift = df_overall['create_to_dash_minutes_lift_pct']
overall_create_to_restaurant_lift = df_overall['create_to_restaurant_minutes_lift_pct']

# Calculate p-values for overall metrics
treatment_orders = int(df_overall['treatment_deliveries'])
control_orders = int(df_overall['control_deliveries'])
treatment_consumers = int(df_overall['treatment_consumers'])
control_consumers = int(df_overall['control_consumers'])

# Delivery count p-value (Poisson rate test for counts)
treatment_rate = treatment_orders / treatment_consumers
control_rate = control_orders / control_consumers

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
p_r2c = calc_time_pvalue(float(df_overall['treatment_r2c_minutes']), 
                          float(df_overall['control_r2c_minutes']),
                          treatment_orders, control_orders)
p_d2c = calc_time_pvalue(float(df_overall['treatment_d2c_minutes']), 
                          float(df_overall['control_d2c_minutes']),
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
r2c_sig_days = int(df_daily['r2c_significant'].sum())
d2c_sig_days = int(df_daily['d2c_significant'].sum())

# Use daily data for plotting
df = df_daily

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
fig.suptitle('Speed Store Card Experiment: ETA Analysis - ALL RESTAURANTS (Treatment vs Control)', 
             fontsize=16, fontweight='bold')

# First plot: Delivery/Consumer counts and ETA metrics
ax1.plot(df['order_date'], df['delivery_count_lift_pct'], marker='o', 
        label=f'Delivery Count: {overall_delivery_lift:.2f}% ({format_sig(p_delivery)})', 
        linewidth=2, color='#2ca02c')
ax1.plot(df['order_date'], df['consumer_count_lift_pct'], marker='*', markersize=10, 
        label=f'Daily Consumers: {overall_consumer_lift:.2f}%', 
        linewidth=2.5, color='#e377c2')
ax1.plot(df['order_date'], df['quoted_minutes_lift_pct'], marker='x', 
        label=f'Quoted Time: {overall_quoted_lift:.2f}% ({format_sig(p_quoted)})', 
        linewidth=2, color='#d62728')
ax1.plot(df['order_date'], df['qdt_min_minutes_lift_pct'], marker='s', 
        label=f'QDT Min: {overall_qdt_min_lift:.2f}% ({format_sig(p_qdt_min)})', 
        linewidth=2, color='#1f77b4')
ax1.plot(df['order_date'], df['qdt_max_minutes_lift_pct'], marker='D', 
        label=f'QDT Max: {overall_qdt_max_lift:.2f}% ({format_sig(p_qdt_max)})', 
        linewidth=2, color='#9467bd')
ax1.plot(df['order_date'], df['actual_minutes_lift_pct'], marker='^', 
        label=f'Actual Time: {overall_actual_lift:.2f}% ({format_sig(p_actual)})', 
        linewidth=2, color='#ff7f0e')

ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)

# Mark significant days
sig_quoted = df[df['quoted_significant'] == 1]
for _, row in sig_quoted.iterrows():
    y_pos = row['quoted_minutes_lift_pct']
    ax1.text(row['order_date'], y_pos + 0.3, '*', fontsize=14, ha='center', va='bottom', color='#d62728')

sig_qdt_min = df[df['qdt_min_significant'] == 1]
for _, row in sig_qdt_min.iterrows():
    y_pos = row['qdt_min_minutes_lift_pct']
    ax1.text(row['order_date'], y_pos + 0.3, '*', fontsize=14, ha='center', va='bottom', color='#1f77b4')

sig_qdt_max = df[df['qdt_max_significant'] == 1]
for _, row in sig_qdt_max.iterrows():
    y_pos = row['qdt_max_minutes_lift_pct']
    ax1.text(row['order_date'], y_pos + 0.3, '*', fontsize=14, ha='center', va='bottom', color='#9467bd')

sig_actual = df[df['actual_significant'] == 1]
for _, row in sig_actual.iterrows():
    y_pos = row['actual_minutes_lift_pct']
    ax1.text(row['order_date'], y_pos - 0.3, '*', fontsize=14, ha='center', va='top', color='#ff7f0e')

ax1.set_xlabel('Order Date', fontsize=12)
ax1.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax1.set_title('Delivery Count & ETA Metrics Lift Over Time (Negative = Treatment Faster)', fontsize=13)
ax1.legend(loc='best', fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Second plot: Delivery stages (R2C, D2C)
ax2.plot(df['order_date'], df['r2c_minutes_lift_pct'], marker='o', 
        label=f'R2C (Restaurant→Customer): {overall_r2c_lift:.2f}% ({format_sig(p_r2c)})', 
        linewidth=2, color='#2ca02c')
ax2.plot(df['order_date'], df['d2c_minutes_lift_pct'], marker='s', 
        label=f'D2C (Dasher→Customer): {overall_d2c_lift:.2f}% ({format_sig(p_d2c)})', 
        linewidth=2, color='#8c564b')
ax2.plot(df['order_date'], df['create_to_dash_minutes_lift_pct'], marker='^', 
        label=f'Create→Dasher: {overall_create_to_dash_lift:.2f}%', 
        linewidth=2, color='#e377c2')
ax2.plot(df['order_date'], df['create_to_restaurant_minutes_lift_pct'], marker='v', 
        label=f'Create→Restaurant: {overall_create_to_restaurant_lift:.2f}%', 
        linewidth=2, color='#7f7f7f')

ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)

# Mark significant days for R2C and D2C
sig_r2c = df[df['r2c_significant'] == 1]
for _, row in sig_r2c.iterrows():
    y_pos = row['r2c_minutes_lift_pct']
    ax2.text(row['order_date'], y_pos + 0.3, '*', fontsize=14, ha='center', va='bottom', color='#2ca02c')

sig_d2c = df[df['d2c_significant'] == 1]
for _, row in sig_d2c.iterrows():
    y_pos = row['d2c_minutes_lift_pct']
    ax2.text(row['order_date'], y_pos + 0.3, '*', fontsize=14, ha='center', va='bottom', color='#8c564b')

ax2.set_xlabel('Order Date', fontsize=12)
ax2.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax2.set_title('Delivery Stage Breakdown (Negative = Treatment Faster)', fontsize=13)
ax2.legend(loc='best', fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('../output/eta_analysis_non_fast_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/eta_analysis_non_fast_plot.png")

# Print summary statistics using overall data
print("\n=== Summary Statistics (Overall) - ALL RESTAURANTS ===")
print(f"\nDelivery Count Lift:")
print(f"  Overall: {overall_delivery_lift:.2f}% ({format_sig(p_delivery)})")
print(f"  Total Treatment Deliveries: {df_overall['treatment_deliveries']:,.0f}")
print(f"  Total Control Deliveries: {df_overall['control_deliveries']:,.0f}")

print(f"\nDaily Consumer Count Lift:")
print(f"  Overall: {overall_consumer_lift:.2f}%")
print(f"  Total Treatment Consumers: {df_overall['treatment_consumers']:,.0f}")
print(f"  Total Control Consumers: {df_overall['control_consumers']:,.0f}")

print(f"\n--- ETA METRICS ---")
print(f"\nQuoted Time Lift:")
print(f"  Overall: {overall_quoted_lift:.2f}% ({format_sig(p_quoted)})")
print(f"  Treatment avg: {df_overall['treatment_quoted_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_quoted_minutes']:.2f} min")
print(f"  Days with significant difference: {quoted_sig_days} out of {len(df)}")

print(f"\nQDT Min Lift:")
print(f"  Overall: {overall_qdt_min_lift:.2f}% ({format_sig(p_qdt_min)})")
print(f"  Treatment avg: {df_overall['treatment_qdt_min_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_qdt_min_minutes']:.2f} min")
print(f"  Days with significant difference: {qdt_min_sig_days} out of {len(df)}")

print(f"\nQDT Max Lift:")
print(f"  Overall: {overall_qdt_max_lift:.2f}% ({format_sig(p_qdt_max)})")
print(f"  Treatment avg: {df_overall['treatment_qdt_max_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_qdt_max_minutes']:.2f} min")
print(f"  Days with significant difference: {qdt_max_sig_days} out of {len(df)}")

print(f"\nActual Delivery Time Lift:")
print(f"  Overall: {overall_actual_lift:.2f}% ({format_sig(p_actual)})")
print(f"  Treatment avg: {df_overall['treatment_actual_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_actual_minutes']:.2f} min")
print(f"  Days with significant difference: {actual_sig_days} out of {len(df)}")

print(f"\n--- DELIVERY STAGES ---")
print(f"\nR2C (Restaurant→Customer) Lift:")
print(f"  Overall: {overall_r2c_lift:.2f}% ({format_sig(p_r2c)})")
print(f"  Treatment avg: {df_overall['treatment_r2c_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_r2c_minutes']:.2f} min")
print(f"  Days with significant difference: {r2c_sig_days} out of {len(df)}")

print(f"\nD2C (Dasher Confirmed→Customer) Lift:")
print(f"  Overall: {overall_d2c_lift:.2f}% ({format_sig(p_d2c)})")
print(f"  Treatment avg: {df_overall['treatment_d2c_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_d2c_minutes']:.2f} min")
print(f"  Days with significant difference: {d2c_sig_days} out of {len(df)}")

print(f"\nCreate→Dasher Confirmed Lift:")
print(f"  Overall: {overall_create_to_dash_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_create_to_dash_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_create_to_dash_minutes']:.2f} min")

print(f"\nCreate→Restaurant Pickup Lift:")
print(f"  Overall: {overall_create_to_restaurant_lift:.2f}%")
print(f"  Treatment avg: {df_overall['treatment_create_to_restaurant_minutes']:.2f} min")
print(f"  Control avg: {df_overall['control_create_to_restaurant_minutes']:.2f} min")

# plt.show()  # Commented out to avoid blocking

