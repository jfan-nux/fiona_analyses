import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/eta_analysis_by_r2c.sql', 'r') as f:
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
numeric_cols = ['delivery_count_lift_pct', 'consumer_count_lift_pct',
                'quoted_minutes_lift_pct', 'qdt_min_minutes_lift_pct',
                'qdt_max_minutes_lift_pct', 'actual_minutes_lift_pct',
                'treatment_deliveries', 'control_deliveries',
                'treatment_consumers', 'control_consumers',
                'treatment_quoted_minutes', 'control_quoted_minutes',
                'treatment_qdt_min_minutes', 'control_qdt_min_minutes',
                'treatment_qdt_max_minutes', 'control_qdt_max_minutes',
                'treatment_actual_minutes', 'control_actual_minutes',
                'treatment_r2c_duration', 'control_r2c_duration']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Split into daily and overall datasets
df_daily = df[df['metric_type'] == 'daily'].copy()
df_overall = df[df['metric_type'] == 'overall'].copy()

# Convert date for daily data
df_daily['order_date'] = pd.to_datetime(df_daily['order_date'])

# Get R2C buckets in order (using duration-based buckets)
r2c_buckets = ['0-1 miles (~0-2 min)', '1-2 miles (~2-4 min)', '2-3 miles (~4-6 min)', 
               '3-5 miles (~6-10 min)', '5+ miles (10+ min)']
r2c_buckets = [b for b in r2c_buckets if b in df_overall['r2c_bucket'].values]

# Helper function to calculate p-value for time metrics
def calc_time_pvalue(treatment_time, control_time, n_treatment, n_control):
    std_treatment = treatment_time * 0.3
    std_control = control_time * 0.3
    se = np.sqrt((std_treatment**2 / n_treatment) + (std_control**2 / n_control))
    z = (treatment_time - control_time) / se
    return 2 * (1 - stats.norm.cdf(abs(z)))

# Helper function to format significance
def format_sig(p_value):
    if np.isnan(p_value):
        return "N/A"
    elif p_value < 0.001:
        return "p<0.001***"
    elif p_value < 0.01:
        return f"p={p_value:.3f}**"
    elif p_value < 0.05:
        return f"p={p_value:.3f}*"
    else:
        return f"p={p_value:.2f}"

# Calculate p-values and format results for each bucket
bucket_stats = []
for bucket in r2c_buckets:
    bucket_data = df_overall[df_overall['r2c_bucket'] == bucket].iloc[0]
    
    treatment_orders = int(bucket_data['treatment_deliveries'])
    control_orders = int(bucket_data['control_deliveries'])
    
    # Calculate p-values
    p_delivery = calc_time_pvalue(
        float(bucket_data['treatment_deliveries']) / float(bucket_data['treatment_consumers']),
        float(bucket_data['control_deliveries']) / float(bucket_data['control_consumers']),
        int(bucket_data['treatment_consumers']),
        int(bucket_data['control_consumers'])
    )
    
    p_quoted = calc_time_pvalue(
        float(bucket_data['treatment_quoted_minutes']),
        float(bucket_data['control_quoted_minutes']),
        treatment_orders, control_orders
    )
    
    p_actual = calc_time_pvalue(
        float(bucket_data['treatment_actual_minutes']),
        float(bucket_data['control_actual_minutes']),
        treatment_orders, control_orders
    )
    
    bucket_stats.append({
        'bucket': bucket,
        'delivery_lift': bucket_data['delivery_count_lift_pct'],
        'quoted_lift': bucket_data['quoted_minutes_lift_pct'],
        'actual_lift': bucket_data['actual_minutes_lift_pct'],
        'p_delivery': p_delivery,
        'p_quoted': p_quoted,
        'p_actual': p_actual,
        'treatment_orders': treatment_orders,
        'control_orders': control_orders,
        'avg_r2c_duration': bucket_data['treatment_r2c_duration']
    })

stats_df = pd.DataFrame(bucket_stats)

# Create visualization
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('ETA Lift Analysis by R2C Distance (Treatment vs Control)', 
             fontsize=16, fontweight='bold')

# Plot 1: Delivery Count Lift by R2C
ax1 = axes[0, 0]
x_pos = np.arange(len(r2c_buckets))
bars1 = ax1.bar(x_pos, stats_df['delivery_lift'], color='#2ca02c', alpha=0.7)
ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax1.set_xlabel('R2C Distance', fontsize=11)
ax1.set_ylabel('Delivery Count Lift %', fontsize=11)
ax1.set_title('Delivery Count Lift by Distance', fontsize=12, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(r2c_buckets, rotation=45, ha='right')
ax1.grid(True, alpha=0.3, axis='y')

# Add p-value labels on bars
for i, (bar, p_val) in enumerate(zip(bars1, stats_df['p_delivery'])):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height + 0.05 if height > 0 else height - 0.05,
             format_sig(p_val), ha='center', va='bottom' if height > 0 else 'top', fontsize=8)

# Plot 2: Quoted Time Lift by R2C
ax2 = axes[0, 1]
bars2 = ax2.bar(x_pos, stats_df['quoted_lift'], color='#d62728', alpha=0.7)
ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax2.set_xlabel('R2C Distance', fontsize=11)
ax2.set_ylabel('Quoted Time Lift %', fontsize=11)
ax2.set_title('Quoted Time (ETA) Lift by Distance', fontsize=12, fontweight='bold')
ax2.set_xticks(x_pos)
ax2.set_xticklabels(r2c_buckets, rotation=45, ha='right')
ax2.grid(True, alpha=0.3, axis='y')

# Add p-value labels
for i, (bar, p_val) in enumerate(zip(bars2, stats_df['p_quoted'])):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height - 0.02 if height < 0 else height + 0.02,
             format_sig(p_val), ha='center', va='top' if height < 0 else 'bottom', fontsize=8)

# Plot 3: Actual Delivery Time Lift by R2C
ax3 = axes[1, 0]
bars3 = ax3.bar(x_pos, stats_df['actual_lift'], color='#ff7f0e', alpha=0.7)
ax3.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax3.set_xlabel('R2C Distance', fontsize=11)
ax3.set_ylabel('Actual Time Lift %', fontsize=11)
ax3.set_title('Actual Delivery Time Lift by Distance', fontsize=12, fontweight='bold')
ax3.set_xticks(x_pos)
ax3.set_xticklabels(r2c_buckets, rotation=45, ha='right')
ax3.grid(True, alpha=0.3, axis='y')

# Add p-value labels
for i, (bar, p_val) in enumerate(zip(bars3, stats_df['p_actual'])):
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height - 0.02 if height < 0 else height + 0.02,
             format_sig(p_val), ha='center', va='top' if height < 0 else 'bottom', fontsize=8)

# Plot 4: Order Volume by R2C (for context)
ax4 = axes[1, 1]
width = 0.35
bars4a = ax4.bar(x_pos - width/2, stats_df['treatment_orders']/1000, width, 
                 label='Treatment', color='#1f77b4', alpha=0.7)
bars4b = ax4.bar(x_pos + width/2, stats_df['control_orders']/1000, width, 
                 label='Control', color='#ff7f0e', alpha=0.7)
ax4.set_xlabel('R2C Distance', fontsize=11)
ax4.set_ylabel('Order Volume (thousands)', fontsize=11)
ax4.set_title('Order Volume by Distance', fontsize=12, fontweight='bold')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(r2c_buckets, rotation=45, ha='right')
ax4.legend()
ax4.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('../output/eta_by_r2c_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/eta_by_r2c_plot.png")

# Print summary statistics
print("\n" + "="*80)
print("SUMMARY: ETA LIFT BY R2C DISTANCE")
print("="*80)

for _, row in stats_df.iterrows():
    print(f"\n{row['bucket']} (avg R2C: {row['avg_r2c_duration']:.0f}s)")
    print(f"  Orders: {row['treatment_orders']:,} (T) vs {row['control_orders']:,} (C)")
    print(f"  Delivery Count Lift: {row['delivery_lift']:.2f}% ({format_sig(row['p_delivery'])})")
    print(f"  Quoted Time Lift:    {row['quoted_lift']:.2f}% ({format_sig(row['p_quoted'])})")
    print(f"  Actual Time Lift:    {row['actual_lift']:.2f}% ({format_sig(row['p_actual'])})")

print("\n" + "="*80)
print("Significance: ***p<0.001  **p<0.01  *p<0.05")
print("Negative lift = Treatment faster (better)")
print("="*80)

