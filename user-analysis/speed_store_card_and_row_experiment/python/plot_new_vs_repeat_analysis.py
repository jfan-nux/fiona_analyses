import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/new_vs_repeat_analysis.sql', 'r') as f:
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
numeric_cols = [col for col in df.columns if col not in ['order_date', 'purchaser_type', 'metric_type']]
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Split into daily and overall datasets
df_daily = df[df['metric_type'] == 'daily'].copy()
df_overall = df[df['metric_type'] == 'overall'].copy()

# Convert date for daily data
df_daily['order_date'] = pd.to_datetime(df_daily['order_date'])

# Split daily data by purchaser type
df_new = df_daily[df_daily['purchaser_type'] == 'new_purchaser'].copy()
df_repeat = df_daily[df_daily['purchaser_type'] == 'repeat_purchaser'].copy()

# Get overall statistics from the overall dataset
new_overall = df_overall[df_overall['purchaser_type'] == 'new_purchaser'].iloc[0]
new_delivery_lift = new_overall['delivery_lift_pct']
new_consumer_lift = new_overall['consumer_lift_pct']
new_freq_lift = new_overall['order_freq_lift_pct']

repeat_overall = df_overall[df_overall['purchaser_type'] == 'repeat_purchaser'].iloc[0]
repeat_delivery_lift = repeat_overall['delivery_lift_pct']
repeat_consumer_lift = repeat_overall['consumer_lift_pct']
repeat_freq_lift = repeat_overall['order_freq_lift_pct']

# Calculate y-axis limits to use same scale for both plots
all_values = pd.concat([
    df_new[['delivery_lift_pct', 'consumer_lift_pct', 'order_freq_lift_pct']],
    df_repeat[['delivery_lift_pct', 'consumer_lift_pct', 'order_freq_lift_pct']]
])
y_min = all_values.min().min()
y_max = all_values.max().max()
y_margin = (y_max - y_min) * 0.1  # Add 10% margin
y_lim = (y_min - y_margin, y_max + y_margin)

# Create figure with 2 subplots side-by-side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
fig.suptitle('Speed Store Card Experiment: New vs Repeat Purchaser Analysis (Treatment vs Control)', fontsize=16, fontweight='bold')

# Subplot 1: New Purchasers
ax1.plot(df_new['order_date'], df_new['delivery_lift_pct'], marker='o', label=f'Order Count Lift: {new_delivery_lift:.2f}%', linewidth=2.5, color='#2ca02c')
ax1.plot(df_new['order_date'], df_new['consumer_lift_pct'], marker='s', label=f'Consumer Count Lift: {new_consumer_lift:.2f}%', linewidth=2.5, color='#1f77b4')
ax1.plot(df_new['order_date'], df_new['order_freq_lift_pct'], marker='^', label=f'Order Frequency Lift: {new_freq_lift:.2f}%', linewidth=2.5, color='#ff7f0e')

ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax1.set_xlabel('Order Date', fontsize=12)
ax1.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax1.set_title('New Purchasers (First Order During Experiment Period)', fontsize=14, fontweight='bold')
ax1.legend(loc='best', fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
ax1.set_ylim(y_lim)
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Subplot 2: Repeat Purchasers
ax2.plot(df_repeat['order_date'], df_repeat['delivery_lift_pct'], marker='o', label=f'Order Count Lift: {repeat_delivery_lift:.2f}%', linewidth=2.5, color='#2ca02c')
ax2.plot(df_repeat['order_date'], df_repeat['consumer_lift_pct'], marker='s', label=f'Consumer Count Lift: {repeat_consumer_lift:.2f}%', linewidth=2.5, color='#1f77b4')
ax2.plot(df_repeat['order_date'], df_repeat['order_freq_lift_pct'], marker='^', label=f'Order Frequency Lift: {repeat_freq_lift:.2f}%', linewidth=2.5, color='#ff7f0e')

ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax2.set_xlabel('Order Date', fontsize=12)
ax2.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax2.set_title('Repeat Purchasers (Had Previous Order Before This Day)', fontsize=14, fontweight='bold')
ax2.legend(loc='best', fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
ax2.set_ylim(y_lim)
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('../output/new_vs_repeat_analysis_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/new_vs_repeat_analysis_plot.png")

# Print summary statistics using overall data
print("\n=== Summary Statistics (Overall) ===")

print("\n--- NEW PURCHASERS (First Order During Experiment) ---")
print(f"\nOrder Count Lift: {new_delivery_lift:.2f}%")
print(f"  Treatment total: {new_overall['treatment_deliveries']:,.0f}")
print(f"  Control total: {new_overall['control_deliveries']:,.0f}")

print(f"\nConsumer Count Lift: {new_consumer_lift:.2f}%")
print(f"  Treatment total: {new_overall['treatment_consumers']:,.0f}")
print(f"  Control total: {new_overall['control_consumers']:,.0f}")

print(f"\nOrder Frequency Lift: {new_freq_lift:.2f}%")
print(f"  Treatment avg: {new_overall['treatment_order_freq']:.2f}")
print(f"  Control avg: {new_overall['control_order_freq']:.2f}")

print("\n--- REPEAT PURCHASERS (Had Previous Orders) ---")
print(f"\nOrder Count Lift: {repeat_delivery_lift:.2f}%")
print(f"  Treatment total: {repeat_overall['treatment_deliveries']:,.0f}")
print(f"  Control total: {repeat_overall['control_deliveries']:,.0f}")

print(f"\nConsumer Count Lift: {repeat_consumer_lift:.2f}%")
print(f"  Treatment total: {repeat_overall['treatment_consumers']:,.0f}")
print(f"  Control total: {repeat_overall['control_consumers']:,.0f}")

print(f"\nOrder Frequency Lift: {repeat_freq_lift:.2f}%")
print(f"  Treatment avg: {repeat_overall['treatment_order_freq']:.2f}")
print(f"  Control avg: {repeat_overall['control_order_freq']:.2f}")

# Compare new vs repeat
print("\n--- COMPARISON: New vs Repeat ---")
print(f"New Purchaser Order Frequency: {new_overall['treatment_order_freq']:.2f} (treatment), {new_overall['control_order_freq']:.2f} (control)")
print(f"Repeat Purchaser Order Frequency: {repeat_overall['treatment_order_freq']:.2f} (treatment), {repeat_overall['control_order_freq']:.2f} (control)")
new_pct = new_overall['treatment_deliveries'] / (new_overall['treatment_deliveries'] + repeat_overall['treatment_deliveries']) * 100
repeat_pct = repeat_overall['treatment_deliveries'] / (new_overall['treatment_deliveries'] + repeat_overall['treatment_deliveries']) * 100
print(f"New Purchaser % of Total Orders: {new_pct:.2f}% (treatment)")
print(f"Repeat Purchaser % of Total Orders: {repeat_pct:.2f}% (treatment)")

# plt.show()  # Commented out to avoid blocking
