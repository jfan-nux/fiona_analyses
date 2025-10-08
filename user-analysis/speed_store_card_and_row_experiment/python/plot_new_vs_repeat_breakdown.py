import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/new_vs_repeat_breakdown.sql', 'r') as f:
    query = f.read()

# Execute query using SnowflakeHook
print("Connecting to Snowflake and executing query...")
with SnowflakeHook() as hook:
    df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")

if len(df) == 0:
    print("ERROR: Query returned 0 rows. Check your filters and date range.")
    exit(1)

df['order_date'] = pd.to_datetime(df['order_date'])

# Convert numeric columns to float
numeric_cols = [col for col in df.columns if col not in ['order_date', 'purchaser_type']]
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Split data by purchaser type
df_new = df[df['purchaser_type'] == 'new_purchaser'].copy()
df_repeat = df[df['purchaser_type'] == 'repeat_purchaser'].copy()

# Create figure with 3 subplots
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 16))
fig.suptitle('Speed Store Card Experiment: New vs Repeat Purchaser Daily Breakdown', fontsize=16, fontweight='bold')

# Subplot 1: Volume breakdown (percentage of daily orders)
ax1.plot(df_new['order_date'], df_new['treatment_pct_of_deliveries'], marker='o', label='New Purchasers (Treatment)', linewidth=2.5, color='#1f77b4')
ax1.plot(df_new['order_date'], df_new['control_pct_of_deliveries'], marker='o', linestyle='--', label='New Purchasers (Control)', linewidth=2, color='#1f77b4', alpha=0.6)
ax1.plot(df_repeat['order_date'], df_repeat['treatment_pct_of_deliveries'], marker='s', label='Repeat Purchasers (Treatment)', linewidth=2.5, color='#ff7f0e')
ax1.plot(df_repeat['order_date'], df_repeat['control_pct_of_deliveries'], marker='s', linestyle='--', label='Repeat Purchasers (Control)', linewidth=2, color='#ff7f0e', alpha=0.6)

ax1.set_ylabel('% of Daily Orders', fontsize=12)
ax1.set_title('Daily Mix: New vs Repeat Purchasers', fontsize=14, fontweight='bold')
ax1.legend(loc='best', fontsize=9)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Subplot 2: Treatment group absolute volumes
ax2.bar(df_new['order_date'], df_new['treatment_deliveries'], alpha=0.7, label='New Purchasers', width=0.7, color='#1f77b4')
ax2.bar(df_repeat['order_date'], df_repeat['treatment_deliveries'], bottom=df_new['treatment_deliveries'], alpha=0.7, label='Repeat Purchasers', width=0.7, color='#ff7f0e')

ax2.set_ylabel('Number of Orders', fontsize=12)
ax2.set_title('Daily Order Volume Breakdown - TREATMENT GROUP', fontsize=14, fontweight='bold')
ax2.legend(loc='best', fontsize=10)
ax2.grid(True, alpha=0.3, axis='y')
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Subplot 3: Control group absolute volumes
ax3.bar(df_new['order_date'], df_new['control_deliveries'], alpha=0.7, label='New Purchasers', width=0.7, color='#1f77b4')
ax3.bar(df_repeat['order_date'], df_repeat['control_deliveries'], bottom=df_new['control_deliveries'], alpha=0.7, label='Repeat Purchasers', width=0.7, color='#ff7f0e')

ax3.set_xlabel('Order Date', fontsize=12)
ax3.set_ylabel('Number of Orders', fontsize=12)
ax3.set_title('Daily Order Volume Breakdown - CONTROL GROUP', fontsize=14, fontweight='bold')
ax3.legend(loc='best', fontsize=10)
ax3.grid(True, alpha=0.3, axis='y')
ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('../output/new_vs_repeat_breakdown_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/new_vs_repeat_breakdown_plot.png")

# Print summary statistics
print("\n=== Daily Breakdown Summary ===")

print("\n--- TREATMENT GROUP ---")
print(f"New Purchasers:")
print(f"  Avg % of daily orders: {df_new['treatment_pct_of_deliveries'].mean():.2f}%")
print(f"  Avg daily volume: {df_new['treatment_deliveries'].mean():,.0f} orders")
print(f"  Total: {df_new['treatment_deliveries'].sum():,.0f} orders")

print(f"\nRepeat Purchasers:")
print(f"  Avg % of daily orders: {df_repeat['treatment_pct_of_deliveries'].mean():.2f}%")
print(f"  Avg daily volume: {df_repeat['treatment_deliveries'].mean():,.0f} orders")
print(f"  Total: {df_repeat['treatment_deliveries'].sum():,.0f} orders")

print("\n--- CONTROL GROUP ---")
print(f"New Purchasers:")
print(f"  Avg % of daily orders: {df_new['control_pct_of_deliveries'].mean():.2f}%")
print(f"  Avg daily volume: {df_new['control_deliveries'].mean():,.0f} orders")
print(f"  Total: {df_new['control_deliveries'].sum():,.0f} orders")

print(f"\nRepeat Purchasers:")
print(f"  Avg % of daily orders: {df_repeat['control_pct_of_deliveries'].mean():.2f}%")
print(f"  Avg daily volume: {df_repeat['control_deliveries'].mean():,.0f} orders")
print(f"  Total: {df_repeat['control_deliveries'].sum():,.0f} orders")

print("\n--- LIFT COMPARISON ---")
print(f"New Purchasers Avg Lift: {df_new['delivery_lift_pct'].mean():.2f}%")
print(f"Repeat Purchasers Avg Lift: {df_repeat['delivery_lift_pct'].mean():.2f}%")

# plt.show()  # Commented out to avoid blocking
