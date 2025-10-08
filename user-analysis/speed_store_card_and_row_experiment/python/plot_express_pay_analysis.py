import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/express_pay_analysis.sql', 'r') as f:
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

# Convert numeric columns to float to avoid Decimal type issues
numeric_cols = [col for col in df.columns if col != 'order_date']
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Calculate overall statistics
overall_express_delivery_pct_diff = df['express_delivery_pct_diff'].mean()
overall_express_consumer_pct_diff = df['express_consumer_pct_diff'].mean()
overall_express_user_order_lift = df['express_user_order_lift_pct'].mean()
overall_express_user_active_lift = df['express_user_active_lift_pct'].mean()
overall_express_user_freq_lift = df['express_user_freq_lift_pct'].mean()

# Create figure with 2 subplots
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
fig.suptitle('Speed Store Card Experiment: Express Pay (Speed Sensitive) Analysis (Treatment vs Control)', fontsize=16, fontweight='bold')

# Subplot 1: Express Pay Adoption (percentage point differences)
ax1.plot(df['order_date'], df['express_delivery_pct_diff'], marker='o', label=f'% Deliveries w/ Express Pay (pp diff): {overall_express_delivery_pct_diff:.2f}pp', linewidth=2.5, color='#1f77b4')
ax1.plot(df['order_date'], df['express_consumer_pct_diff'], marker='s', label=f'% Consumers Using Express Pay (pp diff): {overall_express_consumer_pct_diff:.2f}pp', linewidth=2.5, color='#ff7f0e')

ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax1.set_ylabel('Percentage Point Difference (Treatment - Control)', fontsize=12)
ax1.set_title('Express Pay Adoption Rates (All Users)', fontsize=14, fontweight='bold')
ax1.legend(loc='best', fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Subplot 2: Express Pay User Cohort Performance (lift %)
ax2.plot(df['order_date'], df['express_user_order_lift_pct'], marker='o', label=f'Order Count Lift: {overall_express_user_order_lift:.2f}%', linewidth=2.5, color='#2ca02c')
ax2.plot(df['order_date'], df['express_user_active_lift_pct'], marker='s', label=f'Active Consumers Lift: {overall_express_user_active_lift:.2f}%', linewidth=2.5, color='#d62728')
ax2.plot(df['order_date'], df['express_user_freq_lift_pct'], marker='^', label=f'Order Frequency Lift: {overall_express_user_freq_lift:.2f}%', linewidth=2.5, color='#9467bd')

ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
ax2.set_xlabel('Order Date', fontsize=12)
ax2.set_ylabel('Lift % (Treatment - Control) / Control', fontsize=12)
ax2.set_title('Express Pay User Cohort Behavior (Users Who Used Express Pay ≥1x)', fontsize=14, fontweight='bold')
ax2.legend(loc='best', fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('../output/express_pay_analysis_plot.png', dpi=300, bbox_inches='tight')
print("Plot saved to ../output/express_pay_analysis_plot.png")

# Print summary statistics
print("\n=== Summary Statistics ===")

print("\n--- EXPRESS PAY ADOPTION (All Users) ---")
print(f"\n% Deliveries with Express Pay:")
print(f"  Treatment avg: {df['treatment_pct_deliveries_express'].mean():.2f}%")
print(f"  Control avg: {df['control_pct_deliveries_express'].mean():.2f}%")
print(f"  Difference: {overall_express_delivery_pct_diff:.2f} percentage points")

print(f"\n% Consumers using Express Pay:")
print(f"  Treatment avg: {df['treatment_pct_consumers_express'].mean():.2f}%")
print(f"  Control avg: {df['control_pct_consumers_express'].mean():.2f}%")
print(f"  Difference: {overall_express_consumer_pct_diff:.2f} percentage points")

print("\n--- EXPRESS PAY USER COHORT BEHAVIOR ---")
print(f"\nOrder Count (from express pay users):")
print(f"  Treatment total: {df['treatment_express_user_orders'].sum():,.0f}")
print(f"  Control total: {df['control_express_user_orders'].sum():,.0f}")
print(f"  Lift: {overall_express_user_order_lift:.2f}%")

print(f"\nActive Consumers (express pay users who ordered):")
print(f"  Treatment avg daily: {df['treatment_express_user_active'].mean():,.0f}")
print(f"  Control avg daily: {df['control_express_user_active'].mean():,.0f}")
print(f"  Lift: {overall_express_user_active_lift:.2f}%")

print(f"\nOrder Frequency (orders per active consumer):")
print(f"  Treatment avg: {df['treatment_express_user_freq'].mean():.2f}")
print(f"  Control avg: {df['control_express_user_freq'].mean():.2f}")
print(f"  Lift: {overall_express_user_freq_lift:.2f}%")

# plt.show()  # Commented out to avoid blocking
