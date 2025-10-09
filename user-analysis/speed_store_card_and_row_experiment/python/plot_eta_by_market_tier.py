import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from matplotlib.patches import Patch
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/eta_by_market_tier.sql', 'r') as f:
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
numeric_cols = ['treatment_store_percentile', 'control_store_percentile',
                'treatment_avg_store_eta', 'control_avg_store_eta',
                'treatment_deliveries', 'control_deliveries', 'delivery_lift_pct',
                'treatment_consumers', 'control_consumers', 'consumer_lift_pct',
                'treatment_actual_eta', 'control_actual_eta', 'actual_eta_lift_pct',
                'treatment_quoted_eta', 'control_quoted_eta', 'quoted_eta_lift_pct']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Sort by market tier (handle tier ordering properly)
def tier_sort_key(tier):
    """Sort tiers numerically, with Unknown at the end"""
    if tier == 'Unknown' or pd.isna(tier):
        return 999
    try:
        # Extract number from "Tier 1", "Tier 2", etc.
        return int(''.join(filter(str.isdigit, str(tier))))
    except:
        return 999

df['tier_sort'] = df['market_tier'].apply(tier_sort_key)
df = df.sort_values('tier_sort')

# Calculate p-values for delivery count lift
def calc_count_pvalue(treatment_count, control_count):
    """Calculate p-value for count difference using Poisson approximation"""
    if pd.isna(treatment_count) or pd.isna(control_count):
        return np.nan
    pooled_rate = (treatment_count + control_count) / 2
    se = np.sqrt(pooled_rate / 1 + pooled_rate / 1)
    z = abs(treatment_count - control_count) / se if se > 0 else 0
    p_value = 2 * (1 - stats.norm.cdf(z))
    return p_value

# Calculate p-values
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

df['delivery_sig_marker'] = df['delivery_pvalue'].apply(format_sig)
df['consumer_sig_marker'] = df['consumer_pvalue'].apply(format_sig)

# Print summary table
print("\n=== Lift by Market Tier ===")
print("\nDelivery Count Lift:")
print(df[['market_tier', 'treatment_deliveries', 'control_deliveries', 
          'delivery_lift_pct', 'delivery_sig_marker']].to_string(index=False))

print("\n\nConsumer Count Lift:")
print(df[['market_tier', 'treatment_consumers', 'control_consumers',
          'consumer_lift_pct', 'consumer_sig_marker']].to_string(index=False))

print("\n\nStore ETA Performance by Tier:")
print(df[['market_tier', 'treatment_avg_store_eta', 'control_avg_store_eta',
          'treatment_store_percentile']].to_string(index=False))

print("\n\nQuoted ETA Lift by Tier:")
print(df[['market_tier', 'treatment_quoted_eta', 'control_quoted_eta',
          'quoted_eta_lift_pct']].to_string(index=False))

print("\n\nActual ETA Lift by Tier:")
print(df[['market_tier', 'treatment_actual_eta', 'control_actual_eta',
          'actual_eta_lift_pct']].to_string(index=False))

# Create visualization with 2x2 subplots
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Speed Store Card Experiment: Lift by Market Tier (Treatment vs Control)',
             fontsize=16, fontweight='bold')

# Prepare x-axis positions
x_pos = np.arange(len(df))
tiers = df['market_tier'].values

# Plot 1 (Top-Left): Delivery Count Lift
bars1 = ax1.bar(x_pos, df['delivery_lift_pct'], color='#2ca02c', alpha=0.8, edgecolor='black')

# Color significant bars blue
for i, (idx, row) in enumerate(df.iterrows()):
    if row['delivery_pvalue'] < 0.05:
        bars1[i].set_color('#1f77b4')

# Add value labels
for i, (idx, row) in enumerate(df.iterrows()):
    height = row['delivery_lift_pct']
    label = f"{height:.2f}%{row['delivery_sig_marker']}"
    ax1.text(i, height, label, ha='center', 
            va='bottom' if height >= 0 else 'top', fontsize=10, fontweight='bold')

ax1.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax1.set_xlabel('Market Tier', fontsize=11)
ax1.set_ylabel('Lift %', fontsize=11)
ax1.set_title('Delivery Count Lift', fontsize=13, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(tiers, rotation=0)
ax1.grid(True, alpha=0.3, axis='y')

# Add legend
legend_elements = [
    Patch(facecolor='#1f77b4', alpha=0.8, label='Significant (p<0.05)'),
    Patch(facecolor='#2ca02c', alpha=0.8, label='Not Significant')
]
ax1.legend(handles=legend_elements, loc='best', fontsize=9)

# Plot 2 (Top-Right): Market Tier Volume (Total Deliveries)
total_deliveries = df['treatment_deliveries'] + df['control_deliveries']
bars2 = ax2.bar(x_pos, total_deliveries / 1e6, color='#9467bd', alpha=0.8, edgecolor='black')

# Add value labels with percentage of total
total_all = total_deliveries.sum()
for i, (idx, row) in enumerate(df.iterrows()):
    height = total_deliveries.iloc[i] / 1e6
    pct_of_total = (total_deliveries.iloc[i] / total_all) * 100
    label = f"{height:.2f}M\n({pct_of_total:.1f}%)"
    ax2.text(i, height, label, ha='center', va='bottom', fontsize=9, fontweight='bold')

ax2.set_xlabel('Market Tier', fontsize=11)
ax2.set_ylabel('Total Deliveries (Millions)', fontsize=11)
ax2.set_title('Market Tier Volume (Treatment + Control)', fontsize=13, fontweight='bold')
ax2.set_xticks(x_pos)
ax2.set_xticklabels(tiers, rotation=0)
ax2.grid(True, alpha=0.3, axis='y')
ax2.set_ylim(0, ax2.get_ylim()[1] * 1.15)  # Add space for labels

# Plot 3 (Bottom-Left): Quoted ETA Lift
bars3 = ax3.bar(x_pos, df['quoted_eta_lift_pct'], color='#ff7f0e', alpha=0.7, edgecolor='black')

# Color negative values (faster = better) green
for i, (idx, row) in enumerate(df.iterrows()):
    if row['quoted_eta_lift_pct'] < 0:
        bars3[i].set_color('#2ca02c')

# Add value labels with quoted ETA times
for i, (idx, row) in enumerate(df.iterrows()):
    height = row['quoted_eta_lift_pct']
    treatment_eta = row['treatment_quoted_eta']
    control_eta = row['control_quoted_eta']
    label = f"{height:.2f}%\n({treatment_eta:.1f} vs {control_eta:.1f})"
    ax3.text(i, height, label, ha='center', 
            va='bottom' if height >= 0 else 'top', fontsize=9, fontweight='bold')

ax3.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax3.set_xlabel('Market Tier', fontsize=11)
ax3.set_ylabel('Lift %', fontsize=11)
ax3.set_title('Quoted ETA Lift (Negative = Faster)', fontsize=13, fontweight='bold')
ax3.set_xticks(x_pos)
ax3.set_xticklabels(tiers, rotation=0)
ax3.grid(True, alpha=0.3, axis='y')

# Add legend
legend_elements3 = [
    Patch(facecolor='#2ca02c', alpha=0.7, label='Faster (Negative)'),
    Patch(facecolor='#ff7f0e', alpha=0.7, label='Slower (Positive)')
]
ax3.legend(handles=legend_elements3, loc='best', fontsize=9)

# Plot 4 (Bottom-Right): Actual ETA Lift
bars4 = ax4.bar(x_pos, df['actual_eta_lift_pct'], color='#ff7f0e', alpha=0.7, edgecolor='black')

# Color negative values (faster = better) green
for i, (idx, row) in enumerate(df.iterrows()):
    if row['actual_eta_lift_pct'] < 0:
        bars4[i].set_color('#2ca02c')

# Add value labels with actual ETA times
for i, (idx, row) in enumerate(df.iterrows()):
    height = row['actual_eta_lift_pct']
    treatment_eta = row['treatment_actual_eta']
    control_eta = row['control_actual_eta']
    label = f"{height:.2f}%\n({treatment_eta:.1f} vs {control_eta:.1f})"
    ax4.text(i, height, label, ha='center', 
            va='bottom' if height >= 0 else 'top', fontsize=9, fontweight='bold')

ax4.axhline(y=0, color='black', linestyle='-', linewidth=1)
ax4.set_xlabel('Market Tier', fontsize=11)
ax4.set_ylabel('Lift %', fontsize=11)
ax4.set_title('Actual ETA Lift (Negative = Faster)', fontsize=13, fontweight='bold')
ax4.set_xticks(x_pos)
ax4.set_xticklabels(tiers, rotation=0)
ax4.grid(True, alpha=0.3, axis='y')

# Add legend
legend_elements4 = [
    Patch(facecolor='#2ca02c', alpha=0.7, label='Faster (Negative)'),
    Patch(facecolor='#ff7f0e', alpha=0.7, label='Slower (Positive)')
]
ax4.legend(handles=legend_elements4, loc='best', fontsize=9)

plt.tight_layout()
plt.savefig('../output/eta_by_market_tier_plot.png', dpi=300, bbox_inches='tight')
print("\n\nPlot saved to ../output/eta_by_market_tier_plot.png")

# Calculate correlation between store percentile and delivery lift
print("\n=== Correlation Analysis ===")
corr_percentile_delivery = df['treatment_store_percentile'].corr(df['delivery_lift_pct'])
corr_percentile_eta = df['treatment_store_percentile'].corr(df['actual_eta_lift_pct'])

print(f"Correlation between Store ETA Percentile and Delivery Lift: {corr_percentile_delivery:.3f}")
print(f"Correlation between Store ETA Percentile and Actual ETA Lift: {corr_percentile_eta:.3f}")

if not df['treatment_store_percentile'].isna().all():
    print("\nMarket Tier with:")
    best_delivery_tier = df.loc[df['delivery_lift_pct'].idxmax(), 'market_tier']
    worst_delivery_tier = df.loc[df['delivery_lift_pct'].idxmin(), 'market_tier']
    fastest_stores_tier = df.loc[df['treatment_store_percentile'].idxmin(), 'market_tier']
    slowest_stores_tier = df.loc[df['treatment_store_percentile'].idxmax(), 'market_tier']
    
    print(f"  - Best delivery lift: {best_delivery_tier} ({df['delivery_lift_pct'].max():.2f}%)")
    print(f"  - Worst delivery lift: {worst_delivery_tier} ({df['delivery_lift_pct'].min():.2f}%)")
    print(f"  - Fastest stores: {fastest_stores_tier} (percentile: {df['treatment_store_percentile'].min()*100:.1f}%)")
    print(f"  - Slowest stores: {slowest_stores_tier} (percentile: {df['treatment_store_percentile'].max()*100:.1f}%)")

# plt.show()  # Commented out to avoid blocking

