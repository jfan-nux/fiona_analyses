import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
from scipy import stats
import numpy as np
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/new_vs_repeat_analysis.sql', 'r') as f:
    query = f.read()

# Execute query using SnowflakeHook
print("Connecting to Snowflake and executing query...")
with SnowflakeHook() as hook:
    df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")

# Convert numeric columns to float
numeric_cols = [col for col in df.columns if col not in ['order_date', 'purchaser_type', 'metric_type']]
for col in numeric_cols:
    if col in df.columns:
        df[col] = df[col].astype(float)

# Get overall data
df_overall = df[df['metric_type'] == 'overall'].copy()

print("\n" + "="*80)
print("STATISTICAL SIGNIFICANCE TEST - New vs Repeat Purchasers")
print("="*80)

for _, row in df_overall.iterrows():
    purchaser_type = row['purchaser_type']
    
    print(f"\n{'='*80}")
    print(f"{purchaser_type.upper().replace('_', ' ')}")
    print(f"{'='*80}")
    
    # Order Count Test
    treatment_orders = int(row['treatment_deliveries'])
    control_orders = int(row['control_deliveries'])
    treatment_consumers = int(row['treatment_consumers'])
    control_consumers = int(row['control_consumers'])
    
    print(f"\n1. ORDER COUNT TEST")
    print(f"   Treatment: {treatment_orders:,} orders from {treatment_consumers:,} consumers")
    print(f"   Control:   {control_orders:,} orders from {control_consumers:,} consumers")
    print(f"   Lift:      {row['delivery_lift_pct']:.3f}%")
    
    # Two-proportion z-test for order rates
    # Order rate = orders / consumers
    treatment_rate = treatment_orders / treatment_consumers
    control_rate = control_orders / control_consumers
    
    # Pooled proportion
    pooled_p = (treatment_orders + control_orders) / (treatment_consumers + control_consumers)
    
    # Standard error
    se = np.sqrt(pooled_p * (1 - pooled_p) * (1/treatment_consumers + 1/control_consumers))
    
    # Z-score
    z_score = (treatment_rate - control_rate) / se
    
    # Two-tailed p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
    
    print(f"   Z-score:   {z_score:.4f}")
    print(f"   P-value:   {p_value:.6f}")
    
    if p_value < 0.001:
        print(f"   Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
    elif p_value < 0.01:
        print(f"   Result:    ** SIGNIFICANT (p < 0.01)")
    elif p_value < 0.05:
        print(f"   Result:    * SIGNIFICANT (p < 0.05)")
    elif p_value < 0.10:
        print(f"   Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
    else:
        print(f"   Result:    NOT SIGNIFICANT (p >= 0.10)")
    
    # Consumer Count Test
    print(f"\n2. CONSUMER COUNT TEST")
    print(f"   Treatment: {treatment_consumers:,} unique consumers")
    print(f"   Control:   {control_consumers:,} unique consumers")
    print(f"   Lift:      {row['consumer_lift_pct']:.3f}%")
    
    # For consumer counts, we need exposure data (total eligible users)
    # Since we don't have that directly, we'll note this limitation
    print(f"   Note: Full significance test requires exposure data")
    print(f"         Difference: {treatment_consumers - control_consumers:,} consumers")
    
    # Order Frequency Test
    treatment_freq = row['treatment_order_freq']
    control_freq = row['control_order_freq']
    
    print(f"\n3. ORDER FREQUENCY TEST")
    print(f"   Treatment: {treatment_freq:.4f} orders per consumer")
    print(f"   Control:   {control_freq:.4f} orders per consumer")
    print(f"   Lift:      {row['order_freq_lift_pct']:.3f}%")
    
    # For order frequency, approximate using t-test assumptions
    # Variance approximation: var = mean (Poisson-like)
    treatment_var = treatment_freq / treatment_consumers
    control_var = control_freq / control_consumers
    
    se_freq = np.sqrt(treatment_var + control_var)
    z_freq = (treatment_freq - control_freq) / se_freq
    p_value_freq = 2 * (1 - stats.norm.cdf(abs(z_freq)))
    
    print(f"   Z-score:   {z_freq:.4f}")
    print(f"   P-value:   {p_value_freq:.6f}")
    
    if p_value_freq < 0.001:
        print(f"   Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
    elif p_value_freq < 0.01:
        print(f"   Result:    ** SIGNIFICANT (p < 0.01)")
    elif p_value_freq < 0.05:
        print(f"   Result:    * SIGNIFICANT (p < 0.05)")
    elif p_value_freq < 0.10:
        print(f"   Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
    else:
        print(f"   Result:    NOT SIGNIFICANT (p >= 0.10)")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("\nSignificance Levels:")
print("  *** p < 0.001 (Highly Significant)")
print("  **  p < 0.01  (Significant)")
print("  *   p < 0.05  (Significant)")
print("      p < 0.10  (Marginally Significant)")
print("      p >= 0.10 (Not Significant)")
print("\nNote: Order count test uses two-proportion z-test with large sample sizes.")
print("      Order frequency test uses approximate variance based on Poisson assumptions.")
print("="*80)
