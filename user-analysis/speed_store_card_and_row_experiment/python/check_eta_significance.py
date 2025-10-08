import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
from scipy import stats
import numpy as np
from utils.snowflake_connection import SnowflakeHook

# Read SQL query from file
with open('../sql/eta_analysis.sql', 'r') as f:
    query = f.read()

# Execute query using SnowflakeHook
print("Connecting to Snowflake and executing query...")
with SnowflakeHook() as hook:
    df = hook.query_snowflake(query, method='pandas')

print(f"Retrieved {len(df)} rows")

# Convert numeric columns to float
numeric_cols = ['delivery_count_lift_pct', 'consumer_count_lift_pct',
                'quoted_minutes_lift_pct', 'qdt_min_minutes_lift_pct',
                'qdt_max_minutes_lift_pct', 'actual_minutes_lift_pct', 
                'treatment_deliveries', 'control_deliveries', 
                'treatment_consumers', 'control_consumers',
                'treatment_quoted_minutes', 'control_quoted_minutes',
                'treatment_qdt_min_minutes', 'control_qdt_min_minutes',
                'treatment_qdt_max_minutes', 'control_qdt_max_minutes',
                'treatment_actual_minutes', 'control_actual_minutes']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Get overall data
df_overall = df[df['metric_type'] == 'overall'].iloc[0]

print("\n" + "="*80)
print("STATISTICAL SIGNIFICANCE TEST - ETA ANALYSIS")
print("="*80)

# 1. Delivery Count Test
print(f"\n{'='*80}")
print(f"1. DELIVERY COUNT TEST")
print(f"{'='*80}")

treatment_orders = int(df_overall['treatment_deliveries'])
control_orders = int(df_overall['control_deliveries'])
treatment_consumers = int(df_overall['treatment_consumers'])
control_consumers = int(df_overall['control_consumers'])

print(f"Treatment: {treatment_orders:,} orders from {treatment_consumers:,} daily active consumers")
print(f"Control:   {control_orders:,} orders from {control_consumers:,} daily active consumers")
print(f"Lift:      {df_overall['delivery_count_lift_pct']:.3f}%")

# Two-proportion z-test for order rates
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

print(f"Z-score:   {z_score:.4f}")
print(f"P-value:   {p_value:.6f}")

if p_value < 0.001:
    print(f"Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
elif p_value < 0.01:
    print(f"Result:    ** SIGNIFICANT (p < 0.01)")
elif p_value < 0.05:
    print(f"Result:    * SIGNIFICANT (p < 0.05)")
elif p_value < 0.10:
    print(f"Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
else:
    print(f"Result:    NOT SIGNIFICANT (p >= 0.10)")

# 2. Quoted Time Test
print(f"\n{'='*80}")
print(f"2. QUOTED TIME (ETA) TEST")
print(f"{'='*80}")

treatment_quoted = df_overall['treatment_quoted_minutes']
control_quoted = df_overall['control_quoted_minutes']

print(f"Treatment: {treatment_quoted:.2f} minutes average")
print(f"Control:   {control_quoted:.2f} minutes average")
print(f"Difference: {treatment_quoted - control_quoted:.2f} minutes")
print(f"Lift:      {df_overall['quoted_minutes_lift_pct']:.3f}%")

# For time metrics, we use t-test approximation
# Sample sizes are the delivery counts
n_treatment = treatment_orders
n_control = control_orders

# Assuming standard deviation is roughly proportional to mean (common for time data)
# Conservative estimate: std = mean * 0.3 (30% coefficient of variation)
std_treatment = treatment_quoted * 0.3
std_control = control_quoted * 0.3

# Standard error of difference
se_time = np.sqrt((std_treatment**2 / n_treatment) + (std_control**2 / n_control))

# Z-score (with large n, t-distribution approximates normal)
z_time = (treatment_quoted - control_quoted) / se_time

# Two-tailed p-value
p_value_time = 2 * (1 - stats.norm.cdf(abs(z_time)))

print(f"Assumed std: Treatment={std_treatment:.2f} min, Control={std_control:.2f} min")
print(f"Z-score:   {z_time:.4f}")
print(f"P-value:   {p_value_time:.6f}")

if p_value_time < 0.001:
    print(f"Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
elif p_value_time < 0.01:
    print(f"Result:    ** SIGNIFICANT (p < 0.01)")
elif p_value_time < 0.05:
    print(f"Result:    * SIGNIFICANT (p < 0.05)")
elif p_value_time < 0.10:
    print(f"Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
else:
    print(f"Result:    NOT SIGNIFICANT (p >= 0.10)")

# 3. QDT Min Time Test
print(f"\n{'='*80}")
print(f"3. QDT MIN TIME TEST")
print(f"{'='*80}")

treatment_qdt_min = df_overall['treatment_qdt_min_minutes']
control_qdt_min = df_overall['control_qdt_min_minutes']

print(f"Treatment: {treatment_qdt_min:.2f} minutes average")
print(f"Control:   {control_qdt_min:.2f} minutes average")
print(f"Difference: {treatment_qdt_min - control_qdt_min:.2f} minutes")
print(f"Lift:      {df_overall['qdt_min_minutes_lift_pct']:.3f}%")

std_treatment_qdt = treatment_qdt_min * 0.3
std_control_qdt = control_qdt_min * 0.3

se_qdt_min = np.sqrt((std_treatment_qdt**2 / n_treatment) + (std_control_qdt**2 / n_control))
z_qdt_min = (treatment_qdt_min - control_qdt_min) / se_qdt_min
p_value_qdt_min = 2 * (1 - stats.norm.cdf(abs(z_qdt_min)))

print(f"Z-score:   {z_qdt_min:.4f}")
print(f"P-value:   {p_value_qdt_min:.6f}")

if p_value_qdt_min < 0.001:
    print(f"Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
elif p_value_qdt_min < 0.01:
    print(f"Result:    ** SIGNIFICANT (p < 0.01)")
elif p_value_qdt_min < 0.05:
    print(f"Result:    * SIGNIFICANT (p < 0.05)")
elif p_value_qdt_min < 0.10:
    print(f"Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
else:
    print(f"Result:    NOT SIGNIFICANT (p >= 0.10)")

# 4. QDT Max Time Test
print(f"\n{'='*80}")
print(f"4. QDT MAX TIME TEST")
print(f"{'='*80}")

treatment_qdt_max = df_overall['treatment_qdt_max_minutes']
control_qdt_max = df_overall['control_qdt_max_minutes']

print(f"Treatment: {treatment_qdt_max:.2f} minutes average")
print(f"Control:   {control_qdt_max:.2f} minutes average")
print(f"Difference: {treatment_qdt_max - control_qdt_max:.2f} minutes")
print(f"Lift:      {df_overall['qdt_max_minutes_lift_pct']:.3f}%")

std_treatment_qdt_max = treatment_qdt_max * 0.3
std_control_qdt_max = control_qdt_max * 0.3

se_qdt_max = np.sqrt((std_treatment_qdt_max**2 / n_treatment) + (std_control_qdt_max**2 / n_control))
z_qdt_max = (treatment_qdt_max - control_qdt_max) / se_qdt_max
p_value_qdt_max = 2 * (1 - stats.norm.cdf(abs(z_qdt_max)))

print(f"Z-score:   {z_qdt_max:.4f}")
print(f"P-value:   {p_value_qdt_max:.6f}")

if p_value_qdt_max < 0.001:
    print(f"Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
elif p_value_qdt_max < 0.01:
    print(f"Result:    ** SIGNIFICANT (p < 0.01)")
elif p_value_qdt_max < 0.05:
    print(f"Result:    * SIGNIFICANT (p < 0.05)")
elif p_value_qdt_max < 0.10:
    print(f"Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
else:
    print(f"Result:    NOT SIGNIFICANT (p >= 0.10)")

# 5. Actual Delivery Time Test
print(f"\n{'='*80}")
print(f"5. ACTUAL DELIVERY TIME TEST")
print(f"{'='*80}")

treatment_actual = df_overall['treatment_actual_minutes']
control_actual = df_overall['control_actual_minutes']

print(f"Treatment: {treatment_actual:.2f} minutes average")
print(f"Control:   {control_actual:.2f} minutes average")
print(f"Difference: {treatment_actual - control_actual:.2f} minutes")
print(f"Lift:      {df_overall['actual_minutes_lift_pct']:.3f}%")

std_treatment_actual = treatment_actual * 0.3
std_control_actual = control_actual * 0.3

se_actual = np.sqrt((std_treatment_actual**2 / n_treatment) + (std_control_actual**2 / n_control))
z_actual = (treatment_actual - control_actual) / se_actual
p_value_actual = 2 * (1 - stats.norm.cdf(abs(z_actual)))

print(f"Z-score:   {z_actual:.4f}")
print(f"P-value:   {p_value_actual:.6f}")

if p_value_actual < 0.001:
    print(f"Result:    *** HIGHLY SIGNIFICANT (p < 0.001)")
elif p_value_actual < 0.01:
    print(f"Result:    ** SIGNIFICANT (p < 0.01)")
elif p_value_actual < 0.05:
    print(f"Result:    * SIGNIFICANT (p < 0.05)")
elif p_value_actual < 0.10:
    print(f"Result:    MARGINALLY SIGNIFICANT (p < 0.10)")
else:
    print(f"Result:    NOT SIGNIFICANT (p >= 0.10)")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("\nSignificance Levels:")
print("  *** p < 0.001 (Highly Significant)")
print("  **  p < 0.01  (Significant)")
print("  *   p < 0.05  (Significant)")
print("      p < 0.10  (Marginally Significant)")
print("      p >= 0.10 (Not Significant)")
print("\nNote: Delivery count test uses two-proportion z-test with large sample sizes.")
print("      Time metric tests use z-test with assumed std = 30% of mean (conservative estimate).")
print("      Negative lifts indicate treatment is faster/better.")
print("="*80)
