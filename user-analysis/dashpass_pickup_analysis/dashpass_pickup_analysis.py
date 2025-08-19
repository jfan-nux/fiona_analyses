#!/usr/bin/env python3
"""
DashPass vs Pickup Analysis
Question: Do DashPass Cx have lower % pickup Cx?

Analysis of pickup behavior differences between DashPass and non-DashPass customers
Data period: July 1 - August 16, 2025
"""

import pandas as pd
import numpy as np
from scipy import stats

# Data from SQL query results
data = {
    'Customer_Type': ['Non-DashPass', 'DashPass'],
    'Total_Orders': [93449713, 192336916],
    'Unique_Customers': [24221771, 18706480],
    'Pickup_Orders': [5822118, 5789249],
    'Pct_Orders_Pickup': [6.23, 3.01]
}

df = pd.DataFrame(data)
print("=" * 80)
print("DASHPASS vs PICKUP BEHAVIOR ANALYSIS")
print("Analysis Period: July 1 - August 16, 2025")
print("=" * 80)
print()

# Basic statistics
print("📊 BASIC STATISTICS")
print("-" * 40)
print(f"{'Metric':<25} {'Non-DashPass':<15} {'DashPass':<15} {'Difference':<15}")
print("-" * 70)

total_orders_diff = df.loc[1, 'Total_Orders'] - df.loc[0, 'Total_Orders']
customers_diff = df.loc[1, 'Unique_Customers'] - df.loc[0, 'Unique_Customers']
pickup_rate_diff = df.loc[1, 'Pct_Orders_Pickup'] - df.loc[0, 'Pct_Orders_Pickup']

print(f"{'Total Orders':<25} {df.loc[0, 'Total_Orders']:<15,} {df.loc[1, 'Total_Orders']:<15,} {total_orders_diff:<15,}")
print(f"{'Unique Customers':<25} {df.loc[0, 'Unique_Customers']:<15,} {df.loc[1, 'Unique_Customers']:<15,} {customers_diff:<15,}")
print(f"{'Pickup Orders':<25} {df.loc[0, 'Pickup_Orders']:<15,} {df.loc[1, 'Pickup_Orders']:<15,}")
print(f"{'Pickup Rate (%)':<25} {df.loc[0, 'Pct_Orders_Pickup']:<15} {df.loc[1, 'Pct_Orders_Pickup']:<15} {pickup_rate_diff:<15.2f}")
print()

# Calculate orders per customer
df['Orders_Per_Customer'] = df['Total_Orders'] / df['Unique_Customers']
orders_per_customer_diff = df.loc[1, 'Orders_Per_Customer'] - df.loc[0, 'Orders_Per_Customer']

print(f"{'Orders per Customer':<25} {df.loc[0, 'Orders_Per_Customer']:<15.2f} {df.loc[1, 'Orders_Per_Customer']:<15.2f} {orders_per_customer_diff:<15.2f}")
print()

# Key findings
print("🎯 KEY FINDINGS")
print("-" * 40)
print(f"1. **ANSWER TO MAIN QUESTION**: YES - DashPass customers have lower pickup rates")
print(f"   • DashPass customers: {df.loc[1, 'Pct_Orders_Pickup']:.2f}% of orders are pickup")
print(f"   • Non-DashPass customers: {df.loc[0, 'Pct_Orders_Pickup']:.2f}% of orders are pickup")
print(f"   • Difference: {abs(pickup_rate_diff):.2f} percentage points lower for DashPass")
print()

print(f"2. **RELATIVE DIFFERENCE**: DashPass customers are {(df.loc[0, 'Pct_Orders_Pickup'] / df.loc[1, 'Pct_Orders_Pickup']):.1f}x less likely to use pickup")
print()

print(f"3. **CUSTOMER BEHAVIOR PATTERNS**:")
print(f"   • DashPass customers order more frequently ({df.loc[1, 'Orders_Per_Customer']:.1f} vs {df.loc[0, 'Orders_Per_Customer']:.1f} orders per customer)")
print(f"   • DashPass represents {(df.loc[1, 'Total_Orders'] / (df.loc[0, 'Total_Orders'] + df.loc[1, 'Total_Orders']) * 100):.1f}% of total order volume")
print(f"   • But only {(df.loc[1, 'Unique_Customers'] / (df.loc[0, 'Unique_Customers'] + df.loc[1, 'Unique_Customers']) * 100):.1f}% of unique customers")
print()

# Statistical significance (approximation)
print("📈 STATISTICAL ANALYSIS")
print("-" * 40)

# Calculate approximate confidence intervals for pickup rates
def calculate_ci(successes, total, confidence=0.95):
    """Calculate confidence interval for proportion"""
    p = successes / total
    z = stats.norm.ppf((1 + confidence) / 2)
    margin = z * np.sqrt(p * (1 - p) / total)
    return p - margin, p + margin

# Confidence intervals
dashpass_ci = calculate_ci(df.loc[1, 'Pickup_Orders'], df.loc[1, 'Total_Orders'])
non_dashpass_ci = calculate_ci(df.loc[0, 'Pickup_Orders'], df.loc[0, 'Total_Orders'])

print(f"95% Confidence Intervals for Pickup Rates:")
print(f"• DashPass: {dashpass_ci[0]*100:.3f}% - {dashpass_ci[1]*100:.3f}%")
print(f"• Non-DashPass: {non_dashpass_ci[0]*100:.3f}% - {non_dashpass_ci[1]*100:.3f}%")
print()

# Two-proportion z-test
def two_prop_z_test(x1, n1, x2, n2):
    """Two-proportion z-test"""
    p1 = x1 / n1
    p2 = x2 / n2
    p_pooled = (x1 + x2) / (n1 + n2)
    
    se = np.sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
    z = (p1 - p2) / se
    p_value = 2 * (1 - stats.norm.cdf(abs(z)))
    
    return z, p_value

z_stat, p_value = two_prop_z_test(
    df.loc[0, 'Pickup_Orders'], df.loc[0, 'Total_Orders'],  # Non-DashPass
    df.loc[1, 'Pickup_Orders'], df.loc[1, 'Total_Orders']   # DashPass
)

print(f"Two-proportion Z-test:")
print(f"• Z-statistic: {z_stat:.2f}")
print(f"• P-value: {p_value:.2e}")
print(f"• Result: {'Highly significant' if p_value < 0.001 else 'Significant' if p_value < 0.05 else 'Not significant'} difference")
print()

# Business implications
print("💼 BUSINESS IMPLICATIONS")
print("-" * 40)
print("1. **Customer Behavior**: DashPass customers show strong preference for delivery over pickup")
print("2. **Service Utilization**: DashPass benefits (free delivery) naturally reduce pickup incentive")  
print("3. **Revenue Impact**: Lower pickup rates among DashPass customers align with subscription value prop")
print("4. **Operational**: DashPass drives delivery volume, requiring robust delivery infrastructure")
print()

# Recommendations  
print("🔍 RECOMMENDATIONS FOR FURTHER ANALYSIS")
print("-" * 40)
print("1. **Temporal Analysis**: Track pickup rate trends over time by customer type")
print("2. **Geographic Analysis**: Compare pickup rates by market/submarket")
print("3. **Customer Journey**: Analyze pickup behavior changes after DashPass subscription")
print("4. **Store-level Analysis**: Identify which store types drive pickup behavior differences")
print("5. **Experiment Opportunity**: Test pickup incentives for DashPass customers")
print()

# Summary
print("📋 EXECUTIVE SUMMARY")
print("-" * 40)
print("✅ **CONFIRMED**: DashPass customers have significantly lower pickup rates (3.01% vs 6.23%)")
print("✅ **MAGNITUDE**: 2.1x less likely to use pickup")
print("✅ **SIGNIFICANCE**: Statistically highly significant with massive sample size")
print("✅ **BUSINESS LOGIC**: Aligns with DashPass value proposition of convenient delivery")
print("=" * 80)
