"""
Daily MAU Lift Analysis
Calculates daily lift and statistical significance for MAU metric
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import datetime
from utils.snowflake_connection import SnowflakeHook

def calculate_proportion_ztest(count1, nobs1, count2, nobs2):
    """
    Calculate z-test for proportions
    Returns z-statistic and p-value
    """
    prop1 = count1 / nobs1
    prop2 = count2 / nobs2
    
    # Pooled proportion
    pooled_prop = (count1 + count2) / (nobs1 + nobs2)
    
    # Standard error
    se = np.sqrt(pooled_prop * (1 - pooled_prop) * (1/nobs1 + 1/nobs2))
    
    # Z-statistic
    z_stat = (prop1 - prop2) / se
    
    # Two-tailed p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))
    
    return z_stat, p_value

def main():
    print("=" * 80)
    print("DAILY MAU LIFT ANALYSIS")
    print("=" * 80)
    
    # Read SQL file
    sql_file = "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_guest_conversion_fix/sql/daily_mau_lift.sql"
    with open(sql_file, 'r') as f:
        query = f.read()
    
    print("\n📊 Fetching daily MAU data from Snowflake...")
    
    # Fetch data using SnowflakeHook
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    
    # Normalize column names to uppercase
    df.columns = df.columns.str.upper()
    
    print(f"✓ Retrieved {len(df)} rows")
    print(f"✓ Date range: {df['ANALYSIS_DATE'].min()} to {df['ANALYSIS_DATE'].max()}")
    print(f"✓ Groups: {df['EXPERIMENT_GROUP'].unique()}")
    
    # Pivot data for easier calculation
    df_pivot = df.pivot(
        index='ANALYSIS_DATE',
        columns='EXPERIMENT_GROUP',
        values=['MAU_COUNT', 'TOTAL_USERS', 'MAU_RATE']
    )
    
    # Calculate lift metrics
    results = []
    
    for date in df_pivot.index:
        # Get metrics for each group
        groups = df[df['ANALYSIS_DATE'] == date]
        
        # Assuming we have control and treatment groups
        # Adjust group names based on actual data
        control = groups[groups['EXPERIMENT_GROUP'] == 'control'].iloc[0] if 'control' in groups['EXPERIMENT_GROUP'].values else groups.iloc[0]
        treatment = groups[groups['EXPERIMENT_GROUP'] != 'control'].iloc[0] if len(groups) > 1 else groups.iloc[-1]
        
        control_name = control['EXPERIMENT_GROUP']
        treatment_name = treatment['EXPERIMENT_GROUP']
        
        # Calculate lift
        absolute_lift = treatment['MAU_RATE'] - control['MAU_RATE']
        relative_lift = (absolute_lift / control['MAU_RATE']) * 100 if control['MAU_RATE'] > 0 else 0
        
        # Calculate statistical significance
        z_stat, p_value = calculate_proportion_ztest(
            treatment['MAU_COUNT'],
            treatment['TOTAL_USERS'],
            control['MAU_COUNT'],
            control['TOTAL_USERS']
        )
        
        # Calculate confidence interval for lift (95%)
        se_diff = np.sqrt(
            (treatment['MAU_RATE'] * (1 - treatment['MAU_RATE']) / treatment['TOTAL_USERS']) +
            (control['MAU_RATE'] * (1 - control['MAU_RATE']) / control['TOTAL_USERS'])
        )
        ci_lower = absolute_lift - 1.96 * se_diff
        ci_upper = absolute_lift + 1.96 * se_diff
        
        results.append({
            'date': date,
            'control_group': control_name,
            'treatment_group': treatment_name,
            'control_mau_rate': control['MAU_RATE'],
            'treatment_mau_rate': treatment['MAU_RATE'],
            'control_users': control['TOTAL_USERS'],
            'treatment_users': treatment['TOTAL_USERS'],
            'absolute_lift': absolute_lift,
            'relative_lift_pct': relative_lift,
            'z_statistic': z_stat,
            'p_value': p_value,
            'is_significant': p_value < 0.05,
            'ci_lower': ci_lower,
            'ci_upper': ci_upper
        })
    
    results_df = pd.DataFrame(results)
    
    # Save results
    output_dir = "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_guest_conversion_fix/outputs"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/daily_mau_lift_results.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\n✓ Results saved to: {output_file}")
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"\nAverage Relative Lift: {results_df['relative_lift_pct'].mean():.2f}%")
    print(f"Median Relative Lift: {results_df['relative_lift_pct'].median():.2f}%")
    print(f"Days with Significant Lift (p < 0.05): {results_df['is_significant'].sum()} / {len(results_df)}")
    print(f"\nLatest Day ({results_df['date'].max()}):")
    latest = results_df.iloc[-1]
    print(f"  Relative Lift: {latest['relative_lift_pct']:.2f}%")
    print(f"  P-value: {latest['p_value']:.4f}")
    print(f"  Significant: {'Yes' if latest['is_significant'] else 'No'}")
    
    # Create visualizations
    print("\n📈 Creating visualizations...")
    
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    
    # Plot 1: MAU Rate Comparison
    ax1 = axes[0]
    ax1.plot(results_df['date'], results_df['control_mau_rate'], 
             marker='o', label=f"Control ({results_df['control_group'].iloc[0]})", 
             linewidth=2, markersize=6)
    ax1.plot(results_df['date'], results_df['treatment_mau_rate'], 
             marker='s', label=f"Treatment ({results_df['treatment_group'].iloc[0]})", 
             linewidth=2, markersize=6)
    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('MAU Rate', fontsize=12)
    ax1.set_title('Daily MAU Rate by Group', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Relative Lift with Significance
    ax2 = axes[1]
    colors = ['green' if sig else 'gray' for sig in results_df['is_significant']]
    ax2.bar(results_df['date'], results_df['relative_lift_pct'], 
            color=colors, alpha=0.7, edgecolor='black')
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.set_ylabel('Relative Lift (%)', fontsize=12)
    ax2.set_title('Daily Relative Lift (Green = Significant at p<0.05)', 
                  fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.tick_params(axis='x', rotation=45)
    
    # Plot 3: P-value Trend
    ax3 = axes[2]
    ax3.plot(results_df['date'], results_df['p_value'], 
             marker='o', linewidth=2, markersize=6, color='purple')
    ax3.axhline(y=0.05, color='red', linestyle='--', linewidth=2, label='p=0.05 threshold')
    ax3.set_xlabel('Date', fontsize=12)
    ax3.set_ylabel('P-value', fontsize=12)
    ax3.set_title('Daily P-value Trend', fontsize=14, fontweight='bold')
    ax3.set_yscale('log')
    ax3.legend(fontsize=10)
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    plot_dir = "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_guest_conversion_fix/plots"
    os.makedirs(plot_dir, exist_ok=True)
    plot_file = f"{plot_dir}/daily_mau_lift_trend.png"
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"✓ Plot saved to: {plot_file}")
    
    # Create a detailed lift plot with confidence intervals
    fig2, ax = plt.subplots(figsize=(14, 8))
    
    ax.plot(results_df['date'], results_df['relative_lift_pct'], 
            marker='o', linewidth=2.5, markersize=8, color='steelblue', 
            label='Relative Lift')
    
    # Add confidence interval band (for absolute lift converted to relative)
    rel_ci_lower = (results_df['ci_lower'] / results_df['control_mau_rate']) * 100
    rel_ci_upper = (results_df['ci_upper'] / results_df['control_mau_rate']) * 100
    ax.fill_between(results_df['date'], rel_ci_lower, rel_ci_upper, 
                     alpha=0.2, color='steelblue', label='95% CI')
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1.5)
    
    # Highlight significant points
    sig_points = results_df[results_df['is_significant']]
    if len(sig_points) > 0:
        ax.scatter(sig_points['date'], sig_points['relative_lift_pct'], 
                   s=150, color='green', marker='*', zorder=5, 
                   label='Significant (p<0.05)', edgecolors='darkgreen', linewidths=2)
    
    ax.set_xlabel('Date', fontsize=13, fontweight='bold')
    ax.set_ylabel('Relative Lift (%)', fontsize=13, fontweight='bold')
    ax.set_title('Daily MAU Relative Lift with 95% Confidence Intervals', 
                 fontsize=15, fontweight='bold', pad=20)
    ax.legend(fontsize=11, loc='best')
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plot_file2 = f"{plot_dir}/daily_mau_lift_with_ci.png"
    plt.savefig(plot_file2, dpi=300, bbox_inches='tight')
    print(f"✓ Plot saved to: {plot_file2}")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    return results_df

if __name__ == "__main__":
    results = main()

