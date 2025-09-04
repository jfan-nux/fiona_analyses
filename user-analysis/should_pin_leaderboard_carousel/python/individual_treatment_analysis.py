"""
Individual Treatment Effects Analysis
Analyzes the main effects of each experiment treatment separately.
"""

import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns
from utils.snowflake_connection import SnowflakeHook
import os
from datetime import datetime

# Set up plotting
plt.style.use('default')
sns.set_palette("husl")

def get_analysis_data():
    """Get data for individual treatment analysis from Snowflake."""
    
    query = """
    SELECT 
        consumer_id,
        exp1_status,
        exp2_status,
        
        -- Outcome variables
        orders_count,
        total_variable_profit,
        total_gov,
        ordered,
        new_customer,
        is_mau,
        explored_after_exposure,
        viewed_store_after_exposure,
        viewed_cart_after_exposure,
        viewed_checkout_after_exposure
        
    FROM proddb.fionafan.two_experiment_analysis
    WHERE 1=1
    """
    
    print("Fetching individual treatment analysis data from Snowflake...")
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    print(f"Retrieved {len(df)} records for analysis")
    
    return df

def analyze_experiment_treatment(df, exp_column, exp_name, outcome_metrics):
    """Analyze treatment effect for a single experiment."""
    
    print(f"\n{'='*60}")
    print(f"ANALYZING {exp_name.upper()}")
    print(f"{'='*60}")
    
    # Filter to users in this experiment
    exp_data = df[df[exp_column].isin(['treatment', 'control'])].copy()
    
    print(f"\n📊 Sample Sizes:")
    print(exp_data[exp_column].value_counts().sort_index())
    
    results = {}
    
    # Analyze each outcome metric
    for outcome_var, outcome_name in outcome_metrics:
        print(f"\n--- {outcome_name} ---")
        
        # Group statistics
        group_stats = exp_data.groupby(exp_column)[outcome_var].agg(['count', 'mean', 'std']).round(4)
        print(f"\nGroup Statistics:")
        print(group_stats)
        
        # Treatment effect
        treatment_mean = group_stats.loc['treatment', 'mean']
        control_mean = group_stats.loc['control', 'mean']
        effect_size = treatment_mean - control_mean
        
        # Statistical test
        treatment_group = exp_data[exp_data[exp_column] == 'treatment'][outcome_var]
        control_group = exp_data[exp_data[exp_column] == 'control'][outcome_var]
        
        # T-test
        t_stat, p_value = stats.ttest_ind(treatment_group, control_group, equal_var=False)
        
        # Cohen's d
        pooled_std = np.sqrt(((len(treatment_group)-1)*treatment_group.std()**2 + 
                             (len(control_group)-1)*control_group.std()**2) / 
                            (len(treatment_group) + len(control_group) - 2))
        cohens_d = effect_size / pooled_std if pooled_std > 0 else 0
        
        # 95% Confidence interval for difference
        se_diff = np.sqrt(treatment_group.var()/len(treatment_group) + control_group.var()/len(control_group))
        ci_lower = effect_size - 1.96 * se_diff
        ci_upper = effect_size + 1.96 * se_diff
        
        # Store results
        results[outcome_name] = {
            'control_mean': control_mean,
            'treatment_mean': treatment_mean,
            'effect_size': effect_size,
            'percent_change': (effect_size / control_mean * 100) if control_mean > 0 else 0,
            't_statistic': t_stat,
            'p_value': p_value,
            'cohens_d': cohens_d,
            'ci_lower': ci_lower,
            'ci_upper': ci_upper,
            'sample_size_control': len(control_group),
            'sample_size_treatment': len(treatment_group)
        }
        
        # Print results
        print(f"Treatment Effect: {effect_size:.4f}")
        print(f"Percent Change: {(effect_size / control_mean * 100):.2f}%" if control_mean > 0 else "N/A")
        print(f"P-value: {p_value:.4f}")
        print(f"95% CI: [{ci_lower:.4f}, {ci_upper:.4f}]")
        print(f"Cohen's d: {cohens_d:.4f}")
        
        # Significance interpretation
        if p_value < 0.001:
            significance = "*** HIGHLY SIGNIFICANT"
        elif p_value < 0.01:
            significance = "** VERY SIGNIFICANT"
        elif p_value < 0.05:
            significance = "* SIGNIFICANT"
        elif p_value < 0.10:
            significance = "† MARGINALLY SIGNIFICANT"
        else:
            significance = "NOT SIGNIFICANT"
        
        # Effect size interpretation
        if abs(cohens_d) < 0.2:
            effect_interpretation = "Small effect"
        elif abs(cohens_d) < 0.5:
            effect_interpretation = "Medium effect"
        elif abs(cohens_d) < 0.8:
            effect_interpretation = "Large effect"
        else:
            effect_interpretation = "Very large effect"
        
        print(f"Statistical Significance: {significance}")
        print(f"Effect Size: {effect_interpretation}")
        
        if p_value < 0.05:
            direction = "positive" if effect_size > 0 else "negative"
            print(f"✅ SIGNIFICANT {direction} treatment effect detected!")
        else:
            print("❌ No significant treatment effect.")
    
    return results

def create_treatment_comparison_plots(df, results_exp1, results_exp2):
    """Create comparison plots for both experiments."""
    
    # Prepare data for plotting
    metrics = ['Orders Count', 'Order Rate', 'Variable Profit', 'Exploration Rate']
    
    # Create comparison plot
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Individual Treatment Effects Comparison', fontsize=16, fontweight='bold')
    
    for idx, metric in enumerate(metrics):
        ax = axes[idx // 2, idx % 2]
        
        # Get effect sizes from results
        exp1_effect = results_exp1.get(metric, {}).get('effect_size', 0)
        exp2_effect = results_exp2.get(metric, {}).get('effect_size', 0)
        
        exp1_ci_lower = results_exp1.get(metric, {}).get('ci_lower', 0)
        exp1_ci_upper = results_exp1.get(metric, {}).get('ci_upper', 0)
        exp2_ci_lower = results_exp2.get(metric, {}).get('ci_lower', 0)
        exp2_ci_upper = results_exp2.get(metric, {}).get('ci_upper', 0)
        
        # Create bar plot with error bars
        experiments = ['Exp1\n(Leaderboard)', 'Exp2\n(Onboarding)']
        effects = [exp1_effect, exp2_effect]
        ci_errors = [
            [exp1_effect - exp1_ci_lower, exp1_ci_upper - exp1_effect],
            [exp2_effect - exp2_ci_lower, exp2_ci_upper - exp2_effect]
        ]
        
        colors = ['skyblue', 'lightcoral']
        bars = ax.bar(experiments, effects, color=colors, alpha=0.7, 
                     yerr=ci_errors, capsize=5, ecolor='black')
        
        # Add significance markers
        exp1_p = results_exp1.get(metric, {}).get('p_value', 1)
        exp2_p = results_exp2.get(metric, {}).get('p_value', 1)
        
        for i, (bar, p_val) in enumerate(zip(bars, [exp1_p, exp2_p])):
            height = bar.get_height()
            if p_val < 0.001:
                significance = '***'
            elif p_val < 0.01:
                significance = '**'
            elif p_val < 0.05:
                significance = '*'
            else:
                significance = 'ns'
            
            ax.text(bar.get_x() + bar.get_width()/2., 
                   height + (0.1 * abs(height)) if height > 0 else height - (0.1 * abs(height)),
                   significance, ha='center', va='bottom' if height > 0 else 'top',
                   fontweight='bold')
        
        ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        ax.set_ylabel(f'{metric} Effect Size')
        ax.set_title(f'{metric} - Treatment Effects')
        ax.grid(True, alpha=0.3)
        
        # Add effect size values as text
        for i, (bar, effect) in enumerate(zip(bars, effects)):
            ax.text(bar.get_x() + bar.get_width()/2., effect/2,
                   f'{effect:.4f}', ha='center', va='center',
                   fontweight='bold', color='white' if abs(effect) > 0.01 else 'black')
    
    plt.tight_layout()
    
    # Save plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plot_path = f'user-analysis/should_pin_leaderboard_carousel/plots/individual_treatment_effects_{timestamp}.png'
    os.makedirs(os.path.dirname(plot_path), exist_ok=True)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 Treatment effects plots saved to: {plot_path}")
    
    return plot_path

def create_summary_table(results_exp1, results_exp2):
    """Create a comprehensive summary table."""
    
    print(f"\n{'='*80}")
    print("COMPREHENSIVE TREATMENT EFFECTS SUMMARY")
    print(f"{'='*80}")
    
    # Create summary DataFrame
    summary_data = []
    
    for metric in results_exp1.keys():
        exp1_data = results_exp1[metric]
        exp2_data = results_exp2.get(metric, {})
        
        summary_data.append({
            'Metric': metric,
            'Exp1_Effect': exp1_data.get('effect_size', 0),
            'Exp1_Percent': exp1_data.get('percent_change', 0),
            'Exp1_P_Value': exp1_data.get('p_value', 1),
            'Exp1_Cohens_D': exp1_data.get('cohens_d', 0),
            'Exp2_Effect': exp2_data.get('effect_size', 0),
            'Exp2_Percent': exp2_data.get('percent_change', 0),
            'Exp2_P_Value': exp2_data.get('p_value', 1),
            'Exp2_Cohens_D': exp2_data.get('cohens_d', 0)
        })
    
    summary_df = pd.DataFrame(summary_data)
    
    print("\nTREATMENT EFFECTS COMPARISON:")
    print("=" * 120)
    print(f"{'Metric':<20} {'Exp1 Effect':<12} {'Exp1 %':<10} {'Exp1 p-val':<12} {'Exp2 Effect':<12} {'Exp2 %':<10} {'Exp2 p-val':<12}")
    print("-" * 120)
    
    for _, row in summary_df.iterrows():
        exp1_sig = "***" if row['Exp1_P_Value'] < 0.001 else "**" if row['Exp1_P_Value'] < 0.01 else "*" if row['Exp1_P_Value'] < 0.05 else ""
        exp2_sig = "***" if row['Exp2_P_Value'] < 0.001 else "**" if row['Exp2_P_Value'] < 0.01 else "*" if row['Exp2_P_Value'] < 0.05 else ""
        
        print(f"{row['Metric']:<20} {row['Exp1_Effect']:<9.4f}{exp1_sig:<3} {row['Exp1_Percent']:<8.2f}% {row['Exp1_P_Value']:<12.4f} "
              f"{row['Exp2_Effect']:<9.4f}{exp2_sig:<3} {row['Exp2_Percent']:<8.2f}% {row['Exp2_P_Value']:<12.4f}")
    
    return summary_df

def main():
    """Main analysis function."""
    
    print("🧪 INDIVIDUAL TREATMENT EFFECTS ANALYSIS")
    print("=" * 60)
    
    # Get data
    df = get_analysis_data()
    
    # Define outcome metrics
    outcome_metrics = [
        ('orders_count', 'Orders Count'),
        ('ordered', 'Order Rate'),
        ('total_variable_profit', 'Variable Profit'),
        ('explored_after_exposure', 'Exploration Rate')
    ]
    
    # Analyze Experiment 1
    results_exp1 = analyze_experiment_treatment(
        df, 'exp1_status', 'Should Pin Leaderboard Carousel', outcome_metrics
    )
    
    # Analyze Experiment 2  
    results_exp2 = analyze_experiment_treatment(
        df, 'exp2_status', 'Mobile Onboarding Preferences', outcome_metrics
    )
    
    # Create summary table
    summary_df = create_summary_table(results_exp1, results_exp2)
    
    # Create plots
    plot_path = create_treatment_comparison_plots(df, results_exp1, results_exp2)
    
    print(f"\n✅ Individual treatment analysis complete!")
    print(f"📊 Plots saved to: {plot_path}")
    print(f"📋 Summary table created")

if __name__ == "__main__":
    main()
