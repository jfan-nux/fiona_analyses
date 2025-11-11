"""
Statistical significance testing for promo eligibility experiment
Tests order rate and MAU lifts between control and treatment groups
"""

import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest
import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

def calculate_stats(control_data, treatment_data, metric_name, is_proportion=False):
    """
    Calculate statistical significance between control and treatment
    
    Args:
        control_data: array of metric values for control group
        treatment_data: array of metric values for treatment group
        metric_name: name of the metric being tested
        is_proportion: whether this is a binary/proportion metric
    
    Returns:
        dict with test results
    """
    # Remove nulls
    control = control_data.dropna()
    treatment = treatment_data.dropna()
    
    # Sample sizes
    n_control = len(control)
    n_treatment = len(treatment)
    
    # Calculate means
    control_mean = control.mean()
    treatment_mean = treatment.mean()
    absolute_lift = treatment_mean - control_mean
    relative_lift = (absolute_lift / control_mean) * 100 if control_mean != 0 else 0
    
    if is_proportion:
        # For binary/proportion metrics (MAU, conversion rates)
        # Use z-test for proportions (binary data)
        successes = np.array([treatment.sum(), control.sum()])
        nobs = np.array([n_treatment, n_control])
        stat, pvalue = proportions_ztest(successes, nobs)
        
        # Standard errors for proportions
        p1 = treatment_mean
        p2 = control_mean
        se_control = np.sqrt(p2 * (1 - p2) / n_control)
        se_treatment = np.sqrt(p1 * (1 - p1) / n_treatment)
        
        # Confidence interval for proportion difference
        se_diff = np.sqrt(p1*(1-p1)/n_treatment + p2*(1-p2)/n_control)
        ci_lower = absolute_lift - 1.96 * se_diff
        ci_upper = absolute_lift + 1.96 * se_diff
    else:
        # For continuous metrics (order rate)
        # Use t-test for continuous data
        stat, pvalue = stats.ttest_ind(treatment, control, equal_var=False)
        
        # Standard errors for continuous metrics
        se_control = stats.sem(control)
        se_treatment = stats.sem(treatment)
        
        # Confidence interval using pooled standard error
        se_diff = np.sqrt(se_control**2 + se_treatment**2)
        ci_lower = absolute_lift - 1.96 * se_diff
        ci_upper = absolute_lift + 1.96 * se_diff
    
    return {
        'metric': metric_name,
        'test_type': 'proportion_z_test' if is_proportion else 't_test',
        'n_control': n_control,
        'n_treatment': n_treatment,
        'control_mean': control_mean,
        'control_se': se_control,
        'treatment_mean': treatment_mean,
        'treatment_se': se_treatment,
        'absolute_lift': absolute_lift,
        'relative_lift_pct': relative_lift,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'test_statistic': stat,
        'p_value': pvalue,
        'is_significant_95': pvalue < 0.05,
        'is_significant_90': pvalue < 0.10
    }


def main():
    print("=" * 80)
    print("STATISTICAL SIGNIFICANCE TEST: Promo Eligibility Analysis")
    print("=" * 80)
    
    # Query user-level data
    query = """
    SELECT 
        tag,
        is_promo_eligible,
        dd_device_id,
        total_orders_28d,
        order_rate_28d,
        CASE WHEN total_orders_28d > 0 THEN 1 ELSE 0 END AS had_order_28d,
        saw_promo_page,
        did_reonboarding_flow,
        has_redeemed
    FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
    """
    
    print("\nQuerying user-level data...")
    hook = SnowflakeHook()
    df = hook.query_snowflake(query, method='pandas')
    print(f"Loaded {len(df):,} user records")
    
    # Results storage
    all_results = []
    
    # Test metrics for different segments
    segments = [
        ('Overall', df),
        ('Promo Eligible', df[df['is_promo_eligible'] == 1]),
        ('Not Promo Eligible', df[df['is_promo_eligible'] == 0])
    ]
    
    for segment_name, segment_df in segments:
        print("\n" + "=" * 80)
        print(f"SEGMENT: {segment_name}")
        print("=" * 80)
        
        if len(segment_df) == 0:
            print(f"No data for {segment_name}")
            continue
        
        # Split into control and treatment
        control = segment_df[segment_df['tag'] == 'control']
        treatment = segment_df[segment_df['tag'] == 'treatment']
        
        print(f"\nSample sizes:")
        print(f"  Control:   {len(control):,} users")
        print(f"  Treatment: {len(treatment):,} users")
        
        if len(control) == 0 or len(treatment) == 0:
            print(f"Insufficient data for {segment_name}")
            continue
        
        # Test metrics
        metrics = [
            ('Order Rate (orders/28d)', 'order_rate_28d', False),
            ('MAU (had order in 28d)', 'had_order_28d', True),
            ('Saw Promo Page', 'saw_promo_page', True),
            ('Did Reonboarding Flow', 'did_reonboarding_flow', True),
            ('Redeemed Promo', 'has_redeemed', True)
        ]
        
        segment_results = []
        for metric_display_name, metric_col, is_prop in metrics:
            result = calculate_stats(
                control[metric_col],
                treatment[metric_col],
                metric_display_name,
                is_proportion=is_prop
            )
            result['segment'] = segment_name
            segment_results.append(result)
            all_results.append(result)
        
        # Print results table
        results_df = pd.DataFrame(segment_results)
        
        print(f"\n{segment_name} Results:")
        print("-" * 80)
        for _, row in results_df.iterrows():
            sig_marker = "***" if row['is_significant_95'] else ("*" if row['is_significant_90'] else "")
            test_label = " [Z-test]" if row['test_type'] == 'proportion_z_test' else " [t-test]"
            print(f"\n{row['metric']}{test_label}: {sig_marker}")
            print(f"  Control:   {row['control_mean']:.4f} (SE: {row['control_se']:.4f}, n={row['n_control']:,})")
            print(f"  Treatment: {row['treatment_mean']:.4f} (SE: {row['treatment_se']:.4f}, n={row['n_treatment']:,})")
            print(f"  Lift:      {row['relative_lift_pct']:+.2f}%")
            print(f"  95% CI:    [{row['ci_lower']:.4f}, {row['ci_upper']:.4f}]")
            print(f"  p-value:   {row['p_value']:.4f}")
    
    # Save full results
    results_df = pd.DataFrame(all_results)
    output_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs/statsig_promo_eligibility.csv'
    results_df.to_csv(output_path, index=False)
    print(f"\n\nFull results saved to: {output_path}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    sig_results = results_df[results_df['is_significant_95'] == True]
    if len(sig_results) > 0:
        print(f"\n{len(sig_results)} statistically significant results (p < 0.05):")
        for _, row in sig_results.iterrows():
            print(f"  • {row['segment']} - {row['metric']}: {row['relative_lift_pct']:+.2f}% (p={row['p_value']:.4f})")
    else:
        print("\nNo statistically significant results at 95% confidence level")
    
    print("\n*** = p < 0.05, * = p < 0.10")
    print("[Z-test] = Proportion test for binary metrics, [t-test] = T-test for continuous metrics")
    
    hook.close()


if __name__ == "__main__":
    main()

