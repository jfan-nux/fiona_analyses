"""
Statistical significance test for store content recency lift analysis
"""
import numpy as np
from scipy import stats
import pandas as pd

def two_proportion_ztest(control_conversions, control_total, 
                         treatment_conversions, treatment_total,
                         alpha=0.05):
    """
    Perform two-proportion z-test for A/B test
    
    Returns:
        dict with p_value, z_score, is_significant, confidence_interval
    """
    # Calculate proportions
    p_control = control_conversions / control_total
    p_treatment = treatment_conversions / treatment_total
    
    # Pooled proportion
    p_pooled = (control_conversions + treatment_conversions) / (control_total + treatment_total)
    
    # Standard error
    se = np.sqrt(p_pooled * (1 - p_pooled) * (1/control_total + 1/treatment_total))
    
    # Z-score
    z_score = (p_treatment - p_control) / se
    
    # Two-tailed p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
    
    # 95% confidence interval for the difference
    se_diff = np.sqrt(
        p_control * (1 - p_control) / control_total + 
        p_treatment * (1 - p_treatment) / treatment_total
    )
    z_critical = stats.norm.ppf(1 - alpha/2)
    ci_lower = (p_treatment - p_control) - z_critical * se_diff
    ci_upper = (p_treatment - p_control) + z_critical * se_diff
    
    return {
        'control_rate': p_control,
        'treatment_rate': p_treatment,
        'absolute_lift': p_treatment - p_control,
        'relative_lift': (p_treatment - p_control) / p_control if p_control > 0 else 0,
        'z_score': z_score,
        'p_value': p_value,
        'is_significant': p_value < alpha,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'alpha': alpha
    }


# Data from query results
segments = [
    {
        'segment': 'More than 120 days',
        'control_count': 288616,
        'control_rate': 0.422677,
        'treatment_count': 285868,
        'treatment_rate': 0.42341
    },
    {
        'segment': '120 days or less',
        'control_count': 40336,
        'control_rate': 0.448661,
        'treatment_count': 40220,
        'treatment_rate': 0.447541
    }
]

print("=" * 100)
print("STATISTICAL SIGNIFICANCE TEST - STORE CONTENT RECENCY ANALYSIS")
print("=" * 100)
print()

results_list = []

for segment_data in segments:
    segment = segment_data['segment']
    control_total = segment_data['control_count']
    control_rate = segment_data['control_rate']
    treatment_total = segment_data['treatment_count']
    treatment_rate = segment_data['treatment_rate']
    
    # Calculate conversions (people who ordered)
    control_conversions = int(control_total * control_rate)
    treatment_conversions = int(treatment_total * treatment_rate)
    
    # Run statistical test
    result = two_proportion_ztest(
        control_conversions, control_total,
        treatment_conversions, treatment_total
    )
    
    print(f"Segment: {segment}")
    print("-" * 100)
    print(f"Control:    {control_total:,} users, {result['control_rate']*100:.2f}% order rate")
    print(f"Treatment:  {treatment_total:,} users, {result['treatment_rate']*100:.2f}% order rate")
    print()
    print(f"Absolute Lift:  {result['absolute_lift']*100:+.2f} pp")
    print(f"Relative Lift:  {result['relative_lift']*100:+.2f}%")
    print()
    print(f"95% Confidence Interval: [{result['ci_lower']*100:.2f}pp, {result['ci_upper']*100:.2f}pp]")
    print(f"Z-Score:   {result['z_score']:.3f}")
    print(f"P-Value:   {result['p_value']:.4f}")
    print()
    
    if result['is_significant']:
        print(f"✓ STATISTICALLY SIGNIFICANT at α={result['alpha']} level")
        if result['absolute_lift'] > 0:
            print("  → Treatment is significantly BETTER than control")
        else:
            print("  → Treatment is significantly WORSE than control")
    else:
        print(f"✗ NOT STATISTICALLY SIGNIFICANT at α={result['alpha']} level")
        print("  → No significant difference between treatment and control")
    
    print()
    print("=" * 100)
    print()
    
    # Store results
    results_list.append({
        'segment': segment,
        'control_users': control_total,
        'control_rate_pct': f"{result['control_rate']*100:.2f}%",
        'treatment_users': treatment_total,
        'treatment_rate_pct': f"{result['treatment_rate']*100:.2f}%",
        'absolute_lift_pp': f"{result['absolute_lift']*100:+.2f}",
        'relative_lift_pct': f"{result['relative_lift']*100:+.2f}%",
        'ci_95_lower_pp': f"{result['ci_lower']*100:.2f}",
        'ci_95_upper_pp': f"{result['ci_upper']*100:.2f}",
        'z_score': f"{result['z_score']:.3f}",
        'p_value': f"{result['p_value']:.4f}",
        'is_significant': 'Yes' if result['is_significant'] else 'No'
    })

# Create summary DataFrame
df_results = pd.DataFrame(results_list)
print("\nSUMMARY TABLE:")
print(df_results.to_string(index=False))
print()

# Save to CSV
output_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs/statsig_test_store_content_recency.csv'
df_results.to_csv(output_path, index=False)
print(f"Results saved to: {output_path}")

