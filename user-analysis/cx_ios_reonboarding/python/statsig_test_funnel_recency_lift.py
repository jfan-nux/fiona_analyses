"""
Statistical significance test for funnel recency lift analysis
Tests significance for all funnel stages (store_content, store_page, order_cart, checkout, purchase)
across all recency buckets
"""
import numpy as np
from scipy import stats
import pandas as pd
import json

def two_proportion_ztest(control_conversions, control_total, 
                         treatment_conversions, treatment_total,
                         alpha=0.05):
    """
    Perform two-proportion z-test for A/B test
    
    Returns:
        dict with p_value, z_score, is_significant, confidence_interval
    """
    # Handle edge cases
    if control_total == 0 or treatment_total == 0:
        return None
    
    # Calculate proportions
    p_control = control_conversions / control_total
    p_treatment = treatment_conversions / treatment_total
    
    # Pooled proportion
    p_pooled = (control_conversions + treatment_conversions) / (control_total + treatment_total)
    
    # Standard error
    se = np.sqrt(p_pooled * (1 - p_pooled) * (1/control_total + 1/treatment_total))
    
    # Z-score
    z_score = (p_treatment - p_control) / se if se > 0 else 0
    
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
        'z_score': z_score,
        'p_value': p_value,
        'is_significant': p_value < alpha,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper
    }


# Data from Snowflake query
data = [
    {"recency_bucket":"0-90 days","sc_control_count":325211,"sc_control_rate":0.760076,"sc_treatment_count":323846,"sc_treatment_rate":0.760998,"sp_control_count":305792,"sp_control_rate":0.787089,"sp_treatment_count":304352,"sp_treatment_rate":0.78789,"oc_control_count":252625,"oc_control_rate":0.825098,"oc_treatment_count":251853,"oc_treatment_rate":0.8249,"ch_control_count":269101,"ch_control_rate":0.837929,"ch_treatment_count":268274,"ch_treatment_rate":0.837393,"pu_control_count":248497,"pu_control_rate":0.87096,"pu_treatment_count":247762,"pu_treatment_rate":0.870229},
    {"recency_bucket":"90-120 days","sc_control_count":40336,"sc_control_rate":0.448661,"sc_treatment_count":40220,"sc_treatment_rate":0.447541,"sp_control_count":38661,"sp_control_rate":0.468229,"sp_treatment_count":38697,"sp_treatment_rate":0.468086,"oc_control_count":30737,"oc_control_rate":0.492485,"oc_treatment_count":30623,"oc_treatment_rate":0.498302,"ch_control_count":33646,"ch_control_rate":0.519063,"ch_treatment_count":33483,"ch_treatment_rate":0.520935,"pu_control_count":30102,"pu_control_rate":0.540554,"pu_treatment_count":30128,"pu_treatment_rate":0.542534},
    {"recency_bucket":"120-150 days","sc_control_count":33287,"sc_control_rate":0.433039,"sc_treatment_count":32837,"sc_treatment_rate":0.434893,"sp_control_count":32238,"sp_control_rate":0.449924,"sp_treatment_count":31977,"sp_treatment_rate":0.450011,"oc_control_count":26012,"oc_control_rate":0.471918,"oc_treatment_count":25995,"oc_treatment_rate":0.471745,"ch_control_count":28277,"ch_control_rate":0.488825,"ch_treatment_count":28256,"ch_treatment_rate":0.489984,"pu_control_count":25412,"pu_control_rate":0.51216,"pu_treatment_count":25493,"pu_treatment_rate":0.513376},
    {"recency_bucket":"150-180 days","sc_control_count":28539,"sc_control_rate":0.421289,"sc_treatment_count":28233,"sc_treatment_rate":0.425338,"sp_control_count":27512,"sp_control_rate":0.437414,"sp_treatment_count":27247,"sp_treatment_rate":0.439849,"oc_control_count":22405,"oc_control_rate":0.456156,"oc_treatment_count":22113,"oc_treatment_rate":0.457448,"ch_control_count":24084,"ch_control_rate":0.475297,"ch_treatment_count":24027,"ch_treatment_rate":0.474072,"pu_control_count":22011,"pu_control_rate":0.493708,"pu_treatment_count":21691,"pu_treatment_rate":0.493039},
    {"recency_bucket":"180-365 days","sc_control_count":92158,"sc_control_rate":0.411866,"sc_treatment_count":90935,"sc_treatment_rate":0.412003,"sp_control_count":98387,"sp_control_rate":0.416139,"sp_treatment_count":97360,"sp_treatment_rate":0.416731,"oc_control_count":84716,"oc_control_rate":0.422977,"oc_treatment_count":83811,"oc_treatment_rate":0.424573,"ch_control_count":91949,"ch_control_rate":0.438605,"ch_treatment_count":90918,"ch_treatment_rate":0.439993,"pu_control_count":84944,"pu_control_rate":0.452155,"pu_treatment_count":84025,"pu_treatment_rate":0.453813},
    {"recency_bucket":"365+ days","sc_control_count":134632,"sc_control_rate":0.42781,"sc_treatment_count":133863,"sc_treatment_rate":0.427936,"sp_control_count":126857,"sp_control_rate":0.425356,"sp_treatment_count":126338,"sp_treatment_rate":0.428251,"oc_control_count":112121,"oc_control_rate":0.437472,"oc_treatment_count":111622,"oc_treatment_rate":0.439294,"ch_control_count":108364,"ch_control_rate":0.433904,"ch_treatment_count":107748,"ch_treatment_rate":0.436606,"pu_control_count":97714,"pu_control_rate":0.443012,"pu_treatment_count":97166,"pu_treatment_rate":0.44408},
    {"recency_bucket":"No visit","sc_control_count":122716,"sc_control_rate":0.442851,"sc_treatment_count":123736,"sc_treatment_rate":0.446214,"sp_control_count":147432,"sp_control_rate":0.416389,"sp_treatment_count":147699,"sp_treatment_rate":0.417989,"oc_control_count":248263,"oc_control_rate":0.445744,"oc_treatment_count":247653,"oc_treatment_rate":0.446946,"ch_control_count":221458,"ch_control_rate":0.388535,"ch_treatment_count":220964,"ch_treatment_rate":0.390658,"pu_control_count":268199,"pu_control_rate":0.385287,"pu_treatment_count":267405,"pu_treatment_rate":0.387722}
]

# Funnel stages to test
funnel_stages = {
    'store_content': 'sc',
    'store_page': 'sp',
    'order_cart': 'oc',
    'checkout': 'ch',
    'purchase': 'pu'
}

print("=" * 120)
print("STATISTICAL SIGNIFICANCE TEST - FUNNEL RECENCY LIFT ANALYSIS")
print("=" * 120)
print()

# Lists to store results for lift and significance tables
lift_results = []
sig_results = []

for row in data:
    recency_bucket = row['recency_bucket']
    
    lift_row = {'recency_bucket': recency_bucket}
    sig_row = {'recency_bucket': recency_bucket}
    
    print(f"\n{'='*120}")
    print(f"RECENCY BUCKET: {recency_bucket}")
    print(f"{'='*120}\n")
    
    for stage_name, prefix in funnel_stages.items():
        control_count = row.get(f'{prefix}_control_count', 0)
        control_rate = row.get(f'{prefix}_control_rate', 0)
        treatment_count = row.get(f'{prefix}_treatment_count', 0)
        treatment_rate = row.get(f'{prefix}_treatment_rate', 0)
        
        if control_count is None or treatment_count is None or control_count == 0 or treatment_count == 0:
            lift_row[f'{stage_name}_lift'] = None
            sig_row[f'{stage_name}_statsig'] = 'N/A'
            continue
        
        # Calculate conversions
        control_conversions = int(control_count * control_rate)
        treatment_conversions = int(treatment_count * treatment_rate)
        
        # Run statistical test
        result = two_proportion_ztest(
            control_conversions, control_count,
            treatment_conversions, treatment_count
        )
        
        if result is None:
            lift_row[f'{stage_name}_lift'] = None
            sig_row[f'{stage_name}_statsig'] = 'N/A'
            continue
        
        # Store lift
        lift_pp = result['absolute_lift'] * 100
        lift_row[f'{stage_name}_lift'] = round(lift_pp, 2)
        
        # Store significance
        sig_row[f'{stage_name}_statsig'] = 'Yes' if result['is_significant'] else 'No'
        
        # Print details
        print(f"{stage_name.upper().replace('_', ' ')}")
        print(f"{'-'*120}")
        print(f"Control:    {control_count:,} users, {result['control_rate']*100:.2f}% order rate")
        print(f"Treatment:  {treatment_count:,} users, {result['treatment_rate']*100:.2f}% order rate")
        print(f"Lift:       {lift_pp:+.2f} pp")
        print(f"95% CI:     [{result['ci_lower']*100:.2f}pp, {result['ci_upper']*100:.2f}pp]")
        print(f"P-Value:    {result['p_value']:.4f}")
        
        if result['is_significant']:
            print(f"Result:     ✓ STATISTICALLY SIGNIFICANT (p < 0.05)")
        else:
            print(f"Result:     ✗ NOT STATISTICALLY SIGNIFICANT (p >= 0.05)")
        print()
    
    lift_results.append(lift_row)
    sig_results.append(sig_row)

# Create DataFrames
df_lift = pd.DataFrame(lift_results)
df_sig = pd.DataFrame(sig_results)

print("\n" + "=" * 120)
print("LIFT TABLE (in percentage points)")
print("=" * 120)
print(df_lift.to_string(index=False))

print("\n" + "=" * 120)
print("STATISTICAL SIGNIFICANCE TABLE")
print("=" * 120)
print(df_sig.to_string(index=False))

# Save to CSV
output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs'
lift_path = f'{output_dir}/funnel_recency_lift.csv'
sig_path = f'{output_dir}/funnel_recency_statsig.csv'

df_lift.to_csv(lift_path, index=False)
df_sig.to_csv(sig_path, index=False)

print(f"\n\nResults saved to:")
print(f"  Lift table: {lift_path}")
print(f"  Significance table: {sig_path}")

# Create combined view with lift and significance markers
combined_results = []
for lift_row, sig_row in zip(lift_results, sig_results):
    combined_row = {'recency_bucket': lift_row['recency_bucket']}
    for stage_name in funnel_stages.keys():
        lift_val = lift_row.get(f'{stage_name}_lift')
        sig_val = sig_row.get(f'{stage_name}_statsig')
        
        if lift_val is None:
            combined_row[stage_name] = 'N/A'
        else:
            marker = '✓' if sig_val == 'Yes' else '✗'
            combined_row[stage_name] = f"{lift_val:+.2f} {marker}"
    combined_results.append(combined_row)

df_combined = pd.DataFrame(combined_results)

print("\n" + "=" * 120)
print("COMBINED TABLE: LIFT (pp) with SIGNIFICANCE MARKERS")
print("✓ = Statistically Significant | ✗ = Not Statistically Significant")
print("=" * 120)
print(df_combined.to_string(index=False))

combined_path = f'{output_dir}/funnel_recency_lift_with_sig.csv'
df_combined.to_csv(combined_path, index=False)
print(f"\nCombined table saved to: {combined_path}")

# Summary statistics
print("\n" + "=" * 120)
print("SUMMARY")
print("=" * 120)
total_tests = len(data) * len(funnel_stages)
sig_count = sum(1 for row in sig_results for stage in funnel_stages.keys() 
                if row.get(f'{stage}_statsig') == 'Yes')
print(f"Total tests performed: {total_tests}")
print(f"Statistically significant results: {sig_count}")
print(f"Not significant results: {total_tests - sig_count}")
print(f"Significance rate: {sig_count/total_tests*100:.1f}%")

