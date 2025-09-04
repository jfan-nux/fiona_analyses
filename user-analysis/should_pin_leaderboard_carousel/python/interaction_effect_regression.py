"""
Interaction Effect Regression Analysis
Analyzes the interaction effects between two experiments using proper statistical methods.
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

def get_regression_data():
    """Get data for regression analysis from Snowflake."""
    
    query = """
    SELECT 
        consumer_id,
        
        -- Binary treatment indicators (0/1 for regression)
        CASE WHEN exp1_status = 'treatment' THEN 1 ELSE 0 END AS exp1_treat,
        CASE WHEN exp2_status = 'treatment' THEN 1 ELSE 0 END AS exp2_treat,
        
        -- Interaction term
        CASE WHEN exp1_status = 'treatment' AND exp2_status = 'treatment' THEN 1 ELSE 0 END AS interaction,
        
        -- Treatment group labels
        CASE 
            WHEN exp1_status = 'control' AND exp2_status = 'control' THEN 'Both Control'
            WHEN exp1_status = 'treatment' AND exp2_status = 'control' THEN 'Exp1 Only'
            WHEN exp1_status = 'control' AND exp2_status = 'treatment' THEN 'Exp2 Only'
            WHEN exp1_status = 'treatment' AND exp2_status = 'treatment' THEN 'Both Treatment'
        END AS treatment_group,
        
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
    WHERE exp1_status IN ('treatment', 'control')
      AND exp2_status IN ('treatment', 'control')  -- Both experiments
    """
    
    print("Fetching regression data from Snowflake...")
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    print(f"Retrieved {len(df)} records for analysis")
    
    return df

def run_regression_analysis(df, outcome_var, outcome_name):
    """Run regression analysis for a given outcome variable."""
    
    print(f"\n=== REGRESSION ANALYSIS: {outcome_name} ===")
    
    # Create the regression formula
    formula = f'{outcome_var} ~ exp1_treat + exp2_treat + interaction'
    
    # Run the regression
    model = smf.ols(formula, data=df).fit()
    
    # Print results
    print(f"\nRegression Results for {outcome_name}:")
    print("=" * 60)
    print(model.summary())
    
    # Extract key coefficients
    coeffs = model.params
    p_values = model.pvalues
    conf_int = model.conf_int()
    
    results = {
        'outcome': outcome_name,
        'intercept': coeffs['Intercept'],
        'exp1_main_effect': coeffs['exp1_treat'],
        'exp2_main_effect': coeffs['exp2_treat'], 
        'interaction_effect': coeffs['interaction'],
        'interaction_p_value': p_values['interaction'],
        'interaction_ci_lower': conf_int.loc['interaction', 0],
        'interaction_ci_upper': conf_int.loc['interaction', 1],
        'r_squared': model.rsquared,
        'adj_r_squared': model.rsquared_adj,
        'f_pvalue': model.f_pvalue,
        'n_obs': int(model.nobs)
    }
    
    # Interpretation
    print(f"\n=== INTERPRETATION for {outcome_name} ===")
    print(f"Interaction Effect: {results['interaction_effect']:.4f}")
    print(f"P-value: {results['interaction_p_value']:.4f}")
    print(f"95% CI: [{results['interaction_ci_lower']:.4f}, {results['interaction_ci_upper']:.4f}]")
    
    if results['interaction_p_value'] < 0.05:
        print("✅ SIGNIFICANT interaction effect detected!")
        if results['interaction_effect'] > 0:
            print("📈 The treatments have a POSITIVE synergistic effect when combined.")
        else:
            print("📉 The treatments have a NEGATIVE interaction effect when combined.")
    else:
        print("❌ No significant interaction effect detected.")
        print("The treatments appear to work independently (additive effects).")
    
    return results, model

def create_interaction_plots(df):
    """Create interaction plots to visualize the effects."""
    
    # Calculate means by group for key metrics
    summary = df.groupby('treatment_group').agg({
        'orders_count': 'mean',
        'ordered': 'mean', 
        'total_variable_profit': 'mean',
        'explored_after_exposure': 'mean'
    }).round(4)
    
    # Create interaction plots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Interaction Effects Visualization', fontsize=16, fontweight='bold')
    
    metrics = [
        ('orders_count', 'Average Orders Count'),
        ('ordered', 'Order Rate'), 
        ('total_variable_profit', 'Average Variable Profit'),
        ('explored_after_exposure', 'Exploration Rate')
    ]
    
    for idx, (metric, title) in enumerate(metrics):
        ax = axes[idx // 2, idx % 2]
        
        # Prepare data for interaction plot
        exp1_control = [summary.loc['Both Control', metric], summary.loc['Exp2 Only', metric]]
        exp1_treatment = [summary.loc['Exp1 Only', metric], summary.loc['Both Treatment', metric]]
        exp2_levels = ['Control', 'Treatment']
        
        # Create interaction plot
        ax.plot(exp2_levels, exp1_control, 'o-', label='Exp1 Control', linewidth=2, markersize=8)
        ax.plot(exp2_levels, exp1_treatment, 's-', label='Exp1 Treatment', linewidth=2, markersize=8)
        
        ax.set_xlabel('Experiment 2 Status')
        ax.set_ylabel(title)
        ax.set_title(f'{title} - Interaction Effect')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Check for interaction (parallel lines = no interaction)
        slope_control = exp1_control[1] - exp1_control[0]
        slope_treatment = exp1_treatment[1] - exp1_treatment[0] 
        
        if abs(slope_control - slope_treatment) > 0.001:  # Some threshold for "different" slopes
            ax.text(0.5, 0.95, 'Lines not parallel\n→ Interaction present', 
                   transform=ax.transAxes, ha='center', va='top',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))
    
    plt.tight_layout()
    
    # Save plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plot_path = f'user-analysis/should_pin_leaderboard_carousel/plots/interaction_effects_{timestamp}.png'
    os.makedirs(os.path.dirname(plot_path), exist_ok=True)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 Interaction plots saved to: {plot_path}")
    
    return summary

def calculate_effect_sizes(df):
    """Calculate effect sizes and practical significance."""
    
    print(f"\n=== EFFECT SIZES & PRACTICAL SIGNIFICANCE ===")
    
    # Group means
    group_means = df.groupby('treatment_group').agg({
        'orders_count': ['mean', 'std'],
        'ordered': ['mean', 'std'],
        'total_variable_profit': ['mean', 'std']
    })
    
    # Calculate Cohen's d for interaction effect
    both_treatment = df[df['treatment_group'] == 'Both Treatment']
    both_control = df[df['treatment_group'] == 'Both Control']
    exp1_only = df[df['treatment_group'] == 'Exp1 Only'] 
    exp2_only = df[df['treatment_group'] == 'Exp2 Only']
    
    def cohens_d(group1, group2, metric):
        """Calculate Cohen's d effect size."""
        mean1, mean2 = group1[metric].mean(), group2[metric].mean()
        std1, std2 = group1[metric].std(), group2[metric].std()
        n1, n2 = len(group1), len(group2)
        
        # Pooled standard deviation
        pooled_std = np.sqrt(((n1-1)*std1**2 + (n2-1)*std2**2) / (n1+n2-2))
        
        return (mean1 - mean2) / pooled_std if pooled_std > 0 else 0
    
    # Interaction effect sizes
    print("\nEffect Sizes (Cohen's d):")
    print("=" * 40)
    
    for metric in ['orders_count', 'ordered', 'total_variable_profit']:
        # Calculate interaction effect as difference of differences
        d_interaction = (cohens_d(both_treatment, exp1_only, metric) - 
                        cohens_d(exp2_only, both_control, metric))
        
        print(f"{metric}: {d_interaction:.3f}")
        
        # Interpretation
        if abs(d_interaction) < 0.2:
            interpretation = "Small effect"
        elif abs(d_interaction) < 0.5:
            interpretation = "Medium effect" 
        elif abs(d_interaction) < 0.8:
            interpretation = "Large effect"
        else:
            interpretation = "Very large effect"
            
        print(f"  → {interpretation}")

def main():
    """Main analysis function."""
    
    print("🧪 INTERACTION EFFECT REGRESSION ANALYSIS")
    print("=" * 50)
    
    # Get data
    df = get_regression_data()
    
    # Sample size by group
    print("\n📊 SAMPLE SIZES BY GROUP:")
    print(df['treatment_group'].value_counts().sort_index())
    
    # Define outcomes to analyze
    outcomes = [
        ('orders_count', 'Orders Count'),
        ('ordered', 'Order Rate'),
        ('total_variable_profit', 'Variable Profit'),
        ('explored_after_exposure', 'Exploration Rate')
    ]
    
    # Store all results
    all_results = []
    
    # Run regression for each outcome
    for outcome_var, outcome_name in outcomes:
        results, model = run_regression_analysis(df, outcome_var, outcome_name)
        all_results.append(results)
    
    # Create summary table
    results_df = pd.DataFrame(all_results)
    
    print(f"\n=== INTERACTION EFFECTS SUMMARY ===")
    print("=" * 60)
    print(results_df[['outcome', 'interaction_effect', 'interaction_p_value', 
                     'interaction_ci_lower', 'interaction_ci_upper']].to_string(index=False))
    
    # Create visualization
    summary_stats = create_interaction_plots(df)
    print(f"\n📋 GROUP MEANS SUMMARY:")
    print(summary_stats.to_string())
    
    # Effect sizes
    calculate_effect_sizes(df)
    
    print(f"\n✅ Analysis complete!")
    print(f"📁 Check plots folder for visualizations")

if __name__ == "__main__":
    main()
