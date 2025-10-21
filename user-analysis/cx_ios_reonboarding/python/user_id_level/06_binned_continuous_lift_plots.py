"""
Binned Continuous Features Lift Analysis
Creates bar plots for binned continuous features vs target metrics
- Bins days-related features into meaningful categories
- Dual axis: Volume (bars) and Lift % (line)
- Statistical significance marked with asterisk
- All plots combined into single PDF
"""

import sys
import os
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

from utils.snowflake_connection import SnowflakeHook
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
from scipy import stats

# Set plotting style
sns.set_style("whitegrid")

# Continuous features to bin
CONTINUOUS_FEATURES = [
    'tenure_days_at_exposure',
    'days_since_first_order',
    'days_since_last_order',
    'days_before_exposure_store_content'
]

# Target metrics
TARGETS = [
    ('push_system_optin', 'Push Opt-in'),
    ('has_order_post_exposure', 'Order Rate')
]

def get_binning_query(feature_name):
    """
    Generate SQL query to bin continuous features into categories
    """
    if feature_name == 'tenure_days_at_exposure':
        bin_sql = """
        CASE 
            WHEN {feature} IS NULL THEN 'Unknown'
            WHEN {feature} < 30 THEN '0-30 days'
            WHEN {feature} < 90 THEN '30-90 days'
            WHEN {feature} < 180 THEN '90-180 days'
            WHEN {feature} < 365 THEN '180-365 days'
            WHEN {feature} < 730 THEN '1-2 years'
            ELSE '2+ years'
        END
        """.format(feature=feature_name)
    
    elif feature_name == 'days_since_first_order':
        bin_sql = """
        CASE 
            WHEN {feature} IS NULL THEN 'Never ordered'
            WHEN {feature} < 7 THEN '0-7 days'
            WHEN {feature} < 30 THEN '7-30 days'
            WHEN {feature} < 90 THEN '30-90 days'
            WHEN {feature} < 180 THEN '90-180 days'
            WHEN {feature} < 365 THEN '180-365 days'
            WHEN {feature} < 730 THEN '1-2 years'
            ELSE '2+ years'
        END
        """.format(feature=feature_name)
    
    elif feature_name == 'days_since_last_order':
        bin_sql = """
        CASE 
            WHEN {feature} IS NULL THEN 'Never ordered'
            WHEN {feature} < 7 THEN '0-7 days'
            WHEN {feature} < 14 THEN '7-14 days'
            WHEN {feature} < 30 THEN '14-30 days'
            WHEN {feature} < 60 THEN '30-60 days'
            WHEN {feature} < 90 THEN '60-90 days'
            WHEN {feature} < 180 THEN '90-180 days'
            WHEN {feature} < 365 THEN '180-365 days'
            ELSE '365+ days'
        END
        """.format(feature=feature_name)
    
    elif feature_name == 'days_before_exposure_store_content':
        bin_sql = """
        CASE 
            WHEN {feature} IS NULL THEN 'No visit'
            WHEN {feature} < 1 THEN 'Same day'
            WHEN {feature} < 7 THEN '1-7 days'
            WHEN {feature} < 14 THEN '7-14 days'
            WHEN {feature} < 30 THEN '14-30 days'
            WHEN {feature} < 60 THEN '30-60 days'
            WHEN {feature} < 90 THEN '60-90 days'
            WHEN {feature} < 120 THEN '90-120 days'
            WHEN {feature} < 150 THEN '120-150 days'
            WHEN {feature} < 180 THEN '150-180 days'
            WHEN {feature} < 365 THEN '180-365 days'
            ELSE '365+ days'
        END
        """.format(feature=feature_name)
    
    else:
        # Default binning for any other continuous feature
        bin_sql = """
        CASE 
            WHEN {feature} IS NULL THEN 'Unknown'
            WHEN {feature} < 30 THEN '0-30'
            WHEN {feature} < 90 THEN '30-90'
            WHEN {feature} < 180 THEN '90-180'
            ELSE '180+'
        END
        """.format(feature=feature_name)
    
    return bin_sql

def calculate_significance(treatment_success, treatment_total, control_success, control_total):
    """
    Calculate statistical significance using z-test for proportions
    Returns p-value and whether it's significant at 0.05 level
    """
    if treatment_total == 0 or control_total == 0:
        return 1.0, False
    
    p1 = treatment_success / treatment_total
    p2 = control_success / control_total
    
    # Pooled proportion
    p_pool = (treatment_success + control_success) / (treatment_total + control_total)
    
    # Standard error
    se = np.sqrt(p_pool * (1 - p_pool) * (1/treatment_total + 1/control_total))
    
    if se == 0:
        return 1.0, False
    
    # Z-statistic
    z = (p1 - p2) / se
    
    # Two-tailed p-value
    p_value = 2 * (1 - stats.norm.cdf(abs(z)))
    
    is_significant = p_value < 0.05
    
    return p_value, is_significant

def get_feature_target_data(feature_name, target_name, hook):
    """
    Get lift and significance data for a binned feature-target combination
    """
    bin_sql = get_binning_query(feature_name)
    
    query = f"""
    WITH metrics AS (
        SELECT
            {bin_sql} AS feature_bin,
            result AS is_treatment,
            COUNT(*) AS total_count,
            SUM({target_name}) AS success_count,
            AVG({target_name}) * 100.0 AS rate
            FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
        WHERE result IS NOT NULL
        GROUP BY feature_bin, result
    )
    
    SELECT
        feature_bin,
        
        -- Treatment metrics
        MAX(CASE WHEN is_treatment = TRUE THEN total_count END) AS treatment_total,
        MAX(CASE WHEN is_treatment = TRUE THEN success_count END) AS treatment_success,
        MAX(CASE WHEN is_treatment = TRUE THEN rate END) AS treatment_rate,
        
        -- Control metrics
        MAX(CASE WHEN is_treatment = FALSE THEN total_count END) AS control_total,
        MAX(CASE WHEN is_treatment = FALSE THEN success_count END) AS control_success,
        MAX(CASE WHEN is_treatment = FALSE THEN rate END) AS control_rate
        
    FROM metrics
    GROUP BY feature_bin
    ORDER BY 
        COALESCE(MAX(CASE WHEN is_treatment = TRUE THEN total_count END), 0) + 
        COALESCE(MAX(CASE WHEN is_treatment = FALSE THEN total_count END), 0) DESC
    """
    
    df = hook.query_snowflake(query, method='pandas')
    
    # Calculate lift and significance
    df['lift_pct'] = df.apply(
        lambda row: ((row['treatment_rate'] - row['control_rate']) / row['control_rate'] * 100)
        if row['control_rate'] and row['control_rate'] > 0 else None,
        axis=1
    )
    
    df['total_volume'] = df['treatment_total'] + df['control_total']
    
    # Calculate significance
    sig_results = df.apply(
        lambda row: calculate_significance(
            row['treatment_success'], row['treatment_total'],
            row['control_success'], row['control_total']
        ),
        axis=1
    )
    
    df['p_value'] = sig_results.apply(lambda x: x[0])
    df['is_significant'] = sig_results.apply(lambda x: x[1])
    
    return df

def get_bin_order(feature_name):
    """
    Define the chronological order for bins (most recent to least recent)
    """
    if feature_name == 'tenure_days_at_exposure':
        return ['0-30 days', '30-90 days', '90-180 days', '180-365 days', '1-2 years', '2+ years', 'Unknown']
    
    elif feature_name == 'days_since_first_order':
        return ['0-7 days', '7-30 days', '30-90 days', '90-180 days', '180-365 days', '1-2 years', '2+ years', 'Never ordered']
    
    elif feature_name == 'days_since_last_order':
        return ['0-7 days', '7-14 days', '14-30 days', '30-60 days', '60-90 days', '90-180 days', '180-365 days', '365+ days', 'Never ordered']
    
    elif feature_name == 'days_before_exposure_store_content':
        return ['Same day', '1-7 days', '7-14 days', '14-30 days', '30-60 days', '60-90 days', 
                '90-120 days', '120-150 days', '150-180 days', '180-365 days', '365+ days', 'No visit']
    
    else:
        return None

def create_dual_axis_plot(df, feature_name, target_name, target_label, ax):
    """
    Create a dual-axis bar plot with volume and lift
    X-axis: Binned categories (chronological order - most recent to least recent)
    Y-axis Left: Volume (bars)
    Y-axis Right: Lift % (line)
    """
    # Take all bins (most continuous features won't have more than 10-15 bins)
    df_plot = df.copy()
    
    if len(df_plot) == 0:
        ax.text(0.5, 0.5, 'No data available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f'{feature_name} vs {target_label}')
        return
    
    # Sort by chronological order (most recent to least recent)
    bin_order = get_bin_order(feature_name)
    if bin_order:
        # Create a categorical type with the desired order
        df_plot['feature_bin'] = pd.Categorical(df_plot['feature_bin'], 
                                                  categories=bin_order, 
                                                  ordered=True)
        df_plot = df_plot.sort_values('feature_bin')
        # Remove any NaN rows that don't match the categories
        df_plot = df_plot.dropna(subset=['feature_bin'])
    else:
        # Fallback to volume sorting if no order defined
        df_plot = df_plot.sort_values('total_volume', ascending=False)
    
    # Truncate bin labels if too long
    df_plot['feature_label'] = df_plot['feature_bin'].astype(str).str[:25]
    
    # Create positions
    positions = np.arange(len(df_plot))
    
    # Create dual axis (second Y-axis for lift)
    ax2 = ax.twinx()
    
    # Plot volume as bars (left Y-axis)
    bars = ax.bar(positions, df_plot['total_volume'], 
                  color='lightblue', alpha=0.6, label='Volume', width=0.6)
    
    # Plot lift as line (right Y-axis)
    lift_values = df_plot['lift_pct'].fillna(0)
    colors = ['green' if x > 0 else 'red' if x < 0 else 'gray' for x in lift_values]
    
    # Plot line with color based on positive/negative
    line = ax2.plot(positions, lift_values, 
                    'o-', color='black', linewidth=2.5, markersize=10, 
                    label='Lift %', alpha=0.8, zorder=5)
    
    # Color the markers based on positive/negative
    for i, (pos, val, color) in enumerate(zip(positions, lift_values, colors)):
        ax2.plot(pos, val, 'o', color=color, markersize=10, alpha=0.8, zorder=6)
    
    # Add horizontal line at 0 for lift
    ax2.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.5, zorder=4)
    
    # Add asterisks for significant results
    for i, (idx, row) in enumerate(df_plot.iterrows()):
        if row['is_significant']:
            # Add asterisk next to the lift marker
            y_pos = row['lift_pct'] if pd.notna(row['lift_pct']) else 0
            ax2.text(i, y_pos, ' *', fontsize=20, fontweight='bold', 
                   ha='left', va='center', color='black', zorder=7)
    
    # Formatting
    ax.set_xticks(positions)
    ax.set_xticklabels(df_plot['feature_label'], rotation=45, ha='right', fontsize=8)
    ax.set_ylabel('Total Volume (Treatment + Control)', fontsize=10, color='steelblue')
    ax.tick_params(axis='y', labelcolor='steelblue')
    ax.grid(axis='y', alpha=0.2)
    
    ax2.set_ylabel('Lift (%)', fontsize=10, color='black', fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.grid(False)
    
    # Title
    title = f'{feature_name} → {target_label}'
    ax.set_title(title, fontsize=11, fontweight='bold', pad=10)
    
    # Add legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, 
             loc='upper right', fontsize=8)
    
    # Adjust layout to prevent label cutoff
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

def main():
    print("=" * 80)
    print("BINNED CONTINUOUS FEATURES LIFT ANALYSIS")
    print("=" * 80)
    print()
    
    hook = SnowflakeHook()
    
    # Create PDF
    output_pdf = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/user_id_level/binned_continuous_lift_analysis.pdf'
    
    with PdfPages(output_pdf) as pdf:
        total_plots = len(CONTINUOUS_FEATURES) * len(TARGETS)
        plot_num = 0
        
        for feature in CONTINUOUS_FEATURES:
            print(f"\n{'='*60}")
            print(f"Processing feature: {feature}")
            print(f"{'='*60}")
            
            # Create a figure with 2 subplots (one for each target)
            fig, axes = plt.subplots(2, 1, figsize=(14, 12))
            fig.suptitle(f'Feature: {feature}', fontsize=16, fontweight='bold', y=0.995)
            
            for idx, (target_name, target_label) in enumerate(TARGETS):
                plot_num += 1
                print(f"  [{plot_num}/{total_plots}] {feature} vs {target_label}... ", end='')
                
                try:
                    # Get data
                    df = get_feature_target_data(feature, target_name, hook)
                    
                    # Create plot
                    create_dual_axis_plot(df, feature, target_name, target_label, axes[idx])
                    
                    # Count significant segments
                    n_sig = df['is_significant'].sum()
                    print(f"✓ ({n_sig} significant bins)")
                    
                except Exception as e:
                    print(f"✗ Error: {e}")
                    axes[idx].text(0.5, 0.5, f'Error: {str(e)[:100]}', 
                                  ha='center', va='center', 
                                  transform=axes[idx].transAxes)
            
            plt.tight_layout()
            pdf.savefig(fig, bbox_inches='tight')
            plt.close(fig)
    
    hook.close()
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\n✓ PDF saved to: {output_pdf}")
    print(f"✓ Total plots: {total_plots}")
    print(f"✓ Features analyzed: {len(CONTINUOUS_FEATURES)}")
    print(f"✓ Target metrics: {len(TARGETS)}")
    print("\nNote: * indicates statistically significant at p < 0.05")

if __name__ == "__main__":
    main()

