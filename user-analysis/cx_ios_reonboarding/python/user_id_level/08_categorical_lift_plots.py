"""
Categorical Features Lift Analysis
Creates bar plots for categorical features vs target metrics
- Direct categorical analysis (no binning needed)
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

# Categorical features to analyze
CATEGORICAL_FEATURES = [
    'lifestage',
    'lifestage_bucket',
    'experience',
    'business_vertical_line',
    'region_name',
    'platform',
    'urban_type',
    'frequency_bucket',
    'regularity_pattern',
    'activity_trend',
    'pre_exposure_engagement_level',
    'user_ordering_type'
]

# Target metrics
TARGETS = [
    ('push_system_optin', 'Push Opt-in'),
    ('has_order_post_exposure', 'Order Rate')
]

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
    Get lift and significance data for a categorical feature-target combination
    """
    query = f"""
    WITH metrics AS (
        SELECT
            COALESCE({feature_name}::VARCHAR, 'Unknown') AS feature_value,
            result AS is_treatment,
            COUNT(*) AS total_count,
            SUM({target_name}) AS success_count,
            AVG({target_name}) * 100.0 AS rate
        FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
        WHERE result IS NOT NULL
        GROUP BY feature_value, result
    )
    
    SELECT
        feature_value,
        
        -- Treatment metrics
        MAX(CASE WHEN is_treatment = TRUE THEN total_count END) AS treatment_total,
        MAX(CASE WHEN is_treatment = TRUE THEN success_count END) AS treatment_success,
        MAX(CASE WHEN is_treatment = TRUE THEN rate END) AS treatment_rate,
        
        -- Control metrics
        MAX(CASE WHEN is_treatment = FALSE THEN total_count END) AS control_total,
        MAX(CASE WHEN is_treatment = FALSE THEN success_count END) AS control_success,
        MAX(CASE WHEN is_treatment = FALSE THEN rate END) AS control_rate
        
    FROM metrics
    GROUP BY feature_value
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

def create_dual_axis_plot(df, feature_name, target_name, target_label, ax):
    """
    Create a dual-axis bar plot with volume and lift
    X-axis: Categorical values (sorted by volume)
    Y-axis Left: Volume (bars)
    Y-axis Right: Lift % (line)
    """
    # Limit to top categories by volume
    MAX_CATEGORIES = 15
    df_plot = df.nlargest(MAX_CATEGORIES, 'total_volume').copy()
    
    if len(df_plot) == 0:
        ax.text(0.5, 0.5, 'No data available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f'{feature_name} vs {target_label}')
        return
    
    # Sort by volume descending
    df_plot = df_plot.sort_values('total_volume', ascending=False)
    
    # Truncate labels if too long
    df_plot['feature_label'] = df_plot['feature_value'].astype(str).str[:30]
    
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
    if len(df) > MAX_CATEGORIES:
        title += f'\n(Top {MAX_CATEGORIES} of {len(df)} categories by volume)'
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
    print("CATEGORICAL FEATURES LIFT ANALYSIS")
    print("=" * 80)
    print()
    
    hook = SnowflakeHook()
    
    # Create PDF
    output_pdf = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/user_id_level/categorical_lift_analysis.pdf'
    
    with PdfPages(output_pdf) as pdf:
        total_plots = len(CATEGORICAL_FEATURES) * len(TARGETS)
        plot_num = 0
        
        for feature in CATEGORICAL_FEATURES:
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
                    print(f"✓ ({n_sig} significant categories)")
                    
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
    print(f"✓ Features analyzed: {len(CATEGORICAL_FEATURES)}")
    print(f"✓ Target metrics: {len(TARGETS)}")
    print("\nNote: * indicates statistically significant at p < 0.05")

if __name__ == "__main__":
    main()


