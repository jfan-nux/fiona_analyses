"""
XLearner Top Features - Order Rate Analysis
For top features from XLearner, create plots showing:
1. Order rate by bin of the feature
2. Lift in order rate by bin of the feature

Uses same dual-axis format as step 06:
- X-axis: Feature bins (ordered logically)
- Left Y-axis: Volume (bars)
- Right Y-axis: Lift % (line with percentage labels)
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

def read_top_features(csv_path, top_n=15):
    """Read top N features from XLearner importance CSV"""
    df = pd.read_csv(csv_path)
    # Sort by average importance
    df = df.sort_values('avg_importance', ascending=False)
    return df.head(top_n)['feature'].tolist()

def get_feature_binning(feature_name):
    """
    Define binning logic for each feature type
    """
    # Days-based features (0-90d consolidated)
    if 'days' in feature_name.lower():
        if 'tenure' in feature_name or 'since_first_order' in feature_name:
            return """
            CASE 
                WHEN {feature} IS NULL THEN 'Unknown'
                WHEN {feature} < 90 THEN '0-90 days'
                WHEN {feature} < 180 THEN '90-180 days'
                WHEN {feature} < 365 THEN '180-365 days'
                WHEN {feature} < 730 THEN '1-2 years'
                ELSE '2+ years'
            END
            """.format(feature=feature_name)
        elif 'since_last_order' in feature_name:
            return """
            CASE 
                WHEN {feature} IS NULL THEN 'Never ordered'
                WHEN {feature} < 90 THEN '0-90 days'
                WHEN {feature} < 180 THEN '90-180 days'
                WHEN {feature} < 365 THEN '180-365 days'
                ELSE '365+ days'
            END
            """.format(feature=feature_name)
        elif 'before_exposure' in feature_name:
            return """
            CASE 
                WHEN {feature} < 90 THEN '0-90 days'
                WHEN {feature} < 120 THEN '90-120 days'
                WHEN {feature} < 150 THEN '120-150 days'
                WHEN {feature} < 180 THEN '150-180 days'
                WHEN {feature} < 365 THEN '180-365 days'
                ELSE '365+ days'
            END
            """.format(feature=feature_name)
        elif 'gap_between_orders' in feature_name or 'between_orders' in feature_name:
            return """
            CASE 
                WHEN {feature} IS NULL THEN 'No orders'
                WHEN {feature} < 7 THEN '0-7 days'
                WHEN {feature} < 14 THEN '7-14 days'
                WHEN {feature} < 30 THEN '14-30 days'
                WHEN {feature} < 60 THEN '30-60 days'
                WHEN {feature} < 90 THEN '60-90 days'
                ELSE '90+ days'
            END
            """.format(feature=feature_name)
        else:
            # Generic days binning
            return """
            CASE 
                WHEN {feature} IS NULL THEN 'Unknown'
                WHEN {feature} < 30 THEN '0-30'
                WHEN {feature} < 90 THEN '30-90'
                WHEN {feature} < 180 THEN '90-180'
                ELSE '180+'
            END
            """.format(feature=feature_name)
    
    # Count-based features
    elif 'count' in feature_name.lower() or 'visitor' in feature_name.lower():
        return """
        CASE 
            WHEN {feature} IS NULL THEN '0'
            WHEN {feature} = 0 THEN '0'
            WHEN {feature} = 1 THEN '1'
            WHEN {feature} <= 5 THEN '2-5'
            WHEN {feature} <= 10 THEN '6-10'
            ELSE '11+'
        END
        """.format(feature=feature_name)
    
    # Percentage features
    elif 'pct' in feature_name.lower() or 'percent' in feature_name.lower():
        return """
        CASE 
            WHEN {feature} IS NULL THEN 'Unknown'
            WHEN {feature} < 20 THEN '0-20%'
            WHEN {feature} < 40 THEN '20-40%'
            WHEN {feature} < 60 THEN '40-60%'
            WHEN {feature} < 80 THEN '60-80%'
            ELSE '80-100%'
        END
        """.format(feature=feature_name)
    
    # Spend/monetary features
    elif 'spend' in feature_name.lower():
        return """
        CASE 
            WHEN {feature} IS NULL OR {feature} = 0 THEN '$0'
            WHEN {feature} < 50 THEN '$1-50'
            WHEN {feature} < 100 THEN '$50-100'
            WHEN {feature} < 200 THEN '$100-200'
            WHEN {feature} < 500 THEN '$200-500'
            ELSE '$500+'
        END
        """.format(feature=feature_name)
    
    # Orders count features
    elif 'orders' in feature_name.lower():
        return """
        CASE 
            WHEN {feature} IS NULL OR {feature} = 0 THEN '0 orders'
            WHEN {feature} = 1 THEN '1 order'
            WHEN {feature} <= 3 THEN '2-3 orders'
            WHEN {feature} <= 5 THEN '4-5 orders'
            WHEN {feature} <= 10 THEN '6-10 orders'
            ELSE '11+ orders'
        END
        """.format(feature=feature_name)
    
    else:
        # Generic numeric binning (quartiles will be calculated)
        return None  # Will use pandas qcut instead

def calculate_significance(treatment_success, treatment_total, control_success, control_total):
    """Calculate statistical significance using z-test for proportions"""
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

def get_feature_order_rate_data(feature_name, hook):
    """Get order rate data by feature bins"""
    
    bin_sql = get_feature_binning(feature_name)
    
    # Filter out nulls for funnel features
    funnel_features = ['days_before_exposure_store_content', 'days_before_exposure_store_page',
                       'days_before_exposure_order_cart', 'days_before_exposure_checkout',
                       'days_before_exposure_purchase']
    
    if feature_name in funnel_features:
        where_clause = f"WHERE result IS NOT NULL AND {feature_name} IS NOT NULL"
    else:
        where_clause = "WHERE result IS NOT NULL"
    
    if bin_sql:
        # Use predefined binning
        query = f"""
        WITH metrics AS (
            SELECT
                {bin_sql} AS feature_bin,
                result AS is_treatment,
                COUNT(*) AS total_count,
                SUM(has_order_post_exposure) AS success_count,
                AVG(has_order_post_exposure) * 100.0 AS order_rate
            FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
            {where_clause}
            GROUP BY feature_bin, result
        )
        
        SELECT
            feature_bin,
            
            -- Treatment metrics
            MAX(CASE WHEN is_treatment = TRUE THEN total_count END) AS treatment_total,
            MAX(CASE WHEN is_treatment = TRUE THEN success_count END) AS treatment_success,
            MAX(CASE WHEN is_treatment = TRUE THEN order_rate END) AS treatment_rate,
            
            -- Control metrics
            MAX(CASE WHEN is_treatment = FALSE THEN total_count END) AS control_total,
            MAX(CASE WHEN is_treatment = FALSE THEN success_count END) AS control_success,
            MAX(CASE WHEN is_treatment = FALSE THEN order_rate END) AS control_rate
            
        FROM metrics
        GROUP BY feature_bin
        ORDER BY 
            COALESCE(MAX(CASE WHEN is_treatment = TRUE THEN total_count END), 0) + 
            COALESCE(MAX(CASE WHEN is_treatment = FALSE THEN total_count END), 0) DESC
        """
    else:
        # Use quartile binning for generic numeric features
        query = f"""
        WITH percentiles AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {feature_name}) AS p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {feature_name}) AS p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {feature_name}) AS p75
            FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
            WHERE {feature_name} IS NOT NULL
        ),
        metrics AS (
            SELECT
                CASE 
                    WHEN m.{feature_name} IS NULL THEN 'Unknown'
                    WHEN m.{feature_name} <= p.p25 THEN 'Q1 (Low)'
                    WHEN m.{feature_name} <= p.p50 THEN 'Q2 (Med-Low)'
                    WHEN m.{feature_name} <= p.p75 THEN 'Q3 (Med-High)'
                    ELSE 'Q4 (High)'
                END AS feature_bin,
                result AS is_treatment,
                COUNT(*) AS total_count,
                SUM(has_order_post_exposure) AS success_count,
                AVG(has_order_post_exposure) * 100.0 AS order_rate
            FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level_user_level m
            CROSS JOIN percentiles p
            WHERE result IS NOT NULL
            GROUP BY feature_bin, result
        )
        
        SELECT
            feature_bin,
            
            -- Treatment metrics
            MAX(CASE WHEN is_treatment = TRUE THEN total_count END) AS treatment_total,
            MAX(CASE WHEN is_treatment = TRUE THEN success_count END) AS treatment_success,
            MAX(CASE WHEN is_treatment = TRUE THEN order_rate END) AS treatment_rate,
            
            -- Control metrics
            MAX(CASE WHEN is_treatment = FALSE THEN total_count END) AS control_total,
            MAX(CASE WHEN is_treatment = FALSE THEN success_count END) AS control_success,
            MAX(CASE WHEN is_treatment = FALSE THEN order_rate END) AS control_rate
            
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

def create_dual_axis_plot(df, feature_name, ax):
    """
    Create dual-axis plot showing order rate lift
    X-axis: Feature bins
    Y-axis Left: Volume (bars)
    Y-axis Right: Lift % (line)
    """
    df_plot = df.copy()
    
    if len(df_plot) == 0:
        ax.text(0.5, 0.5, 'No data available', 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f'{feature_name} → Order Rate')
        return
    
    # Truncate bin labels if too long
    df_plot['feature_label'] = df_plot['feature_bin'].astype(str).str[:25]
    
    # Sort by volume (descending) for better visibility
    df_plot = df_plot.sort_values('total_volume', ascending=False)
    
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
    
    # Plot line
    line = ax2.plot(positions, lift_values, 
                    'o-', color='black', linewidth=2.5, markersize=10, 
                    label='Lift %', alpha=0.8, zorder=5)
    
    # Color the markers
    for i, (pos, val, color) in enumerate(zip(positions, lift_values, colors)):
        ax2.plot(pos, val, 'o', color=color, markersize=10, alpha=0.8, zorder=6)
    
    # Add horizontal line at 0 for lift
    ax2.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.5, zorder=4)
    
    # Add percentage labels
    for i, (idx, row) in enumerate(df_plot.iterrows()):
        y_pos = row['lift_pct'] if pd.notna(row['lift_pct']) else 0
        
        if pd.notna(row['lift_pct']):
            pct_label = f"{row['lift_pct']:.1f}%"
            if row['is_significant']:
                pct_label += '*'
            
            # Smart positioning
            y_min, y_max = ax2.get_ylim()
            y_range = float(y_max) - float(y_min)
            
            if y_pos > 0:
                y_offset = max(y_range * 0.03, 0.5)
                va = 'bottom'
            else:
                y_offset = -max(y_range * 0.03, 0.5)
                va = 'top'
            
            ax2.text(i, float(y_pos) + y_offset, pct_label, fontsize=8, fontweight='bold', 
                   ha='center', va=va, color='black', zorder=7,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                            edgecolor='none', alpha=0.7))
    
    # Formatting
    ax.set_xticks(positions)
    ax.set_xticklabels(df_plot['feature_label'], rotation=45, ha='right', fontsize=8)
    ax.set_ylabel('Total Volume (Treatment + Control)', fontsize=10, color='steelblue')
    ax.tick_params(axis='y', labelcolor='steelblue')
    ax.grid(axis='y', alpha=0.2)
    
    ax2.set_ylabel('Order Rate Lift (%)', fontsize=10, color='black', fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.grid(False)
    
    # Title
    ax.set_title(f'{feature_name} → Order Rate', fontsize=11, fontweight='bold', pad=10)
    
    # Add legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, 
             loc='upper right', fontsize=8)
    
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Add padding
    y_min, y_max = ax2.get_ylim()
    y_range = float(y_max) - float(y_min)
    ax2.set_ylim(float(y_min) - y_range * 0.15, float(y_max) + y_range * 0.15)

def main():
    print("=" * 80)
    print("XLEARNER TOP FEATURES - ORDER RATE LIFT ANALYSIS")
    print("=" * 80)
    print()
    
    # Read top features from XLearner
    csv_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs/user_id_level/04_xlearner_residual_feature_importance.csv'
    top_features = read_top_features(csv_path, top_n=20)
    
    print(f"Analyzing top {len(top_features)} features from XLearner:")
    for i, feat in enumerate(top_features, 1):
        print(f"  {i}. {feat}")
    print()
    
    hook = SnowflakeHook()
    
    # Create PDF
    output_pdf = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/user_id_level/xlearner_features_order_rate_lift.pdf'
    
    with PdfPages(output_pdf) as pdf:
        for i, feature in enumerate(top_features, 1):
            print(f"\n[{i}/{len(top_features)}] Analyzing: {feature}... ", end='')
            
            try:
                # Get data
                df = get_feature_order_rate_data(feature, hook)
                
                # Create figure with single plot
                fig, ax = plt.subplots(1, 1, figsize=(14, 8))
                
                # Create plot
                create_dual_axis_plot(df, feature, ax)
                
                # Count significant bins
                n_sig = df['is_significant'].sum()
                print(f"✓ ({n_sig} significant bins)")
                
                plt.tight_layout(pad=2.0)
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)
                
            except Exception as e:
                print(f"✗ Error: {e}")
                # Create error page
                fig, ax = plt.subplots(1, 1, figsize=(14, 8))
                ax.text(0.5, 0.5, f'Error processing {feature}:\n{str(e)[:200]}', 
                       ha='center', va='center', fontsize=10)
                ax.set_title(f'{feature} → Order Rate')
                ax.axis('off')
                pdf.savefig(fig, bbox_inches='tight')
                plt.close(fig)
    
    hook.close()
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\n✓ PDF saved to: {output_pdf}")
    print(f"✓ Features analyzed: {len(top_features)}")
    print("\nNote: * indicates statistically significant at p < 0.05")

if __name__ == "__main__":
    main()

