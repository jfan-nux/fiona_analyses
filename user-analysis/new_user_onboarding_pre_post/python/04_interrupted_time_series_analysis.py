"""
Interrupted Time Series (ITS) Analysis for Causal Impact Estimation
Estimates the incremental impact of the 9/25/2025 onboarding change
"""

import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose
from utils.snowflake_connection import SnowflakeHook

def fetch_funnel_data():
    """Fetch funnel data from Snowflake"""
    
    query = """
    WITH onboarding_top_of_funnel AS (
        SELECT DISTINCT
            DATE(iguazu_partition_date) AS event_date,
            replace(lower(CASE WHEN dd_device_id like 'dx_%' then dd_device_id else 'dx_'||dd_device_id end), '-') AS dd_device_id_filtered,
            consumer_id,
            dd_platform AS platform
        FROM iguazu.consumer.m_onboarding_start_promo_page_view_ice
        WHERE iguazu_partition_date >= '2025-09-12'
            AND iguazu_partition_date <= '2025-10-07'
    ),

    funnel_data AS (
        SELECT 
            o.event_date,
            o.dd_device_id_filtered,
            o.consumer_id,
            o.platform,
            COALESCE(f.unique_store_content_page_visitor, 0) AS unique_store_content_page_visitor,
            COALESCE(f.unique_store_page_visitor, 0) AS unique_store_page_visitor,
            COALESCE(f.unique_order_cart_page_visitor, 0) AS unique_order_cart_page_visitor,
            COALESCE(f.unique_checkout_page_visitor, 0) AS unique_checkout_page_visitor,
            COALESCE(f.unique_purchaser, 0) AS unique_purchaser,
            CASE 
                WHEN o.event_date < '2025-09-25' THEN 'pre'
                ELSE 'post'
            END AS period
        FROM onboarding_top_of_funnel o
        LEFT JOIN proddb.public.fact_unique_visitors_full_pt f
            ON o.dd_device_id_filtered = replace(lower(CASE WHEN f.dd_device_id like 'dx_%' then f.dd_device_id else 'dx_'||f.dd_device_id end), '-')
            AND o.event_date = f.event_date
    ),

    daily_metrics AS (
        SELECT
            event_date,
            period,
            platform,
            COUNT(DISTINCT dd_device_id_filtered) AS unique_onboarding_visitors,
            COUNT(DISTINCT CASE WHEN unique_store_page_visitor = 1 THEN dd_device_id_filtered END) AS store_page_visitors,
            COUNT(DISTINCT CASE WHEN unique_store_content_page_visitor = 1 THEN dd_device_id_filtered END) AS store_content_visitors,
            COUNT(DISTINCT CASE WHEN unique_order_cart_page_visitor = 1 THEN dd_device_id_filtered END) AS cart_visitors,
            COUNT(DISTINCT CASE WHEN unique_checkout_page_visitor = 1 THEN dd_device_id_filtered END) AS checkout_visitors,
            COUNT(DISTINCT CASE WHEN unique_purchaser = 1 THEN dd_device_id_filtered END) AS purchasers,
            DIV0(COUNT(DISTINCT CASE WHEN unique_store_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
                 COUNT(DISTINCT dd_device_id_filtered)) AS store_page_cvr,
            DIV0(COUNT(DISTINCT CASE WHEN unique_store_content_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
                 COUNT(DISTINCT dd_device_id_filtered)) AS store_content_cvr,
            DIV0(COUNT(DISTINCT CASE WHEN unique_order_cart_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
                 COUNT(DISTINCT dd_device_id_filtered)) AS cart_cvr,
            DIV0(COUNT(DISTINCT CASE WHEN unique_checkout_page_visitor = 1 THEN dd_device_id_filtered END) * 100.0,
                 COUNT(DISTINCT dd_device_id_filtered)) AS checkout_cvr,
            DIV0(COUNT(DISTINCT CASE WHEN unique_purchaser = 1 THEN dd_device_id_filtered END) * 100.0,
                 COUNT(DISTINCT dd_device_id_filtered)) AS purchase_cvr
        FROM funnel_data
        GROUP BY event_date, period, platform
    )

    SELECT 
        event_date,
        period,
        platform,
        unique_onboarding_visitors,
        store_page_visitors,
        store_content_visitors,
        cart_visitors,
        checkout_visitors,
        purchasers,
        ROUND(store_page_cvr, 2) AS store_page_cvr_pct,
        ROUND(store_content_cvr, 2) AS store_content_cvr_pct,
        ROUND(cart_cvr, 2) AS cart_cvr_pct,
        ROUND(checkout_cvr, 2) AS checkout_cvr_pct,
        ROUND(purchase_cvr, 2) AS purchase_cvr_pct
    FROM daily_metrics
    ORDER BY event_date, platform
    """
    
    print("Fetching data from Snowflake...")
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query, method='pandas')
    
    print(f"Fetched {len(df)} rows")
    return df

def prepare_its_data(df, platform='all'):
    """
    Prepare data for Interrupted Time Series analysis
    """
    if platform != 'all':
        df_filtered = df[df['platform'] == platform].copy()
        # Convert CVR columns to float
        for col in ['purchase_cvr_pct', 'store_content_cvr_pct', 'cart_cvr_pct', 'checkout_cvr_pct', 'store_page_cvr_pct']:
            if col in df_filtered.columns:
                df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype(float)
    else:
        # Aggregate across platforms
        df_filtered = df.groupby(['event_date', 'period']).agg({
            'unique_onboarding_visitors': 'sum',
            'store_content_visitors': 'sum',
            'purchasers': 'sum'
        }).reset_index()
        
        # Recalculate CVRs as float
        df_filtered['purchase_cvr_pct'] = (
            df_filtered['purchasers'].astype(float) / df_filtered['unique_onboarding_visitors'].astype(float) * 100
        )
        df_filtered['store_content_cvr_pct'] = (
            df_filtered['store_content_visitors'].astype(float) / df_filtered['unique_onboarding_visitors'].astype(float) * 100
        )
    
    df_filtered['event_date'] = pd.to_datetime(df_filtered['event_date'])
    df_filtered = df_filtered.sort_values('event_date').reset_index(drop=True)
    
    # Create time index (days since start)
    df_filtered['time'] = range(len(df_filtered))
    
    # Create intervention indicator (0=pre, 1=post)
    df_filtered['intervention'] = (df_filtered['period'] == 'post').astype(int)
    
    # Create time since intervention (0 for pre-period, 1,2,3... for post-period)
    df_filtered['time_since_intervention'] = df_filtered.apply(
        lambda row: row['time'] - 13 if row['intervention'] == 1 else 0, axis=1
    )
    
    return df_filtered

def run_its_regression(data, metric_col):
    """
    Run Interrupted Time Series regression
    
    Model: Y = β0 + β1*time + β2*intervention + β3*time_since_intervention + ε
    
    Where:
    - β1 = pre-intervention trend
    - β2 = immediate level change at intervention
    - β3 = change in trend post-intervention
    
    Returns regression results and predictions
    """
    # Prepare design matrix - ensure numeric types
    X = data[['time', 'intervention', 'time_since_intervention']].copy().astype(float)
    X = sm.add_constant(X)  # Add intercept
    y = pd.to_numeric(data[metric_col], errors='coerce')
    
    # Fit OLS regression
    model = sm.OLS(y, X)
    results = model.fit()
    
    # Get predictions
    data['predicted'] = results.fittedvalues
    
    # Calculate counterfactual (what would have happened without intervention)
    # For post-period, set intervention and time_since_intervention to 0
    X_counterfactual = X.copy()
    X_counterfactual['intervention'] = 0
    X_counterfactual['time_since_intervention'] = 0
    data['counterfactual'] = model.predict(results.params, X_counterfactual)
    
    # Calculate causal effect (actual - counterfactual in post-period)
    data['causal_effect'] = np.where(
        data['intervention'] == 1,
        data[metric_col] - data['counterfactual'],
        0
    )
    
    return results, data

def calculate_impact_metrics(data, results, metric_col):
    """
    Calculate impact metrics from ITS results
    """
    post_data = data[data['intervention'] == 1].copy()
    
    # Average daily effect
    avg_daily_effect = post_data['causal_effect'].mean()
    
    # Cumulative effect over post-period
    cum_effect = post_data['causal_effect'].sum()
    
    # Relative effect (as percentage)
    avg_counterfactual = post_data['counterfactual'].mean()
    relative_effect = (avg_daily_effect / avg_counterfactual * 100) if avg_counterfactual != 0 else 0
    
    # Get coefficient estimates
    beta_2 = results.params['intervention']  # Immediate level change
    beta_3 = results.params['time_since_intervention']  # Trend change
    
    # Statistical significance
    p_value_level = results.pvalues['intervention']
    p_value_trend = results.pvalues['time_since_intervention']
    
    metrics = {
        'avg_daily_effect': avg_daily_effect,
        'cum_effect': cum_effect,
        'relative_effect_pct': relative_effect,
        'immediate_level_change': beta_2,
        'trend_change': beta_3,
        'p_value_level': p_value_level,
        'p_value_trend': p_value_trend,
        'r_squared': results.rsquared,
        'adj_r_squared': results.rsquared_adj,
        'avg_counterfactual': avg_counterfactual,
        'avg_actual': post_data[metric_col].mean()
    }
    
    return metrics

def plot_its_results(data, metric_col, metric_name, platform, results_metrics, output_path):
    """
    Create visualization of ITS results
    """
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(f'Interrupted Time Series Analysis: {metric_name} - {platform.upper()}', 
                 fontsize=16, fontweight='bold')
    
    change_date = pd.to_datetime('2025-09-25')
    
    # Plot 1: Observed vs Predicted vs Counterfactual
    ax = axes[0]
    pre_data = data[data['intervention'] == 0]
    post_data = data[data['intervention'] == 1]
    
    # Plot observed data
    ax.plot(pre_data['event_date'], pre_data[metric_col], 
            'o-', color='black', linewidth=2, markersize=6, label='Observed (Pre)')
    ax.plot(post_data['event_date'], post_data[metric_col], 
            'o-', color='blue', linewidth=2, markersize=6, label='Observed (Post)')
    
    # Plot counterfactual
    ax.plot(post_data['event_date'], post_data['counterfactual'], 
            '--', color='red', linewidth=2, label='Counterfactual (predicted without intervention)')
    
    # Plot fitted line for pre-period
    ax.plot(pre_data['event_date'], pre_data['predicted'], 
            '-', color='gray', linewidth=1, alpha=0.5, label='Fitted (Pre)')
    
    # Add vertical line at intervention
    ax.axvline(change_date, color='red', linestyle='--', linewidth=2.5, alpha=0.7, label='Intervention (9/25)')
    
    # Add shaded region for causal effect
    avg_effect = results_metrics['avg_daily_effect']
    effect_color = 'green' if avg_effect > 0 else 'orange'
    ax.fill_between(post_data['event_date'], 
                     post_data['counterfactual'], 
                     post_data[metric_col],
                     alpha=0.3, color=effect_color,
                     label=f"Causal Effect (avg: {avg_effect:+.2f})")
    
    ax.set_title(f'{metric_name}: Observed vs Counterfactual', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel(metric_name)
    ax.legend(loc='best', fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 2: Daily Causal Effect
    ax = axes[1]
    bar_colors = ['green' if x > 0 else 'red' for x in post_data['causal_effect']]
    ax.bar(post_data['event_date'], post_data['causal_effect'], 
           color=bar_colors, alpha=0.7, label='Daily Causal Effect')
    ax.axhline(0, color='black', linestyle='-', linewidth=1)
    avg_effect = results_metrics['avg_daily_effect']
    ax.axhline(avg_effect, color='blue', linestyle='--', 
               linewidth=2, label=f'Average Effect: {avg_effect:+.2f}')
    ax.set_title('Daily Causal Effect (Observed - Counterfactual)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Causal Effect')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"  Plot saved to {output_path}")
    plt.close()

def create_its_report(platform, metric_name, results, metrics, data):
    """
    Create formatted report for ITS results
    """
    output = []
    output.append(f"\n{'='*80}")
    output.append(f"PLATFORM: {platform.upper()} - {metric_name}")
    output.append(f"{'='*80}\n")
    
    # Regression results
    output.append("📊 **REGRESSION MODEL SUMMARY**")
    output.append(f"  Model: Y = β0 + β1*time + β2*intervention + β3*time_since_intervention")
    output.append(f"  R-squared: {metrics['r_squared']:.4f}")
    output.append(f"  Adjusted R-squared: {metrics['adj_r_squared']:.4f}\n")
    
    output.append("  Coefficients:")
    output.append(f"    - Intercept (β0):           {results.params['const']:.4f}")
    output.append(f"    - Pre-trend (β1):           {results.params['time']:.4f} (p={results.pvalues['time']:.4f})")
    output.append(f"    - Level change (β2):        {metrics['immediate_level_change']:+.4f} (p={metrics['p_value_level']:.4f})")
    output.append(f"    - Trend change (β3):        {metrics['trend_change']:+.4f} (p={metrics['p_value_trend']:.4f})")
    
    # Impact estimates
    output.append(f"\n📈 **CAUSAL IMPACT ESTIMATES**")
    output.append(f"\n  Post-Period Average:")
    output.append(f"    - Observed (actual):        {metrics['avg_actual']:.2f}")
    output.append(f"    - Counterfactual (no change): {metrics['avg_counterfactual']:.2f}")
    output.append(f"    - **Causal Effect:          {metrics['avg_daily_effect']:+.2f}**")
    output.append(f"    - **Relative Effect:        {metrics['relative_effect_pct']:+.2f}%**")
    
    output.append(f"\n  Cumulative Impact (13-day post-period):")
    output.append(f"    - **Total Effect:           {metrics['cum_effect']:+,.1f}**")
    
    if metric_name.endswith('(%)'):
        output.append(f"\n  Interpretation:")
        output.append(f"    - The intervention changed the metric by {metrics['avg_daily_effect']:+.2f} percentage points on average")
        output.append(f"    - This represents a {abs(metrics['relative_effect_pct']):.2f}% {'increase' if metrics['relative_effect_pct'] > 0 else 'decrease'}")
    else:
        output.append(f"\n  Interpretation:")
        output.append(f"    - The intervention changed the metric by {metrics['avg_daily_effect']:+,.0f} units per day on average")
        output.append(f"    - Over 13 days, this totals {metrics['cum_effect']:+,.0f} units")
    
    # Statistical significance
    output.append(f"\n🔬 **STATISTICAL SIGNIFICANCE**")
    
    level_sig = "YES ✓" if metrics['p_value_level'] < 0.05 else "NO"
    trend_sig = "YES ✓" if metrics['p_value_trend'] < 0.05 else "NO"
    
    output.append(f"  Immediate level change (β2): {level_sig} (p={metrics['p_value_level']:.4f})")
    output.append(f"  Trend change (β3):           {trend_sig} (p={metrics['p_value_trend']:.4f})")
    
    # Overall interpretation
    output.append(f"\n💡 **OVERALL INTERPRETATION**")
    
    if metrics['p_value_level'] < 0.05 or metrics['p_value_trend'] < 0.05:
        output.append(f"  ✅ The intervention had a STATISTICALLY SIGNIFICANT effect.")
        if metrics['p_value_level'] < 0.05:
            direction = "increase" if metrics['immediate_level_change'] > 0 else "decrease"
            output.append(f"     - Immediate level change: {abs(metrics['immediate_level_change']):.2f} point {direction}")
        if metrics['p_value_trend'] < 0.05:
            direction = "acceleration" if metrics['trend_change'] > 0 else "deceleration"
            output.append(f"     - Trend change: {abs(metrics['trend_change']):.4f} per day {direction}")
    else:
        output.append(f"  ⚠️  No statistically significant effect detected at α=0.05 level.")
        output.append(f"     The observed changes could be due to random variation.")
    
    output.append(f"\n{'-'*80}\n")
    
    return "\n".join(output)

if __name__ == "__main__":
    print("=" * 80)
    print("INTERRUPTED TIME SERIES (ITS) CAUSAL ANALYSIS")
    print("Change Date: September 25, 2025")
    print("Method: Segmented Regression Analysis")
    print("=" * 80)
    
    # Fetch data from Snowflake
    df = fetch_funnel_data()
    
    # Save data to CSV
    df.to_csv("../outputs/funnel_data_time_series.csv", index=False)
    print("Data saved to ../outputs/funnel_data_time_series.csv\n")
    
    df['platform'] = df['platform'].str.lower()
    df = df[df['platform'].isin(['ios', 'android'])].copy()
    
    # Metrics to analyze
    metrics_to_analyze = [
        ('purchase_cvr_pct', 'Purchase CVR (%)'),
        ('purchasers', 'Daily Purchasers'),
        ('store_content_cvr_pct', 'Store Content CVR (%)')
    ]
    
    all_reports = []
    
    # Analyze each platform
    for platform in ['all', 'ios', 'android']:
        print(f"\n{'='*80}")
        print(f"ANALYZING PLATFORM: {platform.upper()}")
        print(f"{'='*80}\n")
        
        # Prepare data
        data_its = prepare_its_data(df, platform)
        print(f"Data points: {len(data_its)} (13 pre, 13 post)")
        
        for metric_col, metric_name in metrics_to_analyze:
            print(f"\nAnalyzing: {metric_name}...")
            
            try:
                # Run ITS regression
                results, data_with_predictions = run_its_regression(data_its.copy(), metric_col)
                
                # Calculate impact metrics
                impact_metrics = calculate_impact_metrics(data_with_predictions, results, metric_col)
                
                # Create visualization
                plot_path = f"../outputs/its_{platform}_{metric_col}.png"
                plot_its_results(data_with_predictions, metric_col, metric_name, platform, 
                               impact_metrics, plot_path)
                
                # Create report
                report = create_its_report(platform, metric_name, results, impact_metrics, data_with_predictions)
                print(report)
                
                all_reports.append(report)
                
            except Exception as e:
                print(f"Error analyzing {metric_name} for {platform}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    # Save comprehensive report
    print("\n" + "="*80)
    print("Saving comprehensive report...")
    
    with open("../outputs/its_causal_impact_report.txt", "w") as f:
        f.write("="*80 + "\n")
        f.write("INTERRUPTED TIME SERIES (ITS) CAUSAL ANALYSIS - COMPREHENSIVE REPORT\n")
        f.write("Change Date: September 25, 2025\n")
        f.write("Method: Segmented Regression (Interrupted Time Series)\n")
        f.write("Pre-Period: Sept 12-24, 2025 (13 days)\n")
        f.write("Post-Period: Sept 25 - Oct 7, 2025 (13 days)\n")
        f.write("="*80 + "\n\n")
        
        f.write("METHODOLOGY:\n")
        f.write("-" * 80 + "\n")
        f.write("The ITS regression model estimates causal effects using:\n")
        f.write("  Y = β0 + β1*time + β2*intervention + β3*time_since_intervention + ε\n\n")
        f.write("Where:\n")
        f.write("  - β0: Baseline level\n")
        f.write("  - β1: Pre-intervention trend (slope)\n")
        f.write("  - β2: Immediate level change at intervention point\n")
        f.write("  - β3: Change in trend after intervention\n")
        f.write("  - Causal Effect = Observed - Counterfactual (in post-period)\n")
        f.write("="*80 + "\n")
        
        for report in all_reports:
            f.write(report)
            f.write("\n\n")
    
    print("Report saved to: ../outputs/its_causal_impact_report.txt")
    print(f"\n✅ Analysis complete! Generated {len(all_reports)} ITS analyses")

