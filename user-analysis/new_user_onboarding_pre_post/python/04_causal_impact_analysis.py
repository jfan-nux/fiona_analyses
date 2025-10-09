"""
Causal Impact Analysis using Bayesian Structural Time Series (BSTS)
Estimates the incremental impact of the 9/25/2025 onboarding change
"""

import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from causalimpact import CausalImpact
from datetime import datetime
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

def prepare_data_for_causal_impact(df, platform='all'):
    """
    Prepare time series data for CausalImpact analysis
    
    Args:
        df: DataFrame with funnel metrics
        platform: 'all', 'ios', or 'android'
    
    Returns:
        DataFrame indexed by date with metrics
    """
    if platform != 'all':
        df_filtered = df[df['platform'] == platform].copy()
    else:
        # Aggregate across platforms
        df_filtered = df.groupby('event_date').agg({
            'unique_onboarding_visitors': 'sum',
            'store_page_visitors': 'sum',
            'store_content_visitors': 'sum',
            'cart_visitors': 'sum',
            'checkout_visitors': 'sum',
            'purchasers': 'sum'
        }).reset_index()
        
        # Recalculate CVRs
        df_filtered['purchase_cvr_pct'] = (
            df_filtered['purchasers'] / df_filtered['unique_onboarding_visitors'] * 100
        )
        df_filtered['store_content_cvr_pct'] = (
            df_filtered['store_content_visitors'] / df_filtered['unique_onboarding_visitors'] * 100
        )
    
    # Set date as index
    df_filtered['event_date'] = pd.to_datetime(df_filtered['event_date'])
    df_filtered = df_filtered.sort_values('event_date')
    df_filtered = df_filtered.set_index('event_date')
    
    return df_filtered

def run_causal_impact(data, metric_col):
    """
    Run CausalImpact analysis for a specific metric
    
    Args:
        data: DataFrame with time series data (date indexed)
        metric_col: Column name of the metric to analyze
    
    Returns:
        CausalImpact object
    """
    # Prepare data - just the metric column
    y = data[[metric_col]].copy()
    
    # Define periods as lists (indices)
    pre_period = [0, 12]  # First 13 days (indices 0-12)
    post_period = [13, 25]  # Next 13 days (indices 13-25)
    
    # Run CausalImpact
    try:
        ci = CausalImpact(y, pre_period, post_period)
        return ci
    except Exception as e:
        print(f"Error in CausalImpact: {str(e)}")
        raise

def save_causal_impact_results(ci, metric_name, platform_name, output_dir="../outputs"):
    """
    Save CausalImpact results using library's built-in methods
    """
    # Use library's built-in plot
    fig = ci.plot(figsize=(14, 10))
    plot_path = f"{output_dir}/causal_impact_{platform_name}_{metric_name.replace(' ', '_').replace('(', '').replace(')', '').replace('%', 'pct').lower()}.png"
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Plot saved to {plot_path}")
    
    # Get summary
    summary_text = ci.summary()
    summary_report = ci.summary(output='report')
    
    return summary_text, summary_report

if __name__ == "__main__":
    print("=" * 80)
    print("BAYESIAN CAUSAL IMPACT ANALYSIS")
    print("Change Date: September 25, 2025")
    print("Method: Bayesian Structural Time Series")
    print("=" * 80)
    
    # Fetch data from Snowflake
    df = fetch_funnel_data()
    
    # Save data to CSV for reference
    df.to_csv("../outputs/funnel_data_time_series.csv", index=False)
    print("Data saved to ../outputs/funnel_data_time_series.csv\n")
    
    df['event_date'] = pd.to_datetime(df['event_date'])
    df['platform'] = df['platform'].str.lower()
    df = df[df['platform'].isin(['ios', 'android'])].copy()
    
    # Metrics to analyze
    metrics_to_analyze = [
        ('purchase_cvr_pct', 'Purchase CVR (%)'),
        ('purchasers', 'Daily Purchasers'),
        ('store_content_cvr_pct', 'Store Content CVR (%)')
    ]
    
    all_results = []
    
    # Analyze each platform
    for platform in ['all', 'ios', 'android']:
        print(f"\n{'='*80}")
        print(f"ANALYZING PLATFORM: {platform.upper()}")
        print(f"{'='*80}\n")
        
        # Prepare data
        data = prepare_data_for_causal_impact(df, platform)
        print(f"Data shape: {data.shape}")
        print(f"Date range: {data.index.min()} to {data.index.max()}\n")
        
        for metric_col, metric_name in metrics_to_analyze:
            print(f"Analyzing: {metric_name}...")
            
            try:
                # Run causal impact
                ci = run_causal_impact(data, metric_col)
                
                # Save results
                summary_text, summary_report = save_causal_impact_results(
                    ci, metric_name, platform
                )
                
                # Print summary
                print(f"\n{summary_report}\n")
                
                all_results.append({
                    'platform': platform,
                    'metric': metric_name,
                    'summary': summary_text,
                    'report': summary_report
                })
                
            except Exception as e:
                print(f"Error analyzing {metric_name} for {platform}: {str(e)}\n")
                continue
    
    # Save comprehensive report
    print("\n" + "="*80)
    print("Saving comprehensive report...")
    
    with open("../outputs/causal_impact_report.txt", "w") as f:
        f.write("="*80 + "\n")
        f.write("BAYESIAN CAUSAL IMPACT ANALYSIS - COMPREHENSIVE REPORT\n")
        f.write("Change Date: September 25, 2025\n")
        f.write("Method: Bayesian Structural Time Series (BSTS)\n")
        f.write("Pre-Period: Sept 12-24, 2025 (13 days)\n")
        f.write("Post-Period: Sept 25 - Oct 7, 2025 (13 days)\n")
        f.write("="*80 + "\n\n")
        
        for result in all_results:
            f.write(f"\n{'='*80}\n")
            f.write(f"PLATFORM: {result['platform'].upper()} - {result['metric']}\n")
            f.write(f"{'='*80}\n\n")
            f.write(result['report'])
            f.write("\n\n")
    
    print("Report saved to: ../outputs/causal_impact_report.txt")
    print("\n✅ Analysis complete!")
    print(f"Generated {len(all_results)} causal impact analyses")
