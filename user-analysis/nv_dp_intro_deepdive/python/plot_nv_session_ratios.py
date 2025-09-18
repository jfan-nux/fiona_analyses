#!/usr/bin/env python3
"""
Plot NV vs Non-NV order rate ratios across sessions and time windows.
Shows the impact of NV impressions on order rates for sessions 1, 2, and 3
across different time horizons (1h, 4h, 24h).
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses/utils')
from snowflake_connection import SnowflakeHook

def fetch_data():
    """Fetch data from Snowflake table."""
    query = """
    SELECT 
        order_type,
        session_num,
        had_nv_impression,
        ratio_order_rate_1h_nv_over_non_nv,
        ratio_order_rate_4h_nv_over_non_nv,
        ratio_order_rate_24h_nv_over_non_nv
    FROM proddb.fionafan.nv_dp_new_user_session_order_rates
    WHERE had_nv_impression = 1  -- Only need the ratios from the NV impression rows
    ORDER BY order_type, session_num
    """
    
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query)
    
    return df

def prepare_plot_data(df):
    """Transform data for plotting."""
    # Reshape data for plotting
    plot_data = []
    
    for _, row in df.iterrows():
        order_type = row['order_type']
        session_num = row['session_num']
        
        # Add data points for each time window
        for time_window, ratio in [
            ('1h', row['ratio_order_rate_1h_nv_over_non_nv']),
            ('4h', row['ratio_order_rate_4h_nv_over_non_nv']),
            ('24h', row['ratio_order_rate_24h_nv_over_non_nv'])
        ]:
            plot_data.append({
                'order_type': order_type,
                'session_num': f'Session {session_num}',
                'time_window': time_window,
                'ratio': ratio
            })
    
    return pd.DataFrame(plot_data)

def create_plots(plot_df):
    """Create separate plots for each order type."""
    # Set style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Define time window order for x-axis
    time_order = ['1h', '4h', '24h']
    
    # Create figure with subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle('NV Impression Impact on Order Rates: Ratio of NV vs Non-NV Order Rates', 
                 fontsize=16, fontweight='bold')
    
    order_types = ['overall', 'nv_order', 'non_nv_order']
    titles = ['Overall Orders', 'NV Orders Only', 'Non-NV Orders Only']
    
    for i, (order_type, title) in enumerate(zip(order_types, titles)):
        ax = axes[i]
        
        # Filter data for this order type
        subset = plot_df[plot_df['order_type'] == order_type]
        
        # Create line plot
        for session in sorted(subset['session_num'].unique()):
            session_data = subset[subset['session_num'] == session]
            session_data = session_data.set_index('time_window').reindex(time_order)
            
            ax.plot(session_data.index, session_data['ratio'], 
                   marker='o', linewidth=2.5, markersize=8, 
                   label=session)
        
        # Customize subplot
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xlabel('Time After Session Start', fontsize=12)
        ax.set_ylabel('Ratio (NV View / No NV View)', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.legend(title='Session Number', title_fontsize=10, fontsize=10)
        
        # Add horizontal line at ratio = 1 (no effect)
        ax.axhline(y=1, color='red', linestyle='--', alpha=0.5, 
                  label='No Effect (Ratio = 1)')
        
        # Set y-axis to start from a reasonable minimum
        y_min = max(0.5, subset['ratio'].min() * 0.9)
        y_max = subset['ratio'].max() * 1.1
        ax.set_ylim(y_min, y_max)
        
        # Format y-axis
        ax.tick_params(axis='both', which='major', labelsize=10)
    
    plt.tight_layout()
    
    # Save plot
    output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/nv_dp_intro_deepdive/plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/nv_session_impact_ratios.png', 
                dpi=300, bbox_inches='tight', facecolor='white')
    plt.show()

def create_summary_table(df):
    """Create a summary table of the ratios."""
    print("\n" + "="*80)
    print("SUMMARY: NV Impression Impact Ratios (NV View / No NV View)")
    print("="*80)
    
    for order_type in ['overall', 'nv_order', 'non_nv_order']:
        print(f"\n{order_type.upper().replace('_', ' ')}:")
        print("-" * 50)
        
        subset = df[df['order_type'] == order_type]
        
        for _, row in subset.iterrows():
            session = row['session_num']
            print(f"Session {session}:")
            print(f"  1h:  {row['ratio_order_rate_1h_nv_over_non_nv']:.2f}x")
            print(f"  4h:  {row['ratio_order_rate_4h_nv_over_non_nv']:.2f}x") 
            print(f"  24h: {row['ratio_order_rate_24h_nv_over_non_nv']:.2f}x")
            print()

def main():
    """Main execution function."""
    print("Fetching data from Snowflake...")
    df = fetch_data()
    
    print("Preparing plot data...")
    plot_df = prepare_plot_data(df)
    
    print("Creating plots...")
    create_plots(plot_df)
    
    print("Creating summary table...")
    create_summary_table(df)
    
    print("\nPlot saved to: user-analysis/nv_dp_intro_deepdive/plots/nv_session_impact_ratios.png")

if __name__ == "__main__":
    main()