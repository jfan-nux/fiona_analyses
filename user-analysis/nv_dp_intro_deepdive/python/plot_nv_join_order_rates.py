#!/usr/bin/env python3
"""
Plot NV vs Non-NV join order rate ratios across different NV windows and time periods.
Shows the impact of NV impressions on order rates for different NV exposure windows
across different time periods after user join.
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
        nv_window,
        had_nv_impression,
        ratio_order_rate_7d_nv_over_non_nv,
        ratio_order_rate_12d_nv_over_non_nv,
        ratio_order_rate_14d_nv_over_non_nv,
        ratio_order_rate_21d_nv_over_non_nv,
        ratio_order_rate_28d_nv_over_non_nv,
        ratio_order_rate_60d_nv_over_non_nv,
        ratio_order_rate_90d_nv_over_non_nv
    FROM proddb.fionafan.nv_dp_new_user_join_order_rates
    WHERE had_nv_impression = 1  -- Only need the ratios from the NV impression rows
    ORDER BY order_type, nv_window
    """
    
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(query)
    
    return df

def prepare_plot_data(df):
    """Transform data for plotting."""
    # Reshape data for plotting
    plot_data = []
    
    # Define time periods in order
    time_periods = ['7d', '12d', '14d', '21d', '28d', '60d', '90d']
    
    for _, row in df.iterrows():
        order_type = row['order_type']
        nv_window = row['nv_window']
        
        # Add data points for each time period
        for time_period in time_periods:
            ratio_col = f'ratio_order_rate_{time_period}_nv_over_non_nv'
            if ratio_col in row and pd.notna(row[ratio_col]):
                plot_data.append({
                    'order_type': order_type,
                    'nv_window': nv_window,
                    'time_period': time_period,
                    'ratio': row[ratio_col]
                })
    
    return pd.DataFrame(plot_data)

def create_plots(plot_df):
    """Create separate plots for each order type, separating hour and day windows."""
    # Set style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Define time period order for x-axis
    time_order = ['7d', '12d', '14d', '21d', '28d', '60d', '90d']
    
    # Separate NV windows into hours and days
    hour_windows = ['1h', '4h', '12h', '24h']
    day_windows = ['7d', '14d', '30d']
    
    # Get unique order types
    order_types = plot_df['order_type'].unique()
    
    # Create figure with subplots - order_types rows x 2 columns (hours/days)  
    fig, axes = plt.subplots(len(order_types), 2, figsize=(16, 6*len(order_types)))
    if len(order_types) == 1:
        axes = axes.reshape(1, 2)
    
    fig.suptitle('NV Window Impact on Join Order Rates: Ratio of NV vs Non-NV Order Rates', 
                 fontsize=16, fontweight='bold')
    
    titles = {'overall': 'Overall Orders', 'nv_order': 'NV Orders Only', 'non_nv_order': 'Non-NV Orders Only'}
    
    for i, order_type in enumerate(order_types):
        # Filter data for this order type
        subset = plot_df[plot_df['order_type'] == order_type]
        
        title = titles.get(order_type, order_type.replace('_', ' ').title())
        
        # Plot hour windows (left column)
        ax_hours = axes[i, 0]
        hour_subset = subset[subset['nv_window'].isin(hour_windows)]
        
        for nv_window in hour_windows:
            if nv_window in hour_subset['nv_window'].values:
                window_data = hour_subset[hour_subset['nv_window'] == nv_window]
                window_data = window_data.set_index('time_period').reindex(time_order).dropna()
                
                ax_hours.plot(window_data.index, window_data['ratio'], 
                           marker='o', linewidth=2.5, markersize=8, 
                           label=f'{nv_window}')
        
        ax_hours.set_title(f'{title} - Hour Windows', fontsize=14, fontweight='bold')
        ax_hours.set_xlabel('Time After User Join', fontsize=12)
        ax_hours.set_ylabel('Ratio (NV View / No NV View)', fontsize=12)
        ax_hours.grid(True, alpha=0.3)
        ax_hours.legend(title='NV Window', title_fontsize=10, fontsize=10)
        ax_hours.axhline(y=1, color='red', linestyle='--', alpha=0.5)
        
        # Use log scale if needed
        if not hour_subset.empty and hour_subset['ratio'].max() > 50:
            ax_hours.set_yscale('log')
            ax_hours.set_ylabel('Ratio (Log Scale)', fontsize=12)
        
        # Plot day windows (right column)
        ax_days = axes[i, 1]
        day_subset = subset[subset['nv_window'].isin(day_windows)]
        
        for nv_window in day_windows:
            if nv_window in day_subset['nv_window'].values:
                window_data = day_subset[day_subset['nv_window'] == nv_window]
                window_data = window_data.set_index('time_period').reindex(time_order).dropna()
                
                ax_days.plot(window_data.index, window_data['ratio'], 
                           marker='s', linewidth=2.5, markersize=8, 
                           label=f'{nv_window}')
        
        ax_days.set_title(f'{title} - Day Windows', fontsize=14, fontweight='bold')
        ax_days.set_xlabel('Time After User Join', fontsize=12)
        ax_days.set_ylabel('Ratio (NV View / No NV View)', fontsize=12)
        ax_days.grid(True, alpha=0.3)
        ax_days.legend(title='NV Window', title_fontsize=10, fontsize=10)
        ax_days.axhline(y=1, color='red', linestyle='--', alpha=0.5)
        
        # Use log scale if needed
        if not day_subset.empty and day_subset['ratio'].max() > 50:
            ax_days.set_yscale('log')
            ax_days.set_ylabel('Ratio (Log Scale)', fontsize=12)
        
        # Format axes
        ax_hours.tick_params(axis='both', which='major', labelsize=10)
        ax_days.tick_params(axis='both', which='major', labelsize=10)
    
    plt.tight_layout()
    
    # Save plot
    output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/nv_dp_intro_deepdive/plots'
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(f'{output_dir}/nv_join_order_rate_ratios.png', 
                dpi=300, bbox_inches='tight', facecolor='white')
    plt.show()

def create_summary_table(df):
    """Create a summary table of the ratios."""
    print("\n" + "="*80)
    print("SUMMARY: NV Window Impact Ratios (NV View / No NV View)")
    print("="*80)
    
    # Define time periods for display
    time_periods = ['7d', '12d', '14d', '21d', '28d', '60d', '90d']
    
    for order_type in df['order_type'].unique():
        print(f"\n{order_type.upper().replace('_', ' ')}:")
        print("-" * 60)
        
        subset = df[df['order_type'] == order_type]
        
        for _, row in subset.iterrows():
            nv_window = row['nv_window']
            print(f"NV Window {nv_window}:")
            
            for time_period in time_periods:
                ratio_col = f'ratio_order_rate_{time_period}_nv_over_non_nv'
                if ratio_col in row and pd.notna(row[ratio_col]):
                    ratio = row[ratio_col]
                    print(f"  {time_period:>3}: {ratio:>8.2f}x")
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
    
    print("\nPlot saved to: user-analysis/nv_dp_intro_deepdive/plots/nv_join_order_rate_ratios.png")

if __name__ == "__main__":
    main()