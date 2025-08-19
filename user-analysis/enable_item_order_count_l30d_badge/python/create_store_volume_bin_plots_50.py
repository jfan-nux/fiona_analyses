#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os
from datetime import datetime
from pathlib import Path

# Add utils directory to path
utils_path = str(Path(__file__).parent.parent.parent.parent / 'utils')
sys.path.insert(0, utils_path)
from snowflake_connection import SnowflakeHook

def main():
    # Read and execute the SQL query - using store-level analysis
    sql_file_path = Path(__file__).parent.parent / 'sql' / 'store_volume_bin_analysis_50.sql'
    
    print(f'Reading SQL from: {sql_file_path}')
    with open(sql_file_path, 'r') as f:
        sql_query = f.read()
    
    # Execute the SQL query using SnowflakeHook
    print('Executing SQL query to fetch store-level volume bin analysis data...')
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(sql_query, method='pandas')
    
    print(f'Loaded {len(df)} rows from Snowflake query')
    
    # Extract OVERALL data for horizontal lines
    overall_data = df[df['volume_bin'] == 'OVERALL'].copy()
    print(f'Extracted {len(overall_data)} OVERALL rows for horizontal lines')
    
    # Filter to EXCLUDE OVERALL
    df_filtered = df[df['volume_bin'] != 'OVERALL'].copy()
    print(f'Filtered to {len(df_filtered)} rows (excluding OVERALL)')
    
    # Create pivot tables for plotting
    metrics = ['pct_diff_order_count', 'pct_diff_revenue_usd']
    metric_labels = ['Order Count % Difference vs Control', 'Revenue % Difference vs Control']
    
    # Create x-axis positions - simplified bin order with numerical spacing
    custom_bin_order = ['10', '50', '100', '150', '200', '250', '300', '350', '400', '450', '500', '550', '600', '600+']
    
    # Filter to only bins that exist in data and preserve order
    unique_bins = [bin_name for bin_name in custom_bin_order if bin_name in df_filtered['volume_bin'].unique()]
    # Use actual numerical values for x-positions to reflect true distances
    x_positions = []
    for bin_name in unique_bins:
        if bin_name == '600+':
            x_positions.append(700)  # Position 600+ at 700 for visual spacing
        else:
            x_positions.append(int(bin_name))
    x_labels = unique_bins
    
    print(f'X-axis bins: {list(x_labels)}')
    print(f'Total bins: {len(x_labels)}')
    print(f'Treatment arms: {df_filtered["treatment_arm"].unique()}')
    
    # Create the plots
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle('Store-Level Treatment Arm Performance vs Control by Volume Bin\n(14 Bins: 10,50,100,150...550,600,600+) - % Difference vs Control\nStore Average Volume Analysis', fontsize=14, fontweight='bold')
    
    colors = {'icon treatment': '#1f77b4', 'no icon treatment': '#ff7f0e'}
    markers = {'icon treatment': 'o', 'no icon treatment': 's'}
    
    for i, (metric, label) in enumerate(zip(metrics, metric_labels)):
        ax = axes[i]
        
        # Plot each treatment arm
        for treatment in df_filtered['treatment_arm'].unique():
            treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
            
            # Get values in the correct order
            y_values = []
            for bin_name in x_labels:
                bin_data = treatment_data[treatment_data['volume_bin'] == bin_name]
                if not bin_data.empty:
                    y_values.append(bin_data[metric].iloc[0])
                else:
                    y_values.append(np.nan)  # Use NaN for missing data
            
            ax.plot(x_positions, y_values, 
                   marker=markers[treatment], 
                   color=colors[treatment],
                   label=treatment,
                   linewidth=2.5,
                   markersize=8,
                   alpha=0.8)
        
        # Add horizontal line at y=0
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.7, linewidth=1)
        
        # Add horizontal lines for overall performance
        if not overall_data.empty:
            for treatment in overall_data['treatment_arm'].unique():
                overall_row = overall_data[overall_data['treatment_arm'] == treatment]
                if not overall_row.empty:
                    overall_value = overall_row[metric].iloc[0]
                    if overall_value is not None:
                        ax.axhline(y=overall_value, color=colors[treatment], 
                                  linestyle=':', alpha=0.8, linewidth=2,
                                  label=f'{treatment} (overall: {overall_value*100:+.1f}%)')
        
        # Formatting
        ax.set_title(label, fontweight='bold', fontsize=12)
        ax.set_xlabel('Store Average Volume Bin', fontsize=11)
        ax.set_ylabel('% Difference vs Control', fontsize=11)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # Set x-axis labels - using numerical positions for proper spacing
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, rotation=45, fontsize=10)
        ax.set_xlabel('Store Average Volume Bin (Numerical Scale)', fontsize=12)
        
        # Format y-axis for percentages
        def format_percentage(x, p):
            return f'{x*100:.1f}%'
        ax.yaxis.set_major_formatter(plt.FuncFormatter(format_percentage))
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plots_dir = Path(__file__).parent.parent / 'plots'
    plots_dir.mkdir(exist_ok=True)
    plot_filename = plots_dir / f'store_level_volume_bin_analysis_plots_600plus_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    
    print(f'\n📊 Store-level plots saved to: {plot_filename}')
    
    # Print summary statistics
    print('\n📈 Store-Level Summary Statistics (Granular binning with 600+ combined: 10,50,100,150,200,250,300,350,400,450,500,550,600,600+):')
    print('=' * 80)
    
    # Print OVERALL statistics first
    if not overall_data.empty:
        print('\n🌟 OVERALL Performance (All Store Volume Bins Combined):')
        for _, row in overall_data.iterrows():
            treatment = row['treatment_arm']
            order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
            revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
            print(f'  {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}%')
    
    # Show all volume bins since there are fewer now
    print('\nAll Store Volume Bins:')
    for bin_name in x_labels:
        bin_data = df_filtered[df_filtered['volume_bin'] == bin_name]
        if not bin_data.empty:
            print(f'\n  Store Volume Bin {bin_name}:')
            for _, row in bin_data.iterrows():
                treatment = row['treatment_arm']
                order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
                revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
                print(f'    {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}%')
    
    # Calculate some aggregated stats
    print(f'\n📊 Aggregated Statistics (Store-Level with 600+ combined):')
    
    for treatment in df_filtered['treatment_arm'].unique():
        treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
        
        # Calculate weighted averages for percentage differences
        avg_order_pct = treatment_data['pct_diff_order_count'].mean() * 100
        avg_revenue_pct = treatment_data['pct_diff_revenue_usd'].mean() * 100
        
        print(f'\n  {treatment}:')
        print(f'    Average Order Count % Diff: {avg_order_pct:+.1f}%')
        print(f'    Average Revenue % Diff: {avg_revenue_pct:+.1f}%')
        
        # Find crossover point (where percentage differences become positive)
        positive_orders = treatment_data[treatment_data['pct_diff_order_count'] > 0]
        if not positive_orders.empty:
            # Handle both numeric and '600+' bins
            def extract_bin_value(bin_name):
                if bin_name == '600+':
                    return 600
                return int(bin_name)
            min_positive_bin = positive_orders['volume_bin'].apply(extract_bin_value).min()
            first_positive_bin_name = positive_orders.loc[positive_orders['volume_bin'].apply(extract_bin_value) == min_positive_bin, 'volume_bin'].iloc[0]
            print(f'    First positive order impact at store volume bin: {first_positive_bin_name}')
        else:
            print(f'    No positive order impacts found')
    
    # Show data range
    print(f'\nStore-Level Data Range:')
    print(f'  Volume bins: {x_labels[0]} to {x_labels[-1]} ({len(x_labels)} bins)')
    print(f'  Bin sequence: {", ".join(x_labels)}')
    print(f'  X-axis: Numerical scale with 600+ positioned at 700 for visual spacing')
    print(f'  Total data points: {len(df_filtered)} rows')
    print(f'  Analysis Level: Store-level (averaged avg_recent_orders_volume per store)')
    print(f'  Special: All stores with volume >600 combined into single "600+" bin')

if __name__ == '__main__':
    main()
