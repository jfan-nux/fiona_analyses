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
    # Read and execute the SQL query
    sql_file_path = Path(__file__).parent.parent / 'sql' / 'store_item_level_analysis.sql'
    
    print(f'Reading SQL from: {sql_file_path}')
    with open(sql_file_path, 'r') as f:
        sql_query = f.read()
    
    # Execute the SQL query using SnowflakeHook
    print('Executing SQL query to fetch store-item volume bin analysis data...')
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(sql_query, method='pandas')
    
    print(f'Loaded {len(df)} rows from Snowflake query')
    
    # Extract OVERALL data for horizontal lines
    overall_data = df[df['volume_tier_label'] == 'OVERALL'].copy()
    print(f'Extracted {len(overall_data)} OVERALL rows for horizontal lines')
    
    # Filter to EXCLUDE OVERALL
    df_filtered = df[df['volume_tier_label'] != 'OVERALL'].copy()
    print(f'Filtered to {len(df_filtered)} rows (excluding OVERALL)')
    
    # Create pivot tables for plotting
    metrics = ['pct_diff_order_count', 'pct_diff_revenue_usd']
    metric_labels = ['Order Count % Difference vs Control', 'Revenue % Difference vs Control']
    
    # Volume bins are ordered 1-12 based on order count ranges
    unique_bins = sorted(df_filtered['volume_bin'].unique())
    bin_labels = [f'V{b}' for b in unique_bins]
    x_positions = unique_bins  # Use bin numbers directly for x-axis
    
    print(f'X-axis bins: {unique_bins}')
    print(f'Total bins: {len(unique_bins)}')
    print(f'Treatment arms: {df_filtered["treatment_arm"].unique()}')
    
    # Get bin labels and volume context
    bin_label_map = {}
    bin_volume_map = {}
    bin_pairs_map = {}
    for _, row in df_filtered.iterrows():
        bin_num = row['volume_bin']
        bin_label_map[bin_num] = row['volume_tier_label']
        if bin_num not in bin_volume_map:
            bin_volume_map[bin_num] = row['avg_baseline_order_count']
            bin_pairs_map[bin_num] = row['unique_store_item_pairs']
    
    # Create the plots with secondary y-axis for store-item volume
    fig, axes = plt.subplots(1, 2, figsize=(18, 10))
    fig.suptitle('Badge Treatment Performance vs Control by Store-Item Volume Bins\n(Volume Bins: <50, 50-100, 100-200, 200-300, ..., 1000+ orders)\nWith Historical Store-Item Volume Context (30d baseline)', fontsize=14, fontweight='bold')
    
    colors = {'icon treatment': '#1f77b4', 'no icon treatment': '#ff7f0e'}
    markers = {'icon treatment': 'o', 'no icon treatment': 's'}
    
    for i, (metric, label) in enumerate(zip(metrics, metric_labels)):
        ax = axes[i]
        
        # Create secondary y-axis for store-item volume
        ax2 = ax.twinx()
        
        # Plot store-item volume bars in the background
        volume_values = [float(bin_volume_map[bin_num]) for bin_num in unique_bins]
        bars = ax2.bar(x_positions, volume_values, 
                      alpha=0.2, color='gray', width=0.6, 
                      label='Avg Store-Item Volume (30d baseline)')
        
        # Plot each treatment arm
        for treatment in df_filtered['treatment_arm'].unique():
            treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
            
            # Get values in the correct order by volume bin
            y_values = []
            volume_annotations = []
            pairs_annotations = []
            for bin_num in unique_bins:
                bin_data = treatment_data[treatment_data['volume_bin'] == bin_num]
                if not bin_data.empty:
                    y_values.append(bin_data[metric].iloc[0])
                    volume_annotations.append(float(bin_volume_map[bin_num]))
                    pairs_annotations.append(int(bin_pairs_map[bin_num]))
                else:
                    y_values.append(np.nan)  # Use NaN for missing data
                    volume_annotations.append(0)
                    pairs_annotations.append(0)
            
            line = ax.plot(x_positions, y_values, 
                   marker=markers[treatment], 
                   color=colors[treatment],
                   label=treatment,
                   linewidth=2.5,
                   markersize=8,
                   alpha=0.9,
                   zorder=5)  # Ensure lines are on top
            
            # Add volume annotations on the treatment points
            for j, (x, y, vol, pairs) in enumerate(zip(x_positions, y_values, volume_annotations, pairs_annotations)):
                if not np.isnan(y):
                    # Offset annotations slightly for each treatment to avoid overlap
                    offset = 0.15 if treatment == 'icon treatment' else -0.15
                    ax.annotate(f'{vol:.1f}\n({pairs:,} pairs)', 
                              xy=(x + offset, y), 
                              xytext=(5, 10),
                              textcoords='offset points',
                              fontsize=8,
                              color=colors[treatment],
                              alpha=0.8,
                              ha='center',
                              bbox=dict(boxstyle='round,pad=0.2', 
                                       facecolor='white', 
                                       edgecolor=colors[treatment],
                                       alpha=0.8,
                                       linewidth=0.5))
        
        # Add horizontal line at y=0
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.7, linewidth=1, zorder=3)
        
        # Add horizontal lines for overall performance
        if not overall_data.empty:
            for treatment in overall_data['treatment_arm'].unique():
                overall_row = overall_data[overall_data['treatment_arm'] == treatment]
                if not overall_row.empty:
                    overall_value = overall_row[metric].iloc[0]
                    if overall_value is not None:
                        ax.axhline(y=overall_value, color=colors[treatment], 
                                  linestyle=':', alpha=0.8, linewidth=2, zorder=3,
                                  label=f'{treatment} (overall: {overall_value*100:+.1f}%)')
        
        # Formatting for primary axis (treatment effects)
        ax.set_title(label, fontweight='bold', fontsize=12)
        ax.set_xlabel('Store-Item Volume Bins (<50, 50-100, 100-200, ..., 1000+)', fontsize=12)
        ax.set_ylabel('% Difference vs Control', fontsize=11, color='black')
        ax.tick_params(axis='y', labelcolor='black')
        
        # Formatting for secondary axis (store-item volume)
        ax2.set_ylabel('Avg Store-Item Volume (Orders/30d)', fontsize=11, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
        ax2.set_ylim(0, max(volume_values) * 1.1)  # Give some headroom
        
        # Combine legends from both axes
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, fontsize=10, loc='upper left')
        
        ax.grid(True, alpha=0.3, zorder=1)
        
        # Set x-axis labels - volume bins 1-12
        ax.set_xticks(x_positions)
        ax.set_xticklabels(bin_labels, fontsize=10)
        
        # Format primary y-axis for percentages
        def format_percentage(x, p):
            return f'{x*100:.1f}%'
        ax.yaxis.set_major_formatter(plt.FuncFormatter(format_percentage))
        
        # Format secondary y-axis for volume
        def format_volume(x, p):
            if x >= 10:
                return f'{x:.1f}'
            elif x >= 1:
                return f'{x:.2f}'
            else:
                return f'{x:.3f}'
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(format_volume))
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plots_dir = Path(__file__).parent.parent / 'plots'
    plots_dir.mkdir(exist_ok=True)
    plot_filename = plots_dir / f'store_item_volume_bin_analysis_plots_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight', facecolor='white')
    
    print(f'\n📊 Store-Item Volume Bin Analysis plots saved to: {plot_filename}')
    print('📈 Plot features:')
    print('   • Treatment effect lines with volume and store-item pair counts on each point')
    print('   • Gray bars showing average 30-day store-item baseline volume per bin')
    print('   • Dual y-axis: % treatment effects (left) vs store-item volume (right)')
    print('   • Store-item pairs and volume values annotated directly on treatment effect points')
    
    # Print summary statistics
    print('\n📈 Store-Item Volume Bin Analysis Summary:')
    print('=' * 80)
    
    # Print OVERALL statistics first
    if not overall_data.empty:
        print('\n🌟 OVERALL Performance (All Store-Item Volume Bins Combined):')
        for _, row in overall_data.iterrows():
            treatment = row['treatment_arm']
            order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
            revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
            pairs = row["unique_store_item_pairs"] if row["unique_store_item_pairs"] is not None else 0
            avg_volume = row["avg_baseline_order_count"] if row["avg_baseline_order_count"] is not None else 0
            print(f'  {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}% | {pairs:,} store-item pairs, avg baseline: {avg_volume:.1f}')
    
    # Print detailed volume bin breakdown
    print('\n📊 Performance by Store-Item Volume Bin:')
    for bin_num in unique_bins:
        bin_data = df_filtered[df_filtered['volume_bin'] == bin_num]
        if not bin_data.empty:
            bin_label = bin_label_map.get(bin_num, f'V{bin_num}')
            # Get context from first row (should be same for all treatments in this bin)
            context_row = bin_data.iloc[0]
            avg_volume = context_row["avg_baseline_order_count"] if context_row["avg_baseline_order_count"] is not None else 0
            volume_range = f"{context_row['min_baseline_order_count']}-{context_row['max_baseline_order_count']}"
            
            print(f'\n  {bin_label} (Avg Baseline Volume: {avg_volume:.1f} orders, Range: {volume_range}):')
            for _, row in bin_data.iterrows():
                treatment = row['treatment_arm']
                order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
                revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
                pairs = row["unique_store_item_pairs"] if row["unique_store_item_pairs"] is not None else 0
                print(f'    {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}% | {pairs:,} store-item pairs')
    
    # Calculate aggregated insights
    print(f'\n💡 Key Insights:')
    print('-' * 50)
    
    for treatment in df_filtered['treatment_arm'].unique():
        treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
        
        # Calculate average effects across deciles
        avg_order_pct = treatment_data['pct_diff_order_count'].mean() * 100
        avg_revenue_pct = treatment_data['pct_diff_revenue_usd'].mean() * 100
        
        # Find best performing volume bins
        best_order_bin = treatment_data.loc[treatment_data['pct_diff_order_count'].idxmax(), 'volume_bin']
        best_order_effect = treatment_data['pct_diff_order_count'].max() * 100
        
        best_revenue_bin = treatment_data.loc[treatment_data['pct_diff_revenue_usd'].idxmax(), 'volume_bin']  
        best_revenue_effect = treatment_data['pct_diff_revenue_usd'].max() * 100
        
        print(f'\n  📈 {treatment}:')
        print(f'     Average Effect: Orders {avg_order_pct:+.1f}%, Revenue {avg_revenue_pct:+.1f}%')
        print(f'     Best Order Performance: V{best_order_bin} ({best_order_effect:+.1f}%)')
        print(f'     Best Revenue Performance: V{best_revenue_bin} ({best_revenue_effect:+.1f}%)')
        
        # Count positive effects
        positive_order_bins = (treatment_data['pct_diff_order_count'] > 0).sum()
        positive_revenue_bins = (treatment_data['pct_diff_revenue_usd'] > 0).sum()
        
        print(f'     Positive Effects: {positive_order_bins}/{len(unique_bins)} bins (orders), {positive_revenue_bins}/{len(unique_bins)} bins (revenue)')
    
    # Strategic insights
    print(f'\n🎯 Strategic Insights:')
    print('-' * 50)
    
    # Analyze patterns across volume bins
    for treatment in df_filtered['treatment_arm'].unique():
        treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment].copy()
        treatment_data = treatment_data.sort_values('volume_bin')
        
        # Check if there's a trend with store-item volume
        order_effects = treatment_data['pct_diff_order_count'].values
        bin_nums = treatment_data['volume_bin'].values
        
        # Simple trend analysis
        correlation = np.corrcoef(bin_nums, order_effects)[0,1] if len(order_effects) > 1 else 0
        
        if correlation > 0.3:
            trend = "📈 INCREASING with store-item volume"
        elif correlation < -0.3:
            trend = "📉 DECREASING with store-item volume"  
        else:
            trend = "📊 NO CLEAR PATTERN with store-item volume"
            
        print(f'\n  {treatment}: Treatment effect {trend} (correlation: {correlation:.2f})')
        
        # Identify sweet spots
        high_effect_bins = treatment_data[treatment_data['pct_diff_order_count'] > treatment_data['pct_diff_order_count'].mean()]
        if not high_effect_bins.empty:
            sweet_spots = high_effect_bins['volume_bin'].tolist()
            sweet_spot_labels = [bin_label_map.get(b, f'V{b}') for b in sweet_spots]
            print(f'     Sweet Spot Volume Bins: {", ".join(sweet_spot_labels)}')
    
    # Store-Item volume context
    print(f'\n📋 Store-Item Volume Context:')
    print('-' * 50)
    for bin_num in unique_bins:
        bin_data = df_filtered[df_filtered['volume_bin'] == bin_num]
        if not bin_data.empty:
            context_row = bin_data.iloc[0]
            bin_label = bin_label_map.get(bin_num, f'V{bin_num}')
            avg_volume = context_row["avg_baseline_order_count"]
            total_pairs = bin_data['unique_store_item_pairs'].sum()  # Sum across treatments
            
            print(f'  {bin_label}: ~{total_pairs:,} store-item pairs, avg {avg_volume:.1f} orders/30d baseline')
    
    print(f'\nData Summary:')
    print(f'  Store-item volume bins analyzed: <50, 50-100, 100-200, 200-300, 300-400, 400-500, 500-600, 600-700, 700-800, 800-900, 900-1000, 1000+ orders')
    print(f'  Total data points: {len(df_filtered)} bin-treatment combinations')
    print(f'  Store-item baseline period: 30 days prior to experiment (June 25 - July 24, 2025)')
    print(f'  Experiment period: July 25 - July 31, 2025')

if __name__ == '__main__':
    main()
