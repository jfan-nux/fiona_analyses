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
    sql_file_path = Path(__file__).parent.parent / 'sql' / 'store_decile_analysis.sql'
    
    print(f'Reading SQL from: {sql_file_path}')
    with open(sql_file_path, 'r') as f:
        sql_query = f.read()
    
    # Execute the SQL query using SnowflakeHook
    print('Executing SQL query to fetch store decile analysis data...')
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
    
    # Store deciles are already ordered 1-10
    unique_deciles = sorted(df_filtered['volume_decile'].unique())
    decile_labels = [f'D{d}' for d in unique_deciles]
    x_positions = unique_deciles  # Use decile numbers directly for x-axis
    
    print(f'X-axis deciles: {unique_deciles}')
    print(f'Total deciles: {len(unique_deciles)}')
    print(f'Treatment arms: {df_filtered["treatment_arm"].unique()}')
    
    # Get decile labels and volume context
    decile_label_map = {}
    decile_volume_map = {}
    for _, row in df_filtered.iterrows():
        decile = row['volume_decile']
        decile_label_map[decile] = row['volume_tier_label']
        if decile not in decile_volume_map:
            decile_volume_map[decile] = row['avg_store_baseline_volume']
    
    # Create the plots with secondary y-axis for store volume
    fig, axes = plt.subplots(1, 2, figsize=(18, 10))
    fig.suptitle('Badge Treatment Performance vs Control by Store Volume Decile\n(Store Volume Tiers: D1 Lowest Volume → D10 Highest Volume)\nWith Past 30-Day Store Volume Context', fontsize=14, fontweight='bold')
    
    colors = {'icon treatment': '#1f77b4', 'no icon treatment': '#ff7f0e'}
    markers = {'icon treatment': 'o', 'no icon treatment': 's'}
    
    for i, (metric, label) in enumerate(zip(metrics, metric_labels)):
        ax = axes[i]
        
        # Create secondary y-axis for store volume
        ax2 = ax.twinx()
        
        # Plot store volume bars in the background
        volume_values = [float(decile_volume_map[decile]) for decile in unique_deciles]
        bars = ax2.bar(x_positions, volume_values, 
                      alpha=0.2, color='gray', width=0.6, 
                      label='Avg Store Volume (30d baseline)')
        
        # Plot each treatment arm
        for treatment in df_filtered['treatment_arm'].unique():
            treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
            
            # Get values in the correct order by decile
            y_values = []
            volume_annotations = []
            for decile in unique_deciles:
                decile_data = treatment_data[treatment_data['volume_decile'] == decile]
                if not decile_data.empty:
                    y_values.append(decile_data[metric].iloc[0])
                    volume_annotations.append(float(decile_volume_map[decile]))
                else:
                    y_values.append(np.nan)  # Use NaN for missing data
                    volume_annotations.append(0)
            
            line = ax.plot(x_positions, y_values, 
                   marker=markers[treatment], 
                   color=colors[treatment],
                   label=treatment,
                   linewidth=2.5,
                   markersize=8,
                   alpha=0.9,
                   zorder=5)  # Ensure lines are on top
            
            # Add volume annotations on the treatment points
            for j, (x, y, vol) in enumerate(zip(x_positions, y_values, volume_annotations)):
                if not np.isnan(y):
                    # Offset annotations slightly for each treatment to avoid overlap
                    offset = 0.15 if treatment == 'icon treatment' else -0.15
                    ax.annotate(f'{vol:.0f}', 
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
        ax.set_xlabel('Store Volume Decile (D1=Lowest → D10=Highest)', fontsize=12)
        ax.set_ylabel('% Difference vs Control', fontsize=11, color='black')
        ax.tick_params(axis='y', labelcolor='black')
        
        # Formatting for secondary axis (store volume)
        ax2.set_ylabel('Avg Store Volume (Orders/30d)', fontsize=11, color='gray')
        ax2.tick_params(axis='y', labelcolor='gray')
        ax2.set_ylim(0, max(volume_values) * 1.1)  # Give some headroom
        
        # Combine legends from both axes
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, fontsize=10, loc='upper left')
        
        ax.grid(True, alpha=0.3, zorder=1)
        
        # Set x-axis labels - deciles 1-10
        ax.set_xticks(x_positions)
        ax.set_xticklabels(decile_labels, fontsize=10)
        
        # Format primary y-axis for percentages
        def format_percentage(x, p):
            return f'{x*100:.1f}%'
        ax.yaxis.set_major_formatter(plt.FuncFormatter(format_percentage))
        
        # Format secondary y-axis for volume
        def format_volume(x, p):
            if x >= 1000:
                return f'{x/1000:.1f}K'
            else:
                return f'{x:.0f}'
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(format_volume))
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plot_filename = f'store_decile_analysis_plots_with_volume_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight', facecolor='white')
    
    print(f'\n📊 Enhanced plots with store volume context saved to: {plot_filename}')
    print('📈 Plot features:')
    print('   • Treatment effect lines with volume annotations on each point')
    print('   • Gray bars showing average 30-day store volume per decile')
    print('   • Dual y-axis: % treatment effects (left) vs store volume (right)')
    print('   • Volume values annotated directly on treatment effect points')
    
    # Print summary statistics
    print('\n📈 Store Volume Decile Analysis Summary:')
    print('=' * 80)
    
    # Print OVERALL statistics first
    if not overall_data.empty:
        print('\n🌟 OVERALL Performance (All Store Deciles Combined):')
        for _, row in overall_data.iterrows():
            treatment = row['treatment_arm']
            order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
            revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
            stores = row["unique_stores"] if row["unique_stores"] is not None else 0
            avg_volume = row["avg_store_baseline_volume"] if row["avg_store_baseline_volume"] is not None else 0
            print(f'  {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}% | {stores:,} stores, avg volume: {avg_volume:.0f}')
    
    # Print detailed decile breakdown
    print('\n📊 Performance by Store Volume Decile:')
    for decile in unique_deciles:
        decile_data = df_filtered[df_filtered['volume_decile'] == decile]
        if not decile_data.empty:
            decile_label = decile_label_map.get(decile, f'D{decile}')
            # Get context from first row (should be same for all treatments in this decile)
            context_row = decile_data.iloc[0]
            avg_volume = context_row["avg_store_baseline_volume"] if context_row["avg_store_baseline_volume"] is not None else 0
            volume_range = f"{context_row['min_store_baseline_volume']}-{context_row['max_store_baseline_volume']}"
            
            print(f'\n  {decile_label} (Avg Volume: {avg_volume:.0f} orders, Range: {volume_range}):')
            for _, row in decile_data.iterrows():
                treatment = row['treatment_arm']
                order_pct = row["pct_diff_order_count"] * 100 if row["pct_diff_order_count"] is not None else 0
                revenue_pct = row["pct_diff_revenue_usd"] * 100 if row["pct_diff_revenue_usd"] is not None else 0
                stores = row["unique_stores"] if row["unique_stores"] is not None else 0
                print(f'    {treatment}: Orders {order_pct:+.1f}%, Revenue {revenue_pct:+.1f}% | {stores:,} stores')
    
    # Calculate aggregated insights
    print(f'\n💡 Key Insights:')
    print('-' * 50)
    
    for treatment in df_filtered['treatment_arm'].unique():
        treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment]
        
        # Calculate average effects across deciles
        avg_order_pct = treatment_data['pct_diff_order_count'].mean() * 100
        avg_revenue_pct = treatment_data['pct_diff_revenue_usd'].mean() * 100
        
        # Find best performing deciles
        best_order_decile = treatment_data.loc[treatment_data['pct_diff_order_count'].idxmax(), 'volume_decile']
        best_order_effect = treatment_data['pct_diff_order_count'].max() * 100
        
        best_revenue_decile = treatment_data.loc[treatment_data['pct_diff_revenue_usd'].idxmax(), 'volume_decile']  
        best_revenue_effect = treatment_data['pct_diff_revenue_usd'].max() * 100
        
        print(f'\n  📈 {treatment}:')
        print(f'     Average Effect: Orders {avg_order_pct:+.1f}%, Revenue {avg_revenue_pct:+.1f}%')
        print(f'     Best Order Performance: D{best_order_decile} ({best_order_effect:+.1f}%)')
        print(f'     Best Revenue Performance: D{best_revenue_decile} ({best_revenue_effect:+.1f}%)')
        
        # Count positive effects
        positive_order_deciles = (treatment_data['pct_diff_order_count'] > 0).sum()
        positive_revenue_deciles = (treatment_data['pct_diff_revenue_usd'] > 0).sum()
        
        print(f'     Positive Effects: {positive_order_deciles}/10 deciles (orders), {positive_revenue_deciles}/10 deciles (revenue)')
    
    # Strategic insights
    print(f'\n🎯 Strategic Insights:')
    print('-' * 50)
    
    # Analyze patterns across deciles
    for treatment in df_filtered['treatment_arm'].unique():
        treatment_data = df_filtered[df_filtered['treatment_arm'] == treatment].copy()
        treatment_data = treatment_data.sort_values('volume_decile')
        
        # Check if there's a trend with store volume
        order_effects = treatment_data['pct_diff_order_count'].values
        decile_nums = treatment_data['volume_decile'].values
        
        # Simple trend analysis
        correlation = np.corrcoef(decile_nums, order_effects)[0,1] if len(order_effects) > 1 else 0
        
        if correlation > 0.3:
            trend = "📈 INCREASING with store volume"
        elif correlation < -0.3:
            trend = "📉 DECREASING with store volume"  
        else:
            trend = "📊 NO CLEAR PATTERN with store volume"
            
        print(f'\n  {treatment}: Treatment effect {trend} (correlation: {correlation:.2f})')
        
        # Identify sweet spots
        high_effect_deciles = treatment_data[treatment_data['pct_diff_order_count'] > treatment_data['pct_diff_order_count'].mean()]
        if not high_effect_deciles.empty:
            sweet_spots = high_effect_deciles['volume_decile'].tolist()
            sweet_spot_labels = [decile_label_map.get(d, f'D{d}') for d in sweet_spots]
            print(f'     Sweet Spot Deciles: {", ".join(sweet_spot_labels)}')
    
    # Store volume context
    print(f'\n📋 Store Volume Context:')
    print('-' * 50)
    for decile in unique_deciles:
        decile_data = df_filtered[df_filtered['volume_decile'] == decile]
        if not decile_data.empty:
            context_row = decile_data.iloc[0]
            decile_label = decile_label_map.get(decile, f'D{decile}')
            avg_volume = context_row["avg_store_baseline_volume"]
            total_stores = decile_data['unique_stores'].sum()  # Sum across treatments
            
            print(f'  {decile_label}: ~{total_stores:,} stores, avg {avg_volume:.0f} orders/month')
    
    print(f'\nData Summary:')
    print(f'  Store deciles analyzed: D1 (lowest volume) → D10 (highest volume)')
    print(f'  Total data points: {len(df_filtered)} decile-treatment combinations')
    print(f'  Store volume baseline: 30 days prior to experiment (June 25 - July 24, 2025)')

if __name__ == '__main__':
    main()
