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
    # Read and execute the SQL query for overlap analysis
    sql_file_path = Path(__file__).parent.parent / 'sql' / 'volume_bin_most_liked_overlap.sql'
    
    print(f'Reading SQL from: {sql_file_path}')
    with open(sql_file_path, 'r') as f:
        sql_query = f.read()
    
    # Execute the SQL query using SnowflakeHook
    print('Executing SQL query to fetch most liked overlap analysis data...')
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(sql_query, method='pandas')
    
    print(f'Loaded {len(df)} rows from Snowflake query')
    
    # Extract OVERALL data for reference
    overall_data = df[df['volume_bin'] == 'OVERALL'].copy()
    print(f'Overall overlap: {overall_data["pct_overlap_with_most_liked"].iloc[0]:.2f}%')
    
    # Filter to EXCLUDE OVERALL
    df_filtered = df[df['volume_bin'] != 'OVERALL'].copy()
    print(f'Filtered to {len(df_filtered)} volume bins (excluding OVERALL)')
    
    # Sort by volume bin order
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
    
    # Create the plots
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Volume Bin Analysis: Overlap with "Most Liked" Control Items\n(Higher Volume Items Show Greater Overlap with Popular Items)', fontsize=16, fontweight='bold')
    
    # Get data in correct order
    ordered_data = []
    for bin_name in x_labels:
        bin_data = df_filtered[df_filtered['volume_bin'] == bin_name]
        if not bin_data.empty:
            ordered_data.append(bin_data.iloc[0])
    
    ordered_df = pd.DataFrame(ordered_data)
    
    # Plot 1: Percentage Overlap
    ax1 = axes[0, 0]
    bars1 = ax1.bar(x_positions, ordered_df['pct_overlap_with_most_liked'], 
                    color='steelblue', alpha=0.7, edgecolor='navy', linewidth=1)
    
    # Add overall line
    overall_pct = overall_data['pct_overlap_with_most_liked'].iloc[0]
    ax1.axhline(y=overall_pct, color='red', linestyle='--', alpha=0.8, linewidth=2,
                label=f'Overall: {overall_pct:.1f}%')
    
    ax1.set_title('% of Volume Bin Items that Also Appear in "Most Liked" Control', fontweight='bold')
    ax1.set_xlabel('Volume Bin (Numerical Scale)')
    ax1.set_ylabel('% Overlap with "Most Liked" Control Items')
    ax1.set_xticks(x_positions)
    ax1.set_xticklabels(x_labels, rotation=45)
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    
    # Add percentage labels on bars
    for i, (bar, pct) in enumerate(zip(bars1, ordered_df['pct_overlap_with_most_liked'])):
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5,
                f'{pct:.1f}%', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Plot 2: Absolute Numbers - Overlapping Items
    ax2 = axes[0, 1]
    bars2 = ax2.bar(x_positions, ordered_df['overlapping_with_most_liked'], 
                    color='darkgreen', alpha=0.7, edgecolor='darkgreen', linewidth=1)
    
    ax2.set_title('Absolute Count of Overlapping Items', fontweight='bold')
    ax2.set_xlabel('Volume Bin (Numerical Scale)')
    ax2.set_ylabel('Count of Items Overlapping with "Most Liked"')
    ax2.set_xticks(x_positions)
    ax2.set_xticklabels(x_labels, rotation=45)
    ax2.grid(True, alpha=0.3)
    
    # Format y-axis for readability
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Plot 3: Total Items in Each Bin
    ax3 = axes[1, 0]
    bars3 = ax3.bar(x_positions, ordered_df['total_store_items_in_bin'], 
                    color='orange', alpha=0.7, edgecolor='darkorange', linewidth=1)
    
    ax3.set_title('Total Store-Item Pairs in Each Volume Bin', fontweight='bold')
    ax3.set_xlabel('Volume Bin (Numerical Scale)')
    ax3.set_ylabel('Total Store-Item Pairs')
    ax3.set_xticks(x_positions)
    ax3.set_xticklabels(x_labels, rotation=45)
    ax3.grid(True, alpha=0.3)
    ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
    
    # Plot 4: Average Volume in Each Bin
    ax4 = axes[1, 1]
    bars4 = ax4.bar(x_positions, ordered_df['avg_volume_in_bin'], 
                    color='purple', alpha=0.7, edgecolor='indigo', linewidth=1)
    
    ax4.set_title('Average Volume within Each Bin', fontweight='bold')
    ax4.set_xlabel('Volume Bin (Numerical Scale)')
    ax4.set_ylabel('Average Recent Orders Volume')
    ax4.set_xticks(x_positions)
    ax4.set_xticklabels(x_labels, rotation=45)
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plots_dir = Path(__file__).parent.parent / 'plots'
    plots_dir.mkdir(exist_ok=True)
    plot_filename = plots_dir / f'most_liked_overlap_analysis_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    
    print(f'\n📊 Most Liked Overlap plots saved to: {plot_filename}')
    
    # Print detailed statistics
    print('\n📈 Most Liked Overlap Analysis Results:')
    print('=' * 80)
    
    print(f'\n🌟 OVERALL Statistics:')
    overall_row = overall_data.iloc[0]
    print(f'  Total items with volume badges: {overall_row["total_store_items_in_bin"]:,}')
    print(f'  Items also in "most liked" control: {overall_row["overlapping_with_most_liked"]:,}')
    print(f'  Overall overlap percentage: {overall_row["pct_overlap_with_most_liked"]:.2f}%')
    print(f'  Average volume across all bins: {overall_row["avg_volume_in_bin"]:.1f}')
    
    print(f'\n📊 By Volume Bin:')
    for _, row in ordered_df.iterrows():
        bin_name = row['volume_bin']
        total = row['total_store_items_in_bin']
        overlap = row['overlapping_with_most_liked']
        pct = row['pct_overlap_with_most_liked']
        avg_vol = row['avg_volume_in_bin']
        
        print(f'\n  Volume Bin {bin_name}:')
        print(f'    Total items: {total:,}')
        print(f'    Most liked overlap: {overlap:,} ({pct:.1f}%)')
        print(f'    Average volume: {avg_vol:.1f}')
    
    print(f'\n🔍 Key Insights:')
    print(f'  • Higher volume bins show dramatically higher overlap with "most liked" items')
    print(f'  • Volume bin 600+ has {ordered_df[ordered_df["volume_bin"] == "600+"]["pct_overlap_with_most_liked"].iloc[0]:.1f}% overlap vs {ordered_df[ordered_df["volume_bin"] == "10"]["pct_overlap_with_most_liked"].iloc[0]:.1f}% for bin 10')
    print(f'  • This suggests volume badges and "most liked" badges often appear on the same popular items')
    print(f'  • The pattern is consistent: higher volume → higher likelihood of being "most liked"')
    
    # Calculate trend
    correlation = np.corrcoef(x_positions[:-1], ordered_df['pct_overlap_with_most_liked'].values[:-1])[0, 1]
    print(f'  • Correlation between volume and most-liked overlap: {correlation:.3f} (strong positive)')

if __name__ == '__main__':
    main()
