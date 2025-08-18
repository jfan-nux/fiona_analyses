#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import re
import sys
import os
from datetime import datetime
from pathlib import Path

# Add utils directory to path
utils_path = str(Path(__file__).parent.parent.parent.parent / 'utils')
sys.path.insert(0, utils_path)
from snowflake_connection import SnowflakeHook

def extract_badge_number(badge_text):
    """Extract the number from badge text for sorting purposes"""
    badge_text = str(badge_text).lower()
    
    # Handle "most liked" badges - extract the rank number
    if 'most liked' in badge_text:
        match = re.search(r'#(\d+)', badge_text)
        if match:
            return f"#{match.group(1)} most liked"
        else:
            return badge_text
    
    # Handle "recent orders" badges - extract the number and bin by 50
    elif 'recent orders' in badge_text:
        # Handle special cases
        if '1k+' in badge_text:
            number = 1000
        elif '2k+' in badge_text:
            number = 2000
        elif '3k+' in badge_text:
            number = 3000
        elif '5k+' in badge_text:
            number = 5000
        elif '10k+' in badge_text:
            number = 10000
        else:
            # Extract number from patterns like "10+ recent orders", "100+ recent orders"
            match = re.search(r'(\d+)\+', badge_text)
            if match:
                number = int(match.group(1))
            else:
                return badge_text
        
        # Bin by 50
        binned_number = ((number - 1) // 50 + 1) * 50
        return binned_number
    
    return badge_text

def create_sorting_key(badge_info):
    """Create a sorting key for proper ordering"""
    if isinstance(badge_info, str) and 'most liked' in badge_info.lower():
        # Extract number for most liked badges
        match = re.search(r'#(\d+)', badge_info)
        if match:
            return (0, int(match.group(1)))  # Priority 0 for most liked, then by number
    elif isinstance(badge_info, (int, float)):
        return (1, badge_info)  # Priority 1 for recent orders, then by number
    else:
        return (2, 999999)  # Other badges go last

def main():
    # SQL query to execute - for view counts and positions
    sql_query = """
    SELECT DISTINCT 
        container, 
        experiment_group, 
        badges, 
        COUNT(1) AS cnt, 
        AVG(card_position) AS avg_card_position
    FROM proddb.fionafan.social_cue_m_card_view_full 
    WHERE store_id IS NOT NULL 
        AND (LOWER(badges) LIKE '%recent orders%' OR LOWER(badges) LIKE '%most liked%')
        AND container IN ('recommended_items_for_you', 'popular-items') 
    GROUP BY ALL 
    ORDER BY ALL
    """
    
    # SQL query for distinct items per badge (aggregated across containers)
    distinct_items_query = """
    SELECT 
        badges, 
        COUNT(DISTINCT item_id) AS distinct_items
    FROM proddb.fionafan.social_cue_m_card_view_full 
    WHERE store_id IS NOT NULL 
        AND LOWER(badges) LIKE '%recent orders%'
        AND container IN ('recommended_items_for_you', 'popular-items') 
    GROUP BY badges
    ORDER BY badges
    """
    
    # Execute both queries using SnowflakeHook
    print('Executing SQL queries to fetch badge data...')
    with SnowflakeHook() as hook:
        df = hook.query_snowflake(sql_query, method='pandas')
        print('Executing distinct items query...')
        distinct_items_df = hook.query_snowflake(distinct_items_query, method='pandas')
    
    print(f'Loaded {len(df)} rows from main query and {len(distinct_items_df)} rows from distinct items query')
    
    # FILTER OUT recent orders badges from control group (they shouldn't be there)
    before_filter = len(df)
    df = df[~((df['experiment_group'] == 'control') & 
              (df['badges'].str.contains('recent orders', case=False, na=False)))]
    after_filter = len(df)
    print(f'Filtered out {before_filter - after_filter} control group "recent orders" rows')
    
    # Convert avg_card_position to numeric (already named correctly from SQL query)
    df['avg_card_position'] = pd.to_numeric(df['avg_card_position'], errors='coerce')
    
    # Extract badge numbers for plotting (with 50-unit binning)
    df['badge_number'] = df['badges'].apply(extract_badge_number)
    
    # Create sorting key
    df['sort_key'] = df['badge_number'].apply(create_sorting_key)
    
    # Process distinct items data
    print(f'\nProcessing distinct items data...')
    distinct_items_df['badge_number'] = distinct_items_df['badges'].apply(extract_badge_number)
    distinct_items_df['sort_key'] = distinct_items_df['badge_number'].apply(create_sorting_key)
    
    # Group distinct items by 50-unit bins
    distinct_items_grouped = distinct_items_df.groupby('badge_number')['distinct_items'].sum().reset_index()
    distinct_items_grouped['sort_key'] = distinct_items_grouped['badge_number'].apply(create_sorting_key)
    distinct_items_grouped = distinct_items_grouped.sort_values('sort_key')
    
    # Print some examples of extraction
    print('\nBadge extraction examples (with 50-unit binning):')
    for i, row in df.head(15).iterrows():
        print(f'  {row["badges"]} -> {row["badge_number"]}')
    
    # Get unique containers
    containers = df['container'].unique()
    print(f'\nContainers: {containers}')
    
    # Create plots for each container - back to 2 subplots
    fig, axes = plt.subplots(1, 2, figsize=(20, 8))
    fig.suptitle('Badge Performance Analysis by Container\n(Bar Chart for Counts with Overlaid Line Plot for Average Card Position)', 
                 fontsize=16, fontweight='bold')
    
    # Pretty light colors for bars
    light_colors = {
        'control': '#98D8C8',       # Light teal
        'icon treatment': '#A8C8EC',  # Light blue  
        'no icon treatment': '#F7DC6F'  # Light yellow
    }
    
    # Darker colors for lines
    line_colors = {
        'control': '#2E8B57',       # Dark teal
        'icon treatment': '#1f77b4',  # Dark blue
        'no icon treatment': '#ff7f0e'  # Dark orange
    }
    
    markers = {'control': 'o', 'icon treatment': 's', 'no icon treatment': '^'}
    
    for idx, container in enumerate(containers):
        # Primary axis for count (bar chart)
        count_ax = axes[idx]
        
        container_data = df[df['container'] == container].copy()
        
        # Group by badge_number and experiment_group, sum counts and average positions
        grouped = container_data.groupby(['badge_number', 'experiment_group']).agg({
            'cnt': 'sum',
            'avg_card_position': 'mean'
        }).reset_index()
        
        # Sort by our custom sorting key
        grouped['sort_key'] = grouped['badge_number'].apply(create_sorting_key)
        grouped = grouped.sort_values('sort_key')
        
        # Get unique badge numbers in sorted order
        unique_badges = grouped['badge_number'].unique()
        x_positions = np.arange(len(unique_badges))
        
        # Bar width for grouped bars
        bar_width = 0.25
        exp_groups = grouped['experiment_group'].unique()
        
        # === COUNT BAR CHART ===
        for i, exp_group in enumerate(exp_groups):
            group_data = grouped[grouped['experiment_group'] == exp_group]
            
            # Prepare count data for plotting
            counts = []
            for badge in unique_badges:
                badge_data = group_data[group_data['badge_number'] == badge]
                if not badge_data.empty:
                    counts.append(badge_data['cnt'].iloc[0])
                else:
                    counts.append(0)
            
            # Plot bars with offset
            offset = (i - 1) * bar_width
            count_ax.bar(x_positions + offset, counts, 
                        bar_width, 
                        label=f'{exp_group} (count)',
                        color=light_colors[exp_group],
                        alpha=0.8,
                        edgecolor='white',
                        linewidth=0.5)
        
        # === POSITION LINE PLOT (OVERLAID) ===
        # Create secondary y-axis for position
        position_ax = count_ax.twinx()
        
        for exp_group in exp_groups:
            group_data = grouped[grouped['experiment_group'] == exp_group]
            
            # Prepare position data for plotting
            positions = []
            for badge in unique_badges:
                badge_data = group_data[group_data['badge_number'] == badge]
                if not badge_data.empty:
                    positions.append(badge_data['avg_card_position'].iloc[0])
                else:
                    positions.append(np.nan)
            
            # Plot line
            position_ax.plot(x_positions, positions, 
                           marker=markers[exp_group], 
                           color=line_colors[exp_group],
                           label=f'{exp_group} (position)',
                           linewidth=3,
                           markersize=10,
                           alpha=0.9)
        
        # === FORMATTING ===
        container_title = container.replace("_", " ").title()
        count_ax.set_title(f'{container_title}\nBars: Count | Lines: Average Card Position', 
                          fontweight='bold', fontsize=14)
        
        # Primary axis (count)
        count_ax.set_ylabel('Count of Badge Views', fontsize=12, color='black')
        count_ax.tick_params(axis='y', labelcolor='black')
        count_ax.grid(True, alpha=0.3, axis='y')
        count_ax.set_ylim(bottom=0)
        
        # Secondary axis (position)
        position_ax.set_ylabel('Average Card Position', fontsize=12, color='gray')
        position_ax.tick_params(axis='y', labelcolor='gray')
        position_ax.invert_yaxis()  # Lower position is better
        
        # Set x-axis labels
        x_labels = []
        for badge in unique_badges:
            if isinstance(badge, str) and 'most liked' in badge.lower():
                x_labels.append(badge)
            else:
                x_labels.append(f'{badge}+')
        
        count_ax.set_xticks(x_positions)
        count_ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=10)
        count_ax.set_xlabel('Badge Type', fontsize=12)
        
        # Format y-axes
        count_ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K' if x >= 1e3 else f'{x:.0f}'))
        position_ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}'))
        
        # Combined legend - both bar and line legends
        bars_legend = count_ax.get_legend_handles_labels()
        lines_legend = position_ax.get_legend_handles_labels()
        
        # Combine all legend elements
        all_handles = bars_legend[0] + lines_legend[0]
        all_labels = bars_legend[1] + lines_legend[1]
        
        # Place legend outside plot
        count_ax.legend(all_handles, all_labels, 
                       bbox_to_anchor=(1.15, 1), 
                       loc='upper left',
                       fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plot_filename = f'badge_container_analysis_overlay_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    
    print(f'\n📊 Container plots saved to: {plot_filename}')
    
    # === SEPARATE OVERLAY GRAPH: View Counts (bars) + Distinct Items (line) ===
    print('\nCreating separate overlay graph...')
    
    # Prepare aggregated view count data for recent orders only
    recent_orders_df = df[df['badges'].str.contains('recent orders', case=False, na=False)].copy()
    
    if not recent_orders_df.empty:
        # Aggregate view counts across containers and experiment groups by badge bins
        view_counts_grouped = recent_orders_df.groupby('badge_number')['cnt'].sum().reset_index()
        view_counts_grouped['sort_key'] = view_counts_grouped['badge_number'].apply(create_sorting_key)
        view_counts_grouped = view_counts_grouped.sort_values('sort_key')
        
        # Create separate figure for overlay
        fig2, overlay_ax = plt.subplots(1, 1, figsize=(14, 8))
        fig2.suptitle('Badge Analysis: View Counts (Bars) + Distinct Items (Line)\nwith Cumulative Percentages', 
                     fontsize=16, fontweight='bold')
        
        if not view_counts_grouped.empty and not distinct_items_grouped.empty:
            # Prepare data for plotting
            x_positions = np.arange(len(view_counts_grouped))
            
            # === VIEW COUNTS (BARS) ===
            view_counts = view_counts_grouped['cnt'].values
            bars = overlay_ax.bar(x_positions, view_counts,
                                 color='#A8C8EC',  # Light blue
                                 alpha=0.7,
                                 edgecolor='white',
                                 linewidth=1,
                                 label='Total View Counts')
            
            # === DISTINCT ITEMS (LINE) ===
            # Secondary y-axis for distinct items
            items_ax = overlay_ax.twinx()
            
            # Match distinct items data to view counts x-positions
            distinct_items_matched = []
            for badge_num in view_counts_grouped['badge_number']:
                matching_row = distinct_items_grouped[distinct_items_grouped['badge_number'] == badge_num]
                if not matching_row.empty:
                    distinct_items_matched.append(matching_row['distinct_items'].iloc[0])
                else:
                    distinct_items_matched.append(0)
            
            # Plot line for distinct items
            line = items_ax.plot(x_positions, distinct_items_matched,
                                marker='s',  # Square markers for better distinction
                                color='#E74C3C',  # Darker red for better contrast
                                linewidth=4,  # Thicker line
                                markersize=10,  # Larger markers
                                markerfacecolor='#E74C3C',
                                markeredgecolor='white',
                                markeredgewidth=2,
                                label='Distinct Items')
            
            # === CUMULATIVE PERCENTAGES ===
            # Calculate cumulative percentages for both metrics
            total_views = view_counts.sum()
            total_items = sum(distinct_items_matched)
            
            cumulative_view_pct = 0
            cumulative_items_pct = 0
            
            for i, (view_count, item_count) in enumerate(zip(view_counts, distinct_items_matched)):
                cumulative_view_pct += (view_count / total_views) * 100
                cumulative_items_pct += (item_count / total_items) * 100 if total_items > 0 else 0
                
                # Add cumulative percentage labels
                bar_height = bars[i].get_height()
                overlay_ax.text(bars[i].get_x() + bars[i].get_width()/2., 
                               bar_height + max(view_counts)*0.05,
                               f'{cumulative_view_pct:.1f}%', 
                               ha='center', va='bottom', fontsize=9, fontweight='bold')
                
                if item_count > 0:
                    items_ax.text(i, item_count + max(distinct_items_matched)*0.02,
                                 f'{cumulative_items_pct:.1f}%',
                                 ha='center', va='bottom', fontsize=9, fontweight='bold', color='#E74C3C')
            
            # === FORMATTING ===
            # Primary axis (view counts)
            overlay_ax.set_ylabel('Total View Counts', fontsize=12, color='blue')
            overlay_ax.tick_params(axis='y', labelcolor='blue')
            overlay_ax.grid(True, alpha=0.3, axis='y')
            overlay_ax.set_ylim(bottom=0)
            
            # Secondary axis (distinct items)
            items_ax.set_ylabel('Number of Distinct Items', fontsize=12, color='#E74C3C')
            items_ax.tick_params(axis='y', labelcolor='#E74C3C')
            items_ax.set_ylim(bottom=0)
            
            # X-axis
            x_labels = [f'{badge_num}+' for badge_num in view_counts_grouped['badge_number']]
            overlay_ax.set_xticks(x_positions)
            overlay_ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=11)
            overlay_ax.set_xlabel('Badge Type (50-unit bins)', fontsize=12)
            
            # Format y-axes
            overlay_ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K' if x >= 1e3 else f'{x:.0f}'))
            items_ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K' if x >= 1e3 else f'{x:.0f}'))
            
            # Combined legend
            bars_legend = overlay_ax.get_legend_handles_labels()
            lines_legend = items_ax.get_legend_handles_labels()
            all_handles = bars_legend[0] + lines_legend[0]
            all_labels = bars_legend[1] + lines_legend[1]
            overlay_ax.legend(all_handles, all_labels, loc='upper right', fontsize=11)
            
        plt.tight_layout()
        
        # Save the overlay plot
        overlay_filename = f'badge_overlay_view_items_{timestamp}.png'
        plt.savefig(overlay_filename, dpi=300, bbox_inches='tight')
        print(f'📊 Overlay plot saved to: {overlay_filename}')
    else:
        print('⚠️  No recent orders data available for overlay plot')
    
    # Print summary statistics with filtered data
    print('\n📈 Summary Statistics (control recent orders filtered out, 50-unit binning):')
    print('=' * 75)
    
    # Print distinct items summary first
    print(f'\n🔍 Distinct Items Summary (50-unit bins):')
    if not distinct_items_grouped.empty:
        total_distinct_items = distinct_items_grouped['distinct_items'].sum()
        print(f'  Total distinct items across all badge bins: {total_distinct_items:,}')
        print('  Top 5 badge bins by distinct items:')
        top_5_bins = distinct_items_grouped.nlargest(5, 'distinct_items')
        for _, row in top_5_bins.iterrows():
            badge_display = f'{row["badge_number"]}+' if isinstance(row["badge_number"], (int, float)) else row["badge_number"]
            print(f'    {badge_display}: {row["distinct_items"]:,} items')
    else:
        print('  No distinct items data available')
    
    for container in containers:
        container_data = df[df['container'] == container]
        print(f'\n{container.replace("_", " ").title()}:')
        
        # Most liked badges summary
        most_liked = container_data[container_data['badges'].str.contains('most liked', case=False, na=False)]
        if not most_liked.empty:
            print('  Most Liked Badges:')
            for exp_group in most_liked['experiment_group'].unique():
                group_data = most_liked[most_liked['experiment_group'] == exp_group]
                total_count = group_data['cnt'].sum()
                avg_position = group_data['avg_card_position'].mean()
                print(f'    {exp_group}: {total_count:,} total count, {avg_position:.2f} avg position')
        
        # Recent orders badges summary (binned) - control should now be empty
        recent_orders = container_data[container_data['badges'].str.contains('recent orders', case=False, na=False)]
        if not recent_orders.empty:
            print('  Recent Orders Badges (50-unit bins):')
            
            # Show breakdown by bins
            recent_grouped = recent_orders.groupby(['badge_number', 'experiment_group']).agg({
                'cnt': 'sum',
                'avg_card_position': 'mean'
            }).reset_index()
            
            for exp_group in recent_orders['experiment_group'].unique():
                group_data = recent_grouped[recent_grouped['experiment_group'] == exp_group]
                total_count = group_data['cnt'].sum()
                avg_position = group_data['avg_card_position'].mean()
                print(f'    {exp_group}: {total_count:,} total count, {avg_position:.2f} avg position')
                
                # Show top bins
                top_bins = group_data.nlargest(3, 'cnt')
                if not top_bins.empty:
                    bin_strings = [f'{int(row["badge_number"])}+ ({row["cnt"]:,})' for _, row in top_bins.iterrows()]
                    print(f'      Top bins: {", ".join(bin_strings)}')
        else:
            print('  Recent Orders Badges: None (filtered out for control)')

if __name__ == '__main__':
    main()
