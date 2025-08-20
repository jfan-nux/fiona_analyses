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
    print('Executing SQL queries to fetch card position analysis data...')
    
    # SQL Query 1: No Icon Treatment vs Control
    no_icon_query = """
    select 
        no_icon_tagged_recent_order_volume, 
        avg(control_card_position) as avg_control_position, 
        avg(no_icon_card_position) as avg_no_icon_position, 
        avg(icon_card_position) as avg_icon_position, 
        count(1) as count, 
        avg(case when no_icon_card_position > control_card_position then 1 else 0 end) as pct_item_greater
    from proddb.fionafan.social_cue_m_card_click_item_card_position 
    where control_card_position is not null 
      and no_icon_card_position is not null 
    group by all
    order by no_icon_tagged_recent_order_volume;
    """
    
    # SQL Query 2: Icon Treatment vs Control  
    icon_query = """
    select 
        icon_tagged_recent_order_volume, 
        avg(control_card_position) as avg_control_position, 
        avg(icon_card_position) as avg_icon_position, 
        count(1) as count, 
        avg(case when icon_card_position > control_card_position then 1 else 0 end) as pct_item_greater
    from proddb.fionafan.social_cue_m_card_click_item_card_position 
    where control_card_position is not null 
      and icon_card_position is not null
    group by all
    order by icon_tagged_recent_order_volume;
    """
    
    # Execute queries using SnowflakeHook
    with SnowflakeHook() as hook:
        print('Fetching No Icon treatment data...')
        df_no_icon = hook.query_snowflake(no_icon_query, method='pandas')
        print(f'Loaded {len(df_no_icon)} rows for No Icon analysis')
        
        print('Fetching Icon treatment data...')
        df_icon = hook.query_snowflake(icon_query, method='pandas')
        print(f'Loaded {len(df_icon)} rows for Icon analysis')
    
    # Create the plots
    fig, axes = plt.subplots(1, 2, figsize=(20, 8))
    fig.suptitle('Item-Level Card Position Analysis by Recent Order Volume\n(Treatment vs Control Card Positions)', fontsize=16, fontweight='bold')
    
    # Plot 1: No Icon Treatment vs Control
    ax1 = axes[0]
    if not df_no_icon.empty:
        x_no_icon = df_no_icon['no_icon_tagged_recent_order_volume']
        y_control_1 = df_no_icon['avg_control_position']
        y_no_icon = df_no_icon['avg_no_icon_position']
        
        ax1.plot(x_no_icon, y_control_1, marker='o', color='#1f77b4', label='Control', linewidth=2.5, markersize=8, alpha=0.8)
        ax1.plot(x_no_icon, y_no_icon, marker='s', color='#ff7f0e', label='No Icon Treatment', linewidth=2.5, markersize=8, alpha=0.8)
        
        ax1.set_title('No Icon Treatment vs Control', fontweight='bold', fontsize=14)
        ax1.set_xlabel('No Icon Tagged Recent Order Volume', fontsize=12)
        ax1.set_ylabel('Average Card Position', fontsize=12)
        ax1.legend(fontsize=11)
        ax1.grid(True, alpha=0.3)
        
        # Invert y-axis since lower position numbers are better (position 1 is top)
        ax1.invert_yaxis()
    
    # Plot 2: Icon Treatment vs Control  
    ax2 = axes[1]
    if not df_icon.empty:
        x_icon = df_icon['icon_tagged_recent_order_volume']
        y_control_2 = df_icon['avg_control_position']
        y_icon = df_icon['avg_icon_position']
        
        ax2.plot(x_icon, y_control_2, marker='o', color='#1f77b4', label='Control', linewidth=2.5, markersize=8, alpha=0.8)
        ax2.plot(x_icon, y_icon, marker='^', color='#2ca02c', label='Icon Treatment', linewidth=2.5, markersize=8, alpha=0.8)
        
        ax2.set_title('Icon Treatment vs Control', fontweight='bold', fontsize=14)
        ax2.set_xlabel('Icon Tagged Recent Order Volume', fontsize=12)
        ax2.set_ylabel('Average Card Position', fontsize=12)
        ax2.legend(fontsize=11)
        ax2.grid(True, alpha=0.3)
        
        # Invert y-axis since lower position numbers are better (position 1 is top)
        ax2.invert_yaxis()
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    plots_dir = Path(__file__).parent.parent / 'plots'
    plots_dir.mkdir(exist_ok=True)
    plot_filename = plots_dir / f'card_position_analysis_plots_{timestamp}.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    
    print(f'\n📊 Plots saved to: {plot_filename}')
    
    # Print summary statistics
    print('\n📈 Card Position Analysis Summary:')
    print('=' * 80)
    
    if not df_no_icon.empty:
        print('\n🔸 No Icon Treatment vs Control:')
        print(f'  Data points: {len(df_no_icon)} volume levels')
        print(f'  Volume range: {df_no_icon["no_icon_tagged_recent_order_volume"].min():.0f} to {df_no_icon["no_icon_tagged_recent_order_volume"].max():.0f}')
        print(f'  Control avg position: {df_no_icon["avg_control_position"].mean():.2f}')
        print(f'  No Icon avg position: {df_no_icon["avg_no_icon_position"].mean():.2f}')
        print(f'  Average % items with worse position: {df_no_icon["pct_item_greater"].mean()*100:.1f}%')
        
        # Find where no icon performs better (lower position = better)
        better_positions = df_no_icon[df_no_icon['avg_no_icon_position'] < df_no_icon['avg_control_position']]
        print(f'  Volume levels where No Icon has better position: {len(better_positions)}/{len(df_no_icon)}')
        
    if not df_icon.empty:
        print('\n🔹 Icon Treatment vs Control:')
        print(f'  Data points: {len(df_icon)} volume levels')
        print(f'  Volume range: {df_icon["icon_tagged_recent_order_volume"].min():.0f} to {df_icon["icon_tagged_recent_order_volume"].max():.0f}')
        print(f'  Control avg position: {df_icon["avg_control_position"].mean():.2f}')
        print(f'  Icon avg position: {df_icon["avg_icon_position"].mean():.2f}')
        print(f'  Average % items with worse position: {df_icon["pct_item_greater"].mean()*100:.1f}%')
        
        # Find where icon performs better (lower position = better)
        better_positions = df_icon[df_icon['avg_icon_position'] < df_icon['avg_control_position']]
        print(f'  Volume levels where Icon has better position: {len(better_positions)}/{len(df_icon)}')
    
    # Print detailed breakdown for key volume levels
    if not df_no_icon.empty:
        print('\n📋 No Icon Treatment Details by Volume Level:')
        for _, row in df_no_icon.iterrows():
            vol = row['no_icon_tagged_recent_order_volume']
            ctrl_pos = row['avg_control_position']
            treatment_pos = row['avg_no_icon_position']
            pct_worse = row['pct_item_greater'] * 100
            count = row['count']
            position_diff = treatment_pos - ctrl_pos
            
            print(f'  Volume {vol:.0f}: Control {ctrl_pos:.2f} vs No Icon {treatment_pos:.2f} (diff: {position_diff:+.2f}, {pct_worse:.1f}% worse, n={count})')
    
    if not df_icon.empty:
        print('\n📋 Icon Treatment Details by Volume Level:')
        for _, row in df_icon.iterrows():
            vol = row['icon_tagged_recent_order_volume']
            ctrl_pos = row['avg_control_position']
            treatment_pos = row['avg_icon_position']
            pct_worse = row['pct_item_greater'] * 100
            count = row['count']
            position_diff = treatment_pos - ctrl_pos
            
            print(f'  Volume {vol:.0f}: Control {ctrl_pos:.2f} vs Icon {treatment_pos:.2f} (diff: {position_diff:+.2f}, {pct_worse:.1f}% worse, n={count})')
    
    print(f'\n📊 Card Position Analysis Summary:')
    print(f'  Analysis: Treatment vs Control card positioning by recent order volume')
    print(f'  Interpretation: Lower position numbers are better (position 1 = top of results)')
    print(f'  Key Metric: Difference in average card position (negative = treatment performs better)')
    print(f'  Source Table: proddb.fionafan.social_cue_m_card_click_item_card_position')

if __name__ == '__main__':
    main()
