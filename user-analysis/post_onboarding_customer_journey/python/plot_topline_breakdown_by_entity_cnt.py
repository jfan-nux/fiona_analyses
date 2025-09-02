#!/usr/bin/env python3
"""
Plot topline breakdown by entity count analysis
Creates a dual y-axis plot showing lift metrics and exposure
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import seaborn as sns
import os
import sys

# Add utils path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'utils'))
from snowflake_connection import SnowflakeHook

def read_sql_file(file_path):
    """Read SQL query from file"""
    with open(file_path, 'r') as file:
        return file.read()

def create_ordered_entity_segments(df):
    """Create ordered list of entity segments for x-axis"""
    # Get unique entity segments from the data
    segments = df['entity_segment'].unique()
    
    ordered_segments = []
    
    # Add control_overall first
    if 'control_overall' in segments:
        ordered_segments.append('control_overall')
    
    # Add overall_treatment second (overall treatment vs control comparison)
    if 'overall_treatment' in segments:
        ordered_segments.append('overall_treatment')
    
    # Add entity_cnt_1 through entity_cnt_20 in numerical order
    for i in range(1, 21):
        seg_name = f'entity_cnt_{i}'
        if seg_name in segments:
            ordered_segments.append(seg_name)
    
    # Add no_preference last
    if 'no_preference' in segments:
        ordered_segments.append('no_preference')
    
    # Add any remaining segments
    remaining = [s for s in segments if s not in ordered_segments]
    ordered_segments.extend(remaining)
    
    return ordered_segments

def plot_topline_breakdown(df):
    """Create dual y-axis plot with lift metrics and exposure"""
    
    # Create ordered segments for x-axis
    ordered_segments = create_ordered_entity_segments(df)
    
    # Filter and reorder the dataframe to include all segments in proper order
    plot_df = df[df['entity_segment'].isin(ordered_segments)].copy()
    plot_df['entity_segment'] = pd.Categorical(
        plot_df['entity_segment'], 
        categories=ordered_segments, 
        ordered=True
    )
    plot_df = plot_df.sort_values('entity_segment')
    
    # Set up the plot
    fig, ax1 = plt.subplots(figsize=(16, 10))
    
    # Define colors for lift metrics (excluding VP and GOV per device)
    colors = {
        'lift_order_rate': '#1f77b4',
        'lift_new_cx_rate': '#ff7f0e', 
        'lift_mau_rate': '#9467bd'
    }
    
    # Plot lift metrics as line plots on left y-axis
    x_pos = np.arange(len(plot_df))
    
    for metric, color in colors.items():
        if metric in plot_df.columns:
            values = plot_df[metric].fillna(0)
            
            # Only plot non-control segments for lift metrics (control should be baseline)
            plot_mask = plot_df['entity_segment'] != 'control_overall'
            x_plot = x_pos[plot_mask]
            values_plot = values[plot_mask]
            
            # Plot line with markers
            ax1.plot(x_plot, values_plot, 
                    label=metric.replace('lift_', '').replace('_', ' ').title(), 
                    color=color, marker='o', linewidth=2, markersize=6, alpha=0.8)
            
            # Add value labels on points
            for x, val in zip(x_plot, values_plot):
                if abs(val) > 0.001:  # Only show if non-zero
                    ax1.annotate(f'{val:.1%}',
                               xy=(x, val),
                               xytext=(0, 8 if val >= 0 else -15),
                               textcoords="offset points",
                               ha='center', va='bottom' if val >= 0 else 'top',
                               fontsize=8, rotation=0)
    
    # Set up left y-axis
    ax1.set_xlabel('Entity Segment', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Lift (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Topline Breakdown by Entity Count - Lift Metrics vs Exposure', 
                  fontsize=14, fontweight='bold', pad=20)
    
    # Format y-axis as percentage
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.1%}'))
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    
    # Set x-axis labels
    ax1.set_xticks(x_pos)  # Center the labels
    
    # Format labels properly for different segment types
    formatted_labels = []
    for segment in plot_df['entity_segment']:
        if segment == 'control_overall':
            formatted_labels.append('Control Overall')
        elif segment == 'overall_treatment':
            formatted_labels.append('Overall Treatment')
        elif segment.startswith('entity_cnt_'):
            num = segment.replace('entity_cnt_', '')
            formatted_labels.append(f'Entity Count {num}')
        elif segment == 'no_preference':
            formatted_labels.append('No Preference')
        else:
            formatted_labels.append(segment.replace('_', ' ').title())
    
    ax1.set_xticklabels(formatted_labels, rotation=45, ha='right')
    
    # Create second y-axis for exposure
    ax2 = ax1.twinx()
    
    # Plot exposure as bars on the second y-axis
    bar_width = 0.4  # Width for exposure bars
    exposure_bars = ax2.bar(x_pos, plot_df['exposure'], 
                           bar_width, label='Exposure', 
                           color='red', alpha=0.6, edgecolor='darkred')
    
    # Add exposure value labels on bars
    for bar, val in zip(exposure_bars, plot_df['exposure']):
        height = bar.get_height()
        ax2.annotate(f'{int(val):,}',
                   xy=(bar.get_x() + bar.get_width() / 2, height),
                   xytext=(0, 5),
                   textcoords="offset points",
                   ha='center', va='bottom',
                   fontsize=8, fontweight='bold',
                   color='darkred', rotation=90)
    
    ax2.set_ylabel('Exposure (Count)', fontsize=12, fontweight='bold', color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    
    # Format exposure axis
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{int(y):,}'))
    
    # Align both y-axes at 0
    y1_min, y1_max = ax1.get_ylim()
    y2_min, y2_max = ax2.get_ylim()
    
    # Calculate the ratio to align 0 points
    # We want 0 on both axes to be at the same position
    if y1_min < 0 and y1_max > 0:
        # Lift axis has both positive and negative values
        y1_zero_pos = -y1_min / (y1_max - y1_min)
    else:
        # All positive or all negative, set zero position to bottom
        y1_zero_pos = 0
    
    # Adjust y2 axis to align zero at the same position
    if y1_zero_pos > 0:
        y2_range = y2_max - y2_min
        y2_new_min = -y1_zero_pos * y2_range / (1 - y1_zero_pos)
        y2_new_max = y2_max
        ax2.set_ylim(y2_new_min, y2_new_max)
    
    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    
    ax1.legend(lines1 + lines2, labels1 + labels2, 
              loc='upper left', bbox_to_anchor=(0, 1), framealpha=0.9)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot in the plots directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plots_dir = "plots"
    filename = f"topline_breakdown_by_entity_cnt_{timestamp}.png"
    filepath = os.path.join(plots_dir, filename)
    
    # Create plots directory if it doesn't exist
    os.makedirs(plots_dir, exist_ok=True)
    
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {filepath}")
    
    plt.show()
    
    return fig

def execute_query_and_get_data():
    """Execute the SQL query and return DataFrame"""
    
    # Read SQL query from file
    sql_query = read_sql_file("sql/topline_breakdown_by_entity_cnt.sql")
    
    print("Executing Snowflake query...")
    
    try:
        # Create Snowflake hook and execute query
        hook = SnowflakeHook()
        df = hook.query_snowflake(sql_query, method='pandas')
        
        print(f"Query executed successfully. Retrieved {len(df)} rows.")
        
        # Clean up
        hook.close()
        
        return df
        
    except Exception as e:
        print(f"Error executing query: {e}")
        raise e



def main():
    """Main execution function"""
    print("Loading topline breakdown by entity count analysis...")
    
    # Execute SQL query and get data
    df = execute_query_and_get_data()
    
    if df is None or df.empty:
        print("No data returned from query!")
        return
    
    print(f"Data loaded: {len(df)} rows")
    print("\nEntity segments in data:")
    print(df['entity_segment'].value_counts())
    
    print("\nColumns in data:")
    print(df.columns.tolist())
    
    print("\nSample of data:")
    lift_cols = [col for col in df.columns if col.startswith('lift_')]  # lowercase for Snowflake
    available_cols = ['entity_segment', 'exposure'] + [col for col in lift_cols if col in df.columns]
    print(df[available_cols].head(10))
    
    print(f"\nFound lift columns: {lift_cols}")
    
    # Create the plot
    print("\nCreating plot...")
    fig = plot_topline_breakdown(df)
    
    print("Analysis complete!")

if __name__ == "__main__":
    # Change to the analysis directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    analysis_dir = os.path.dirname(script_dir)
    os.chdir(analysis_dir)
    
    main()
