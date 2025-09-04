#!/usr/bin/env python3
"""
Time Window Lift Analysis for Should Pin Leaderboard Carousel Experiment

This script runs the time window analysis template with different post-onboarding
time windows and creates lift plots showing how metrics evolve over time.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Add the utils directory to the path to import Snowflake connection
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../utils'))

from snowflake_connection import SnowflakeHook

def run_time_window_analysis():
    """Run time window analysis for multiple time windows and create lift plots."""
    
    # Define time windows to analyze
    time_windows = [
        {'time_value': 1, 'time_unit': 'hour', 'display_name': '1H', 'hours': 1},
        {'time_value': 4, 'time_unit': 'hour', 'display_name': '4H', 'hours': 4}, 
        {'time_value': 12, 'time_unit': 'hour', 'display_name': '12H', 'hours': 12},
        {'time_value': 24, 'time_unit': 'hour', 'display_name': '24H', 'hours': 24},
        {'time_value': 48, 'time_unit': 'hour', 'display_name': '48H', 'hours': 48},
        {'time_value': 1, 'time_unit': 'week', 'display_name': '1W', 'hours': 168}
    ]
    
    # Load SQL template
    template_path = '../sql/time_window_analysis_template.sql'
    with open(template_path, 'r') as f:
        sql_template = f.read()
    
    results = []
    
    print("Running time window analysis for multiple windows...")
    
    for window in time_windows:
        print(f"Processing {window['display_name']} window...")
        
        # Replace template parameters
        sql_query = sql_template.replace('{time_value}', str(window['time_value']))
        sql_query = sql_query.replace('{time_unit}', window['time_unit'])
        
        try:
            # Execute query using SnowflakeHook
            with SnowflakeHook() as hook:
                df = hook.query_snowflake(sql_query, method='pandas')
            
            # Add time window metadata
            df['hours_post_onboarding'] = window['hours']
            df['window_display'] = window['display_name']
            
            results.append(df)
            print(f"✓ Completed {window['display_name']} - {len(df)} rows")
            
        except Exception as e:
            print(f"✗ Error processing {window['display_name']}: {str(e)}")
            continue
    
    if not results:
        print("No results obtained. Cannot create plots.")
        return None
    
    # Combine all results
    all_results = pd.concat(results, ignore_index=True)
    
    # Create lift plots
    create_lift_plots(all_results)
    
    return all_results


def create_lift_plots(df):
    """Create lift plots showing metric evolution over time."""
    
    # Filter to only treatment group (has lift values)
    treatment_df = df[df['tag'] == 'treatment'].copy()
    
    if len(treatment_df) == 0:
        print("No treatment data found for plotting.")
        return
    
    # Sort by hours post onboarding
    treatment_df = treatment_df.sort_values('hours_post_onboarding')
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Lift Evolution: Should Pin Leaderboard Carousel Experiment\n'
                 'Post-Onboarding Time Windows', fontsize=16, fontweight='bold')
    
    # Plot 1: Order Rate Lift
    ax1 = axes[0, 0]
    ax1.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_order_rate'] * 100, 
             marker='o', linewidth=2.5, markersize=8, 
             color='#2E86AB', alpha=0.8)
    ax1.set_title('Order Rate Lift (%)', fontweight='bold', fontsize=12)
    ax1.set_xlabel('Hours Post-Onboarding')
    ax1.set_ylabel('Lift (%)')
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.7)
    
    # Add annotations for each point
    for _, row in treatment_df.iterrows():
        ax1.annotate(f"{row['lift_order_rate']*100:.1f}%", 
                    (row['hours_post_onboarding'], row['lift_order_rate']*100),
                    textcoords="offset points", xytext=(0,10), ha='center',
                    fontsize=9, alpha=0.8)
    
    # Plot 2: New Customer Rate Lift  
    ax2 = axes[0, 1]
    ax2.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_new_cx_rate'] * 100, 
             marker='s', linewidth=2.5, markersize=8, 
             color='#A23B72', alpha=0.8)
    ax2.set_title('New Customer Rate Lift (%)', fontweight='bold', fontsize=12)
    ax2.set_xlabel('Hours Post-Onboarding')
    ax2.set_ylabel('Lift (%)')
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=0, color='red', linestyle='--', alpha=0.7)
    
    for _, row in treatment_df.iterrows():
        ax2.annotate(f"{row['lift_new_cx_rate']*100:.1f}%", 
                    (row['hours_post_onboarding'], row['lift_new_cx_rate']*100),
                    textcoords="offset points", xytext=(0,10), ha='center',
                    fontsize=9, alpha=0.8)
    
    # Plot 3: MAU Rate Lift
    ax3 = axes[1, 0] 
    ax3.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_mau_rate'] * 100, 
             marker='^', linewidth=2.5, markersize=8, 
             color='#F18F01', alpha=0.8)
    ax3.set_title('MAU Rate Lift (%)', fontweight='bold', fontsize=12)
    ax3.set_xlabel('Hours Post-Onboarding')
    ax3.set_ylabel('Lift (%)')
    ax3.grid(True, alpha=0.3)
    ax3.axhline(y=0, color='red', linestyle='--', alpha=0.7)
    
    for _, row in treatment_df.iterrows():
        ax3.annotate(f"{row['lift_mau_rate']*100:.1f}%", 
                    (row['hours_post_onboarding'], row['lift_mau_rate']*100),
                    textcoords="offset points", xytext=(0,10), ha='center',
                    fontsize=9, alpha=0.8)
    
    # Plot 4: Combined Metrics
    ax4 = axes[1, 1]
    ax4.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_order_rate'] * 100, 
             marker='o', linewidth=2, label='Order Rate', alpha=0.8)
    ax4.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_new_cx_rate'] * 100, 
             marker='s', linewidth=2, label='New Customer Rate', alpha=0.8)
    ax4.plot(treatment_df['hours_post_onboarding'], 
             treatment_df['lift_mau_rate'] * 100, 
             marker='^', linewidth=2, label='MAU Rate', alpha=0.8)
    
    ax4.set_title('All Metrics Combined', fontweight='bold', fontsize=12)
    ax4.set_xlabel('Hours Post-Onboarding')
    ax4.set_ylabel('Lift (%)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.axhline(y=0, color='red', linestyle='--', alpha=0.7)
    
    # Adjust layout and save
    plt.tight_layout()
    
    # Save plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"../plots/time_window_lift_analysis_{timestamp}.png"
    
    # Create plots directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    plt.savefig(filename, dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    print(f"✓ Lift plots saved to: {filename}")
    
    # Show plot if running interactively
    plt.show()
    
    # Create a summary table
    create_summary_table(treatment_df, timestamp)

def create_summary_table(df, timestamp):
    """Create and save a summary table of results."""
    
    # Prepare summary data
    summary = df[['window_display', 'hours_post_onboarding', 'exposure_onboard', 
                  'order_rate', 'lift_order_rate', 'new_cx_rate', 'lift_new_cx_rate', 
                  'mau_rate', 'lift_mau_rate']].copy()
    
    # Format percentages
    pct_cols = ['order_rate', 'lift_order_rate', 'new_cx_rate', 'lift_new_cx_rate', 
                'mau_rate', 'lift_mau_rate']
    
    for col in pct_cols:
        summary[col] = (summary[col] * 100).round(2)
    
    # Rename columns for clarity
    summary.columns = ['Time Window', 'Hours Post-Onboarding', 'Users', 
                       'Order Rate (%)', 'Order Rate Lift (%)', 
                       'New Cx Rate (%)', 'New Cx Rate Lift (%)',
                       'MAU Rate (%)', 'MAU Rate Lift (%)']
    
    # Save to CSV
    summary_file = f"../plots/time_window_summary_{timestamp}.csv"
    summary.to_csv(summary_file, index=False)
    print(f"✓ Summary table saved to: {summary_file}")
    
    # Print summary to console
    print("\n" + "="*80)
    print("TIME WINDOW ANALYSIS SUMMARY")
    print("="*80)
    print(summary.to_string(index=False))
    print("="*80)

def main():
    """Main function to run the analysis."""
    print("Starting Time Window Lift Analysis...")
    print("Experiment: Should Pin Leaderboard Carousel")
    print("Analysis: Post-onboarding time windows\n")
    
    try:
        results = run_time_window_analysis()
        if results is not None:
            print(f"\n✓ Analysis complete! Processed {len(results)} total result rows.")
        else:
            print("\n✗ Analysis failed - no results obtained.")
    
    except Exception as e:
        print(f"\n✗ Analysis failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
