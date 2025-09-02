#!/usr/bin/env python3
"""
Plot funnel view rate analysis by intent level.

This script visualizes treatment view rates and lift values across the onboarding funnel
broken down by user intent levels (no preference, low intent, high intent).
Only view steps are included (no click steps). Exposure is shown as text annotation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from pathlib import Path

# Add utils directory to path for imports
utils_dir = Path(__file__).parent.parent.parent.parent / "utils"
sys.path.append(str(utils_dir))

from snowflake_connection import SnowflakeHook

def load_funnel_data():
    """Load funnel data broken down by intent level."""
    
    # Read the SQL query from file
    sql_file = Path(__file__).parent.parent / "sql" / "funnel_breakdown_by_intent_level.sql"
    with open(sql_file, 'r') as f:
        query = f.read()
    
    # Load data
    with SnowflakeHook() as snowflake_conn:
        df = snowflake_conn.query_snowflake(query, method='pandas')
    
    return df

def prepare_funnel_data(df):
    """Prepare data for plotting."""
    
    # Define funnel step order (only view steps, no click steps)
    funnel_steps = [
        'start_page_view', 
        'notification_view',
        'marketing_sms_view',
        'att_view',
        'end_page_view'
    ]
    
    rate_columns = [
        'start_page_view_rate',
        'notification_view_rate',
        'marketing_sms_view_rate', 
        'att_view_rate',
        'end_page_view_rate'
    ]
    
    lift_columns = [
        'lift_start_page_view_rate',
        'lift_notification_view_rate', 
        'lift_marketing_sms_view_rate',
        'lift_att_view_rate',
        'lift_end_page_view_rate'
    ]
    
    # Filter treatment data for rate plots
    treatment_df = df[df['tag'] == 'treatment'].copy()
    
    # Prepare data for each intent level
    intent_levels = ['no preference', 'low intent', 'high intent']
    
    plot_data = {}
    
    for intent in intent_levels:
        intent_data = treatment_df[treatment_df['intent_level'] == intent].copy()
        
        if len(intent_data) > 0:
            # Treatment rates (only view rates, no exposure)
            rates = [intent_data[col].iloc[0] if col in intent_data.columns else 0 
                     for col in rate_columns]
            
            # Lift values (only view lifts, no exposure)
            lifts = [intent_data[col].iloc[0] if col in intent_data.columns and pd.notna(intent_data[col].iloc[0]) else 0 
                     for col in lift_columns]
            
        else:
            # Handle case where no data for this intent level
            rates = [0] * len(funnel_steps)
            lifts = [0] * len(funnel_steps)
        
        plot_data[intent] = {
            'steps': funnel_steps,
            'rates': rates,
            'lifts': lifts,
            'exposure': intent_data['exposure'].iloc[0] if len(intent_data) > 0 else 0
        }
    
    return plot_data

def create_funnel_plots(plot_data):
    """Create funnel visualization with 6 subplots."""
    
    # Set up the figure with 2 rows and 3 columns
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    
    intent_levels = ['no preference', 'low intent', 'high intent']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # Blue, Orange, Green
    
    # Top row: Treatment rates
    for i, intent in enumerate(intent_levels):
        ax = axes[0, i]
        data = plot_data[intent]
        
        # Plot treatment rates
        x_pos = range(len(data['steps']))
        ax.plot(x_pos, data['rates'], marker='o', linewidth=2.5, 
                markersize=6, color=colors[i], alpha=0.8)
        
        # Customize the subplot
        ax.set_title(f'View Rates: {intent.title()}', 
                    fontsize=12, fontweight='bold')
        ax.set_ylabel('View Rate', fontsize=11)
        ax.set_xticks(x_pos)
        ax.set_xticklabels([step.replace('_', '\n').replace('view', '') for step in data['steps']], 
                          rotation=45, ha='right', fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 1.1)
        
        # Add exposure as text annotation
        ax.text(0.02, 0.98, f'Exposure: {data["exposure"]:,}', 
                transform=ax.transAxes, fontsize=10, fontweight='bold',
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # Add value labels on points
        for j, (x, y) in enumerate(zip(x_pos, data['rates'])):
            if y > 0:  # Only show label if rate > 0
                ax.annotate(f'{y:.3f}', (x, y), textcoords="offset points", 
                           xytext=(0,10), ha='center', fontsize=8)
    
    # Bottom row: Lift values
    for i, intent in enumerate(intent_levels):
        ax = axes[1, i]
        data = plot_data[intent]
        
        # Plot lift values (all steps since no exposure)
        x_pos = range(len(data['steps']))
        lifts = data['lifts']
        
        # Create bar plot for lifts
        bars = ax.bar(x_pos, lifts, color=colors[i], alpha=0.7, width=0.6)
        
        # Color bars: green for positive lift, red for negative
        for bar, lift in zip(bars, lifts):
            if lift > 0:
                bar.set_color('#2ca02c')  # Green
            elif lift < 0:
                bar.set_color('#d62728')  # Red
            else:
                bar.set_color('#7f7f7f')  # Gray
        
        # Add horizontal line at y=0
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        
        # Customize the subplot
        ax.set_title(f'View Rate Lift vs Control: {intent.title()}', 
                    fontsize=12, fontweight='bold')
        ax.set_ylabel('Lift (% points)', fontsize=11)
        ax.set_xticks(x_pos)
        ax.set_xticklabels([step.replace('_', '\n').replace('view', '') for step in data['steps']], 
                          rotation=45, ha='right', fontsize=9)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for j, (x, y) in enumerate(zip(x_pos, lifts)):
            if abs(y) > 0.001:  # Only show label if lift is meaningful
                ax.annotate(f'{y:.3f}', (x, y), textcoords="offset points", 
                           xytext=(0, 5 if y >= 0 else -15), ha='center', fontsize=8)
    
    # Add overall title
    fig.suptitle('Onboarding Funnel Analysis by Intent Level\nView Rates & Lift vs Control', 
                fontsize=16, fontweight='bold', y=0.95)
    
    # Adjust layout
    plt.tight_layout()
    plt.subplots_adjust(top=0.90, hspace=0.4, wspace=0.3)
    
    return fig

def main():
    """Main function to execute the plotting workflow."""
    print("Loading funnel data by intent level...")
    df = load_funnel_data()
    
    print(f"Data loaded: {len(df)} rows")
    print(f"Intent levels: {sorted(df['intent_level'].unique())}")
    print(f"Tags: {sorted(df['tag'].unique())}")
    
    print("\nPreparing data for plotting...")
    plot_data = prepare_funnel_data(df)
    
    print("\nCreating funnel plots...")
    fig = create_funnel_plots(plot_data)
    
    # Save the plot
    output_dir = Path(__file__).parent.parent / "plots"
    output_dir.mkdir(exist_ok=True)
    
    output_path = output_dir / "funnel_view_rates_by_intent_level.png"
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    
    print(f"\nPlot saved to: {output_path}")
    
    # Show summary statistics
    print("\n" + "="*60)
    print("FUNNEL SUMMARY BY INTENT LEVEL (VIEW RATES ONLY)")
    print("="*60)
    
    for intent, data in plot_data.items():
        print(f"\n{intent.upper()}:")
        print(f"  Exposure: {data['exposure']:,}")
        print(f"  Start Page View Rate: {data['rates'][0]:.3f}")
        print(f"  ATT View Rate: {data['rates'][3]:.3f}")  
        print(f"  End Page View Rate: {data['rates'][4]:.3f}")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    main()
