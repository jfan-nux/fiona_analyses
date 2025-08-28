#!/usr/bin/env python3
"""
Plot preference entity distribution by page.

This script queries the preference toggle data and creates a visualization showing
the percentage distribution of number of preferred entities by page.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
from pathlib import Path

# Add utils directory to path for imports
utils_dir = Path(__file__).parent.parent.parent.parent / "utils"
sys.path.append(str(utils_dir))

from snowflake_connection import SnowflakeHook

def load_preference_data():
    """Load preference distribution data from Snowflake."""
    
    query = """
    with consumer_entity as( 
    SELECT consumer_id, entity_id, toggle_type, page
    FROM (
      SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY consumer_id, entity_id ORDER BY iguazu_event_time DESC) as rn
      FROM IGUAZU.CONSUMER.M_PREFERENCE_TOGGLE_ICE 
    ) WHERE rn = 1 and toggle_type = 'add' 
    )
    , second_base as (
    select consumer_id, page, count(distinct entity_id) entity_cnt
    from consumer_entity
    group by all
    )
    select 
        page, 
        entity_cnt,
        count(1) cnt,
        round(100.0 * count(1) / sum(count(1)) over (partition by page), 2) as pct
    from second_base 
    group by all 
    order by all;
    """
    
    # Use the SnowflakeHook class
    with SnowflakeHook() as snowflake_conn:
        df = snowflake_conn.query_snowflake(query, method='pandas')
    
    return df

def create_preference_distribution_plot(df):
    """Create a plot showing preference entity distribution by page."""
    
    # Set up the plot style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Get unique pages for different colors  
    pages = df['page'].unique()
    
    # Create grouped bar chart
    bar_width = 0.35
    x_positions = df['entity_cnt'].unique()
    x_indices = range(len(x_positions))
    
    # Plot bars for each page
    for i, page in enumerate(pages):
        page_data = df[df['page'] == page]
        
        # Create x positions for this page's bars
        x_pos = [x + (i * bar_width) for x in x_indices]
        
        # Map entity_cnt to percentages, filling 0 for missing values
        percentages = []
        for entity_cnt in x_positions:
            pct_row = page_data[page_data['entity_cnt'] == entity_cnt]
            if len(pct_row) > 0:
                percentages.append(pct_row['pct'].iloc[0])
            else:
                percentages.append(0)
        
        ax.bar(x_pos, percentages, bar_width, 
               label=f'{page}', alpha=0.8)
    
    # Customize the plot
    ax.set_xlabel('Number of Preferred Entities (entity_cnt)', fontsize=12)
    ax.set_ylabel('Percentage Distribution (%)', fontsize=12)
    ax.set_title('Distribution of Number of Preferred Entities by Page', fontsize=14, fontweight='bold')
    
    # Set x-axis labels
    ax.set_xticks([x + bar_width/2 for x in x_indices])
    ax.set_xticklabels(x_positions)
    
    # Add legend
    ax.legend(title='Page', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%.1f%%', rotation=90, padding=3, fontsize=9)
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    return fig

def main():
    """Main function to execute the plotting workflow."""
    print("Loading preference distribution data...")
    df = load_preference_data()
    
    print(f"Data loaded: {len(df)} rows")
    print("\nData preview:")
    print(df.head(10))
    
    print("\nCreating plot...")
    fig = create_preference_distribution_plot(df)
    
    # Save the plot
    output_dir = Path(__file__).parent.parent / "plots"
    output_dir.mkdir(exist_ok=True)
    
    output_path = output_dir / "preference_entity_distribution_by_page.png"
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    
    print(f"Plot saved to: {output_path}")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    main()
