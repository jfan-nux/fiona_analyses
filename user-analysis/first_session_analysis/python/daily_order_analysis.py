"""
Daily Order Analysis for July Cohorts
Creates visualizations showing:
1. Average orders per consumer over 28 days
2. Cumulative conversion rate (% who have ordered)
3. Day-over-day changes in ordering behavior
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from utils.snowflake_connection import SnowflakeHook


def get_order_data(hook):
    """
    Query order data with daily metrics indexed to day 0
    
    Args:
        hook: SnowflakeHook instance
    
    Returns:
        DataFrame with daily order metrics by cohort
    """
    
    query = """
WITH total_cohort AS (
    SELECT 
        cohort_type,
        COUNT(DISTINCT consumer_id) as total_consumers
    FROM proddb.fionafan.all_user_july_cohort
    GROUP BY cohort_type
),

daily_orders AS (
    SELECT
        cohort_type,
        days_since_onboarding,
        COUNT(DISTINCT consumer_id) AS consumers_ordering_today,
        COUNT(DISTINCT delivery_id) AS orders_today
    FROM proddb.fionafan.july_cohort_deliveries_28d
    WHERE days_since_onboarding BETWEEN 0 AND 28
    GROUP BY cohort_type, days_since_onboarding
),

day_0_baseline AS (
    SELECT
        cohort_type,
        consumers_ordering_today as day_0_consumers,
        orders_today as day_0_orders
    FROM daily_orders
    WHERE days_since_onboarding = 0
)

SELECT
    d.cohort_type,
    d.days_since_onboarding,
    c.total_consumers,
    
    -- Daily metrics
    d.consumers_ordering_today,
    d.orders_today,
    
    -- Day 0 baseline
    b.day_0_consumers,
    b.day_0_orders,
    
    -- Percentage of day 0
    ROUND(100.0 * d.consumers_ordering_today::FLOAT / NULLIF(b.day_0_consumers, 0), 2) as pct_of_day_0_consumers,
    ROUND(100.0 * d.orders_today::FLOAT / NULLIF(b.day_0_orders, 0), 2) as pct_of_day_0_orders,
    
    -- Day-over-day changes
    LAG(d.consumers_ordering_today) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as prev_day_consumers,
    d.consumers_ordering_today - LAG(d.consumers_ordering_today) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as consumer_change,
    LAG(d.orders_today) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as prev_day_orders,
    d.orders_today - LAG(d.orders_today) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as order_change

FROM daily_orders d
INNER JOIN total_cohort c ON d.cohort_type = c.cohort_type
INNER JOIN day_0_baseline b ON d.cohort_type = b.cohort_type
WHERE d.days_since_onboarding BETWEEN 1 AND 28
ORDER BY d.cohort_type, d.days_since_onboarding
    """
    
    df = hook.query_snowflake(query, method='pandas')
    return df


def plot_order_analysis(df, output_pdf):
    """
    Create a figure with 2 subplots:
    1. Consumers ordering each day (% of day 0)
    2. Orders per day (% of day 0)
    
    Args:
        df: DataFrame with order data
        output_pdf: PdfPages object to save to
    """
    
    # Get unique cohorts
    cohorts = df['cohort_type'].unique()
    colors = {'active': '#1f77b4', 'new': '#ff7f0e', 'post_onboarding': '#2ca02c'}
    
    # Create figure with 2 subplots
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('28-Day Order Analysis by Cohort (% of Day 0)', fontsize=16, fontweight='bold')
    
    # Subplot 1: Consumers Ordering Each Day (% of Day 0)
    ax1 = axes[0]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax1.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['pct_of_day_0_consumers'],
            marker='o',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax1.set_xlabel('Days Since Onboarding', fontsize=12)
    ax1.set_ylabel('% of Day 0 Consumers', fontsize=12)
    ax1.set_title('Daily Active Ordering Consumers (% of Day 0)', fontsize=13, fontweight='bold')
    ax1.legend(loc='best', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(1, 28)
    ax1.axhline(y=100, color='gray', linestyle='--', linewidth=1, alpha=0.5, label='Day 0 Baseline')
    
    # Subplot 2: Orders Per Day (% of Day 0)
    ax2 = axes[1]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax2.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['pct_of_day_0_orders'],
            marker='s',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax2.set_xlabel('Days Since Onboarding', fontsize=12)
    ax2.set_ylabel('% of Day 0 Orders', fontsize=12)
    ax2.set_title('Daily Order Volume (% of Day 0)', fontsize=13, fontweight='bold')
    ax2.legend(loc='best', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, 28)
    ax2.axhline(y=100, color='gray', linestyle='--', linewidth=1, alpha=0.5, label='Day 0 Baseline')
    
    plt.tight_layout()
    output_pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def plot_day_over_day_changes(df, output_pdf):
    """
    Create a figure with 2 subplots showing day-over-day changes:
    1. Change in consumers ordering
    2. Change in order volume
    
    Args:
        df: DataFrame with order data
        output_pdf: PdfPages object to save to
    """
    
    cohorts = df['cohort_type'].unique()
    colors = {'active': '#1f77b4', 'new': '#ff7f0e', 'post_onboarding': '#2ca02c'}
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('Day-over-Day Changes in Ordering Behavior', fontsize=16, fontweight='bold')
    
    # Subplot 1: Day-over-day change in consumers ordering
    ax1 = axes[0]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax1.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['consumer_change'],
            marker='o',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax1.set_xlabel('Days Since Onboarding', fontsize=12)
    ax1.set_ylabel('Change in Consumers Ordering', fontsize=12)
    ax1.set_title('Day-over-Day Change in Active Ordering Consumers', fontsize=13, fontweight='bold')
    ax1.legend(loc='best', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(1, 28)
    ax1.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    # Subplot 2: Day-over-day change in orders
    ax2 = axes[1]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax2.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['order_change'],
            marker='s',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax2.set_xlabel('Days Since Onboarding', fontsize=12)
    ax2.set_ylabel('Change in Order Volume', fontsize=12)
    ax2.set_title('Day-over-Day Change in Order Volume', fontsize=13, fontweight='bold')
    ax2.legend(loc='best', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, 28)
    ax2.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    plt.tight_layout()
    output_pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def main():
    """
    Main function to run order analysis and create visualizations
    """
    
    print("=" * 80)
    print("28-Day Order Analysis by Cohort")
    print("=" * 80)
    
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'plots'
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # Create PDF filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pdf_filename = f'order_analysis_28d_{timestamp}.pdf'
    pdf_path = os.path.join(output_dir, pdf_filename)
    
    print(f"\nOutput PDF: {pdf_path}")
    print("\nQuerying order data...")
    
    # Initialize Snowflake connection
    hook = SnowflakeHook(
        database='PRODDB',
        schema='FIONAFAN',
        warehouse='TEAM_DATA_ANALYTICS',
        role='FIONAFAN'
    )
    
    try:
        # Set warehouse explicitly
        hook.connect()
        hook.query_without_result("USE WAREHOUSE TEAM_DATA_ANALYTICS")
        hook.query_without_result("USE DATABASE PRODDB")
        hook.query_without_result("USE SCHEMA FIONAFAN")
        
        # Get order data
        print("  - Fetching daily order metrics...")
        df = get_order_data(hook)
        
        # Create PDF with plots
        print("  - Creating visualizations...")
        with PdfPages(pdf_path) as pdf:
            # Main analysis plots
            plot_order_analysis(df, pdf)
            
            # Day-over-day changes
            plot_day_over_day_changes(df, pdf)
            
            # Add metadata page
            fig = plt.figure(figsize=(12, 8))
            fig.text(0.5, 0.5, 
                    f'28-Day Order Analysis\n\n'
                    f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n'
                    f'Analysis includes:\n'
                    f'  1. Daily active ordering consumers (% of Day 0)\n'
                    f'  2. Daily order volume (% of Day 0)\n'
                    f'  3. Day-over-day changes in consumers\n'
                    f'  4. Day-over-day changes in orders\n\n'
                    f'Baseline: All metrics expressed as % of Day 0 values\n'
                    f'Time Period: Days 1-28 since onboarding\n'
                    f'Cohorts: Active, New, Post Onboarding\n'
                    f'Data Source: proddb.fionafan.july_cohort_deliveries_28d',
                    ha='center', va='center', fontsize=12, family='monospace')
            pdf.savefig(fig)
            plt.close()
        
        # Print summary stats
        print("\n" + "=" * 80)
        print("Summary Statistics by Cohort:")
        print("=" * 80)
        for cohort in df['cohort_type'].unique():
            cohort_data = df[df['cohort_type'] == cohort]
            total_consumers = cohort_data['total_consumers'].iloc[0]
            day_0_consumers = cohort_data['day_0_consumers'].iloc[0]
            day_0_orders = cohort_data['day_0_orders'].iloc[0]
            day_28_pct_consumers = cohort_data['pct_of_day_0_consumers'].iloc[-1]
            day_28_pct_orders = cohort_data['pct_of_day_0_orders'].iloc[-1]
            
            print(f"\n{cohort.replace('_', ' ').title()}:")
            print(f"  Total Consumers in Cohort: {total_consumers:,}")
            print(f"  Day 0 Consumers Ordering: {day_0_consumers:,}")
            print(f"  Day 0 Orders: {day_0_orders:,}")
            print(f"  Day 28 (% of Day 0 Consumers): {day_28_pct_consumers:.2f}%")
            print(f"  Day 28 (% of Day 0 Orders): {day_28_pct_orders:.2f}%")
        
        print("\n" + "=" * 80)
        print(f"✓ Analysis complete! PDF saved to:")
        print(f"  {pdf_path}")
        print("=" * 80)
    
    finally:
        # Close Snowflake connection
        hook.close()


if __name__ == "__main__":
    main()

