"""
Daily Session and Retention Analysis by Day of Week
Creates visualizations showing:
1. Average sessions per person over 28 days
2. Retention rate from baseline over 28 days

Runs analysis for each day of week (Mon-Sun) plus overall aggregate
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

# Day of week mapping (Snowflake DAYOFWEEK: 0=Sunday, 1=Monday, ..., 6=Saturday)
DAY_NAMES = {
    0: 'Sunday',
    1: 'Monday',
    2: 'Tuesday', 
    3: 'Wednesday',
    4: 'Thursday',
    5: 'Friday',
    6: 'Saturday',
    None: 'Overall (All Days)'
}

def get_session_dropoff_data(hook, day_of_week=None):
    """
    Query session and dropoff data for a specific day of week or overall
    
    Args:
        hook: SnowflakeHook instance
        day_of_week: Integer 1-7 for specific day, None for overall
    
    Returns:
        DataFrame with daily session and dropoff metrics by cohort
    """
    
    # Build day filter
    day_filter = f"AND DAYOFWEEK(onboarding_day) = {day_of_week}" if day_of_week else ""
    
    query = f"""
WITH total_cohort AS (
  SELECT 
    cohort_type,
    COUNT(DISTINCT consumer_id) as total_consumers
  FROM proddb.fionafan.all_user_sessions_28d
  WHERE 1=1
    {day_filter}
  GROUP BY cohort_type
),

daily_active_consumers AS (
  SELECT
    cohort_type,
    days_since_onboarding,
    COUNT(DISTINCT consumer_id) as active_consumers,
    COUNT(DISTINCT dd_session_id) as total_sessions
  FROM proddb.fionafan.all_user_sessions_28d
  WHERE days_since_onboarding BETWEEN 0 AND 28
    {day_filter}
  GROUP BY cohort_type, days_since_onboarding
),

all_days AS (
  SELECT 
    cohort_type,
    -1 as days_since_onboarding,
    total_consumers as active_consumers,
    0 as total_sessions
  FROM total_cohort
  
  UNION ALL
  
  SELECT 
    cohort_type,
    days_since_onboarding,
    active_consumers,
    total_sessions
  FROM daily_active_consumers
)

SELECT
  d.cohort_type,
  d.days_since_onboarding,
  d.active_consumers,
  d.total_sessions,
  CASE WHEN d.total_sessions > 0 
    THEN ROUND(d.total_sessions::FLOAT / NULLIF(d.active_consumers, 0), 2)
    ELSE NULL 
  END as avg_sessions_per_consumer,
  
  -- Drop-off from previous day
  LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as prev_day_consumers,
  d.active_consumers - LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding) as dropoff_from_prev_day,
  ROUND(
    100.0 * (d.active_consumers - LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding))::FLOAT 
    / NULLIF(LAG(d.active_consumers) OVER (PARTITION BY d.cohort_type ORDER BY d.days_since_onboarding), 0),
    2
  ) as pct_change_from_prev_day,
  
  -- Retention from day -1 (baseline)
  c.total_consumers as day_minus_1_baseline,
  d.active_consumers - c.total_consumers as dropoff_from_baseline,
  ROUND(100.0 * d.active_consumers::FLOAT / NULLIF(c.total_consumers, 0), 2) as retention_rate_from_baseline,
  ROUND(100.0 * (c.total_consumers - d.active_consumers)::FLOAT / NULLIF(c.total_consumers, 0), 2) as dropoff_rate_from_baseline

FROM all_days d
INNER JOIN total_cohort c
  ON d.cohort_type = c.cohort_type
WHERE d.days_since_onboarding >= 1  -- Start from day 1, exclude day 0
ORDER BY d.cohort_type, d.days_since_onboarding
    """
    
    df = hook.query_snowflake(query, method='pandas')
    return df


def plot_session_dropoff(df, day_label, output_pdf):
    """
    Create a figure with 4 subplots for a given day of week:
    1. Average sessions per consumer
    2. Retention rate from baseline
    3. Day-over-day change in retention rate
    4. Day-over-day change in average sessions
    
    Args:
        df: DataFrame with session and dropoff data
        day_label: String label for the day (e.g., "Monday" or "Overall")
        output_pdf: PdfPages object to save to
    """
    
    # Get unique cohorts (column names are lowercase from SnowflakeHook)
    cohorts = df['cohort_type'].unique()
    colors = {'active': '#1f77b4', 'new': '#ff7f0e', 'post_onboarding': '#2ca02c'}
    
    # Create figure with 4 subplots
    fig, axes = plt.subplots(4, 1, figsize=(12, 16))
    fig.suptitle(f'Session and Retention Analysis - {day_label}', fontsize=16, fontweight='bold')
    
    # Subplot 1: Average Sessions Per Consumer
    ax1 = axes[0]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax1.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['avg_sessions_per_consumer'],
            marker='o',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax1.set_xlabel('Days Since Onboarding', fontsize=12)
    ax1.set_ylabel('Average Sessions Per Consumer', fontsize=12)
    ax1.set_title('Average Sessions Per Consumer Over Time', fontsize=13, fontweight='bold')
    ax1.legend(loc='best', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(1, 28)
    
    # Subplot 2: Retention Rate from Baseline
    ax2 = axes[1]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding')
        ax2.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['retention_rate_from_baseline'],
            marker='s',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax2.set_xlabel('Days Since Onboarding', fontsize=12)
    ax2.set_ylabel('Retention Rate from Baseline (%)', fontsize=12)
    ax2.set_title('Retention Rate from Baseline Over Time', fontsize=13, fontweight='bold')
    ax2.legend(loc='best', fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, 28)
    ax2.set_ylim(0, 100)
    
    # Subplot 3: Day-over-Day Changes (Retention)
    ax3 = axes[2]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding').copy()
        # Calculate day-over-day change in retention rate
        cohort_data['retention_change'] = cohort_data['retention_rate_from_baseline'].diff()
        ax3.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['retention_change'],
            marker='^',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax3.set_xlabel('Days Since Onboarding', fontsize=12)
    ax3.set_ylabel('Daily Change in Retention (%)', fontsize=12)
    ax3.set_title('Day-over-Day Change in Retention Rate', fontsize=13, fontweight='bold')
    ax3.legend(loc='best', fontsize=10)
    ax3.grid(True, alpha=0.3)
    ax3.set_xlim(1, 28)
    ax3.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    # Subplot 4: Day-over-Day Changes (Sessions)
    ax4 = axes[3]
    for cohort in cohorts:
        cohort_data = df[df['cohort_type'] == cohort].sort_values('days_since_onboarding').copy()
        # Calculate day-over-day change in average sessions
        cohort_data['sessions_change'] = cohort_data['avg_sessions_per_consumer'].diff()
        ax4.plot(
            cohort_data['days_since_onboarding'],
            cohort_data['sessions_change'],
            marker='d',
            label=cohort.replace('_', ' ').title(),
            color=colors.get(cohort, None),
            linewidth=2,
            markersize=4
        )
    
    ax4.set_xlabel('Days Since Onboarding', fontsize=12)
    ax4.set_ylabel('Daily Change in Avg Sessions', fontsize=12)
    ax4.set_title('Day-over-Day Change in Average Sessions Per Consumer', fontsize=13, fontweight='bold')
    ax4.legend(loc='best', fontsize=10)
    ax4.grid(True, alpha=0.3)
    ax4.set_xlim(1, 28)
    ax4.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    plt.tight_layout()
    output_pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def main():
    """
    Main function to run analysis for all days of week + overall
    and create PDF with all visualizations
    """
    
    print("=" * 80)
    print("Daily Session and Retention Analysis by Day of Week")
    print("=" * 80)
    
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'plots'
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # Create PDF filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pdf_filename = f'session_dropoff_by_day_of_week_{timestamp}.pdf'
    pdf_path = os.path.join(output_dir, pdf_filename)
    
    print(f"\nOutput PDF: {pdf_path}")
    print("\nRunning analysis for:")
    
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
        
        # Create PDF
        with PdfPages(pdf_path) as pdf:
            
            # First, run overall analysis (no day filter)
            print("  - Overall (All Days)")
            df_overall = get_session_dropoff_data(hook, day_of_week=None)
            plot_session_dropoff(df_overall, DAY_NAMES[None], pdf)
            
            # Then run for each day of week (0-6: Sunday-Saturday)
            for day_num in range(0, 7):
                day_name = DAY_NAMES[day_num]
                print(f"  - {day_name} (DAYOFWEEK={day_num})")
                
                df = get_session_dropoff_data(hook, day_of_week=day_num)
                plot_session_dropoff(df, day_name, pdf)
        
            # Add metadata page
            fig = plt.figure(figsize=(12, 8))
            fig.text(0.5, 0.5, 
                    f'Session and Retention Analysis\n\n'
                    f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n'
                    f'Analysis includes:\n'
                    f'  - Overall (all days combined)\n'
                    f'  - Each day of week (Sunday - Saturday)\n\n'
                    f'Metrics:\n'
                    f'  1. Average Sessions Per Consumer\n'
                    f'  2. Retention Rate from Baseline (%)\n'
                    f'  3. Day-over-Day Change in Retention (%)\n'
                    f'  4. Day-over-Day Change in Sessions\n\n'
                    f'Days Since Onboarding: 1-28 days\n'
                    f'Cohorts: Active, New, Post Onboarding',
                    ha='center', va='center', fontsize=12, family='monospace')
            pdf.savefig(fig)
            plt.close()
        
        print("\n" + "=" * 80)
        print(f"✓ Analysis complete! PDF saved to:")
        print(f"  {pdf_path}")
        print("=" * 80)
    
    finally:
        # Close Snowflake connection
        hook.close()
    

if __name__ == "__main__":
    main()

