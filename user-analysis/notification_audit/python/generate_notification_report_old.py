"""
Generate PDF report for notification metrics by pivot_by categories.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Set backend before importing pyplot
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
from datetime import datetime
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.snowflake_connection import SnowflakeHook

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

# Define the order of pivot_by categories
PIVOT_ORDER = [
    'overall',
    'is_braze',
    'is_fpn',
    'is_recommendation',
    'is_doordash_offer',
    'is_store_offer',
    'is_reminder',
    'is_nv',
    'is_dashpass',
    'is_new',
    'is_npws',
    'is_fmx',
    'is_challenge',
    'is_abandon_campaign',
    'is_post_order',
    'is_reorder',
    'is_gift_card_campaign'
]

# Color palette for cohort types
COHORT_COLORS = {
    'active': '#1f77b4',
    'new': '#ff7f0e',
    'post_onboarding': '#2ca02c'
}


def fetch_data():
    """Fetch data from Snowflake."""
    print("Fetching data from Snowflake...")
    hook = SnowflakeHook()
    query = """
    SELECT * 
    FROM proddb.fionafan.all_user_notifications_cohort_by_day_sent_pct 
    ORDER BY cohort_type, days_since_onboarding, pivot_by
    """
    df = hook.query_snowflake(query, method='pandas')
    hook.close()
    print(f"Loaded {len(df)} rows")
    return df


def create_toc_page(pdf, pivot_categories):
    """Create a table of contents page."""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    # Title
    ax.text(0.5, 0.95, 'Notification Performance Report', 
            ha='center', va='top', fontsize=20, fontweight='bold')
    ax.text(0.5, 0.92, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
            ha='center', va='top', fontsize=12, color='gray')
    
    # Table of contents
    ax.text(0.1, 0.85, 'Table of Contents', 
            ha='left', va='top', fontsize=16, fontweight='bold')
    
    y_pos = 0.80
    for i, pivot in enumerate(pivot_categories, 1):
        display_name = pivot.replace('_', ' ').title()
        ax.text(0.15, y_pos, f"{i}. {display_name}", 
                ha='left', va='top', fontsize=12)
        y_pos -= 0.03
        
        if y_pos < 0.1:  # Start new column if needed
            y_pos = 0.80
    
    # Footer
    ax.text(0.5, 0.05, 'Each section contains 5 charts:', 
            ha='center', va='bottom', fontsize=10, style='italic')
    ax.text(0.5, 0.03, 
            '1) % Messages with Tag  2) Open Rate*  3) Unsubscribe Rate*  4) Uninstall Rate*  5) Order Rate',
            ha='center', va='bottom', fontsize=9, color='gray')
    ax.text(0.5, 0.01, '* Asterisks indicate metrics exceeding thresholds',
            ha='center', va='bottom', fontsize=8, color='red')
    
    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def plot_metric(df_pivot, metric_col, ylabel, title, flag_col=None, ax=None):
    """
    Plot a metric over days since onboarding by cohort type.
    
    Args:
        df_pivot: DataFrame filtered to one pivot_by
        metric_col: Column name for y-axis
        ylabel: Label for y-axis
        title: Plot title
        flag_col: Optional column for asterisk markers (e.g., 'is_low_open')
        ax: Matplotlib axis object
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot lines for each cohort type
    for cohort in df_pivot['cohort_type'].unique():
        df_cohort = df_pivot[df_pivot['cohort_type'] == cohort].sort_values('days_since_onboarding')
        
        color = COHORT_COLORS.get(cohort, 'gray')
        
        # Plot line
        ax.plot(df_cohort['days_since_onboarding'], 
                df_cohort[metric_col], 
                marker='o', 
                label=cohort.replace('_', ' ').title(),
                color=color,
                linewidth=2,
                markersize=6)
        
        # Add asterisks for flagged points
        if flag_col and flag_col in df_cohort.columns:
            flagged = df_cohort[df_cohort[flag_col] == 1]
            if len(flagged) > 0:
                ax.scatter(flagged['days_since_onboarding'], 
                          flagged[metric_col],
                          marker='*',
                          s=80,
                          color='red',
                          zorder=5,
                          edgecolors='darkred',
                          linewidths=0.5)
    
    ax.set_xlabel('Days Since Onboarding', fontsize=12, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='best', frameon=True, shadow=True)
    ax.grid(True, alpha=0.3)
    
    # Limit x-axis to day 28
    ax.set_xlim(0, 28)
    
    # Format y-axis as percentage if it's a rate
    if 'rate' in metric_col.lower() or 'pct' in metric_col.lower():
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.1%}'))
    
    return ax


def create_pivot_section(pdf, df, pivot_by):
    """Create a 5-chart section for one pivot_by category."""
    df_pivot = df[df['pivot_by'] == pivot_by].copy()
    
    if len(df_pivot) == 0:
        print(f"  No data for {pivot_by}, skipping...")
        return
    
    # Create figure with 3x2 subplots (5 charts + 1 empty)
    fig, axes = plt.subplots(3, 2, figsize=(16, 18))
    fig.suptitle(f'{pivot_by.replace("_", " ").title()} - Notification Metrics',
                 fontsize=18, fontweight='bold', y=0.997)
    
    # 1. Percentage of messages with tag
    plot_metric(df_pivot, 
                'pct_messages_with_tag',
                'Percentage of Messages',
                '1. Percentage of Messages with Tag by Days Since Onboarding',
                ax=axes[0, 0])
    
    # 2. Open rate with low open flags
    plot_metric(df_pivot,
                'open_rate',
                'Open Rate',
                '2. Open Rate by Days Since Onboarding (* = Below Threshold)',
                flag_col='is_low_open',
                ax=axes[0, 1])
    
    # 3. Unsubscribe rate with high unsub flags
    plot_metric(df_pivot,
                'unsub_rate',
                'Unsubscribe Rate',
                '3. Unsubscribe Rate by Days Since Onboarding (* = Above Threshold)',
                flag_col='is_high_unsub',
                ax=axes[1, 0])
    
    # 4. Uninstall rate with high uninstall flags
    plot_metric(df_pivot,
                'uninstall_rate',
                'Uninstall Rate',
                '4. Uninstall Rate by Days Since Onboarding (* = Above Threshold)',
                flag_col='is_high_uninstall',
                ax=axes[1, 1])
    
    # 5. Order rate
    plot_metric(df_pivot,
                'order_rate',
                'Order Rate',
                '5. Order Rate by Days Since Onboarding',
                ax=axes[2, 0])
    
    # Hide the last subplot (3,2) since we only have 5 charts
    axes[2, 1].axis('off')
    
    plt.tight_layout()
    pdf.savefig(fig, bbox_inches='tight')
    plt.close()


def generate_report(output_path='notification_report.pdf'):
    """Generate the full PDF report."""
    print("Starting report generation...")
    
    # Fetch data
    df = fetch_data()
    
    # Get list of pivot categories in the data (in specified order)
    pivot_categories = [p for p in PIVOT_ORDER if p in df['pivot_by'].unique()]
    
    # Add any categories not in the predefined order
    remaining = [p for p in df['pivot_by'].unique() if p not in PIVOT_ORDER]
    pivot_categories.extend(sorted(remaining))
    
    print(f"Creating report with {len(pivot_categories)} sections...")
    
    # Create PDF
    with PdfPages(output_path) as pdf:
        # Table of contents
        print("Creating table of contents...")
        create_toc_page(pdf, pivot_categories)
        
        # Create section for each pivot_by
        for i, pivot in enumerate(pivot_categories, 1):
            print(f"Creating section {i}/{len(pivot_categories)}: {pivot}")
            create_pivot_section(pdf, df, pivot)
        
        # Set PDF metadata
        d = pdf.infodict()
        d['Title'] = 'Notification Performance Report'
        d['Author'] = 'Analytics Team'
        d['Subject'] = 'Notification metrics by campaign type and cohort'
        d['Keywords'] = 'Notifications, Metrics, Cohort Analysis'
        d['CreationDate'] = datetime.now()
    
    print(f"\n✅ Report saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"notification_report_{timestamp}.pdf"
    generate_report(output_path)

