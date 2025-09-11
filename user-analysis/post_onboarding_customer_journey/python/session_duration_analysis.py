import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import seaborn as sns
import sys
sys.path.append('..')
from utils.snowflake_connection import SnowflakeHook

def get_session_duration_data():
    """
    Query session duration data from Snowflake table
    """
    query = """
    SELECT 
        session_type,
        duration_bucket,
        sessions,
        pct_within_type,
        avg_duration_seconds,
        median_duration_seconds
    FROM proddb.fionafan.session_duration_by_order_outcome
    ORDER BY session_type DESC, 
        CASE duration_bucket
            WHEN '0-1 min' THEN 1
            WHEN '1-5 min' THEN 2
            WHEN '5-15 min' THEN 3
            WHEN '15-30 min' THEN 4
            WHEN '30+ min' THEN 5
        END
    """
    
    print("Executing query with SnowflakeHook...")
    print(f"Query: {query}")
    print("\n" + "="*80)
    
    # Execute query using SnowflakeHook
    try:
        with SnowflakeHook() as hook:
            df = hook.query_snowflake(query, method='pandas')
            print(f"✅ Query executed successfully. Retrieved {len(df)} rows.")
            print(f"Columns: {list(df.columns)}")
            print(f"Data types: {df.dtypes.to_dict()}")
            return df
    except Exception as e:
        print(f"❌ Error executing query: {str(e)}")
        return None

# Get data from Snowflake
print("🔍 Fetching session duration data from Snowflake...")
df = get_session_duration_data()

# Define duration order for consistent ordering across the script
duration_order = ["0-1 min", "1-5 min", "5-15 min", "15-30 min", "30+ min"]

if df is None or df.empty:
    print("❌ No data available or empty result set")
    print("\nTo execute this analysis:")
    print("1. Ensure the table proddb.fionafan.session_duration_by_order_outcome exists")
    print("2. Run the table creation queries in preference_scope.sql first")
    exit()

print("✅ Data retrieved successfully")

def create_session_duration_plots(df):
    """
    Create comprehensive visualizations of session duration patterns
    """
    
    # Set up the plot style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Session Duration Analysis: With Order vs Without Order\nJuly 2025 Onboarded Users', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Use global duration order for consistent ordering
    df['duration_bucket'] = pd.Categorical(df['duration_bucket'], categories=duration_order, ordered=True)
    df = df.sort_values(['session_type', 'duration_bucket'])
    
    # Colors for the two session types
    colors = ['#E74C3C', '#3498DB']  # Red for With Order, Blue for Without Order
    
    # 1. Percentage Distribution (Side-by-side bars)
    pivot_pct = df.pivot(index='duration_bucket', columns='session_type', values='pct_within_type')
    x = np.arange(len(duration_order))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, pivot_pct['With Order'], width, label='With Order', 
                   color=colors[0], alpha=0.8)
    bars2 = ax1.bar(x + width/2, pivot_pct['Without Order'], width, label='Without Order', 
                   color=colors[1], alpha=0.8)
    
    ax1.set_title('Session Duration Distribution (%)', fontweight='bold', fontsize=14)
    ax1.set_xlabel('Session Duration')
    ax1.set_ylabel('Percentage of Sessions')
    ax1.set_xticks(x)
    ax1.set_xticklabels(duration_order, rotation=45)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # Add percentage labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax1.annotate(f'{height:.1f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
    
    # 2. Median Session Duration
    pivot_median = df.pivot(index='duration_bucket', columns='session_type', values='median_duration_seconds')
    bars2 = ax2.bar(x - width/2, pivot_median['With Order'], width, label='With Order',
                   color=colors[0], alpha=0.8)
    bars3 = ax2.bar(x + width/2, pivot_median['Without Order'], width, label='Without Order',
                   color=colors[1], alpha=0.8)

    ax2.set_title('Median Session Duration (Seconds)', fontweight='bold', fontsize=14)
    ax2.set_xlabel('Session Duration Bucket')
    ax2.set_ylabel('Median Duration (Seconds)')
    ax2.set_xticks(x)
    ax2.set_xticklabels(duration_order, rotation=45)
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bars in [bars2, bars3]:
        for bar in bars:
            height = bar.get_height()
            ax2.annotate(f'{height:.0f}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
    
    # 3. Average Duration Comparison
    pivot_avg = df.pivot(index='duration_bucket', columns='session_type', values='avg_duration_seconds')
    bars_avg1 = ax3.bar(x - width/2, pivot_avg['With Order'], width, label='With Order',
                   color=colors[0], alpha=0.8)
    bars_avg2 = ax3.bar(x + width/2, pivot_avg['Without Order'], width, label='Without Order',
                   color=colors[1], alpha=0.8)
    
    ax3.set_title('Average Duration by Bucket (Seconds)', fontweight='bold', fontsize=14)
    ax3.set_xlabel('Session Duration Bucket')
    ax3.set_ylabel('Average Duration (Seconds)')
    ax3.set_xticks(x)
    ax3.set_xticklabels(duration_order, rotation=45)
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Key Insights Text Box
    insights_text = """
    KEY INSIGHTS:
    
    • Sessions WITH orders are much longer:
      - 65.7% are 30+ minutes
      - Only 0.5% are under 1 minute
      
    • Sessions WITHOUT orders are much shorter:
      - 48.4% are 0-1 minute 
      - Only 8.8% are 30+ minutes
    
    • Conversion pattern: Users who place orders
      spend significantly more time browsing,
      comparing options, and going through checkout.
      
    • Quick bounces: Nearly half of non-ordering 
      sessions last less than a minute, suggesting
      users quickly leave if they don't find what
      they want immediately.
    """
    
    ax4.text(0.05, 0.95, insights_text, transform=ax4.transAxes, 
             fontsize=11, verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
    ax4.set_xlim(0, 1)
    ax4.set_ylim(0, 1)
    ax4.axis('off')
    
    plt.tight_layout()
    plt.subplots_adjust(top=0.93)
    
    # Save the plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'session_duration_analysis_{timestamp}.png'
    plt.savefig(f'user-analysis/post_onboarding_customer_journey/plots/{filename}', 
                dpi=300, bbox_inches='tight')
    
    print(f"Session duration analysis plot saved as: {filename}")
    return filename

# Display summary statistics
print("\n" + "="*60)
print("SESSION DURATION ANALYSIS SUMMARY")
print("="*60)

with_order = df[df['session_type'] == 'With Order']
without_order = df[df['session_type'] == 'Without Order']

print(f"\nTOTAL SESSIONS:")
print(f"With Order:    {with_order['sessions'].sum():,}")
print(f"Without Order: {without_order['sessions'].sum():,}")

print(f"\nMOST COMMON DURATION:")
with_order_max = with_order.loc[with_order['pct_within_type'].idxmax()]
without_order_max = without_order.loc[without_order['pct_within_type'].idxmax()]

print(f"With Order:    {with_order_max['duration_bucket']} ({with_order_max['pct_within_type']:.1f}%)")
print(f"Without Order: {without_order_max['duration_bucket']} ({without_order_max['pct_within_type']:.1f}%)")

print(f"\nCONVERSION RATE BY DURATION:")
for duration in duration_order:
    with_count = with_order[with_order['duration_bucket'] == duration]['sessions'].iloc[0]
    without_count = without_order[without_order['duration_bucket'] == duration]['sessions'].iloc[0]
    total_count = with_count + without_count
    conversion_rate = (with_count / total_count) * 100 if total_count > 0 else 0
    print(f"{duration:>10}: {conversion_rate:5.1f}% conversion ({with_count:,} / {total_count:,})")

# Create and save plots
print("\n📊 Creating session duration plots...")
filename = create_session_duration_plots(df)
print(f"✅ Session duration analysis plot saved as: {filename}")

plt.show()
