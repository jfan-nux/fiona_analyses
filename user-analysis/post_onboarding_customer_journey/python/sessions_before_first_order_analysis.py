import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import seaborn as sns
import sys
sys.path.append('..')
from utils.snowflake_connection import SnowflakeHook

def get_sessions_before_first_order_data():
    """
    Query sessions before first order data from Snowflake table
    """
    query = """
    SELECT
        day_type,
        sessions_before_first_order,
        consumers,
        pct_within_day_type AS pct_of_total
    FROM proddb.fionafan.sessions_before_first_order
    ORDER BY day_type, sessions_before_first_order
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
            
            # The actual table doesn't have day_type distinction, so we'll analyze overall patterns
            # Rename column for consistency with rest of the code
            df = df.rename(columns={'pct_of_total': 'pct_within_type'})
            
            return df
    except Exception as e:
        print(f"❌ Error executing query: {str(e)}")
        return None

# Get data from Snowflake
print("🔍 Fetching sessions before first order data from Snowflake...")
df = get_sessions_before_first_order_data()

if df is None or df.empty:
    print("❌ No data available or empty result set")
    print("\nTo execute this analysis:")
    print("1. Ensure the table proddb.fionafan.sessions_before_first_order exists")
    print("2. Run the table creation queries in preference_scope.sql first")
    exit()

print("✅ Data retrieved successfully")

def create_sessions_before_first_order_plots(df):
    """
    Create comprehensive visualizations of sessions before first order patterns
    """
    
    # Set up the plot style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Sessions Before First Cart/Order Analysis\nJuly 2025 Onboarded Users', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Colors
    colors = {'Same Day': '#3498DB', 'Different Day': '#E74C3C'}

    # Split data by day type
    df_same_day = df[df['day_type'] == 'Same Day']
    df_diff_day = df[df['day_type'] == 'Different Day']

    # 1. Distribution comparison by day type
    df_same_top15 = df_same_day[df_same_day['sessions_before_first_order'] <= 15]
    df_diff_top15 = df_diff_day[df_diff_day['sessions_before_first_order'] <= 15]

    bars1 = ax1.bar(df_same_top15['sessions_before_first_order'] - 0.2, df_same_top15['pct_within_type'],
                   width=0.4, color=colors['Same Day'], alpha=0.8, label='Same Day', edgecolor='white', linewidth=0.5)
    bars2 = ax1.bar(df_diff_top15['sessions_before_first_order'] + 0.2, df_diff_top15['pct_within_type'],
                   width=0.4, color=colors['Different Day'], alpha=0.8, label='Different Day', edgecolor='white', linewidth=0.5)
    
    ax1.set_title('Distribution: Sessions Before First Order by Day Type', fontweight='bold', fontsize=14)
    ax1.set_xlabel('Number of Sessions Before First Order')
    ax1.set_ylabel('Percentage of Users (%)')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # Add percentage labels on bars for values > 2%
    for bar in bars1:
        height = bar.get_height()
        if height > 2:
            ax1.annotate(f'{height:.1f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
    
    # 2. Cumulative conversion pattern
    df_sorted = df.sort_values('sessions_before_first_order')
    cumsum_pct = df_sorted['pct_within_type'].cumsum()
    
    ax2.plot(df_sorted['sessions_before_first_order'], cumsum_pct,
             marker='o', linewidth=3, markersize=6, color=colors['Different Day'])
    
    # Add key milestone lines
    ax2.axhline(y=50, color='gray', linestyle='--', alpha=0.7, label='50% of users')
    ax2.axhline(y=80, color='gray', linestyle=':', alpha=0.7, label='80% of users')
    
    ax2.set_title('Cumulative Conversion Pattern', fontweight='bold', fontsize=14)
    ax2.set_xlabel('Sessions Before First Cart')
    ax2.set_ylabel('Cumulative Percentage (%)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xlim(1, min(25, df['sessions_before_first_order'].max()))
    
    # 3. Absolute consumer counts (Log scale) - by day type
    ax3.bar(df_same_top15['sessions_before_first_order'] - 0.2, df_same_top15['consumers'],
            width=0.4, color=colors['Same Day'], alpha=0.8, label='Same Day', edgecolor='white', linewidth=0.5)
    ax3.bar(df_diff_top15['sessions_before_first_order'] + 0.2, df_diff_top15['consumers'],
            width=0.4, color=colors['Different Day'], alpha=0.8, label='Different Day', edgecolor='white', linewidth=0.5)
    
    ax3.set_title('Absolute Consumer Counts (Log Scale)', fontweight='bold', fontsize=14)
    ax3.set_xlabel('Number of Sessions Before First Order')
    ax3.set_ylabel('Number of Consumers (Log Scale)')
    ax3.set_yscale('log')
    ax3.legend()
    ax3.grid(axis='y', alpha=0.3)
    
    # 4. Key Insights and Statistics
    total_consumers = df['consumers'].sum()
    median_sessions = df.loc[cumsum_pct >= 50, 'sessions_before_first_order'].iloc[0]
    pct_1_session = df[df['sessions_before_first_order'] == 1]['pct_within_type'].iloc[0] if len(df[df['sessions_before_first_order'] == 1]) > 0 else 0
    pct_within_3 = df[df['sessions_before_first_order'] <= 3]['pct_within_type'].sum()
    
    insights_text = f"""
    KEY INSIGHTS FROM DATA:
    
    📊 TOTAL ANALYSIS:
    • Total consumers who placed first cart: {total_consumers:,}
    • Median sessions before first cart: {median_sessions}
    
    🚀 CONVERSION SPEED:
    • {pct_1_session:.1f}% convert after just 1 session (impulse)
    • {pct_within_3:.1f}% convert within 3 sessions (fast)
    • 50% convert by session {median_sessions}
    
    🎯 BEHAVIOR PATTERNS:
    • Quick decision makers: {pct_1_session:.1f}%
    • Research-oriented users need more sessions
    • Long tail: Some users take 20+ sessions
    
    💡 IMPLICATIONS:
    • Strong first impression is critical
    • Multiple touchpoints needed for conversion
    • Personalization can accelerate decisions
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
    filename = f'sessions_before_first_order_analysis_{timestamp}.png'
    plt.savefig(f'user-analysis/post_onboarding_customer_journey/plots/{filename}', 
                dpi=300, bbox_inches='tight')
    
    print(f"Sessions before first order analysis plot saved as: {filename}")
    return filename

def print_analysis_summary(df):
    """
    Print summary statistics for sessions before first order
    """
    print("\n" + "="*70)
    print("SESSIONS BEFORE FIRST CART/ORDER ANALYSIS SUMMARY")
    print("="*70)
    
    total_consumers = df['consumers'].sum()
    
    # Calculate key metrics
    df_sorted = df.sort_values('sessions_before_first_order')
    cumsum_pct = df_sorted['pct_within_type'].cumsum()
    
    # Find key percentiles
    median_sessions = df_sorted.loc[cumsum_pct >= 50, 'sessions_before_first_order'].iloc[0]
    p25_sessions = df_sorted.loc[cumsum_pct >= 25, 'sessions_before_first_order'].iloc[0]
    p75_sessions = df_sorted.loc[cumsum_pct >= 75, 'sessions_before_first_order'].iloc[0]
    
    # Immediate vs researched behavior
    pct_1_session = df[df['sessions_before_first_order'] == 1]['pct_within_type'].iloc[0] if len(df[df['sessions_before_first_order'] == 1]) > 0 else 0
    pct_within_3 = df[df['sessions_before_first_order'] <= 3]['pct_within_type'].sum()
    pct_within_5 = df[df['sessions_before_first_order'] <= 5]['pct_within_type'].sum()
    
    print(f"\n📊 OVERALL STATISTICS:")
    print(f"Total consumers analyzed: {total_consumers:,}")
    print(f"25th percentile: {p25_sessions} sessions")
    print(f"Median sessions: {median_sessions} sessions") 
    print(f"75th percentile: {p75_sessions} sessions")
    
    print(f"\n🚀 CONVERSION SPEED:")
    print(f"Immediate (1 session): {pct_1_session:.1f}%")
    print(f"Fast (≤3 sessions): {pct_within_3:.1f}%")
    print(f"Quick (≤5 sessions): {pct_within_5:.1f}%")
    
    print(f"\n💡 KEY INSIGHTS:")
    print(f"• {pct_1_session:.1f}% of users convert immediately (impulse buyers)")
    print(f"• {100-pct_within_5:.1f}% of users need >5 sessions (researchers)")
    print(f"• Median user needs {median_sessions} sessions to convert")
    print(f"• Wide distribution suggests diverse user behavior patterns")

# Execute analysis
if df is not None and not df.empty:
    print("📊 Creating sessions before first order plots...")
    filename = create_sessions_before_first_order_plots(df)
    
    # Print summary
    print_analysis_summary(df)
    
    print(f"\n✅ Analysis complete! Plots saved as: {filename}")
else:
    print("❌ No data available for analysis")
