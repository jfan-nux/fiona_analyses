import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import seaborn as sns
import sys
sys.path.append('..')
from utils.snowflake_connection import SnowflakeHook

def get_stores_visited_data():
    """
    Query the stores visited data and aggregate by store counts
    """
    
    # Query 1: Aggregate by loaded_store_count
    loaded_query = """
    SELECT 
        session_type,
        loaded_store_count AS store_count,
        SUM(sessions) AS total_sessions,
        'Loaded Stores' AS metric_type
    FROM proddb.fionafan.stores_visited_by_order_outcome
    WHERE loaded_store_count IS NOT NULL
    GROUP BY session_type, loaded_store_count
    ORDER BY session_type, loaded_store_count
    """
    
    # Query 2: Aggregate by viewed_stores_count  
    viewed_query = """
    SELECT 
        session_type,
        viewed_stores_count AS store_count,
        SUM(sessions) AS total_sessions,
        'Viewed Stores' AS metric_type
    FROM proddb.fionafan.stores_visited_by_order_outcome
    WHERE viewed_stores_count IS NOT NULL
    GROUP BY session_type, viewed_stores_count
    ORDER BY session_type, viewed_stores_count
    """
    
    # Combined query to get both metrics
    combined_query = f"""
    WITH loaded_data AS (
        {loaded_query}
    ),
    viewed_data AS (
        {viewed_query}
    )
    SELECT * FROM loaded_data
    UNION ALL
    SELECT * FROM viewed_data
    ORDER BY metric_type, session_type, store_count
    """
    
    print("Executing query with SnowflakeHook...")
    print(f"Query preview: {combined_query[:200]}...")
    print("\n" + "="*80)
    
    # Execute query using SnowflakeHook
    try:
        with SnowflakeHook() as hook:
            df = hook.query_snowflake(combined_query, method='pandas')
            print(f"✅ Query executed successfully. Retrieved {len(df)} rows.")
            print(f"Columns: {list(df.columns)}")
            print(f"Data types: {df.dtypes.to_dict()}")
            return df
    except Exception as e:
        print(f"❌ Error executing query: {str(e)}")
        return None

def create_stores_visited_plots(df):
    """
    Create comprehensive visualizations of store visitation patterns
    """
    
    # Set up the plot style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 14))
    fig.suptitle('Store Visitation Analysis: Sessions With vs Without Orders\nJuly 2025 Onboarded Users', 
                 fontsize=16, fontweight='bold', y=0.98)
    
    # Colors for session types
    colors = {'With Order': '#E74C3C', 'Without Order': '#3498DB'}
    
    # Separate data for loaded vs viewed stores
    loaded_data = df[df['metric_type'] == 'Loaded Stores'].copy()
    viewed_data = df[df['metric_type'] == 'Viewed Stores'].copy()
    
    # Check if we have both types of data
    if loaded_data.empty:
        print("⚠️ Warning: No 'Loaded Stores' data found")
    if viewed_data.empty:
        print("⚠️ Warning: No 'Viewed Stores' data found")
    
    # 1. Loaded Stores Distribution (Side-by-side bars)
    if not loaded_data.empty:
        loaded_pivot = loaded_data.pivot(index='store_count', columns='session_type', values='total_sessions').fillna(0)
    else:
        loaded_pivot = pd.DataFrame()  # Empty DataFrame for safety
    
    # Limit to first 15 store counts for readability
    if not loaded_pivot.empty:
        loaded_pivot_top = loaded_pivot.head(15)
        x_pos = np.arange(len(loaded_pivot_top))
        width = 0.35
        
        if 'With Order' in loaded_pivot_top.columns and 'Without Order' in loaded_pivot_top.columns:
            bars1 = ax1.bar(x_pos - width/2, loaded_pivot_top['With Order'], width, 
                           label='With Order', color=colors['With Order'], alpha=0.8)
            bars2 = ax1.bar(x_pos + width/2, loaded_pivot_top['Without Order'], width, 
                           label='Without Order', color=colors['Without Order'], alpha=0.8)
            ax1.set_xticks(x_pos)
            ax1.set_xticklabels(loaded_pivot_top.index)
    else:
        ax1.text(0.5, 0.5, 'No Loaded Stores Data Available', 
                transform=ax1.transAxes, ha='center', va='center', fontsize=12)
    
    ax1.set_title('Loaded Stores Distribution (Store Clicks/Page Loads)', fontweight='bold', fontsize=14)
    ax1.set_xlabel('Number of Stores Loaded')
    ax1.set_ylabel('Number of Sessions')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    if not loaded_pivot.empty:
        ax1.set_yscale('log')  # Log scale due to expected large differences
    
    # 2. Viewed Stores Distribution (Side-by-side bars)
    if not viewed_data.empty:
        viewed_pivot = viewed_data.pivot(index='store_count', columns='session_type', values='total_sessions').fillna(0)
        viewed_pivot_top = viewed_pivot.head(15)
        x_pos2 = np.arange(len(viewed_pivot_top))
        
        if 'With Order' in viewed_pivot_top.columns and 'Without Order' in viewed_pivot_top.columns:
            bars3 = ax2.bar(x_pos2 - width/2, viewed_pivot_top['With Order'], width, 
                           label='With Order', color=colors['With Order'], alpha=0.8)
            bars4 = ax2.bar(x_pos2 + width/2, viewed_pivot_top['Without Order'], width, 
                           label='Without Order', color=colors['Without Order'], alpha=0.8)
            ax2.set_xticks(x_pos2)
            ax2.set_xticklabels(viewed_pivot_top.index)
    else:
        ax2.text(0.5, 0.5, 'No Viewed Stores Data Available', 
                transform=ax2.transAxes, ha='center', va='center', fontsize=12)
    
    ax2.set_title('Viewed Stores Distribution (All Store Impressions)', fontweight='bold', fontsize=14)
    ax2.set_xlabel('Number of Stores Viewed')
    ax2.set_ylabel('Number of Sessions')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)
    if not viewed_data.empty:
        ax2.set_yscale('log')
    
    # 3. Percentage Distribution Comparison
    # Calculate percentages within each session type
    if not loaded_data.empty:
        loaded_pct = loaded_data.copy()
        loaded_pct['pct_within_session_type'] = loaded_pct.groupby('session_type')['total_sessions'].transform(
            lambda x: x / x.sum() * 100
        )
    else:
        loaded_pct = pd.DataFrame()
    
    if not viewed_data.empty:
        viewed_pct = viewed_data.copy()
        viewed_pct['pct_within_session_type'] = viewed_pct.groupby('session_type')['total_sessions'].transform(
            lambda x: x / x.sum() * 100
        )
    else:
        viewed_pct = pd.DataFrame()
    
    # Plot percentage distributions
    plot_added = False
    for session_type, color in colors.items():
        if not loaded_pct.empty:
            loaded_subset = loaded_pct[loaded_pct['session_type'] == session_type].head(20)
            if not loaded_subset.empty:
                ax3.plot(loaded_subset['store_count'], loaded_subset['pct_within_session_type'], 
                        marker='o', linewidth=2, label=f'{session_type} - Loaded', 
                        color=color, linestyle='-')
                plot_added = True
        
        if not viewed_pct.empty:
            viewed_subset = viewed_pct[viewed_pct['session_type'] == session_type].head(20)
            if not viewed_subset.empty:
                ax3.plot(viewed_subset['store_count'], viewed_subset['pct_within_session_type'], 
                        marker='s', linewidth=2, label=f'{session_type} - Viewed', 
                        color=color, linestyle='--', alpha=0.7)
                plot_added = True
    
    if not plot_added:
        ax3.text(0.5, 0.5, 'No Data Available for Percentage Analysis', 
                transform=ax3.transAxes, ha='center', va='center', fontsize=12)
    
    ax3.set_title('Store Visitation Patterns (Percentage Distribution)', fontweight='bold', fontsize=14)
    ax3.set_xlabel('Number of Stores')
    ax3.set_ylabel('Percentage of Sessions (%)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    if plot_added:
        ax3.set_xlim(0, 20)
    
    # 4. Summary Statistics and Insights
    insights_text = """
    KEY METRICS TO ANALYZE:
    
    🎯 LOADED STORES (Deep Engagement):
    • Store page clicks and loads
    • Indicates serious browsing intent
    • Higher loaded count = more exploration
    
    👁️ VIEWED STORES (Surface Engagement): 
    • All store impressions/card views
    • Includes passive scrolling behavior
    • Higher viewed count = more discovery
    
    🔍 EXPECTED PATTERNS:
    • Sessions WITH orders may show:
      - Higher loaded store counts (exploration)
      - More focused viewed/loaded ratios
    
    • Sessions WITHOUT orders may show:
      - Higher viewed counts (browsing)
      - Lower loaded counts (less intent)
    
    📊 X-AXIS ALIGNMENT:
    • Both metrics use same store count scale
    • Enables direct comparison of behaviors
    • Log scale handles large session volumes
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
    filename = f'stores_visited_analysis_{timestamp}.png'
    plt.savefig(f'user-analysis/post_onboarding_customer_journey/plots/{filename}', 
                dpi=300, bbox_inches='tight')
    
    print(f"Store visitation analysis plot saved as: {filename}")
    return filename

def print_analysis_summary(df):
    """
    Print summary statistics for store visitation patterns
    """
    if df is None:
        print("No data available for analysis")
        return
    
    print("\n" + "="*80)
    print("STORE VISITATION ANALYSIS SUMMARY")
    print("="*80)
    
    # Calculate key metrics for each combination
    for metric_type in ['Loaded Stores', 'Viewed Stores']:
        metric_data = df[df['metric_type'] == metric_type]
        print(f"\n📊 {metric_type.upper()}:")
        
        for session_type in ['With Order', 'Without Order']:
            session_data = metric_data[metric_data['session_type'] == session_type]
            
            if not session_data.empty:
                total_sessions = session_data['total_sessions'].sum()
                avg_stores = (session_data['store_count'] * session_data['total_sessions']).sum() / total_sessions
                max_stores = session_data['store_count'].max()
                
                # Most common store count
                most_common_idx = session_data['total_sessions'].idxmax()
                most_common_stores = session_data.loc[most_common_idx, 'store_count']
                most_common_sessions = session_data.loc[most_common_idx, 'total_sessions']
                most_common_pct = most_common_sessions / total_sessions * 100
                
                print(f"  {session_type}:")
                print(f"    Total Sessions: {total_sessions:,}")
                print(f"    Avg Stores: {avg_stores:.2f}")
                print(f"    Max Stores: {max_stores}")
                print(f"    Most Common: {most_common_stores} stores ({most_common_pct:.1f}% of sessions)")

def main():
    """
    Main function to execute the store visitation analysis
    """
    print("Store Visitation Analysis")
    print("="*50)
    
    # Get data from Snowflake
    print("🔍 Fetching store visitation data from Snowflake...")
    df = get_stores_visited_data()
    
    if df is not None and not df.empty:
        print("✅ Data retrieved successfully")
        
        # Create visualizations
        print("📊 Creating store visitation plots...")
        filename = create_stores_visited_plots(df)
        
        # Print summary
        print_analysis_summary(df)
        
        print(f"\n✅ Analysis complete! Plots saved as: {filename}")
    else:
        print("❌ No data available or empty result set")
        print("\nTo execute this analysis:")
        print("1. Ensure the table proddb.fionafan.stores_visited_by_order_outcome exists")
        print("2. Run the table creation queries in preference_scope.sql first")
        print("3. Check that the table has data with both loaded_store_count and viewed_stores_count")

if __name__ == "__main__":
    main()
