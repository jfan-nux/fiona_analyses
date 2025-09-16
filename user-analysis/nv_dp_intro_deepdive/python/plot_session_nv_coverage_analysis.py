#!/usr/bin/env python3
"""
Plot 2: Session-Level NV Coverage Analysis
Shows how NV impressions are distributed across user sessions and engagement patterns
Reads data from Snowflake using utils connection
"""

import sys
from pathlib import Path

# Add the root utils directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "utils"))

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

from snowflake_connection import SnowflakeHook

def fetch_session_nv_coverage_data():
    """Fetch session NV coverage data from Snowflake"""
    hook = SnowflakeHook()
    
    query = """
    SELECT * 
    FROM proddb.fionafan.nv_dp_new_user_session_nv_coverage 
    ORDER BY session_num
    """
    
    print("🔄 Fetching session NV coverage data from Snowflake...")
    df = hook.fetch_pandas_all(query)
    print(f"✅ Loaded {len(df)} rows from nv_dp_new_user_session_nv_coverage")
    
    return df

def create_session_nv_coverage_plots(df):
    """Create comprehensive session NV coverage visualizations"""
    
    # Filter to sessions 1-10 for cleaner visualization
    df_filtered = df[df['SESSION_NUM'] <= 10].copy()
    df_filtered = df_filtered[df_filtered['SESSION_NUM'].notna()].copy()

    # Create figure with a single subplot (keep only the first subgraph)
    fig, ax1 = plt.subplots(1, 1, figsize=(12, 8))
    fig.suptitle('Session-Level NV Coverage Analysis\nApril 2024 New User Cohort (n=1.49M)', 
                 fontsize=16, fontweight='bold')

    # Plot 1: Engagement Funnel by Session
    sessions = df_filtered['SESSION_NUM'].astype(int)
    total_users = df_filtered['CONSUMERS_WITH_SESSION'] / 1000  # Convert to thousands
    nv_users = df_filtered['CONSUMERS_WITH_NV_IMPRESSION'] / 1000
    non_nv_users = df_filtered['CONSUMERS_WITH_NON_NV_IMPRESSION'] / 1000

    # Stacked bar chart showing composition
    ax1.bar(sessions, nv_users, label='Users with NV Impressions', color='#2E8B57', alpha=0.8)
    ax1.bar(sessions, non_nv_users, bottom=nv_users, label='Users with Non-NV Impressions', color='#4682B4', alpha=0.8)

    # Add total session line
    ax1_twin = ax1.twinx()
    ax1_twin.plot(sessions, total_users, color='red', linewidth=2, marker='o', label='Total Active Users')
    ax1_twin.set_ylabel('Total Active Users (thousands)', color='red')

    ax1.set_xlabel('Session Number')
    ax1.set_ylabel('Users (thousands)')
    ax1.set_title('User Engagement Funnel by Session', fontweight='bold')
    ax1.legend(loc='upper right')
    ax1_twin.legend(loc='upper center')
    ax1.grid(axis='y', alpha=0.3)
    ax1.set_xticks(sessions)

    # Only the first subgraph is retained; other subgraphs removed

    return fig, df_filtered

def main():
    """Main execution function"""
    try:
        # Fetch data from Snowflake
        df = fetch_session_nv_coverage_data()
        
        # Create plots
        print("🔄 Creating session NV coverage visualizations...")
        fig, df_filtered = create_session_nv_coverage_plots(df)
        
        # Save plot
        output_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/nv_dp_intro_deepdive/plots/session_nv_coverage_analysis.png'
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✅ Plot 2 complete: Session-level NV coverage analysis")
        print(f"📊 Saved to: {output_path}")

        # Additional analysis: Create a summary table
        print("\n📊 SESSION NV COVERAGE SUMMARY")
        print("=" * 50)
        summary_df = df_filtered[['SESSION_NUM', 'CONSUMERS_WITH_SESSION', 'CONSUMERS_WITH_NV_IMPRESSION', 'NV_SHARE_AMONG_SESSION']].copy()
        summary_df['session_num'] = summary_df['SESSION_NUM'].astype(int)
        summary_df['users_k'] = (summary_df['CONSUMERS_WITH_SESSION'] / 1000).round(1)
        summary_df['nv_users_k'] = (summary_df['CONSUMERS_WITH_NV_IMPRESSION'] / 1000).round(1)
        summary_df['nv_coverage_pct'] = (summary_df['NV_SHARE_AMONG_SESSION'] * 100).round(1)

        print(summary_df[['session_num', 'users_k', 'nv_users_k', 'nv_coverage_pct']].to_string(index=False))

        # Print key metrics
        print(f"\n🔍 KEY METRICS")
        print("=" * 30)
        total_cohort = df_filtered['CONSUMERS_TOTAL'].iloc[0]
        s1_data = df_filtered[df_filtered['SESSION_NUM'] == 1].iloc[0]
        
        print(f"Total cohort size: {total_cohort:,}")
        print(f"Session 1 reach: {s1_data['CONSUMERS_WITH_SESSION']:,} ({s1_data['CONSUMERS_WITH_SESSION']/total_cohort*100:.1f}%)")
        print(f"Session 1 NV reach: {s1_data['CONSUMERS_WITH_NV_IMPRESSION']:,} ({s1_data['NV_SHARE']*100:.1f}% of cohort)")
        print(f"NV coverage among S1 active users: {s1_data['NV_SHARE_AMONG_SESSION']*100:.1f}%")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()