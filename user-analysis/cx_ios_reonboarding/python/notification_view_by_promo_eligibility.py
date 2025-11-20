"""
Analyze notification page views by promo eligibility for treatment users
Date Range: 09/08/2025 - 11/03/2025
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import sys
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

def main():
    print("=" * 80)
    print("NOTIFICATION PAGE VIEW ANALYSIS BY PROMO ELIGIBILITY")
    print("Treatment Users Only | Date Range: 09/08/2025 - 11/03/2025")
    print("=" * 80)
    
    hook = SnowflakeHook()
    
    # Query 1: Overall summary
    query_summary = """
    WITH notif_view AS (
        SELECT 
            CAST(iguazu_timestamp AS DATE) AS day, 
            consumer_id,
            COUNT(*) AS notification_page_views
        FROM iguazu.consumer.M_onboarding_page_view_ice
        WHERE iguazu_timestamp >= '2025-09-08' 
            AND iguazu_timestamp < '2025-11-04'
            AND LOWER(page) = 'notification'
        GROUP BY 1, 2
    ),
    promo_treatment_users AS (
        SELECT 
            user_id,
            dd_device_id,
            tag,
            is_promo_eligible,
            exposure_time,
            saw_promo_page,
            did_reonboarding_flow,
            has_redeemed
        FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
        WHERE tag = 'treatment'
            AND exposure_time >= '2025-09-08'
            AND exposure_time < '2025-11-04'
    )
    SELECT 
        CASE 
            WHEN p.is_promo_eligible = 1 THEN 'Promo Eligible'
            WHEN p.is_promo_eligible = 0 THEN 'Not Promo Eligible'
        END AS promo_eligibility_status,
        COUNT(DISTINCT p.dd_device_id) AS total_treatment_users,
        COUNT(DISTINCT p.user_id) AS total_treatment_consumers,
        COUNT(DISTINCT n.consumer_id) AS users_saw_notification,
        SUM(n.notification_page_views) AS total_notification_views,
        ROUND(COUNT(DISTINCT n.consumer_id)::FLOAT / 
              NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS notification_view_rate,
        ROUND(AVG(n.notification_page_views), 2) AS avg_views_per_viewer,
        SUM(p.saw_promo_page) AS users_saw_promo_page,
        SUM(p.did_reonboarding_flow) AS users_did_reonboarding,
        SUM(p.has_redeemed) AS users_redeemed
    FROM promo_treatment_users p
    LEFT JOIN notif_view n
        ON p.user_id = n.consumer_id
    GROUP BY p.is_promo_eligible
    ORDER BY p.is_promo_eligible DESC
    """
    
    print("\nQuerying overall summary...")
    df_summary = hook.query_snowflake(query_summary, method='pandas')
    print(f"Retrieved {len(df_summary)} promo eligibility segments")
    
    # Query 2: Daily breakdown
    query_daily = """
    WITH notif_view AS (
        SELECT 
            CAST(iguazu_timestamp AS DATE) AS day, 
            consumer_id
        FROM iguazu.consumer.M_onboarding_page_view_ice
        WHERE iguazu_timestamp >= '2025-09-08' 
            AND iguazu_timestamp < '2025-11-04'
            AND LOWER(page) = 'notification'
        GROUP BY 1, 2
    ),
    promo_treatment_users AS (
        SELECT 
            user_id,
            dd_device_id,
            tag,
            is_promo_eligible,
            CAST(exposure_time AS DATE) AS exposure_day
        FROM proddb.fionafan.cx_ios_reonboarding_promo_user_level
        WHERE tag = 'treatment'
            AND exposure_time >= '2025-09-08'
            AND exposure_time < '2025-11-04'
    )
    SELECT 
        n.day,
        CASE 
            WHEN p.is_promo_eligible = 1 THEN 'Promo Eligible'
            WHEN p.is_promo_eligible = 0 THEN 'Not Promo Eligible'
        END AS promo_eligibility_status,
        COUNT(DISTINCT n.consumer_id) AS users_saw_notification,
        COUNT(DISTINCT p.user_id) AS total_treatment_users_exposed
    FROM notif_view n
    LEFT JOIN promo_treatment_users p
        ON n.consumer_id = p.user_id
        AND n.day >= p.exposure_day
    WHERE p.user_id IS NOT NULL
    GROUP BY n.day, p.is_promo_eligible
    ORDER BY n.day, p.is_promo_eligible DESC
    """
    
    print("Querying daily breakdown...")
    df_daily = hook.query_snowflake(query_daily, method='pandas')
    df_daily['day'] = pd.to_datetime(df_daily['day'])
    print(f"Retrieved {len(df_daily)} daily records")
    
    # Save results
    output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs'
    df_summary.to_csv(f'{output_dir}/notification_view_by_promo_summary.csv', index=False)
    df_daily.to_csv(f'{output_dir}/notification_view_by_promo_daily.csv', index=False)
    print(f"\nSaved results to {output_dir}/")
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY: NOTIFICATION PAGE VIEW RATES BY PROMO ELIGIBILITY")
    print("=" * 80)
    
    for _, row in df_summary.iterrows():
        print(f"\n{row['promo_eligibility_status']}:")
        print(f"  Total Treatment Users:        {row['total_treatment_users']:,}")
        print(f"  Saw Notification Page:        {row['users_saw_notification']:,} ({row['notification_view_rate']*100:.2f}%)")
        print(f"  Total Notification Views:     {int(row['total_notification_views']):,}")
        print(f"  Avg Views per Viewer:         {row['avg_views_per_viewer']:.2f}")
        print(f"\n  Other Engagement:")
        print(f"    Saw Promo Page:             {row['users_saw_promo_page']:,} ({row['users_saw_promo_page']/row['total_treatment_users']*100:.2f}%)")
        print(f"    Did Reonboarding Flow:      {row['users_did_reonboarding']:,} ({row['users_did_reonboarding']/row['total_treatment_users']*100:.2f}%)")
        print(f"    Redeemed Promo:             {row['users_redeemed']:,} ({row['users_redeemed']/row['total_treatment_users']*100:.2f}%)")
    
    # Calculate difference
    if len(df_summary) == 2:
        promo_eligible = df_summary[df_summary['promo_eligibility_status'] == 'Promo Eligible'].iloc[0]
        not_eligible = df_summary[df_summary['promo_eligibility_status'] == 'Not Promo Eligible'].iloc[0]
        
        rate_diff = (not_eligible['notification_view_rate'] - promo_eligible['notification_view_rate']) * 100
        print(f"\n{'='*80}")
        print(f"KEY FINDING:")
        print(f"  Not Promo Eligible users have {rate_diff:+.2f}pp HIGHER notification view rate")
        print(f"  ({not_eligible['notification_view_rate']*100:.2f}% vs {promo_eligible['notification_view_rate']*100:.2f}%)")
        print(f"{'='*80}")
    
    # Create visualizations
    print("\n" + "=" * 80)
    print("CREATING VISUALIZATIONS")
    print("=" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Notification view rate comparison
    ax = axes[0, 0]
    x_pos = range(len(df_summary))
    colors = ['#2ecc71', '#e74c3c']
    
    bars = ax.bar(x_pos, df_summary['notification_view_rate'] * 100, color=colors, alpha=0.7)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(df_summary['promo_eligibility_status'], rotation=0)
    ax.set_ylabel('Notification View Rate (%)', fontsize=11)
    ax.set_title('Notification Page View Rate by Promo Eligibility (Treatment)', 
                 fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for i, (bar, rate) in enumerate(zip(bars, df_summary['notification_view_rate'])):
        ax.text(bar.get_x() + bar.get_width()/2, rate * 100 + 0.5, 
               f'{rate*100:.2f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    # 2. User counts
    ax = axes[0, 1]
    df_counts = df_summary[['promo_eligibility_status', 'total_treatment_users', 
                            'users_saw_notification']].set_index('promo_eligibility_status')
    
    df_counts.plot(kind='bar', ax=ax, color=['#3498db', '#e67e22'], alpha=0.7)
    ax.set_ylabel('User Count', fontsize=11)
    ax.set_title('Total Users vs. Notification Viewers', fontsize=14, fontweight='bold')
    ax.set_xlabel('')
    ax.legend(['Total Treatment Users', 'Saw Notification'], loc='upper right')
    ax.grid(axis='y', alpha=0.3)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # 3. Daily trend
    ax = axes[1, 0]
    for status in ['Promo Eligible', 'Not Promo Eligible']:
        subset = df_daily[df_daily['promo_eligibility_status'] == status]
        color = '#2ecc71' if status == 'Promo Eligible' else '#e74c3c'
        ax.plot(subset['day'], subset['users_saw_notification'], 
               marker='o', label=status, color=color, linewidth=2, markersize=4, alpha=0.7)
    
    ax.set_xlabel('Date', fontsize=11)
    ax.set_ylabel('Users Who Saw Notification', fontsize=11)
    ax.set_title('Daily Notification Views by Promo Eligibility', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # 4. Engagement funnel
    ax = axes[1, 1]
    
    # Calculate engagement rates
    engagement_data = []
    for _, row in df_summary.iterrows():
        engagement_data.append({
            'Status': row['promo_eligibility_status'],
            'Saw Notification': row['notification_view_rate'] * 100,
            'Saw Promo Page': (row['users_saw_promo_page'] / row['total_treatment_users']) * 100,
            'Did Reonboarding': (row['users_did_reonboarding'] / row['total_treatment_users']) * 100,
            'Redeemed': (row['users_redeemed'] / row['total_treatment_users']) * 100
        })
    
    df_engagement = pd.DataFrame(engagement_data).set_index('Status')
    df_engagement.plot(kind='bar', ax=ax, alpha=0.7)
    ax.set_ylabel('Percentage (%)', fontsize=11)
    ax.set_title('Engagement Funnel by Promo Eligibility', fontsize=14, fontweight='bold')
    ax.set_xlabel('')
    ax.legend(loc='upper right', fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plot_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/notification_view_by_promo_eligibility.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\nSaved plot to {plot_path}")
    plt.close()
    
    hook.close()
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()





