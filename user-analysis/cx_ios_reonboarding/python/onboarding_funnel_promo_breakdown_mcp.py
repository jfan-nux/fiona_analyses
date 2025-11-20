"""
Analyze onboarding funnel cohort breakdown by promo eligibility
Shows overlap between onboarding_start_funnel_curr and promo experiment
Uses pre-fetched data from CSV files
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)

def create_visualizations(df_overall, df_daily, output_dir, plot_dir):
    """Create all visualizations"""
    
    print("\n" + "=" * 80)
    print("CREATING VISUALIZATIONS")
    print("=" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Overall breakdown pie chart
    ax = axes[0, 0]
    exp_summary = df_overall.groupby('promo_eligibility_status')['unique_devices'].sum()
    colors = ['#2ecc71', '#e74c3c', '#95a5a6']
    exp_summary.plot(kind='pie', ax=ax, autopct='%1.1f%%', colors=colors, startangle=90)
    ax.set_title('Onboarding Funnel Breakdown by Promo Eligibility', fontsize=14, fontweight='bold')
    ax.set_ylabel('')
    
    # 2. Experiment cohort breakdown
    ax = axes[0, 1]
    exp_cohort = df_overall[df_overall['promo_eligibility_status'] != 'Not in Experiment']
    exp_cohort_plot = exp_cohort[exp_cohort['onboarding_type'] == 'new_user'].copy()
    if len(exp_cohort_plot) > 0:
        exp_cohort_plot['label'] = (exp_cohort_plot['promo_eligibility_status'] + '\n' + 
                                     exp_cohort_plot['experiment_tag'].fillna(''))
        exp_cohort_plot = exp_cohort_plot.sort_values(['promo_eligibility_status', 'experiment_tag'])
        
        x_pos = range(len(exp_cohort_plot))
        colors = ['#3498db' if t == 'treatment' else '#e67e22' 
                  for t in exp_cohort_plot['experiment_tag']]
        
        ax.bar(x_pos, exp_cohort_plot['unique_devices'], color=colors, alpha=0.7)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(exp_cohort_plot['label'], rotation=45, ha='right')
        ax.set_ylabel('Unique Devices', fontsize=11)
        ax.set_title('Experiment Cohort: New Users Only', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for i, (idx, row) in enumerate(exp_cohort_plot.iterrows()):
            ax.text(i, row['unique_devices'], f"{int(row['unique_devices']):,}", 
                   ha='center', va='bottom', fontsize=9)
    
    # 3. Daily trend - Promo Eligible
    ax = axes[1, 0]
    promo_eligible = df_daily[df_daily['promo_eligibility_status'] == 'Promo Eligible']
    if len(promo_eligible) > 0:
        for tag in ['control', 'treatment']:
            subset = promo_eligible[promo_eligible['experiment_tag'] == tag]
            daily_agg = subset.groupby('day')['unique_devices'].sum()
            color = '#e67e22' if tag == 'control' else '#3498db'
            ax.plot(daily_agg.index, daily_agg.values, marker='o', label=tag.title(), 
                   color=color, linewidth=2, markersize=4, alpha=0.7)
        
        ax.set_xlabel('Date', fontsize=11)
        ax.set_ylabel('Unique Devices', fontsize=11)
        ax.set_title('Daily Trend: Promo Eligible Users', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # 4. Daily trend - Not Promo Eligible
    ax = axes[1, 1]
    not_eligible = df_daily[df_daily['promo_eligibility_status'] == 'Not Promo Eligible']
    if len(not_eligible) > 0:
        for tag in ['control', 'treatment']:
            subset = not_eligible[not_eligible['experiment_tag'] == tag]
            daily_agg = subset.groupby('day')['unique_devices'].sum()
            color = '#e67e22' if tag == 'control' else '#3498db'
            ax.plot(daily_agg.index, daily_agg.values, marker='o', label=tag.title(), 
                   color=color, linewidth=2, markersize=4, alpha=0.7)
        
        ax.set_xlabel('Date', fontsize=11)
        ax.set_ylabel('Unique Devices', fontsize=11)
        ax.set_title('Daily Trend: Not Promo Eligible Users', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    plot_path = f'{plot_dir}/onboarding_funnel_promo_breakdown.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"Saved plot to {plot_path}")
    plt.close()
    
    # Create engagement comparison plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    exp_cohort = df_overall[df_overall['promo_eligibility_status'] != 'Not in Experiment']
    exp_new_users = exp_cohort[exp_cohort['onboarding_type'] == 'new_user'].copy()
    if len(exp_new_users) > 0:
        exp_new_users['label'] = (exp_new_users['promo_eligibility_status'].str[:10] + '\n' + 
                                   exp_new_users['experiment_tag'].fillna(''))
        
        metrics = ['pct_saw_promo', 'pct_did_reonboarding', 'pct_redeemed']
        metric_labels = ['Saw Promo', 'Did Reonboarding', 'Redeemed']
        x = np.arange(len(exp_new_users))
        width = 0.25
        
        for i, (metric, label) in enumerate(zip(metrics, metric_labels)):
            offset = width * (i - 1)
            values = exp_new_users[metric] * 100
            ax.bar(x + offset, values, width, label=label, alpha=0.8)
        
        ax.set_ylabel('Percentage (%)', fontsize=11)
        ax.set_title('Promo Engagement by Eligibility & Experiment Group (New Users)', 
                    fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(exp_new_users['label'], rotation=45, ha='right')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plot_path2 = f'{plot_dir}/onboarding_funnel_engagement_comparison.png'
        plt.savefig(plot_path2, dpi=300, bbox_inches='tight')
        print(f"Saved engagement plot to {plot_path2}")
        plt.close()


def print_summary(df_overall):
    """Print text summary"""
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    total_devices = df_overall['unique_devices'].sum()
    print(f"\nTotal unique devices in onboarding funnel: {total_devices:,}")
    
    print("\nBreakdown by promo eligibility:")
    print("-" * 80)
    for status in ['Promo Eligible', 'Not Promo Eligible', 'Not in Experiment']:
        subset = df_overall[df_overall['promo_eligibility_status'] == status]
        devices = subset['unique_devices'].sum()
        pct = (devices / total_devices) * 100
        print(f"  {status:25s}: {devices:10,} devices ({pct:5.2f}%)")
    
    # Experiment cohort details
    exp_cohort = df_overall[df_overall['promo_eligibility_status'] != 'Not in Experiment']
    if len(exp_cohort) > 0:
        print("\n" + "=" * 80)
        print("EXPERIMENT COHORT DETAILS")
        print("=" * 80)
        print("\nBy Promo Eligibility & Experiment Tag:")
        print("-" * 80)
        for _, row in exp_cohort.iterrows():
            print(f"\n{row['promo_eligibility_status']} - {row['experiment_tag']} ({row['onboarding_type']}):")
            print(f"  Devices:              {row['unique_devices']:,}")
            print(f"  Saw promo page:       {row['users_saw_promo']:,} ({row['pct_saw_promo']*100:.1f}%)")
            print(f"  Did reonboarding:     {row['users_did_reonboarding']:,} ({row['pct_did_reonboarding']*100:.1f}%)")
            print(f"  Redeemed promo:       {row['users_redeemed']:,} ({row['pct_redeemed']*100:.1f}%)")


def main():
    print("=" * 80)
    print("ONBOARDING FUNNEL BREAKDOWN BY PROMO ELIGIBILITY")
    print("=" * 80)
    
    # Load pre-saved data
    output_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs'
    plot_dir = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots'
    
    # Check if data exists
    import os
    overall_path = f'{output_dir}/onboarding_funnel_promo_breakdown.csv'
    daily_path = f'{output_dir}/onboarding_funnel_promo_daily.csv'
    
    if os.path.exists(overall_path) and os.path.exists(daily_path):
        print("\nLoading existing data files...")
        df_overall = pd.read_csv(overall_path)
        df_daily = pd.read_csv(daily_path)
        df_daily['day'] = pd.to_datetime(df_daily['day'])
        print(f"Loaded {len(df_overall)} breakdown segments")
        print(f"Loaded {len(df_daily)} daily records")
    else:
        print("\nError: Data files not found. Please run the SQL query first to generate:")
        print(f"  - {overall_path}")
        print(f"  - {daily_path}")
        return
    
    # Print summary
    print_summary(df_overall)
    
    # Create visualizations
    create_visualizations(df_overall, df_daily, output_dir, plot_dir)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()





