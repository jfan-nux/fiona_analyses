"""
Simple visualization for notification view analysis
Uses hardcoded data from query results
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")

# Hardcoded data from query results
data = {
    'promo_eligibility_status': ['Promo Eligible', 'Not Promo Eligible'],
    'total_treatment_users': [83636, 690075],
    'users_saw_notification': [28696, 256785],
    'notification_view_rate': [0.3431, 0.3721],
    'users_saw_promo_page': [10551, 2644],
    'users_did_reonboarding': [28700, 256791],
    'users_redeemed': [5512, 2185]
}

df_summary = pd.DataFrame(data)

print("=" * 80)
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
x = np.arange(len(df_summary))
width = 0.35

bars1 = ax.bar(x - width/2, df_summary['total_treatment_users'], width, 
               label='Total Treatment Users', color='#3498db', alpha=0.7)
bars2 = ax.bar(x + width/2, df_summary['users_saw_notification'], width,
               label='Saw Notification', color='#e67e22', alpha=0.7)

ax.set_ylabel('User Count', fontsize=11)
ax.set_title('Total Users vs. Notification Viewers', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(df_summary['promo_eligibility_status'], rotation=0)
ax.legend()
ax.grid(axis='y', alpha=0.3)

# Add value labels
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height,
               f'{int(height):,}', ha='center', va='bottom', fontsize=8)

# 3. Engagement breakdown
ax = axes[1, 0]

metrics = ['Saw Notification', 'Saw Promo Page', 'Did Reonboarding', 'Redeemed']
promo_eligible_rates = [
    df_summary.loc[0, 'notification_view_rate'] * 100,
    (df_summary.loc[0, 'users_saw_promo_page'] / df_summary.loc[0, 'total_treatment_users']) * 100,
    (df_summary.loc[0, 'users_did_reonboarding'] / df_summary.loc[0, 'total_treatment_users']) * 100,
    (df_summary.loc[0, 'users_redeemed'] / df_summary.loc[0, 'total_treatment_users']) * 100
]
not_eligible_rates = [
    df_summary.loc[1, 'notification_view_rate'] * 100,
    (df_summary.loc[1, 'users_saw_promo_page'] / df_summary.loc[1, 'total_treatment_users']) * 100,
    (df_summary.loc[1, 'users_did_reonboarding'] / df_summary.loc[1, 'total_treatment_users']) * 100,
    (df_summary.loc[1, 'users_redeemed'] / df_summary.loc[1, 'total_treatment_users']) * 100
]

x = np.arange(len(metrics))
width = 0.35

ax.bar(x - width/2, promo_eligible_rates, width, label='Promo Eligible', 
       color='#2ecc71', alpha=0.7)
ax.bar(x + width/2, not_eligible_rates, width, label='Not Promo Eligible',
       color='#e74c3c', alpha=0.7)

ax.set_ylabel('Percentage (%)', fontsize=11)
ax.set_title('Engagement Funnel by Promo Eligibility', fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(metrics, rotation=45, ha='right')
ax.legend()
ax.grid(axis='y', alpha=0.3)

# 4. Redemption rate comparison (zoomed)
ax = axes[1, 1]

redemption_data = df_summary[['promo_eligibility_status', 'users_redeemed', 'total_treatment_users']].copy()
redemption_data['redemption_rate'] = (redemption_data['users_redeemed'] / 
                                       redemption_data['total_treatment_users']) * 100

x_pos = range(len(redemption_data))
bars = ax.bar(x_pos, redemption_data['redemption_rate'], color=colors, alpha=0.7)
ax.set_xticks(x_pos)
ax.set_xticklabels(redemption_data['promo_eligibility_status'], rotation=0)
ax.set_ylabel('Redemption Rate (%)', fontsize=11)
ax.set_title('Promo Redemption Rate by Eligibility', fontsize=14, fontweight='bold')
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, (bar, rate) in enumerate(zip(bars, redemption_data['redemption_rate'])):
    count = redemption_data.iloc[i]['users_redeemed']
    ax.text(bar.get_x() + bar.get_width()/2, rate + 0.2, 
           f'{rate:.2f}%\n({int(count):,})', ha='center', va='bottom', 
           fontsize=10, fontweight='bold')

plt.tight_layout()
plot_path = '/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/notification_view_by_promo_eligibility.png'
plt.savefig(plot_path, dpi=300, bbox_inches='tight')
print(f"\nSaved plot to {plot_path}")
plt.close()

print("\n" + "=" * 80)
print("VISUALIZATION COMPLETE")
print("=" * 80)





