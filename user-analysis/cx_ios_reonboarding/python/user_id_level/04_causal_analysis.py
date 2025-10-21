"""
Phase 4: Causal Analysis
Heterogeneous treatment effects, uplift modeling, and subgroup analysis
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from scipy.stats import ttest_ind
import warnings
warnings.filterwarnings('ignore')

# Setup
pd.set_option('display.max_columns', None)
sns.set_style('whitegrid')

# Directories
PLOTS_DIR = Path('plots/user_id_level')
OUTPUT_DIR = Path('outputs/user_id_level')

print("="*80)
print("PHASE 4: CAUSAL ANALYSIS")
print("Heterogeneous Treatment Effects & Uplift Modeling")
print("="*80)

# =======================
# 1. LOAD DATA
# =======================
print("\n[1] Loading cleaned data...")

df = pd.read_csv(OUTPUT_DIR / 'master_features_cleaned.csv', low_memory=False)
print(f"✓ Loaded {len(df):,} rows")

# Load feature lists
with open(OUTPUT_DIR / '02_continuous_features.txt') as f:
    continuous_features = [line.strip() for line in f if line.strip()]

with open(OUTPUT_DIR / '02_categorical_features.txt') as f:
    categorical_features = [line.strip() for line in f if line.strip()]

# Convert to numeric
for col in continuous_features:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Target variables
PRIMARY_TARGET = 'has_order_post_exposure'
SECONDARY_TARGETS = ['did_reonboarding', 'push_system_optin', 'total_orders_post_exposure']

# Convert targets to numeric
for target in [PRIMARY_TARGET] + SECONDARY_TARGETS:
    if target in df.columns:
        df[target] = pd.to_numeric(df[target], errors='coerce').fillna(0)

print(f"  Primary target: {PRIMARY_TARGET}")
print(f"  Features: {len(continuous_features)} continuous, {len(categorical_features)} categorical")

# =======================
# 2. PREPARE MODELING DATA
# =======================
print("\n[2] Preparing Data for Modeling")
print("-" * 80)

# Use only low-cardinality categorical features for modeling
LOW_CARD_CATEGORICALS = [
    'lifestage', 'lifestage_bucket', 'user_ordering_type',
    'frequency_bucket', 'merchant_diversity', 'regularity_pattern',
    'activity_trend', 'pre_exposure_engagement_level',
    'is_dashpass', 'has_store_content_visit', 'has_store_page_visit',
    'has_order_cart_visit', 'has_checkout_visit', 'has_purchase',
    'is_consistent_orderer', 'has_pre_orders', 'had_recent_activity_30d',
    'had_recent_activity_90d', 'is_high_value'
]

# Filter to existing columns
modeling_categoricals = [c for c in LOW_CARD_CATEGORICALS if c in df.columns]
modeling_continuous = [c for c in continuous_features if c in df.columns]

print(f"Using for modeling:")
print(f"  Continuous: {len(modeling_continuous)}")
print(f"  Categorical: {len(modeling_categoricals)}")

# One-hot encode categorical features
print("\nOne-hot encoding categorical features...")
df_encoded = df.copy()

# Create dummy columns
dummy_cols = []
for col in modeling_categoricals:
    dummies = pd.get_dummies(df[col], prefix=col, drop_first=True, dummy_na=False)
    df_encoded = pd.concat([df_encoded, dummies], axis=1)
    dummy_cols.extend(dummies.columns.tolist())

# Get feature matrix - only continuous + dummy columns
feature_cols = modeling_continuous + dummy_cols
# Filter to columns that actually exist
feature_cols = [c for c in feature_cols if c in df_encoded.columns]

print(f"\nTotal features after encoding: {len(feature_cols)}")
print(f"  Continuous: {len(modeling_continuous)}")
print(f"  Dummy variables: {len(dummy_cols)}")

# Prepare X, T, y - ensure all numeric and handle missing
print("\nPreparing feature matrix...")

# Start with continuous features
X_continuous = df_encoded[modeling_continuous].copy()
for col in modeling_continuous:
    X_continuous[col] = pd.to_numeric(X_continuous[col], errors='coerce').fillna(0)

# Add dummy variables (already numeric 0/1)
X_dummies = df_encoded[dummy_cols].copy()

# Combine
X_df = pd.concat([X_continuous, X_dummies], axis=1)

# Final check - ensure everything is numeric
X_df = X_df.apply(pd.to_numeric, errors='coerce').fillna(0)

print(f"  Feature matrix shape: {X_df.shape}")
print(f"  Missing values: {X_df.isnull().sum().sum()} (should be 0)")

# Verify no missing
assert X_df.isnull().sum().sum() == 0, "Still have missing values!"

X = X_df.values
T = df_encoded['is_treatment'].values
y = df_encoded[PRIMARY_TARGET].values

# Update feature_cols to match X_df columns
feature_cols = X_df.columns.tolist()

print(f"\nData shapes:")
print(f"  X (features): {X.shape}")
print(f"  T (treatment): {T.shape}")
print(f"  y (outcome): {y.shape}")
print(f"  Treatment rate: {T.mean():.1%}")
print(f"  Outcome rate: {y.mean():.3%}")

# =======================
# 3. OVERALL TREATMENT EFFECT
# =======================
print("\n[3] Overall Intent-to-Treat (ITT) Effect")
print("=" * 80)

control_outcome = df_encoded[df_encoded['is_treatment']==0][PRIMARY_TARGET].mean()
treatment_outcome = df_encoded[df_encoded['is_treatment']==1][PRIMARY_TARGET].mean()
ate = treatment_outcome - control_outcome
relative_lift = (ate / control_outcome * 100) if control_outcome > 0 else 0

print(f"\nAverage Treatment Effect (ATE):")
print(f"  Control outcome rate:   {control_outcome:.4f}")
print(f"  Treatment outcome rate: {treatment_outcome:.4f}")
print(f"  ATE (absolute):         {ate:+.4f}")
print(f"  Relative lift:          {relative_lift:+.2f}%")

# Statistical test
control_y = df_encoded[df_encoded['is_treatment']==0][PRIMARY_TARGET].values
treatment_y = df_encoded[df_encoded['is_treatment']==1][PRIMARY_TARGET].values
t_stat, p_val = ttest_ind(treatment_y, control_y)
print(f"  t-statistic:            {t_stat:.4f}")
print(f"  p-value:                {p_val:.4f}")
print(f"  Significant (p<0.05):   {'Yes' if p_val < 0.05 else 'No'}")

# =======================
# 4. T-LEARNER (Primary Method)
# =======================
print("\n[4] T-Learner: Training Separate Models for Treatment & Control")
print("=" * 80)

# Split data
control_data = df_encoded[df_encoded['is_treatment'] == 0]
treatment_data = df_encoded[df_encoded['is_treatment'] == 1]

X_control_train = control_data[feature_cols].fillna(0).values
y_control_train = control_data[PRIMARY_TARGET].values

X_treatment_train = treatment_data[feature_cols].fillna(0).values
y_treatment_train = treatment_data[PRIMARY_TARGET].values

print(f"Control model data: {X_control_train.shape}")
print(f"Treatment model data: {X_treatment_train.shape}")

# Train control model
print("\nTraining control model...")
model_control = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)
model_control.fit(X_control_train, y_control_train)

# Train treatment model
print("Training treatment model...")
model_treatment = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)
model_treatment.fit(X_treatment_train, y_treatment_train)

# Predict for everyone
X_all = df_encoded[feature_cols].fillna(0).values
pred_y0 = model_control.predict_proba(X_all)[:, 1]
pred_y1 = model_treatment.predict_proba(X_all)[:, 1]

df_encoded['cate_t_learner'] = pred_y1 - pred_y0
df_encoded['uplift_score'] = df_encoded['cate_t_learner']  # Use T-Learner for uplift

print(f"✓ Computed T-Learner CATE")
print(f"\nT-Learner CATE distribution:")
print(df_encoded['cate_t_learner'].describe())

# Feature importance from treatment model
treatment_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': model_treatment.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\nTop 20 Most Important Features (Treatment Model):")
print(treatment_importance.head(20).to_string(index=False))

treatment_importance.to_csv(OUTPUT_DIR / '04_t_learner_feature_importance.csv', index=False)
print(f"\n✓ Saved T-Learner feature importance")

# =======================
# 6. UPLIFT ANALYSIS
# =======================
print("\n[5] Uplift Analysis: Who Benefits Most from Treatment?")
print("=" * 80)

# Uplift score already set from T-Learner

# Create uplift quartiles
df_encoded['uplift_quartile'] = pd.qcut(
    df_encoded['uplift_score'], 
    q=4, 
    labels=['Q1 (Lowest)', 'Q2', 'Q3', 'Q4 (Highest)']
)

print("\nUplift by Quartile:")
print("-" * 80)

quartile_analysis = []
for q in ['Q1 (Lowest)', 'Q2', 'Q3', 'Q4 (Highest)']:
    subset = df_encoded[df_encoded['uplift_quartile'] == q]
    
    control_subset = subset[subset['is_treatment'] == 0]
    treatment_subset = subset[subset['is_treatment'] == 1]
    
    control_rate = control_subset[PRIMARY_TARGET].mean()
    treatment_rate = treatment_subset[PRIMARY_TARGET].mean()
    observed_lift = treatment_rate - control_rate
    relative_lift = (observed_lift / control_rate * 100) if control_rate > 0 else 0
    
    avg_uplift = subset['uplift_score'].mean()
    
    quartile_analysis.append({
        'quartile': q,
        'n': len(subset),
        'control_rate': control_rate,
        'treatment_rate': treatment_rate,
        'observed_lift': observed_lift,
        'relative_lift_pct': relative_lift,
        'avg_predicted_uplift': avg_uplift
    })
    
    print(f"\n{q}:")
    print(f"  N: {len(subset):,}")
    print(f"  Control rate:   {control_rate:.4f}")
    print(f"  Treatment rate: {treatment_rate:.4f}")
    print(f"  Observed lift:  {observed_lift:+.4f} ({relative_lift:+.2f}%)")
    print(f"  Predicted uplift: {avg_uplift:+.4f}")

quartile_df = pd.DataFrame(quartile_analysis)
quartile_df.to_csv(OUTPUT_DIR / '04_uplift_quartile_analysis.csv', index=False)
print(f"\n✓ Saved uplift quartile analysis")

# =======================
# 7. SUBGROUP ANALYSIS
# =======================
print("\n[6] Subgroup Analysis: Treatment Effects by Segments")
print("=" * 80)

# Define key segments
SEGMENTS = {
    'lifestage': 'Lifestage',
    'user_ordering_type': 'Ordering Type',
    'frequency_bucket': 'Frequency Bucket',
    'pre_exposure_engagement_level': 'Engagement Level',
    'merchant_diversity': 'Merchant Diversity',
    'had_recent_activity_30d': 'Recent Activity (30d)',
    'has_pre_orders': 'Has Pre-Orders',
}

subgroup_results = []

for segment_var, segment_name in SEGMENTS.items():
    if segment_var in df_encoded.columns:
        print(f"\n[7.{list(SEGMENTS.keys()).index(segment_var)+1}] {segment_name} ({segment_var})")
        print("-" * 80)
        
        for segment_value in df_encoded[segment_var].unique():
            if pd.notna(segment_value):
                subset = df_encoded[df_encoded[segment_var] == segment_value]
                
                if len(subset) >= 100:  # Only analyze segments with sufficient size
                    control = subset[subset['is_treatment']==0][PRIMARY_TARGET].mean()
                    treatment = subset[subset['is_treatment']==1][PRIMARY_TARGET].mean()
                    lift_abs = treatment - control
                    lift_rel = (lift_abs / control * 100) if control > 0 else 0
                    
                    subgroup_results.append({
                        'segment_variable': segment_var,
                        'segment_name': segment_name,
                        'segment_value': segment_value,
                        'n': len(subset),
                        'n_control': (subset['is_treatment']==0).sum(),
                        'n_treatment': (subset['is_treatment']==1).sum(),
                        'control_rate': control,
                        'treatment_rate': treatment,
                        'absolute_lift': lift_abs,
                        'relative_lift_pct': lift_rel
                    })
                    
                    print(f"  {str(segment_value):30s}: N={len(subset):6,}, "
                          f"Control={control:.3%}, Treatment={treatment:.3%}, "
                          f"Lift={lift_rel:+6.2f}%")

subgroup_df = pd.DataFrame(subgroup_results)
subgroup_df.to_csv(OUTPUT_DIR / '04_subgroup_analysis.csv', index=False)
print(f"\n✓ Saved subgroup analysis")

# Find best and worst performing segments
if len(subgroup_df) > 0:
    print("\n🏆 TOP 10 SEGMENTS (Highest Positive Lift):")
    top_segments = subgroup_df.nlargest(10, 'relative_lift_pct')
    print(top_segments[['segment_name', 'segment_value', 'n', 'control_rate', 
                        'treatment_rate', 'relative_lift_pct']].to_string(index=False))
    
    print("\n⚠️  BOTTOM 10 SEGMENTS (Negative or Lowest Lift):")
    bottom_segments = subgroup_df.nsmallest(10, 'relative_lift_pct')
    print(bottom_segments[['segment_name', 'segment_value', 'n', 'control_rate', 
                           'treatment_rate', 'relative_lift_pct']].to_string(index=False))

# =======================
# 8. COMPLIANCE ANALYSIS (TREATMENT GROUP)
# =======================
print("\n[7] Compliance Analysis: What Drives Reonboarding?")
print("=" * 80)

if 'did_reonboarding' in df_encoded.columns:
    treatment_only = df_encoded[df_encoded['is_treatment'] == 1].copy()
    
    print(f"Treatment group size: {len(treatment_only):,}")
    print(f"Compliers (did reonboarding): {treatment_only['did_reonboarding'].sum():,}")
    print(f"Compliance rate: {treatment_only['did_reonboarding'].mean():.2%}")
    
    # What predicts compliance?
    print("\n[7.1] Predicting Compliance (Logistic Regression)")
    print("-" * 80)
    
    X_compliance = treatment_only[feature_cols].fillna(0).values
    y_compliance = treatment_only['did_reonboarding'].values
    
    # Train logistic regression
    compliance_model = LogisticRegression(max_iter=1000, random_state=42)
    compliance_model.fit(X_compliance, y_compliance)
    
    # Get coefficients
    compliance_coefs = pd.DataFrame({
        'feature': feature_cols,
        'coefficient': compliance_model.coef_[0]
    }).sort_values('coefficient', key=abs, ascending=False)
    
    print("\nTop 20 Predictors of Compliance:")
    print(compliance_coefs.head(20).to_string(index=False))
    
    compliance_coefs.to_csv(OUTPUT_DIR / '04_compliance_predictors.csv', index=False)
    print("\n✓ Saved compliance predictors")
    
    # Compare compliers vs non-compliers
    print("\n[7.2] Compliers vs Non-Compliers: Key Differences")
    print("-" * 80)
    
    compliers = treatment_only[treatment_only['did_reonboarding'] == 1]
    non_compliers = treatment_only[treatment_only['did_reonboarding'] == 0]
    
    print(f"Compliers: {len(compliers):,}")
    print(f"Non-compliers: {len(non_compliers):,}")
    
    # Compare order rates
    complier_order_rate = compliers['has_order_post_exposure'].mean()
    non_complier_order_rate = non_compliers['has_order_post_exposure'].mean()
    
    print(f"\nOrder rates:")
    print(f"  Compliers:     {complier_order_rate:.3%}")
    print(f"  Non-compliers: {non_complier_order_rate:.3%}")
    print(f"  Difference:    {complier_order_rate - non_complier_order_rate:+.3%}")
    
    # Key feature differences
    print(f"\nKey Feature Differences (top 10):")
    feature_diffs = []
    for feat in modeling_continuous[:20]:
        if feat in treatment_only.columns:
            comp_mean = compliers[feat].mean()
            non_comp_mean = non_compliers[feat].mean()
            diff = comp_mean - non_comp_mean
            
            feature_diffs.append({
                'feature': feat,
                'compliers_mean': comp_mean,
                'non_compliers_mean': non_comp_mean,
                'difference': diff
            })
    
    feature_diff_df = pd.DataFrame(feature_diffs).sort_values('difference', key=abs, ascending=False)
    print(feature_diff_df.head(10).to_string(index=False))
    
    feature_diff_df.to_csv(OUTPUT_DIR / '04_complier_vs_noncomplier_features.csv', index=False)

# =======================
# 9. VISUALIZATIONS
# =======================
print("\n[8] Creating Visualizations...")
print("-" * 80)

# 9.1 Uplift distribution
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Uplift score distribution
axes[0].hist(df_encoded['uplift_score'], bins=100, edgecolor='black', alpha=0.7)
axes[0].axvline(0, color='red', linestyle='--', linewidth=2, label='No effect')
axes[0].axvline(df_encoded['uplift_score'].mean(), color='green', linestyle='--', 
                linewidth=2, label=f'Mean: {df_encoded["uplift_score"].mean():.4f}')
axes[0].set_xlabel('Predicted Treatment Effect (CATE)', fontsize=12)
axes[0].set_ylabel('Frequency', fontsize=12)
axes[0].set_title('Distribution of Individual Treatment Effects', fontsize=13, fontweight='bold')
axes[0].legend()
axes[0].grid(alpha=0.3)

# Uplift by quartile
quartile_plot_data = quartile_df.sort_values('quartile')
x_pos = np.arange(len(quartile_plot_data))

axes[1].bar(x_pos, quartile_plot_data['relative_lift_pct'].values, 
           color=['#e74c3c' if v < 0 else '#2ecc71' for v in quartile_plot_data['relative_lift_pct'].values],
           edgecolor='black')
axes[1].axhline(0, color='black', linestyle='-', linewidth=1)
axes[1].set_xticks(x_pos)
axes[1].set_xticklabels(quartile_plot_data['quartile'].values, fontsize=10)
axes[1].set_ylabel('Relative Lift (%)', fontsize=12)
axes[1].set_title('Treatment Effect by Uplift Quartile', fontsize=13, fontweight='bold')
axes[1].grid(axis='y', alpha=0.3)

for i, v in enumerate(quartile_plot_data['relative_lift_pct'].values):
    axes[1].text(i, v, f'{v:+.2f}%', ha='center', va='bottom' if v >= 0 else 'top', fontsize=10)

plt.tight_layout()
plt.savefig(PLOTS_DIR / '04_uplift_analysis.png', dpi=150, bbox_inches='tight')
plt.close()
print("✓ Saved: 04_uplift_analysis.png")

# 9.2 Top segments visualization
if len(subgroup_df) > 0:
    # Get top 15 segments by absolute lift
    top_15 = subgroup_df.nlargest(15, 'relative_lift_pct')
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    y_pos = np.arange(len(top_15))
    colors = ['#2ecc71' if v > 0 else '#e74c3c' for v in top_15['relative_lift_pct'].values]
    
    ax.barh(y_pos, top_15['relative_lift_pct'].values, color=colors, edgecolor='black')
    ax.axvline(0, color='black', linestyle='-', linewidth=1)
    ax.set_yticks(y_pos)
    
    # Create labels
    labels = [f"{row['segment_name']}: {row['segment_value']}\n(n={row['n']:,})" 
              for _, row in top_15.iterrows()]
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel('Relative Lift (%)', fontsize=12, fontweight='bold')
    ax.set_title('Top 15 Segments by Treatment Effect\n(Ordered Post-Exposure)', 
                fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, v in enumerate(top_15['relative_lift_pct'].values):
        ax.text(v, i, f' {v:+.2f}%', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / '04_top_segments_lift.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Saved: 04_top_segments_lift.png")

# 9.3 Feature importance plot
top_20_features = feature_importance.head(20)
fig, ax = plt.subplots(figsize=(10, 8))

y_pos = np.arange(len(top_20_features))
ax.barh(y_pos, top_20_features['importance'].values, color='#3498db', edgecolor='black')
ax.set_yticks(y_pos)
ax.set_yticklabels(top_20_features['feature'].values, fontsize=9)
ax.set_xlabel('Importance', fontsize=12, fontweight='bold')
ax.set_title('Top 20 Most Important Features\n(S-Learner Model)', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(PLOTS_DIR / '04_feature_importance.png', dpi=150, bbox_inches='tight')
plt.close()
print("✓ Saved: 04_feature_importance.png")

# =======================
# 10. SUMMARY
# =======================
print("\n" + "="*80)
print("PHASE 4 COMPLETE: CAUSAL ANALYSIS")
print("="*80)

summary_text = f"""
📊 CAUSAL ANALYSIS RESULTS:

1️⃣  OVERALL TREATMENT EFFECT (ITT):
   • ATE: {ate:+.4f} ({relative_lift:+.2f}%)
   • p-value: {p_val:.4f}
   • Significant: {'Yes' if p_val < 0.05 else 'No'}

2️⃣  HETEROGENEOUS TREATMENT EFFECTS:
   • CATE range: [{df_encoded['cate_s_learner'].min():.4f}, {df_encoded['cate_s_learner'].max():.4f}]
   • CATE mean: {df_encoded['cate_s_learner'].mean():.4f}
   • High responders (Q4): {quartile_df[quartile_df['quartile']=='Q4 (Highest)']['relative_lift_pct'].values[0]:.2f}% lift
   • Low responders (Q1): {quartile_df[quartile_df['quartile']=='Q1 (Lowest)']['relative_lift_pct'].values[0]:.2f}% lift

3️⃣  TOP PERFORMING SEGMENTS:
"""

if len(top_segments) > 0:
    for i, row in top_segments.head(5).iterrows():
        summary_text += f"   • {row['segment_name']}: {row['segment_value']}\n"
        summary_text += f"     → Lift: {row['relative_lift_pct']:+.2f}% (n={row['n']:,})\n"

if 'did_reonboarding' in df_encoded.columns:
    summary_text += f"""
4️⃣  COMPLIANCE ANALYSIS:
   • Compliance rate: {treatment_only['did_reonboarding'].mean():.2%}
   • Complier order rate: {complier_order_rate:.3%}
   • Non-complier order rate: {non_complier_order_rate:.3%}
   • Compliance effect: {complier_order_rate - non_complier_order_rate:+.3%}
"""

summary_text += f"""
📁 OUTPUTS GENERATED:
   • 04_s_learner_feature_importance.csv
   • 04_uplift_quartile_analysis.csv
   • 04_subgroup_analysis.csv
   • 04_compliance_predictors.csv
   • 04_complier_vs_noncomplier_features.csv
   
📊 VISUALIZATIONS:
   • 04_uplift_analysis.png
   • 04_top_segments_lift.png
   • 04_feature_importance.png
"""

print(summary_text)

# Save summary
with open(OUTPUT_DIR / '04_causal_analysis_summary.txt', 'w') as f:
    f.write(summary_text)

print("✅ Causal analysis complete!")
print("\n➡️  Next: Run Phase 5 - Generate Insights Report")
print("   python user-analysis/cx_ios_reonboarding/python/05_insights_report.py")

