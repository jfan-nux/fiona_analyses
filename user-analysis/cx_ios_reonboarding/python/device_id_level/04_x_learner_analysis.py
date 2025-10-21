"""
Phase 4: X-Learner Causal Analysis
Three-stage approach to estimate heterogeneous treatment effects
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from scipy.stats import ttest_ind
import sys
import warnings
warnings.filterwarnings('ignore')

# Add utils to path
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Try to import scikit-uplift for Qini score
try:
    from sklift.metrics import qini_auc_score, uplift_auc_score
    SKLIFT_AVAILABLE = True
except ImportError:
    print("⚠ scikit-uplift not available. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-uplift"])
    from sklift.metrics import qini_auc_score, uplift_auc_score
    SKLIFT_AVAILABLE = True

# Setup
pd.set_option('display.max_columns', None)
sns.set_style('whitegrid')

# Directories
PLOTS_DIR = Path('plots/device_id_level')
OUTPUT_DIR = Path('outputs/device_id_level')

print("="*80)
print("X-LEARNER: THREE-STAGE HETEROGENEOUS TREATMENT EFFECT ESTIMATION")
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

PRIMARY_TARGET = 'has_order_post_exposure'
df[PRIMARY_TARGET] = pd.to_numeric(df[PRIMARY_TARGET], errors='coerce').fillna(0)

print(f"  Features: {len(continuous_features)} continuous, {len(categorical_features)} categorical")
print(f"  Target: {PRIMARY_TARGET}")

# =======================
# 2. PREPARE DATA
# =======================
print("\n[2] Preparing Data for X-Learner")
print("-" * 80)

# Use low-cardinality categoricals
LOW_CARD_CATEGORICALS = [c for c in categorical_features if c in df.columns and df[c].nunique() <= 20]

# One-hot encode
df_encoded = df.copy()
dummy_cols = []
for col in LOW_CARD_CATEGORICALS:
    dummies = pd.get_dummies(df[col], prefix=col, drop_first=True, dummy_na=False)
    df_encoded = pd.concat([df_encoded, dummies], axis=1)
    dummy_cols.extend(dummies.columns.tolist())

# Feature matrix
feature_cols = continuous_features + dummy_cols
feature_cols = [c for c in feature_cols if c in df_encoded.columns]

# Ensure all numeric
X_df = df_encoded[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

print(f"Total features: {len(feature_cols)}")
print(f"  Continuous: {len(continuous_features)}")
print(f"  Dummy variables: {len(dummy_cols)}")

# Split by treatment
control_df = df_encoded[df_encoded['is_treatment'] == 0].copy()
treatment_df = df_encoded[df_encoded['is_treatment'] == 1].copy()

X0 = control_df[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y0 = control_df[PRIMARY_TARGET].values

X1 = treatment_df[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y1 = treatment_df[PRIMARY_TARGET].values

X_all = X_df.values

print(f"\nData prepared:")
print(f"  Control group: X0={X0.shape}, y0={y0.shape}, mean={y0.mean():.4f}")
print(f"  Treatment group: X1={X1.shape}, y1={y1.shape}, mean={y1.mean():.4f}")
print(f"  All data: X_all={X_all.shape}")

# =======================
# 3. STAGE 1: Train Base Models
# =======================
print("\n[STAGE 1] Training Base Outcome Models")
print("=" * 80)

# Stage 1a: Train control model (μ0)
print("\nTraining μ0 (control outcome model)...")
mu0_model = GradientBoostingRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
    verbose=0
)
mu0_model.fit(X0, y0)
print(f"✓ Control model trained")

# Stage 1b: Train treatment model (μ1)
print("Training μ1 (treatment outcome model)...")
mu1_model = GradientBoostingRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
    verbose=0
)
mu1_model.fit(X1, y1)
print(f"✓ Treatment model trained")

# =======================
# 4. STAGE 2: Compute Pseudo-Outcomes (Residuals)
# =======================
print("\n[STAGE 2] Computing Pseudo-Outcomes (Imputed Treatment Effects)")
print("=" * 80)

# For treatment group: D1 = Y1 - μ0(X1)
# (observed outcome minus predicted control outcome)
print("\nComputing D1 for treatment group...")
mu0_pred_on_treatment = mu0_model.predict(X1)
D1 = y1 - mu0_pred_on_treatment
print(f"✓ D1 computed: mean={D1.mean():.4f}, std={D1.std():.4f}")

# For control group: D0 = μ1(X0) - Y0
# (predicted treatment outcome minus observed outcome)
print("Computing D0 for control group...")
mu1_pred_on_control = mu1_model.predict(X0)
D0 = mu1_pred_on_control - y0
print(f"✓ D0 computed: mean={D0.mean():.4f}, std={D0.std():.4f}")

print(f"\nPseudo-outcome summary:")
print(f"  Treatment residuals (D1): {len(D1):,} observations")
print(f"  Control residuals (D0): {len(D0):,} observations")
print(f"  Both represent imputed individual treatment effects")

# =======================
# 5. STAGE 3: Train Models on Residuals
# =======================
print("\n[STAGE 3] Training Models on Pseudo-Outcomes (Treatment Effect Models)")
print("=" * 80)

# Stage 3a: Train τ1 on treatment group residuals
print("\nTraining τ1 (treatment effect model on treatment group)...")
tau1_model = GradientBoostingRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
    verbose=0
)
tau1_model.fit(X1, D1)
print(f"✓ τ1 model trained")

# Stage 3b: Train τ0 on control group residuals
print("Training τ0 (treatment effect model on control group)...")
tau0_model = GradientBoostingRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42,
    verbose=0
)
tau0_model.fit(X0, D0)
print(f"✓ τ0 model trained")

# =======================
# 6. PREDICT CATE (Individual Treatment Effects)
# =======================
print("\n[6] Estimating Individual Treatment Effects (CATE)")
print("=" * 80)

# Predict CATE for all observations
tau1_pred = tau1_model.predict(X_all)
tau0_pred = tau0_model.predict(X_all)

# Weighted average (using propensity score = 0.5 since randomized)
# CATE = e(X) * τ0(X) + (1 - e(X)) * τ1(X)
# For randomized experiment with 50/50 split: e(X) = 0.5
propensity = 0.5
cate_xlearner = propensity * tau0_pred + (1 - propensity) * tau1_pred

df_encoded['cate_xlearner'] = cate_xlearner
df_encoded['uplift_score'] = cate_xlearner

print(f"✓ Computed X-Learner CATE for all {len(df_encoded):,} observations")
print(f"\nCATE distribution:")
print(pd.Series(cate_xlearner).describe())

# =======================
# 7. FEATURE IMPORTANCE FROM RESIDUAL MODELS
# =======================
print("\n[7] Feature Importance: What Drives Treatment Effect Heterogeneity?")
print("=" * 80)

# Get feature importance from both residual models
tau1_importance = pd.DataFrame({
    'feature': feature_cols,
    'tau1_importance': tau1_model.feature_importances_,
    'tau0_importance': tau0_model.feature_importances_
})

# Average importance
tau1_importance['avg_importance'] = (tau1_importance['tau1_importance'] + tau1_importance['tau0_importance']) / 2
tau1_importance = tau1_importance.sort_values('avg_importance', ascending=False)

print("\nTop 30 Features Predicting Treatment Effect Heterogeneity:")
print("(These features explain WHO benefits most from treatment)")
print("-" * 80)
print(tau1_importance.head(30).to_string(index=False))

tau1_importance.to_csv(OUTPUT_DIR / '04_xlearner_residual_feature_importance.csv', index=False)
print(f"\n✓ Saved X-Learner residual feature importance")

# Compare with treatment model importance
treatment_model_importance = pd.DataFrame({
    'feature': feature_cols,
    'importance': mu1_model.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\n\nTop 20 Features for Treatment Outcome (μ1 model):")
print("(These features predict ordering in treatment group)")
print("-" * 80)
print(treatment_model_importance.head(20).to_string(index=False))

treatment_model_importance.to_csv(OUTPUT_DIR / '04_treatment_outcome_feature_importance.csv', index=False)

# =======================
# 8. UPLIFT QUARTILE ANALYSIS
# =======================
print("\n[8] Uplift Quartile Analysis")
print("=" * 80)

df_encoded['uplift_quartile'] = pd.qcut(
    df_encoded['uplift_score'],
    q=4,
    labels=['Q1 (Lowest)', 'Q2', 'Q3', 'Q4 (Highest)'],
    duplicates='drop'
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
        'n_control': len(control_subset),
        'n_treatment': len(treatment_subset),
        'control_rate': control_rate,
        'treatment_rate': treatment_rate,
        'observed_lift': observed_lift,
        'relative_lift_pct': relative_lift,
        'avg_predicted_uplift': avg_uplift
    })
    
    print(f"\n{q}:")
    print(f"  N: {len(subset):,} (Control: {len(control_subset):,}, Treatment: {len(treatment_subset):,})")
    print(f"  Control rate:   {control_rate:.4f}")
    print(f"  Treatment rate: {treatment_rate:.4f}")
    print(f"  Observed lift:  {observed_lift:+.4f} ({relative_lift:+.2f}%)")
    print(f"  Predicted CATE: {avg_uplift:+.4f}")

quartile_df = pd.DataFrame(quartile_analysis)
quartile_df.to_csv(OUTPUT_DIR / '04_xlearner_quartile_analysis.csv', index=False)
print(f"\n✓ Saved quartile analysis")

# =======================
# 8b. CALCULATE QINI SCORE
# =======================
print("\n[8b] Calculating Qini AUC Score")
print("-" * 80)

if SKLIFT_AVAILABLE:
    # Prepare data for Qini calculation
    y_true = df_encoded[PRIMARY_TARGET].values
    uplift_pred = df_encoded['uplift_score'].values
    treatment = df_encoded['is_treatment'].values
    
    # Calculate Qini AUC
    qini_score = qini_auc_score(y_true, uplift_pred, treatment, negative_effect=True)
    uplift_auc = uplift_auc_score(y_true, uplift_pred, treatment)
    
    print(f"X-Learner Model Performance:")
    print(f"  Qini AUC Score: {qini_score:.6f}")
    print(f"  Uplift AUC Score: {uplift_auc:.6f}")
    
    # Save metrics
    metrics_df = pd.DataFrame({
        'model': ['X-Learner'],
        'qini_auc': [qini_score],
        'uplift_auc': [uplift_auc],
        'cate_mean': [cate_xlearner.mean()],
        'cate_std': [cate_xlearner.std()],
        'ate': [ate]
    })
    metrics_df.to_csv(OUTPUT_DIR / '04_xlearner_metrics.csv', index=False)
    print(f"\n✓ Saved Qini metrics")
else:
    print("⚠ Qini score calculation skipped (scikit-uplift not available)")
    qini_score = None
    uplift_auc = None

# =======================
# 9. CHARACTERIZE HIGH vs LOW RESPONDERS
# =======================
print("\n[9] Characterizing High vs Low Responders")
print("=" * 80)

# Define high and low responders
q4_users = df_encoded[df_encoded['uplift_quartile'] == 'Q4 (Highest)']
q1_users = df_encoded[df_encoded['uplift_quartile'] == 'Q1 (Lowest)']

print(f"\nHigh Responders (Q4): {len(q4_users):,}")
print(f"Low Responders (Q1): {len(q1_users):,}")

# Compare key features
feature_comparison = []
for feat in continuous_features[:30]:  # Top 30 continuous features
    if feat in df_encoded.columns:
        q4_mean = q4_users[feat].mean()
        q1_mean = q1_users[feat].mean()
        diff = q4_mean - q1_mean
        
        feature_comparison.append({
            'feature': feat,
            'high_responders_mean': q4_mean,
            'low_responders_mean': q1_mean,
            'difference': diff,
            'abs_difference': abs(diff)
        })

comparison_df = pd.DataFrame(feature_comparison).sort_values('abs_difference', ascending=False)

print("\nTop 20 Features Differentiating High vs Low Responders:")
print("-" * 80)
print(comparison_df.head(20)[['feature', 'high_responders_mean', 'low_responders_mean', 'difference']].to_string(index=False))

comparison_df.to_csv(OUTPUT_DIR / '04_high_vs_low_responder_features.csv', index=False)
print(f"\n✓ Saved high vs low responder comparison")

# =======================
# 10. VISUALIZATIONS
# =======================
print("\n[10] Creating Visualizations...")
print("-" * 80)

# 10.1 Uplift distribution
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# CATE distribution
axes[0].hist(cate_xlearner, bins=100, edgecolor='black', alpha=0.7, color='#3498db')
axes[0].axvline(0, color='red', linestyle='--', linewidth=2, label='No effect')
axes[0].axvline(cate_xlearner.mean(), color='green', linestyle='--', 
                linewidth=2, label=f'Mean: {cate_xlearner.mean():.4f}')
axes[0].set_xlabel('Individual Treatment Effect (CATE)', fontsize=12, fontweight='bold')
axes[0].set_ylabel('Frequency', fontsize=12)
axes[0].set_title('X-Learner: Distribution of Treatment Effects', fontsize=13, fontweight='bold')
axes[0].legend()
axes[0].grid(alpha=0.3)

# Uplift by quartile
x_pos = np.arange(len(quartile_df))
colors = ['#e74c3c' if v < 0 else '#2ecc71' for v in quartile_df['relative_lift_pct'].values]

axes[1].bar(x_pos, quartile_df['relative_lift_pct'].values, color=colors, edgecolor='black')
axes[1].axhline(0, color='black', linestyle='-', linewidth=1)
axes[1].set_xticks(x_pos)
axes[1].set_xticklabels(quartile_df['quartile'].values, fontsize=10)
axes[1].set_ylabel('Observed Lift (%)', fontsize=12, fontweight='bold')
axes[1].set_title('Treatment Effect by Predicted Uplift Quartile', fontsize=13, fontweight='bold')
axes[1].grid(axis='y', alpha=0.3)

for i, v in enumerate(quartile_df['relative_lift_pct'].values):
    axes[1].text(i, v, f'{v:+.1f}%', ha='center', 
                va='bottom' if v >= 0 else 'top', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig(PLOTS_DIR / '04_xlearner_uplift_analysis.png', dpi=150, bbox_inches='tight')
plt.close()
print("✓ Saved: 04_xlearner_uplift_analysis.png")

# 10.2 Feature importance from residual models
top_30_residual = tau1_importance.head(30)

fig, ax = plt.subplots(figsize=(12, 10))
y_pos = np.arange(len(top_30_residual))

ax.barh(y_pos, top_30_residual['avg_importance'].values, color='#9b59b6', edgecolor='black')
ax.set_yticks(y_pos)
ax.set_yticklabels(top_30_residual['feature'].values, fontsize=9)
ax.set_xlabel('Average Importance (τ0 + τ1)', fontsize=12, fontweight='bold')
ax.set_title('Top 30 Features Predicting Treatment Effect Heterogeneity\n(X-Learner Residual Models)', 
            fontsize=13, fontweight='bold')
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(PLOTS_DIR / '04_xlearner_residual_feature_importance.png', dpi=150, bbox_inches='tight')
plt.close()
print("✓ Saved: 04_xlearner_residual_feature_importance.png")

# 10.3 High vs Low Responder Comparison
top_diff_features = comparison_df.head(15)

fig, ax = plt.subplots(figsize=(12, 8))

x = np.arange(len(top_diff_features))
width = 0.35

bars1 = ax.barh(x - width/2, top_diff_features['low_responders_mean'].values, 
                width, label='Low Responders (Q1)', color='#e74c3c', edgecolor='black')
bars2 = ax.barh(x + width/2, top_diff_features['high_responders_mean'].values, 
                width, label='High Responders (Q4)', color='#2ecc71', edgecolor='black')

ax.set_yticks(x)
ax.set_yticklabels(top_diff_features['feature'].values, fontsize=9)
ax.set_xlabel('Average Value', fontsize=12, fontweight='bold')
ax.set_title('Top 15 Features: High vs Low Responders\n(Who benefits most vs least from treatment)', 
            fontsize=13, fontweight='bold')
ax.legend()
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(PLOTS_DIR / '04_high_vs_low_responders.png', dpi=150, bbox_inches='tight')
plt.close()
print("✓ Saved: 04_high_vs_low_responders.png")

# =======================
# 11. PERSIST SCORES TO SNOWFLAKE
# =======================
print("\n[11] Persisting X-Learner Scores to Snowflake")
print("=" * 80)

# Prepare scores table
scores_df = df_encoded[['dd_device_id', 'user_id', 'tag', 'result', 'is_treatment']].copy()
scores_df['xlearner_cate'] = cate_xlearner
scores_df['uplift_quartile'] = df_encoded['uplift_quartile'].astype(str)

print(f"Preparing to upload {len(scores_df):,} rows to Snowflake...")

# Upload to Snowflake
table_name = 'proddb.fionafan.cx_ios_reonboarding_xlearner_scores'
print(f"Uploading to: {table_name}")

try:
    with SnowflakeHook() as sf:
        success = sf.create_and_populate_table(
            df=scores_df,
            table_name='cx_ios_reonboarding_xlearner_scores',
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        if success:
            print(f"✓ Successfully uploaded X-Learner scores to {table_name}")
        else:
            print(f"⚠ Upload may have had issues")
except Exception as e:
    print(f"Error uploading to Snowflake: {e}")
    print("Scores saved locally in cleaned CSV")

# =======================
# 12. SUMMARY REPORT
# =======================
print("\n[12] X-Learner Summary")
print("=" * 80)

# Overall treatment effect
control_outcome = y0.mean()
treatment_outcome = y1.mean()
ate = treatment_outcome - control_outcome

summary_text = f"""
{'='*80}
X-LEARNER ANALYSIS COMPLETE
{'='*80}

📊 OVERALL TREATMENT EFFECT:
   • Control outcome: {control_outcome:.4f}
   • Treatment outcome: {treatment_outcome:.4f}
   • ATE: {ate:+.4f} ({ate/control_outcome*100:+.2f}%)

📈 HETEROGENEOUS TREATMENT EFFECTS:
   • CATE range: [{cate_xlearner.min():.4f}, {cate_xlearner.max():.4f}]
   • CATE mean: {cate_xlearner.mean():.4f}
   • CATE std: {cate_xlearner.std():.4f}
   
🎯 UPLIFT QUARTILES:
"""

for _, row in quartile_df.iterrows():
    summary_text += f"\n   {row['quartile']:15s}: "
    summary_text += f"N={row['n']:7,}, "
    summary_text += f"Lift={row['relative_lift_pct']:+6.2f}%, "
    summary_text += f"Control={row['control_rate']:.3%}, "
    summary_text += f"Treatment={row['treatment_rate']:.3%}"

if SKLIFT_AVAILABLE and qini_score is not None:
    summary_text += f"""

📊 MODEL PERFORMANCE (Qini Metrics):
   • Qini AUC Score: {qini_score:.6f}
   • Uplift AUC Score: {uplift_auc:.6f}
"""

summary_text += f"""

🔑 TOP 10 FEATURES PREDICTING TREATMENT EFFECT HETEROGENEITY:
   (Features that determine WHO benefits from treatment)
"""

for i, row in tau1_importance.head(10).iterrows():
    summary_text += f"\n   {i+1:2d}. {row['feature']:50s} (importance: {row['avg_importance']:.6f})"

summary_text += f"""

💾 SNOWFLAKE TABLE:
   • proddb.fionafan.cx_ios_reonboarding_xlearner_scores
     (Contains CATE scores for all devices)

📁 OUTPUTS GENERATED:
   • 04_xlearner_residual_feature_importance.csv - Features predicting HTE
   • 04_treatment_outcome_feature_importance.csv - Features predicting ordering
   • 04_xlearner_quartile_analysis.csv - Uplift by quartile
   • 04_high_vs_low_responder_features.csv - Characteristics comparison
   
📊 VISUALIZATIONS:
   • 04_xlearner_uplift_analysis.png - CATE distribution & quartile lifts
   • 04_xlearner_residual_feature_importance.png - Top HTE features
   • 04_high_vs_low_responders.png - Feature comparison

🎯 KEY INSIGHTS:
   • Q4 (High responders) lift: {quartile_df[quartile_df['quartile']=='Q4 (Highest)']['relative_lift_pct'].values[0]:+.2f}%
   • Q1 (Low responders) lift: {quartile_df[quartile_df['quartile']=='Q1 (Lowest)']['relative_lift_pct'].values[0]:+.2f}%
   • Range: {quartile_df['relative_lift_pct'].max() - quartile_df['relative_lift_pct'].min():.2f} percentage points
   
{'='*80}
"""

print(summary_text)

# Save summary
with open(OUTPUT_DIR / '04_xlearner_summary.txt', 'w') as f:
    f.write(summary_text)

print("\n✅ X-LEARNER ANALYSIS COMPLETE!")
print(f"\nThe analysis identified treatment effect heterogeneity using a 3-stage approach:")
print(f"  Stage 1: Trained outcome models (μ0, μ1)")
print(f"  Stage 2: Computed pseudo-outcomes (D0, D1)")
print(f"  Stage 3: Trained treatment effect models (τ0, τ1)")
print(f"\n📊 Review outputs/ and plots/ directories for detailed results")

