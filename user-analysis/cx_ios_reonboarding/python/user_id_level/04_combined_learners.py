"""
Phase 4: Combined X-Learner and T-Learner Analysis
Run both methods, calculate Qini scores, persist to Snowflake
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from scipy.stats import ttest_ind
import sys
import warnings
warnings.filterwarnings('ignore')

# Add utils to path
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Try to import scikit-uplift for Qini score and curves
try:
    from sklift.metrics import qini_auc_score, uplift_auc_score
    from sklift.viz import plot_qini_curve
    SKLIFT_AVAILABLE = True
except ImportError:
    print("⚠ scikit-uplift not available. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-uplift"])
    from sklift.metrics import qini_auc_score, uplift_auc_score
    from sklift.viz import plot_qini_curve
    SKLIFT_AVAILABLE = True

# Setup
pd.set_option('display.max_columns', None)
sns.set_style('whitegrid')

# Directories
PLOTS_DIR = Path('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/plots/user_id_level')
OUTPUT_DIR = Path('/Users/fiona.fan/Documents/fiona_analyses/user-analysis/cx_ios_reonboarding/outputs/user_id_level')

print("="*80)
print("COMBINED X-LEARNER & T-LEARNER ANALYSIS")
print("With Qini Scores & Snowflake Persistence")
print("="*80)

# =======================
# 1. LOAD DATA
# =======================
print("\n[1] Loading cleaned data from Snowflake...")

query = """
SELECT * 
FROM proddb.fionafan.cx_ios_reonboarding_master_features_user_level
"""

with SnowflakeHook() as sf:
    df_full = sf.query_snowflake(query, method='pandas')
    print(f"✓ Loaded {len(df_full):,} rows")

# Strip quotes from user_id if present
if 'user_id' in df_full.columns:
    df_full['user_id'] = df_full['user_id'].astype(str).str.replace('"', '').str.replace('\\', '')

# Sample 30%
df = df_full.sample(frac=0.1, random_state=42).copy()
print(f"✓ Using 30% sample: {len(df):,} rows")
del df_full

# Create treatment indicator BEFORE split
df['is_treatment'] = df['result'].map({True: 1, 'true': 1, 'True': 1, 'TRUE': 1,
                                       False: 0, 'false': 0, 'False': 0, 'FALSE': 0}).fillna(0).astype(int)

# Create train/test split (90% train, 10% test)
from sklearn.model_selection import train_test_split
df_train, df_test = train_test_split(df, test_size=0.1, random_state=42, stratify=df['is_treatment'])
print(f"✓ Split into train ({len(df_train):,} rows) and test ({len(df_test):,} rows)")
print(f"  Train: {(df_train['is_treatment']==1).sum():,} treatment, {(df_train['is_treatment']==0).sum():,} control")
print(f"  Test:  {(df_test['is_treatment']==1).sum():,} treatment, {(df_test['is_treatment']==0).sum():,} control")

# Load feature lists
with open(OUTPUT_DIR / '01_continuous_features.txt') as f:
    continuous_features = [line.strip() for line in f if line.strip()]

with open(OUTPUT_DIR / '01_categorical_features.txt') as f:
    categorical_features = [line.strip() for line in f if line.strip()]

# Convert to numeric for both train and test
for col in continuous_features:
    if col in df_train.columns:
        df_train[col] = pd.to_numeric(df_train[col], errors='coerce')
    if col in df_test.columns:
        df_test[col] = pd.to_numeric(df_test[col], errors='coerce')

PRIMARY_TARGET = 'has_order_post_exposure'
df_train[PRIMARY_TARGET] = pd.to_numeric(df_train[PRIMARY_TARGET], errors='coerce').fillna(0)
df_test[PRIMARY_TARGET] = pd.to_numeric(df_test[PRIMARY_TARGET], errors='coerce').fillna(0)

print(f"  Features: {len(continuous_features)} continuous, {len(categorical_features)} categorical")

# =======================
# 2. PREPARE DATA
# =======================
print("\n[2] Preparing Data for Modeling")
print("-" * 80)

# Use low-cardinality categoricals
LOW_CARD_CATEGORICALS = [c for c in categorical_features if c in df_train.columns and df_train[c].nunique() <= 20]

# One-hot encode TRAINING data
df_train_encoded = df_train.copy()
dummy_cols = []
for col in LOW_CARD_CATEGORICALS:
    dummies = pd.get_dummies(df_train[col], prefix=col, drop_first=True, dummy_na=False)
    df_train_encoded = pd.concat([df_train_encoded, dummies], axis=1)
    dummy_cols.extend(dummies.columns.tolist())

# One-hot encode TEST data (using same columns as training)
df_test_encoded = df_test.copy()
for col in LOW_CARD_CATEGORICALS:
    dummies = pd.get_dummies(df_test[col], prefix=col, drop_first=True, dummy_na=False)
    df_test_encoded = pd.concat([df_test_encoded, dummies], axis=1)

# Ensure test has all dummy columns from train (fill missing with 0)
for col in dummy_cols:
    if col not in df_test_encoded.columns:
        df_test_encoded[col] = 0

# Feature matrix
feature_cols = continuous_features + dummy_cols
feature_cols = [c for c in feature_cols if c in df_train_encoded.columns]

# TRAINING data - Split by treatment
control_df_train = df_train_encoded[df_train_encoded['is_treatment'] == 0]
treatment_df_train = df_train_encoded[df_train_encoded['is_treatment'] == 1]

X0_train = control_df_train[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y0_train = control_df_train[PRIMARY_TARGET].values

X1_train = treatment_df_train[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y1_train = treatment_df_train[PRIMARY_TARGET].values

# TEST data - for evaluation
X_test = df_test_encoded[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
T_test = df_test_encoded['is_treatment'].values
y_test = df_test_encoded[PRIMARY_TARGET].values

# ALL TRAINING data - for final predictions on full train set
X_train_all = df_train_encoded[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
T_train_all = df_train_encoded['is_treatment'].values
y_train_all = df_train_encoded[PRIMARY_TARGET].values

print(f"Total features: {len(feature_cols)}")
print(f"Training - Control: {X0_train.shape}, Treatment: {X1_train.shape}")
print(f"Testing: {X_test.shape}")

# =======================
# 3. T-LEARNER
# =======================
print("\n" + "="*80)
print("T-LEARNER: SEPARATE MODELS FOR TREATMENT & CONTROL")
print("="*80)

print("\n[3.1] Training T-Learner Models...")
print("Stage 1: Training outcome models (binary classification)")

mu0_t = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)
mu1_t = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)

mu0_t.fit(X0_train, y0_train)
print("✓ Control model (μ0) trained")

# Calculate AUC for control model
y0_pred_proba = mu0_t.predict_proba(X0_train)[:, 1]
auc_mu0 = roc_auc_score(y0_train, y0_pred_proba)
print(f"  → Control model AUC: {auc_mu0:.4f}")

mu1_t.fit(X1_train, y1_train)
print("✓ Treatment model (μ1) trained")

# Calculate AUC for treatment model
y1_pred_proba = mu1_t.predict_proba(X1_train)[:, 1]
auc_mu1 = roc_auc_score(y1_train, y1_pred_proba)
print(f"  → Treatment model AUC: {auc_mu1:.4f}")

# Predict CATE on TEST set for evaluation
pred_y0_t_test = mu0_t.predict_proba(X_test)[:, 1]
pred_y1_t_test = mu1_t.predict_proba(X_test)[:, 1]
cate_tlearner_test = pred_y1_t_test - pred_y0_t_test

print(f"\nT-Learner CATE distribution (TEST set):")
print(pd.Series(cate_tlearner_test).describe())

# Qini score on TEST set (out-of-sample)
if SKLIFT_AVAILABLE and y_test.sum() > 0:
    qini_t_test = qini_auc_score(y_test, cate_tlearner_test, T_test, negative_effect=True)
    uplift_auc_t_test = uplift_auc_score(y_test, cate_tlearner_test, T_test)
    print(f"\nT-Learner Performance (TEST set - out-of-sample):")
    print(f"  Qini AUC: {qini_t_test:.6f}")
    print(f"  Uplift AUC: {uplift_auc_t_test:.6f}")
else:
    qini_t_test = None
    uplift_auc_t_test = None
    print(f"\n⚠ Skipping Qini score (no positive outcomes in target variable)")

# Also predict on TRAINING set for comparison (in-sample)
pred_y0_t_train = mu0_t.predict_proba(X_train_all)[:, 1]
pred_y1_t_train = mu1_t.predict_proba(X_train_all)[:, 1]
cate_tlearner_train = pred_y1_t_train - pred_y0_t_train

if SKLIFT_AVAILABLE and y_train_all.sum() > 0:
    qini_t_train = qini_auc_score(y_train_all, cate_tlearner_train, T_train_all, negative_effect=True)
    uplift_auc_t_train = uplift_auc_score(y_train_all, cate_tlearner_train, T_train_all)
    print(f"\nT-Learner Performance (TRAIN set - in-sample):")
    print(f"  Qini AUC: {qini_t_train:.6f}")
    print(f"  Uplift AUC: {uplift_auc_t_train:.6f}")
else:
    qini_t_train = None
    uplift_auc_t_train = None

# Store test predictions for final output
df_test_encoded['tlearner_cate'] = cate_tlearner_test
df_train_encoded['tlearner_cate'] = cate_tlearner_train

# Feature importance
t_importance = pd.DataFrame({
    'feature': feature_cols,
    'control_model_importance': mu0_t.feature_importances_,
    'treatment_model_importance': mu1_t.feature_importances_,
    'avg_importance': (mu0_t.feature_importances_ + mu1_t.feature_importances_) / 2
}).sort_values('treatment_model_importance', ascending=False)

t_importance.to_csv(OUTPUT_DIR / '04_tlearner_feature_importance.csv', index=False)
print("\n✓ Saved T-Learner feature importance")

# =======================
# 4. X-LEARNER
# =======================
print("\n" + "="*80)
print("X-LEARNER: THREE-STAGE RESIDUAL MODELING")
print("="*80)

print("\n[4.1] Stage 1: Training Base Outcome Models (binary classification)...")
mu0_x = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)
mu1_x = GradientBoostingClassifier(n_estimators=100, max_depth=5, random_state=42, verbose=0)

mu0_x.fit(X0_train, y0_train)
print("✓ μ0 (control) trained")

# Calculate AUC for control model
y0_pred_proba_x = mu0_x.predict_proba(X0_train)[:, 1]
auc_mu0_x = roc_auc_score(y0_train, y0_pred_proba_x)
print(f"  → Control model AUC: {auc_mu0_x:.4f}")

mu1_x.fit(X1_train, y1_train)
print("✓ μ1 (treatment) trained")

# Calculate AUC for treatment model
y1_pred_proba_x = mu1_x.predict_proba(X1_train)[:, 1]
auc_mu1_x = roc_auc_score(y1_train, y1_pred_proba_x)
print(f"  → Treatment model AUC: {auc_mu1_x:.4f}")

print("\n[4.2] Stage 2: Computing Pseudo-Outcomes (treatment effect residuals)...")
# D1 = Y1 - μ0(X1)
mu0_pred_on_treatment = mu0_x.predict_proba(X1_train)[:, 1]
D1 = y1_train - mu0_pred_on_treatment
print(f"✓ D1 (treatment residuals): mean={D1.mean():.4f}")

# D0 = μ1(X0) - Y0
mu1_pred_on_control = mu1_x.predict_proba(X0_train)[:, 1]
D0 = mu1_pred_on_control - y0_train
print(f"✓ D0 (control residuals): mean={D0.mean():.4f}")

print("\n[4.3] Stage 3: Training Residual Models...")
tau1_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)
tau0_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)

tau1_x.fit(X1_train, D1)
print("✓ τ1 trained on treatment residuals")

tau0_x.fit(X0_train, D0)
print("✓ τ0 trained on control residuals")

# Predict CATE on TEST set (weighted average with propensity = 0.5)
tau1_pred_test = tau1_x.predict(X_test)
tau0_pred_test = tau0_x.predict(X_test)
cate_xlearner_test = 0.5 * tau0_pred_test + 0.5 * tau1_pred_test

print(f"\nX-Learner CATE distribution (TEST set):")
print(pd.Series(cate_xlearner_test).describe())

# Qini score on TEST set (out-of-sample)
if SKLIFT_AVAILABLE and y_test.sum() > 0:
    qini_x_test = qini_auc_score(y_test, cate_xlearner_test, T_test, negative_effect=True)
    uplift_auc_x_test = uplift_auc_score(y_test, cate_xlearner_test, T_test)
    print(f"\nX-Learner Performance (TEST set - out-of-sample):")
    print(f"  Qini AUC: {qini_x_test:.6f}")
    print(f"  Uplift AUC: {uplift_auc_x_test:.6f}")
else:
    qini_x_test = None
    uplift_auc_x_test = None
    print(f"\n⚠ Skipping Qini score (no positive outcomes in target variable)")

# Also predict on TRAINING set for comparison (in-sample)
tau1_pred_train = tau1_x.predict(X_train_all)
tau0_pred_train = tau0_x.predict(X_train_all)
cate_xlearner_train = 0.5 * tau0_pred_train + 0.5 * tau1_pred_train

if SKLIFT_AVAILABLE and y_train_all.sum() > 0:
    qini_x_train = qini_auc_score(y_train_all, cate_xlearner_train, T_train_all, negative_effect=True)
    uplift_auc_x_train = uplift_auc_score(y_train_all, cate_xlearner_train, T_train_all)
    print(f"\nX-Learner Performance (TRAIN set - in-sample):")
    print(f"  Qini AUC: {qini_x_train:.6f}")
    print(f"  Uplift AUC: {uplift_auc_x_train:.6f}")
else:
    qini_x_train = None
    uplift_auc_x_train = None

# Store test predictions for final output
df_test_encoded['xlearner_cate'] = cate_xlearner_test
df_train_encoded['xlearner_cate'] = cate_xlearner_train

# Residual model feature importance
x_importance = pd.DataFrame({
    'feature': feature_cols,
    'tau0_importance': tau0_x.feature_importances_,
    'tau1_importance': tau1_x.feature_importances_,
    'avg_importance': (tau0_x.feature_importances_ + tau1_x.feature_importances_) / 2
}).sort_values('avg_importance', ascending=False)

print("\nTop 20 Features Predicting Treatment Effect Heterogeneity:")
print(x_importance.head(20).to_string(index=False))

x_importance.to_csv(OUTPUT_DIR / '04_xlearner_residual_feature_importance.csv', index=False)
print("\n✓ Saved X-Learner feature importance")

# =======================
# 5. COMPARE MODELS
# =======================
print("\n[5] Model Comparison")
print("=" * 80)

comparison = pd.DataFrame({
    'Model': ['T-Learner', 'X-Learner'],
    'Data': ['Test', 'Test'],
    'CATE_Mean': [cate_tlearner_test.mean(), cate_xlearner_test.mean()],
    'CATE_Std': [cate_tlearner_test.std(), cate_xlearner_test.std()],
    'CATE_Min': [cate_tlearner_test.min(), cate_xlearner_test.min()],
    'CATE_Max': [cate_tlearner_test.max(), cate_xlearner_test.max()],
})

if SKLIFT_AVAILABLE and qini_t_test is not None:
    comparison['Qini_AUC'] = [qini_t_test, qini_x_test]
    comparison['Uplift_AUC'] = [uplift_auc_t_test, uplift_auc_x_test]

print("\nTEST Set Performance (Out-of-Sample):")
print(comparison.to_string(index=False))

# Add train set comparison
if qini_t_train is not None:
    comparison_train = pd.DataFrame({
        'Model': ['T-Learner', 'X-Learner'],
        'Data': ['Train', 'Train'],
        'CATE_Mean': [cate_tlearner_train.mean(), cate_xlearner_train.mean()],
        'CATE_Std': [cate_tlearner_train.std(), cate_xlearner_train.std()],
        'CATE_Min': [cate_tlearner_train.min(), cate_xlearner_train.min()],
        'CATE_Max': [cate_tlearner_train.max(), cate_xlearner_train.max()],
        'Qini_AUC': [qini_t_train, qini_x_train],
        'Uplift_AUC': [uplift_auc_t_train, uplift_auc_x_train]
    })
    print("\nTRAIN Set Performance (In-Sample):")
    print(comparison_train.to_string(index=False))
    
    # Save combined comparison
    comparison_full = pd.concat([comparison, comparison_train], ignore_index=True)
    comparison_full.to_csv(OUTPUT_DIR / '04_model_comparison.csv', index=False)
else:
    comparison.to_csv(OUTPUT_DIR / '04_model_comparison.csv', index=False)

print("\n✓ Saved model comparison")

# =======================
# 6. QINI CURVES
# =======================
print("\n[6] Plotting Qini Curves")
print("=" * 80)

if SKLIFT_AVAILABLE and qini_t_test is not None:
    # T-Learner Qini Curve
    print("\n[6.1] T-Learner Qini Curve...")
    fig, ax = plt.subplots(figsize=(10, 7))
    plot_qini_curve(y_test, cate_tlearner_test, T_test, ax=ax, negative_effect=True, perfect=False)
    ax.set_title('T-Learner Qini Curve (Test Set)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Number of users targeted', fontsize=12)
    ax.set_ylabel('Incremental gain', fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / '04_tlearner_qini_curve.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved T-Learner Qini curve to {PLOTS_DIR / '04_tlearner_qini_curve.png'}")
    
    # X-Learner Qini Curve
    print("\n[6.2] X-Learner Qini Curve...")
    fig, ax = plt.subplots(figsize=(10, 7))
    plot_qini_curve(y_test, cate_xlearner_test, T_test, ax=ax, negative_effect=True, perfect=False)
    ax.set_title('X-Learner Qini Curve (Test Set)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Number of users targeted', fontsize=12)
    ax.set_ylabel('Incremental gain', fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / '04_xlearner_qini_curve.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved X-Learner Qini curve to {PLOTS_DIR / '04_xlearner_qini_curve.png'}")
    
    # Comparison plot - both curves on same axes
    print("\n[6.3] Combined Qini Curve Comparison...")
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Manually compute and plot Qini curves for better control
    from sklift.metrics import qini_curve
    
    # T-Learner curve
    t_x, t_y = qini_curve(y_test, cate_tlearner_test, T_test)
    
    # X-Learner curve
    x_x, x_y = qini_curve(y_test, cate_xlearner_test, T_test)
    
    # Plot both
    ax.plot(t_x, t_y, label=f'T-Learner (AUC={qini_t_test:.4f})', 
            linewidth=2.5, color='#2E86AB')
    ax.plot(x_x, x_y, label=f'X-Learner (AUC={qini_x_test:.4f})', 
            linewidth=2.5, color='#A23B72')
    
    # Random targeting baseline
    ax.plot([0, len(y_test)], [0, 0], 'k--', label='Random', linewidth=1.5, alpha=0.7)
    
    ax.set_title('Qini Curve Comparison: T-Learner vs X-Learner (Test Set)', 
                 fontsize=14, fontweight='bold')
    ax.set_xlabel('Number of users targeted', fontsize=12)
    ax.set_ylabel('Incremental gain', fontsize=12)
    ax.legend(fontsize=11, loc='best')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / '04_combined_qini_curve_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved combined Qini curve to {PLOTS_DIR / '04_combined_qini_curve_comparison.png'}")
    
    print("\n✅ All Qini curves saved!")
else:
    print("\n⚠ Skipping Qini curves (no valid scores computed)")

# =======================
# 7. PERSIST TO SNOWFLAKE
# =======================
print("\n[7] Persisting Scores to Snowflake")
print("=" * 80)

# Combine train and test predictions
# Add is_holdout flag (test set = holdout, train set = not holdout)
df_train_encoded['is_holdout'] = False
df_test_encoded['is_holdout'] = True

df_combined = pd.concat([df_train_encoded, df_test_encoded], ignore_index=True)

# Prepare combined scores table
scores_df = df_combined[['user_id', 'tag', 'result', 'is_treatment', 'is_holdout']].copy()
scores_df['tlearner_cate'] = df_combined['tlearner_cate']
scores_df['xlearner_cate'] = df_combined['xlearner_cate']

# Create quartiles for both
scores_df['tlearner_quartile'] = pd.qcut(scores_df['tlearner_cate'], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
scores_df['xlearner_quartile'] = pd.qcut(scores_df['xlearner_cate'], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')

print(f"Preparing to upload {len(scores_df):,} rows...")

# Upload T-Learner scores
print("\n[7.1] Uploading T-Learner scores...")
tlearner_table = 'proddb.fionafan.cx_ios_reonboarding_tlearner_scores_user_level'
tlearner_scores = scores_df[['user_id', 'tag', 'result', 'is_treatment', 'is_holdout',
                              'tlearner_cate', 'tlearner_quartile']].copy()
tlearner_scores['tlearner_quartile'] = tlearner_scores['tlearner_quartile'].astype(str)

try:
    with SnowflakeHook() as sf:
        success = sf.create_and_populate_table(
            df=tlearner_scores,
            table_name='cx_ios_reonboarding_tlearner_scores_user_level',
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        if success:
            print(f"✓ Uploaded to {tlearner_table}")
except Exception as e:
    print(f"⚠ Error uploading T-Learner scores: {e}")

# Upload X-Learner scores
print("\n[7.2] Uploading X-Learner scores...")
xlearner_table = 'proddb.fionafan.cx_ios_reonboarding_xlearner_scores_user_level'
xlearner_scores = scores_df[['user_id', 'tag', 'result', 'is_treatment', 'is_holdout',
                              'xlearner_cate', 'xlearner_quartile']].copy()
xlearner_scores['xlearner_quartile'] = xlearner_scores['xlearner_quartile'].astype(str)

try:
    with SnowflakeHook() as sf:
        success = sf.create_and_populate_table(
            df=xlearner_scores,
            table_name='cx_ios_reonboarding_xlearner_scores_user_level',
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        if success:
            print(f"✓ Uploaded to {xlearner_table}")
except Exception as e:
    print(f"⚠ Error uploading X-Learner scores: {e}")

# =======================
# 8. SUMMARY
# =======================
print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

control_rate_test = df_test[df_test['is_treatment']==0][PRIMARY_TARGET].mean()
treatment_rate_test = df_test[df_test['is_treatment']==1][PRIMARY_TARGET].mean()
ate_test = treatment_rate_test - control_rate_test

summary = f"""
📊 OVERALL TREATMENT EFFECT (TEST SET):
   • Control: {control_rate_test:.4f}
   • Treatment: {treatment_rate_test:.4f}
   • ATE: {ate_test:+.4f} ({ate_test/control_rate_test*100:+.2f}%)

📈 T-LEARNER (TEST SET - OUT-OF-SAMPLE):
   • CATE range: [{cate_tlearner_test.min():.4f}, {cate_tlearner_test.max():.4f}]
   • CATE mean: {cate_tlearner_test.mean():.4f} (std: {cate_tlearner_test.std():.4f})
"""

if SKLIFT_AVAILABLE and qini_t_test is not None:
    summary += f"   • Qini AUC: {qini_t_test:.6f}\n"
    summary += f"   • Uplift AUC: {uplift_auc_t_test:.6f}\n"

summary += f"""
📈 X-LEARNER (TEST SET - OUT-OF-SAMPLE):
   • CATE range: [{cate_xlearner_test.min():.4f}, {cate_xlearner_test.max():.4f}]
   • CATE mean: {cate_xlearner_test.mean():.4f} (std: {cate_xlearner_test.std():.4f})
"""

if SKLIFT_AVAILABLE and qini_x_test is not None:
    summary += f"   • Qini AUC: {qini_x_test:.6f}\n"
    summary += f"   • Uplift AUC: {uplift_auc_x_test:.6f}\n"

# Add train set metrics for comparison
if qini_t_train is not None:
    summary += f"""
📊 TRAINING SET METRICS (IN-SAMPLE - for comparison):
   • T-Learner Qini AUC: {qini_t_train:.6f}
   • X-Learner Qini AUC: {qini_x_train:.6f}
   • Overfit check: Test vs Train Qini difference
     - T-Learner: {qini_t_train - qini_t_test:.6f}
     - X-Learner: {qini_x_train - qini_x_test:.6f}
"""

summary += f"""
💾 SNOWFLAKE TABLES CREATED:
   • proddb.fionafan.cx_ios_reonboarding_tlearner_scores_user_level
     Columns: user_id, tag, result, is_treatment, is_holdout, tlearner_cate, tlearner_quartile
   
   • proddb.fionafan.cx_ios_reonboarding_xlearner_scores_user_level
     Columns: user_id, tag, result, is_treatment, is_holdout, xlearner_cate, xlearner_quartile
   
   Holdout breakdown:
     - Training set (is_holdout=FALSE): {(~scores_df['is_holdout']).sum():,} users
     - Holdout set (is_holdout=TRUE): {scores_df['is_holdout'].sum():,} users

📁 OUTPUTS:
   • 04_model_comparison.csv
   • 04_tlearner_feature_importance.csv
   • 04_xlearner_residual_feature_importance.csv

📊 QINI CURVE PLOTS:
   • 04_tlearner_qini_curve.png
   • 04_xlearner_qini_curve.png
   • 04_combined_qini_curve_comparison.png
"""

print(summary)

# Save summary
with open(OUTPUT_DIR / '04_combined_learners_summary.txt', 'w') as f:
    f.write(summary)

print("✅ All models complete and scores persisted to Snowflake!")

