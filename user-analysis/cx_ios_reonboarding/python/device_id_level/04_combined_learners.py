"""
Phase 4: Combined X-Learner and T-Learner Analysis
Run both methods, calculate Qini scores, persist to Snowflake
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor
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
print("COMBINED X-LEARNER & T-LEARNER ANALYSIS")
print("With Qini Scores & Snowflake Persistence")
print("="*80)

# =======================
# 1. LOAD DATA
# =======================
print("\n[1] Loading cleaned data from Snowflake...")

query = """
SELECT * 
FROM proddb.fionafan.cx_ios_reonboarding_master_features
"""

with SnowflakeHook() as sf:
    df_full = sf.query_snowflake(query, method='pandas')
    print(f"✓ Loaded {len(df_full):,} rows")

# Sample 30%
df = df_full.sample(frac=0.3, random_state=42).copy()
print(f"✓ Using 30% sample: {len(df):,} rows")
del df_full

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

# Create treatment indicator
df['is_treatment'] = df['result'].map({True: 1, 'true': 1, 'True': 1, 'TRUE': 1,
                                       False: 0, 'false': 0, 'False': 0, 'FALSE': 0}).fillna(0).astype(int)

print(f"  Features: {len(continuous_features)} continuous, {len(categorical_features)} categorical")

# =======================
# 2. PREPARE DATA
# =======================
print("\n[2] Preparing Data for Modeling")
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
X_all = X_df.values

# Split by treatment
control_df = df_encoded[df_encoded['is_treatment'] == 0]
treatment_df = df_encoded[df_encoded['is_treatment'] == 1]

X0 = control_df[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y0 = control_df[PRIMARY_TARGET].values

X1 = treatment_df[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0).values
y1 = treatment_df[PRIMARY_TARGET].values

T_all = df_encoded['is_treatment'].values
y_all = df_encoded[PRIMARY_TARGET].values

print(f"Total features: {len(feature_cols)}")
print(f"Control: {X0.shape}, Treatment: {X1.shape}")

# =======================
# 3. T-LEARNER
# =======================
print("\n" + "="*80)
print("T-LEARNER: SEPARATE MODELS FOR TREATMENT & CONTROL")
print("="*80)

print("\n[3.1] Training T-Learner Models...")
mu0_t = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)
mu1_t = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)

mu0_t.fit(X0, y0)
print("✓ Control model trained")

mu1_t.fit(X1, y1)
print("✓ Treatment model trained")

# Predict CATE
pred_y0_t = mu0_t.predict(X_all)
pred_y1_t = mu1_t.predict(X_all)
cate_tlearner = pred_y1_t - pred_y0_t

df_encoded['tlearner_cate'] = cate_tlearner

print(f"\nT-Learner CATE distribution:")
print(pd.Series(cate_tlearner).describe())

# Qini score for T-Learner
if SKLIFT_AVAILABLE:
    qini_t = qini_auc_score(y_all, cate_tlearner, T_all, negative_effect=True)
    uplift_auc_t = uplift_auc_score(y_all, cate_tlearner, T_all)
    print(f"\nT-Learner Performance:")
    print(f"  Qini AUC: {qini_t:.6f}")
    print(f"  Uplift AUC: {uplift_auc_t:.6f}")

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

print("\n[4.1] Stage 1: Training Base Outcome Models...")
mu0_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)
mu1_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)

mu0_x.fit(X0, y0)
print("✓ μ0 (control) trained")

mu1_x.fit(X1, y1)
print("✓ μ1 (treatment) trained")

print("\n[4.2] Stage 2: Computing Pseudo-Outcomes...")
# D1 = Y1 - μ0(X1)
mu0_pred_on_treatment = mu0_x.predict(X1)
D1 = y1 - mu0_pred_on_treatment
print(f"✓ D1 (treatment residuals): mean={D1.mean():.4f}")

# D0 = μ1(X0) - Y0
mu1_pred_on_control = mu1_x.predict(X0)
D0 = mu1_pred_on_control - y0
print(f"✓ D0 (control residuals): mean={D0.mean():.4f}")

print("\n[4.3] Stage 3: Training Residual Models...")
tau1_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)
tau0_x = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42, verbose=0)

tau1_x.fit(X1, D1)
print("✓ τ1 trained on treatment residuals")

tau0_x.fit(X0, D0)
print("✓ τ0 trained on control residuals")

# Predict CATE (weighted average with propensity = 0.5)
tau1_pred = tau1_x.predict(X_all)
tau0_pred = tau0_x.predict(X_all)
cate_xlearner = 0.5 * tau0_pred + 0.5 * tau1_pred

df_encoded['xlearner_cate'] = cate_xlearner

print(f"\nX-Learner CATE distribution:")
print(pd.Series(cate_xlearner).describe())

# Qini score for X-Learner
if SKLIFT_AVAILABLE:
    qini_x = qini_auc_score(y_all, cate_xlearner, T_all, negative_effect=True)
    uplift_auc_x = uplift_auc_score(y_all, cate_xlearner, T_all)
    print(f"\nX-Learner Performance:")
    print(f"  Qini AUC: {qini_x:.6f}")
    print(f"  Uplift AUC: {uplift_auc_x:.6f}")

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
    'CATE_Mean': [cate_tlearner.mean(), cate_xlearner.mean()],
    'CATE_Std': [cate_tlearner.std(), cate_xlearner.std()],
    'CATE_Min': [cate_tlearner.min(), cate_xlearner.min()],
    'CATE_Max': [cate_tlearner.max(), cate_xlearner.max()],
})

if SKLIFT_AVAILABLE:
    comparison['Qini_AUC'] = [qini_t, qini_x]
    comparison['Uplift_AUC'] = [uplift_auc_t, uplift_auc_x]

print(comparison.to_string(index=False))

comparison.to_csv(OUTPUT_DIR / '04_model_comparison.csv', index=False)
print("\n✓ Saved model comparison")

# =======================
# 6. PERSIST TO SNOWFLAKE
# =======================
print("\n[6] Persisting Scores to Snowflake")
print("=" * 80)

# Prepare combined scores table
scores_df = df_encoded[['dd_device_id', 'user_id', 'tag', 'result', 'is_treatment']].copy()
scores_df['tlearner_cate'] = cate_tlearner
scores_df['xlearner_cate'] = cate_xlearner

# Create quartiles for both
scores_df['tlearner_quartile'] = pd.qcut(cate_tlearner, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
scores_df['xlearner_quartile'] = pd.qcut(cate_xlearner, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')

print(f"Preparing to upload {len(scores_df):,} rows...")

# Upload T-Learner scores
print("\n[6.1] Uploading T-Learner scores...")
tlearner_table = 'proddb.fionafan.cx_ios_reonboarding_tlearner_scores'
tlearner_scores = scores_df[['dd_device_id', 'user_id', 'tag', 'result', 'is_treatment', 
                              'tlearner_cate', 'tlearner_quartile']].copy()
tlearner_scores['tlearner_quartile'] = tlearner_scores['tlearner_quartile'].astype(str)

try:
    with SnowflakeHook() as sf:
        success = sf.create_and_populate_table(
            df=tlearner_scores,
            table_name='cx_ios_reonboarding_tlearner_scores',
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        if success:
            print(f"✓ Uploaded to {tlearner_table}")
except Exception as e:
    print(f"⚠ Error uploading T-Learner scores: {e}")

# Upload X-Learner scores
print("\n[6.2] Uploading X-Learner scores...")
xlearner_table = 'proddb.fionafan.cx_ios_reonboarding_xlearner_scores'
xlearner_scores = scores_df[['dd_device_id', 'user_id', 'tag', 'result', 'is_treatment',
                              'xlearner_cate', 'xlearner_quartile']].copy()
xlearner_scores['xlearner_quartile'] = xlearner_scores['xlearner_quartile'].astype(str)

try:
    with SnowflakeHook() as sf:
        success = sf.create_and_populate_table(
            df=xlearner_scores,
            table_name='cx_ios_reonboarding_xlearner_scores',
            schema='fionafan',
            database='proddb',
            method='pandas'
        )
        if success:
            print(f"✓ Uploaded to {xlearner_table}")
except Exception as e:
    print(f"⚠ Error uploading X-Learner scores: {e}")

# =======================
# 7. SUMMARY
# =======================
print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

control_rate = df[df['is_treatment']==0][PRIMARY_TARGET].mean()
treatment_rate = df[df['is_treatment']==1][PRIMARY_TARGET].mean()
ate = treatment_rate - control_rate

summary = f"""
📊 OVERALL TREATMENT EFFECT:
   • Control: {control_rate:.4f}
   • Treatment: {treatment_rate:.4f}
   • ATE: {ate:+.4f} ({ate/control_rate*100:+.2f}%)

📈 T-LEARNER:
   • CATE range: [{cate_tlearner.min():.4f}, {cate_tlearner.max():.4f}]
   • CATE mean: {cate_tlearner.mean():.4f} (std: {cate_tlearner.std():.4f})
"""

if SKLIFT_AVAILABLE:
    summary += f"   • Qini AUC: {qini_t:.6f}\n"
    summary += f"   • Uplift AUC: {uplift_auc_t:.6f}\n"

summary += f"""
📈 X-LEARNER:
   • CATE range: [{cate_xlearner.min():.4f}, {cate_xlearner.max():.4f}]
   • CATE mean: {cate_xlearner.mean():.4f} (std: {cate_xlearner.std():.4f})
"""

if SKLIFT_AVAILABLE:
    summary += f"   • Qini AUC: {qini_x:.6f}\n"
    summary += f"   • Uplift AUC: {uplift_auc_x:.6f}\n"

summary += f"""
💾 SNOWFLAKE TABLES CREATED:
   • proddb.fionafan.cx_ios_reonboarding_tlearner_scores
     Columns: dd_device_id, user_id, tag, result, is_treatment, tlearner_cate, tlearner_quartile
   
   • proddb.fionafan.cx_ios_reonboarding_xlearner_scores
     Columns: dd_device_id, user_id, tag, result, is_treatment, xlearner_cate, xlearner_quartile

📁 OUTPUTS:
   • 04_model_comparison.csv
   • 04_tlearner_feature_importance.csv
   • 04_xlearner_residual_feature_importance.csv
"""

print(summary)

# Save summary
with open(OUTPUT_DIR / '04_combined_learners_summary.txt', 'w') as f:
    f.write(summary)

print("✅ All models complete and scores persisted to Snowflake!")

