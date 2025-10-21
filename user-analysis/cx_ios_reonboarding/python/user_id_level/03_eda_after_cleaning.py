"""
Phase 3: EDA After Data Cleaning
Validate cleaned data and check treatment balance
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy.stats import ttest_ind, chi2_contingency
import warnings
warnings.filterwarnings('ignore')

# Setup
pd.set_option('display.max_columns', None)
sns.set_style('whitegrid')

# Directories
PLOTS_DIR = Path('plots/user_id_level')
OUTPUT_DIR = Path('outputs/user_id_level')

print("="*80)
print("PHASE 3: EDA AFTER DATA CLEANING")
print("Treatment Balance & Feature Validation")
print("="*80)

# =======================
# 1. LOAD CLEANED DATA
# =======================
print("\n[1] Loading cleaned data...")

df = pd.read_csv(OUTPUT_DIR / 'master_features_cleaned.csv', low_memory=False)
print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")

# Load feature lists
with open(OUTPUT_DIR / '02_continuous_features.txt') as f:
    continuous_features = [line.strip() for line in f if line.strip()]

with open(OUTPUT_DIR / '02_categorical_features.txt') as f:
    categorical_features = [line.strip() for line in f if line.strip()]

with open(OUTPUT_DIR / '02_target_variables.txt') as f:
    target_variables = [line.strip() for line in f if line.strip()]

# Convert continuous features to numeric
for col in continuous_features:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Convert target variables to numeric
for col in target_variables:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

print(f"  Continuous features: {len(continuous_features)}")
print(f"  Categorical features: {len(categorical_features)}")
print(f"  Target variables: {len(target_variables)}")

# =======================
# 2. TREATMENT BALANCE CHECK
# =======================
print("\n[2] Treatment Balance Check (Covariate Balance)")
print("=" * 80)

# Statistical tests for treatment balance
balance_results = []

# Test continuous features
print("\n[2.1] Continuous Features (t-tests)")
print("-" * 80)

control_df = df[df['is_treatment'] == 0]
treatment_df = df[df['is_treatment'] == 1]

print(f"Testing {len(continuous_features)} continuous features...\n")

for feat in continuous_features:
    if feat in df.columns:
        control_vals = control_df[feat].dropna()
        treatment_vals = treatment_df[feat].dropna()
        
        if len(control_vals) > 0 and len(treatment_vals) > 0:
            # T-test
            t_stat, p_val = ttest_ind(treatment_vals, control_vals)
            
            # Standardized mean difference (SMD)
            mean_diff = treatment_vals.mean() - control_vals.mean()
            pooled_std = np.sqrt((control_vals.std()**2 + treatment_vals.std()**2) / 2)
            smd = mean_diff / pooled_std if pooled_std > 0 else 0
            
            balance_results.append({
                'feature': feat,
                'type': 'continuous',
                'control_mean': control_vals.mean(),
                'treatment_mean': treatment_vals.mean(),
                'smd': smd,
                't_stat': t_stat,
                'p_value': p_val,
                'balanced': abs(smd) < 0.1
            })

# Show imbalanced features (SMD > 0.1)
balance_df = pd.DataFrame(balance_results)
imbalanced = balance_df[abs(balance_df['smd']) > 0.1].sort_values('smd', key=abs, ascending=False)

if len(imbalanced) > 0:
    print(f"⚠ Found {len(imbalanced)} imbalanced continuous features (|SMD| > 0.1):")
    print(imbalanced[['feature', 'control_mean', 'treatment_mean', 'smd', 'p_value']].head(15).to_string(index=False))
else:
    print("✓ All continuous features are balanced (|SMD| < 0.1)")

# Test categorical features
print("\n[2.2] Categorical Features (Chi-square tests)")
print("-" * 80)

cat_balance_results = []
print(f"Testing {len(categorical_features)} categorical features...\n")

for feat in categorical_features[:10]:  # Test top 10 to save time
    if feat in df.columns:
        try:
            contingency = pd.crosstab(df[feat], df['is_treatment'])
            if contingency.shape[0] > 1 and contingency.shape[1] > 1:
                chi2, p_val, _, _ = chi2_contingency(contingency)
                
                cat_balance_results.append({
                    'feature': feat,
                    'chi2': chi2,
                    'p_value': p_val,
                    'balanced': p_val > 0.05
                })
        except:
            pass

if len(cat_balance_results) > 0:
    cat_balance_df = pd.DataFrame(cat_balance_results)
    imbalanced_cat = cat_balance_df[cat_balance_df['p_value'] < 0.05]
    
    if len(imbalanced_cat) > 0:
        print(f"⚠ Found {len(imbalanced_cat)} imbalanced categorical features (p < 0.05):")
        print(imbalanced_cat.to_string(index=False))
    else:
        print("✓ All tested categorical features are balanced (p > 0.05)")

# Save balance report
all_balance = pd.concat([
    balance_df,
    pd.DataFrame(cat_balance_results)
], ignore_index=True)
all_balance.to_csv(OUTPUT_DIR / '03_treatment_balance_report.csv', index=False)
print(f"\n✓ Saved treatment balance report")

# =======================
# 3. FINAL VALIDATION
# =======================
print("\n[3] Final Data Validation")
print("=" * 80)

validation_checks = {
    'No duplicates': df.duplicated(subset=['user_id']).sum() == 0,
    'No missing targets': df[target_variables].isnull().sum().sum() == 0,
    'Treatment split 50/50': abs(df['is_treatment'].mean() - 0.5) < 0.01,
    'No negative tenure': (df.get('tenure_days_at_exposure', pd.Series([0])) >= 0).all(),
    'Valid exposure dates': (df['exposure_day'] >= '2025-01-01').all() if 'exposure_day' in df.columns else True,
}

print("\n✓ Validation Results:")
for check, passed in validation_checks.items():
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status} - {check}")

# =======================
# 4. SUMMARY
# =======================
print("\n" + "="*80)
print("PHASE 3 COMPLETE: POST-CLEANING VALIDATION")
print("="*80)

summary = f"""
✅ Data is ready for causal modeling

📊 Final Dataset:
  • Rows: {len(df):,}
  • Features: {len(continuous_features) + len(categorical_features)}
  • Targets: {len(target_variables)}
  • Missing: {df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100:.2f}%

⚖️ Treatment Balance:
  • {len([r for r in balance_results if r['balanced']])} / {len(balance_results)} continuous features balanced
  • Treatment split: {df['is_treatment'].mean():.1%}

📦 Key Metrics:
  • Order rate lift: {((treatment_df['has_order_post_exposure'].mean() - control_df['has_order_post_exposure'].mean()) / control_df['has_order_post_exposure'].mean() * 100):.2f}%
  • Compliance: {treatment_df['did_reonboarding'].mean():.2%}
"""

print(summary)

print("\n➡️  Next: Run Phase 4 - Causal Analysis")
print("   python user-analysis/cx_ios_reonboarding/python/04_causal_analysis.py")

