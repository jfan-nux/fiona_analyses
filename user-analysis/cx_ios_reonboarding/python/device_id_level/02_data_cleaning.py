"""
Phase 2: Data Cleaning and Preparation
Clean data, handle missing values, create modeling-ready dataset
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

# Add utils to path
sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Setup
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)

# Directories
OUTPUT_DIR = Path('outputs/device_id_level')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("PHASE 2: DATA CLEANING & PREPARATION")
print("="*80)

# =======================
# 1. LOAD DATA
# =======================
print("\n[1] Loading master features table...")

query = """
SELECT * 
FROM proddb.fionafan.cx_ios_reonboarding_master_features
"""

with SnowflakeHook() as sf:
    df = sf.query_snowflake(query, method='pandas')
    print(f"✓ Loaded {len(df):,} rows")

# Sample 30%
df = df.sample(frac=0.3, random_state=42).copy()
print(f"✓ Using 30% sample: {len(df):,} rows")

# Create treatment indicator
df['is_treatment'] = df['result'].map({True: 1, 'true': 1, 'True': 1, 'TRUE': 1,
                                       False: 0, 'false': 0, 'False': 0, 'FALSE': 0}).fillna(0).astype(int)

print(f"\nInitial shape: {df.shape}")

# =======================
# 2. REMOVE INVALID ROWS
# =======================
print("\n[2] Filtering Invalid Data")
print("-" * 80)

initial_count = len(df)

# Remove test/internal tags
if 'tag' in df.columns:
    test_tags = df['tag'].isin(['internal_test', 'reserved', 'INTERNAL_TEST', 'RESERVED'])
    removed_test = test_tags.sum()
    df = df[~test_tags]
    print(f"Removed {removed_test:,} test/internal rows")

# Remove impossible tenure values
if 'tenure_days_at_exposure' in df.columns:
    negative_tenure = df['tenure_days_at_exposure'] < 0
    removed_tenure = negative_tenure.sum()
    df = df[~negative_tenure]
    print(f"Removed {removed_tenure:,} rows with negative tenure")

# Remove invalid exposure dates
if 'exposure_day' in df.columns:
    df['exposure_day'] = pd.to_datetime(df['exposure_day'])
    invalid_dates = df['exposure_day'] < '2025-01-01'
    removed_dates = invalid_dates.sum()
    df = df[~invalid_dates]
    print(f"Removed {removed_dates:,} rows with exposure before 2025-01-01")

total_removed = initial_count - len(df)
print(f"\nTotal rows removed: {total_removed:,}")
print(f"Remaining rows: {len(df):,}")

# =======================
# 3. HANDLE MISSING VALUES
# =======================
print("\n[3] Handling Missing Values")
print("-" * 80)

# Define imputation strategy
IMPUTE_ZERO = [
    # Behavioral metrics (0 means didn't do it)
    'store_content_visitor_count', 'store_page_visitor_count',
    'order_cart_visitor_count', 'checkout_visitor_count', 'purchaser_count',
    'has_store_content_visit', 'has_store_page_visit', 'has_order_cart_visit',
    'has_checkout_visit', 'has_purchase',
    'merchant_total_orders_ytd', 'unique_merchants_ytd', 'merchant_total_spend_ytd',
    'frequency_total_orders_ytd', 'frequency_total_spend_ytd',
    'num_favorite_merchants', 'top_merchant_orders', 'top_merchant_pct_orders',
    'unique_verticals_ytd', 'distinct_order_days', 'unique_stores',
    'longest_streak_weeks', 'orders_in_longest_streak', 'number_of_streaks',
    'active_weeks', 'pct_weeks_active', 'num_event_days',
    'orders_0_7d', 'orders_8_14d', 'orders_15_30d', 'orders_31_60d',
    'orders_61_90d', 'orders_91_180d', 'orders_180plus_d',
    'orders_early_period', 'orders_late_period',
    'avg_orders_per_week', 'avg_orders_per_month',
    'avg_days_between_orders', 'avg_days_between_orders_actual',
    'median_days_between_orders', 'min_gap_between_orders',
    'max_gap_between_orders', 'stddev_gap_between_orders',
    'max_merchant_order_concentration', 'avg_merchant_order_share',
    'ytd_period_days', 'ytd_period_weeks', 'is_dashpass',
    'is_consistent_orderer'
]

IMPUTE_MAX_PLUS_1 = [
    # Recency metrics (high value means never happened)
    'days_before_exposure_store_content', 'days_before_exposure_store_page',
    'days_before_exposure_order_cart', 'days_before_exposure_checkout',
    'days_before_exposure_purchase', 'days_since_last_funnel_action',
    'days_since_last_order', 'days_since_first_order'
]

IMPUTE_MEDIAN = [
    # Tenure and other metrics where median makes sense
    'tenure_days_at_exposure'
]

IMPUTE_MODE = [
    # Categorical features
    'lifestage', 'lifestage_bucket', 'experience', 'business_vertical_line',
    'country_id', 'region_name', 'user_ordering_type', 'frequency_bucket',
    'merchant_diversity', 'regularity_pattern', 'activity_trend',
    'pre_exposure_engagement_level', 'platform', 'urban_type'
]

IMPUTE_UNKNOWN = [
    # High-cardinality categoricals
    'submarket_id', 'app_version', 'top_merchant_id', 'top_merchant_name',
    'top_merchant_vertical'
]

# Apply imputation
print("\n[3.1] Imputing with 0 (behavioral metrics)...")
imputed_zero = 0
for col in IMPUTE_ZERO:
    if col in df.columns:
        before = df[col].isnull().sum()
        df[col] = df[col].fillna(0)
        imputed_zero += before
print(f"  Imputed {imputed_zero:,} missing values across {len([c for c in IMPUTE_ZERO if c in df.columns])} columns")

print("\n[3.2] Imputing recency metrics (max+1 means never happened)...")
imputed_recency = 0
for col in IMPUTE_MAX_PLUS_1:
    if col in df.columns:
        before = df[col].isnull().sum()
        max_val = df[col].max()
        df[col] = df[col].fillna(max_val + 1)
        imputed_recency += before
        print(f"  {col}: filled {before:,} with {max_val + 1:.0f}")
print(f"  Total imputed: {imputed_recency:,} values")

print("\n[3.3] Imputing with median...")
imputed_median = 0
for col in IMPUTE_MEDIAN:
    if col in df.columns:
        before = df[col].isnull().sum()
        median_val = df[col].median()
        df[col] = df[col].fillna(median_val)
        imputed_median += before
        print(f"  {col}: filled {before:,} with {median_val:.1f}")

print("\n[3.4] Imputing categorical with mode...")
imputed_mode = 0
for col in IMPUTE_MODE:
    if col in df.columns:
        before = df[col].isnull().sum()
        if before > 0:
            mode_val = df[col].mode()[0] if len(df[col].mode()) > 0 else 'Unknown'
            df[col] = df[col].fillna(mode_val)
            imputed_mode += before
            print(f"  {col}: filled {before:,} with '{mode_val}'")
print(f"  Total imputed: {imputed_mode:,} values")

print("\n[3.5] Imputing high-cardinality categoricals with 'Unknown'...")
imputed_unknown = 0
for col in IMPUTE_UNKNOWN:
    if col in df.columns:
        before = df[col].isnull().sum()
        df[col] = df[col].fillna('Unknown')
        imputed_unknown += before
print(f"  Imputed {imputed_unknown:,} values")

# Check remaining missing
remaining_missing = df.isnull().sum().sum()
print(f"\n✓ Missing values after imputation: {remaining_missing:,}")
if remaining_missing > 0:
    still_missing = df.isnull().sum()
    still_missing = still_missing[still_missing > 0].sort_values(ascending=False)
    print(f"  Columns still with missing data:")
    print(still_missing.head(10))

# =======================
# 4. CAP OUTLIERS
# =======================
print("\n[4] Capping Outliers (Winsorization at 1st/99th percentile)")
print("-" * 80)

WINSORIZE_COLS = [
    'merchant_total_orders_ytd', 'merchant_total_spend_ytd',
    'avg_orders_per_week', 'total_orders_post_exposure',
    'total_gov_post_exposure', 'frequency_total_orders_ytd',
    'frequency_total_spend_ytd', 'longest_streak_weeks',
    'tenure_days_at_exposure'
]

capped_stats = []
for col in WINSORIZE_COLS:
    if col in df.columns:
        p01 = df[col].quantile(0.01)
        p99 = df[col].quantile(0.99)
        
        # Count how many would be capped
        below = (df[col] < p01).sum()
        above = (df[col] > p99).sum()
        
        # Cap
        df[col] = df[col].clip(lower=p01, upper=p99)
        
        capped_stats.append({
            'feature': col,
            'p01': p01,
            'p99': p99,
            'capped_below': below,
            'capped_above': above,
            'total_capped': below + above
        })
        
        if below + above > 0:
            print(f"  {col:40s}: capped {below:,} below, {above:,} above")

if len(capped_stats) > 0:
    pd.DataFrame(capped_stats).to_csv(OUTPUT_DIR / '02_winsorization_report.csv', index=False)
    print(f"\n✓ Saved winsorization report")

# =======================
# 5. CREATE DERIVED FEATURES
# =======================
print("\n[5] Creating Derived Features")
print("-" * 80)

# Binary indicators
df['has_pre_orders'] = (df['merchant_total_orders_ytd'] > 0).astype(int)
df['had_recent_activity_30d'] = (df['days_since_last_order'] <= 30).astype(int)
df['had_recent_activity_90d'] = (df['days_since_last_order'] <= 90).astype(int)

# High value customer
if 'merchant_total_spend_ytd' in df.columns:
    median_spend = df['merchant_total_spend_ytd'].median()
    df['is_high_value'] = (df['merchant_total_spend_ytd'] > median_spend).astype(int)

# Engagement level numeric
engagement_map = {
    'High Engagement': 3,
    'Medium Engagement': 2,
    'Low Engagement': 1,
    'No Engagement': 0
}
if 'pre_exposure_engagement_level' in df.columns:
    df['engagement_level_numeric'] = df['pre_exposure_engagement_level'].map(engagement_map).fillna(0)

print("✓ Created derived features:")
print("  - has_pre_orders")
print("  - had_recent_activity_30d")
print("  - had_recent_activity_90d")
print("  - is_high_value")
print("  - engagement_level_numeric")

# =======================
# 6. FINAL FEATURE SELECTION
# =======================
print("\n[6] Final Feature Selection for Modeling")
print("-" * 80)

# Target variables
TARGET_VARS = [
    'has_order_post_exposure',
    'did_reonboarding',
    'push_system_optin',
    'sms_marketing_optin',
    'total_orders_post_exposure',
    'total_gov_post_exposure'
]

# ID columns to keep
ID_COLS = ['dd_device_id', 'user_id', 'tag', 'result', 'is_treatment', 'exposure_day']

# Feature columns (everything else that's not target or ID)
feature_cols = [c for c in df.columns if c not in TARGET_VARS + ID_COLS]

print(f"Target variables: {len([t for t in TARGET_VARS if t in df.columns])}")
print(f"Feature columns: {len(feature_cols)}")
print(f"ID/metadata columns: {len(ID_COLS)}")

# Separate continuous and categorical
continuous_features = []
categorical_features = []

for col in feature_cols:
    nunique = df[col].nunique()
    dtype = df[col].dtype
    
    # Categorical if: object type, OR has few unique values, OR is binary
    if dtype == 'object' or nunique <= 20:
        categorical_features.append(col)
    else:
        continuous_features.append(col)

print(f"\n  Continuous: {len(continuous_features)}")
print(f"  Categorical: {len(categorical_features)}")

# =======================
# 7. SAVE CLEANED DATA
# =======================
print("\n[7] Saving Cleaned Dataset")
print("-" * 80)

# Save full cleaned dataset
output_file = OUTPUT_DIR / 'master_features_cleaned.csv'
df.to_csv(output_file, index=False)
print(f"✓ Saved cleaned data: {output_file}")
print(f"  Rows: {len(df):,}")
print(f"  Columns: {len(df.columns)}")
print(f"  Size: {output_file.stat().st_size / 1024**2:.1f} MB")

# Save feature lists (only if they don't exist - don't overwrite manual edits)
if not (OUTPUT_DIR / '02_continuous_features.txt').exists():
    with open(OUTPUT_DIR / '02_continuous_features.txt', 'w') as f:
        f.write('\n'.join(continuous_features))
    print("✓ Saved continuous features list")
else:
    print("⚠ 02_continuous_features.txt already exists - skipping (preserving manual edits)")

if not (OUTPUT_DIR / '02_categorical_features.txt').exists():
    with open(OUTPUT_DIR / '02_categorical_features.txt', 'w') as f:
        f.write('\n'.join(categorical_features))
    print("✓ Saved categorical features list")
else:
    print("⚠ 02_categorical_features.txt already exists - skipping (preserving manual edits)")

if not (OUTPUT_DIR / '02_target_variables.txt').exists():
    with open(OUTPUT_DIR / '02_target_variables.txt', 'w') as f:
        f.write('\n'.join([t for t in TARGET_VARS if t in df.columns]))
    print("✓ Saved target variables list")
else:
    print("⚠ 02_target_variables.txt already exists - skipping (preserving manual edits)")

print("✓ Saved feature lists")

# =======================
# 8. DATA QUALITY REPORT
# =======================
print("\n[8] Final Data Quality Report")
print("=" * 80)

quality_report = {
    'Total Rows (Cleaned)': len(df),
    'Total Columns': len(df.columns),
    'Features': len(feature_cols),
    '  - Continuous': len(continuous_features),
    '  - Categorical': len(categorical_features),
    'Target Variables': len([t for t in TARGET_VARS if t in df.columns]),
    'Missing Values': df.isnull().sum().sum(),
    'Missing %': f"{df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100:.2f}%",
    'Duplicates': df.duplicated(subset=['dd_device_id']).sum(),
    'Treatment': (df['is_treatment']==1).sum(),
    'Control': (df['is_treatment']==0).sum(),
}

print("\n📋 CLEANED DATASET SUMMARY:")
for key, value in quality_report.items():
    print(f"  {key:30s}: {value}")

# Treatment balance check
print("\n📊 TREATMENT BALANCE:")
print(f"  Control:   {(df['is_treatment']==0).sum():,} ({(df['is_treatment']==0).mean():.1%})")
print(f"  Treatment: {(df['is_treatment']==1).sum():,} ({(df['is_treatment']==1).mean():.1%})")

# Outcome rates
if 'has_order_post_exposure' in df.columns:
    control_order = df[df['is_treatment']==0]['has_order_post_exposure'].mean()
    treatment_order = df[df['is_treatment']==1]['has_order_post_exposure'].mean()
    lift = ((treatment_order - control_order) / control_order * 100) if control_order > 0 else 0
    
    print(f"\n📦 ORDER RATES (Cleaned Data):")
    print(f"  Control:   {control_order:.3%}")
    print(f"  Treatment: {treatment_order:.3%}")
    print(f"  Lift:      {lift:+.2f}%")

if 'did_reonboarding' in df.columns:
    treatment_df = df[df['is_treatment'] == 1]
    compliance = treatment_df['did_reonboarding'].mean()
    print(f"\n🎯 COMPLIANCE:")
    print(f"  Reonboarding rate (treatment): {compliance:.2%}")

# Save quality report
quality_df = pd.DataFrame(list(quality_report.items()), columns=['Metric', 'Value'])
quality_df.to_csv(OUTPUT_DIR / '02_data_quality_report.csv', index=False)

print("\n" + "="*80)
print("PHASE 2 COMPLETE: DATA CLEANING")
print("="*80)
print(f"\n✅ Cleaned dataset ready for modeling:")
print(f"   {output_file}")
print(f"\n📁 Outputs:")
print(f"   • 02_data_quality_report.csv")
print(f"   • 02_continuous_features.txt ({len(continuous_features)} features)")
print(f"   • 02_categorical_features.txt ({len(categorical_features)} features)")
print(f"   • 02_target_variables.txt ({len([t for t in TARGET_VARS if t in df.columns])} targets)")

print(f"\n➡️  Next: Run Phase 3 - EDA After Cleaning")
print(f"   python user-analysis/cx_ios_reonboarding/python/03_eda_after_cleaning.py")

