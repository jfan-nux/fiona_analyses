"""
Phase 1: Exploratory Data Analysis - Before Data Cleaning
Load master features table and identify features for modeling
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
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
pd.set_option('display.width', None)
pd.set_option('display.float_format', '{:.4f}'.format)
sns.set_style('whitegrid')

# Output directories
PLOTS_DIR = Path('plots')
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = Path('outputs')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("="*80)
print("PHASE 1: EDA BEFORE DATA CLEANING")
print("CX iOS Reonboarding - Feature Identification & Target Analysis")
print("="*80)

# =======================
# 1. LOAD DATA
# =======================
print("\n[1] Loading master features table from Snowflake...")

query = """
SELECT * 
FROM proddb.fionafan.cx_ios_reonboarding_master_features
"""

# Use Snowflake connection
with SnowflakeHook() as sf:
    print("Executing query...")
    df_full = sf.query_snowflake(query, method='pandas')
    print(f"✓ Loaded {len(df_full):,} rows, {len(df_full.columns)} columns")

# Random sample 30%
print("\nSampling 30% of data for EDA...")
df = df_full.sample(frac=0.3, random_state=42).copy()
print(f"✓ Sampled {len(df):,} rows for analysis")
del df_full  # Free memory

# =======================
# 2. IDENTIFY FEATURES
# =======================
print("\n[2] Identifying Features for Modeling")
print("-" * 80)

# Define target variables (outcomes)
TARGET_VARIABLES = {
    'has_order_post_exposure': 'Primary - Ordered Post-Exposure (Binary)',
    'did_reonboarding': 'Treatment Compliance - Did Reonboarding (Binary)',
    'push_system_optin': 'Opt-in - Push Notifications (Binary)',
    'sms_marketing_optin': 'Opt-in - SMS Marketing (Binary)',
    'total_orders_post_exposure': 'Order Volume - Total Orders (Continuous)',
    'total_gov_post_exposure': 'Order Value - Total GOV (Continuous)'
}

# Exclude columns from features
EXCLUDE_COLUMNS = [
    # Identifiers
    'dd_device_id', 'dd_device_id_filtered', 'user_id', 'scd_consumer_id',
    
    # Treatment assignment (will be used separately)
    'tag', 'result', 'is_treatment',
    
    # Exposure timing
    'exposure_time', 'exposure_day',
    
    # Post-exposure outcomes (targets)
    'has_order_post_exposure', 'did_reonboarding',
    'push_system_optin', 'push_doordash_offers_optin', 'push_order_updates_optin',
    'sms_marketing_optin', 'sms_order_updates_optin',
    'total_orders_post_exposure', 'new_cx_orders_post_exposure',
    'total_gov_post_exposure', 'total_vp_post_exposure', 'avg_order_value',
    'first_order_date_post_exposure', 'last_order_date_post_exposure',
    'days_to_first_order',
    
    # SCD metadata (not predictive features)
    'scd_start_date', 'scd_end_date',
    
    # Dates (use derived metrics instead)
    'signup_date', 'first_order_date', 'last_order_date',
    'first_event_date', 'last_event_date'
]

# Get all columns
all_columns = df.columns.tolist()

# Filter to feature columns
feature_columns = [c for c in all_columns if c not in EXCLUDE_COLUMNS]

print(f"\nTotal columns in dataset: {len(all_columns)}")
print(f"Excluded columns: {len(EXCLUDE_COLUMNS)}")
print(f"Feature columns: {len(feature_columns)}")

# Categorize features
continuous_features = []
categorical_features = []

for col in feature_columns:
    if col in df.columns:
        # Check data type and unique values
        nunique = df[col].nunique()
        dtype = df[col].dtype
        
        if dtype in ['object', 'category'] or nunique <= 20:
            categorical_features.append(col)
        else:
            continuous_features.append(col)

print(f"\nContinuous features: {len(continuous_features)}")
print(f"Categorical features: {len(categorical_features)}")

# =======================
# 3. FEATURE INVENTORY
# =======================
print("\n[3] Complete Feature Inventory")
print("=" * 80)

print("\n📊 TARGET VARIABLES (Outcomes):")
for i, (var, desc) in enumerate(TARGET_VARIABLES.items(), 1):
    if var in df.columns:
        print(f"  {i}. {var:35s} - {desc}")

print(f"\n📈 CONTINUOUS FEATURES ({len(continuous_features)} total):")
print("-" * 80)

# Group continuous features by category
feature_groups = {
    'Tenure & Timing': [f for f in continuous_features if 'tenure' in f or 'days_since' in f or 'days_before' in f],
    'Order Volume': [f for f in continuous_features if 'total_orders' in f or 'orders_' in f and 'ytd' in f],
    'Order Frequency': [f for f in continuous_features if 'avg_orders' in f or 'gap' in f or 'streak' in f],
    'Merchant Behavior': [f for f in continuous_features if 'merchant' in f or 'store' in f],
    'Funnel Metrics': [f for f in continuous_features if 'visitor' in f or 'purchaser' in f],
    'Other Metrics': []
}

# Categorize
categorized = set()
for group in feature_groups.values():
    categorized.update(group)

feature_groups['Other Metrics'] = [f for f in continuous_features if f not in categorized]

for group_name, features in feature_groups.items():
    if len(features) > 0:
        print(f"\n  {group_name}:")
        for feat in features:
            print(f"    - {feat}")

print(f"\n📋 CATEGORICAL FEATURES ({len(categorical_features)} total):")
print("-" * 80)
for feat in categorical_features:
    nunique = df[feat].nunique()
    print(f"  - {feat:45s} ({nunique} unique values)")

# Save feature inventory
feature_inventory = pd.DataFrame({
    'feature_name': continuous_features + categorical_features,
    'feature_type': ['continuous'] * len(continuous_features) + ['categorical'] * len(categorical_features),
    'missing_pct': [(df[f].isnull().sum() / len(df) * 100) for f in continuous_features + categorical_features]
})
feature_inventory.to_csv(OUTPUT_DIR / '01_feature_inventory.csv', index=False)
print(f"\n✓ Saved feature inventory to outputs/01_feature_inventory.csv")

# =======================
# 4. TREATMENT ASSIGNMENT
# =======================
print("\n[4] Treatment Assignment & Compliance")
print("-" * 80)

# Create treatment indicator (handle string boolean values)
df['is_treatment'] = df['result'].map({True: 1, 'true': 1, 'True': 1, 'TRUE': 1,
                                       False: 0, 'false': 0, 'False': 0, 'FALSE': 0}).fillna(0).astype(int)

print(f"Treatment split:")
control_count = (df['is_treatment']==0).sum()
treatment_count = (df['is_treatment']==1).sum()
print(f"  Control:   {control_count:,} ({control_count/len(df):.1%})")
print(f"  Treatment: {treatment_count:,} ({treatment_count/len(df):.1%})")

if 'did_reonboarding' in df.columns:
    treatment_df = df[df['is_treatment'] == 1]
    control_df = df[df['is_treatment'] == 0]
    
    compliance = treatment_df['did_reonboarding'].mean()
    print(f"\nTreatment Compliance:")
    print(f"  Did reonboarding: {treatment_df['did_reonboarding'].sum():,} / {len(treatment_df):,}")
    print(f"  Compliance rate: {compliance:.2%}")
    
    control_contamination = control_df['did_reonboarding'].sum()
    if control_contamination > 0:
        print(f"\n⚠ WARNING: {control_contamination:,} control users saw reonboarding (contamination!)")

# =======================
# 5. TARGET VARIABLE ANALYSIS
# =======================
print("\n[5] Target Variable Summary")
print("-" * 80)

target_summary = []
for var, desc in TARGET_VARIABLES.items():
    if var in df.columns:
        overall = df[var].mean()
        control = df[df['is_treatment']==0][var].mean()
        treatment = df[df['is_treatment']==1][var].mean()
        
        lift = ((treatment - control) / control * 100) if control > 0 else np.nan
        
        target_summary.append({
            'target': var,
            'description': desc,
            'overall': overall,
            'control': control,
            'treatment': treatment,
            'absolute_diff': treatment - control,
            'lift_pct': lift
        })
        
        print(f"\n{desc}")
        print(f"  {var}")
        print(f"    Overall:   {overall:.6f}")
        print(f"    Control:   {control:.6f}")
        print(f"    Treatment: {treatment:.6f}")
        print(f"    Diff:      {treatment - control:+.6f}")
        if not np.isnan(lift):
            print(f"    Lift:      {lift:+.2f}%")

target_df = pd.DataFrame(target_summary)
target_df.to_csv(OUTPUT_DIR / '01_target_variable_summary.csv', index=False)
print(f"\n✓ Saved target summary")

# =======================
# 6. MISSING DATA ANALYSIS
# =======================
print("\n[6] Missing Data Analysis")
print("-" * 80)

# Feature missingness
feature_missing = pd.DataFrame({
    'feature': feature_columns,
    'missing_count': [df[c].isnull().sum() for c in feature_columns],
    'missing_pct': [(df[c].isnull().sum() / len(df) * 100) for c in feature_columns]
}).sort_values('missing_pct', ascending=False)

high_missing = feature_missing[feature_missing['missing_pct'] > 0]
print(f"\nFeatures with missing data: {len(high_missing)} / {len(feature_columns)}")

if len(high_missing) > 0:
    print("\nTop 15 features with most missing data:")
    print(high_missing.head(15).to_string(index=False))
    feature_missing.to_csv(OUTPUT_DIR / '01_feature_missing_data.csv', index=False)

# =======================
# 7. CORRELATION HEATMAP: TARGETS vs FEATURES
# =======================
print("\n[7] Creating Target-Feature Correlation Heatmap")
print("-" * 80)

# Get target variables that exist
existing_targets = [t for t in TARGET_VARIABLES.keys() if t in df.columns]
print(f"\nTarget variables: {len(existing_targets)}")
for t in existing_targets:
    print(f"  - {t}")

# Get continuous features that exist
existing_continuous = [f for f in continuous_features if f in df.columns]
print(f"\nContinuous features for correlation: {len(existing_continuous)}")

if len(existing_targets) > 0 and len(existing_continuous) > 0:
    # Calculate correlations between features and targets
    corr_data = df[existing_continuous + existing_targets].corr()
    
    # Extract only feature-target correlations
    feature_target_corr = corr_data.loc[existing_continuous, existing_targets]
    
    # Get top N features by absolute correlation with primary target
    primary_target = 'has_order_post_exposure'
    if primary_target in existing_targets:
        # Sort by absolute correlation
        top_features_idx = feature_target_corr[primary_target].abs().sort_values(ascending=False).head(30).index
        feature_target_corr_top = feature_target_corr.loc[top_features_idx, :]
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=(14, 16))
        sns.heatmap(feature_target_corr_top, annot=True, fmt='.3f', cmap='RdBu_r', 
                    center=0, vmin=-0.5, vmax=0.5,
                    square=False, linewidths=0.5, 
                    cbar_kws={"shrink": 0.8, "label": "Correlation"},
                    ax=ax)
        ax.set_title('Top 30 Features: Correlation with Target Variables\n(Ordered by correlation with has_order_post_exposure)', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xlabel('Target Variables', fontsize=12, fontweight='bold')
        ax.set_ylabel('Pre-Exposure Features', fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(PLOTS_DIR / '01_target_feature_correlation_heatmap.png', dpi=150, bbox_inches='tight')
        plt.close()
        print("✓ Saved: 01_target_feature_correlation_heatmap.png")
        
        # Save correlation matrix
        feature_target_corr.to_csv(OUTPUT_DIR / '01_target_feature_correlations.csv')
        print("✓ Saved: 01_target_feature_correlations.csv")
        
        # Print top correlated features
        print("\n📊 Top 15 Features Correlated with Ordering (has_order_post_exposure):")
        top_corr = feature_target_corr[primary_target].sort_values(key=abs, ascending=False).head(15)
        for feat, corr in top_corr.items():
            print(f"  {feat:50s}: {corr:+.4f}")

# =======================
# 8. FEATURE SUMMARY BY TYPE
# =======================
print("\n[8] Feature Summary for Modeling")
print("=" * 80)

print(f"\n📋 MODELING FEATURES BREAKDOWN:")
print(f"\nContinuous Features ({len(continuous_features)}):")

# Categorize continuous features by domain
continuous_by_domain = {
    '1. User Tenure & Recency': [
        'tenure_days_at_exposure', 'days_since_first_order', 'days_since_last_order',
        'days_before_exposure_store_content', 'days_before_exposure_store_page',
        'days_before_exposure_order_cart', 'days_before_exposure_checkout',
        'days_before_exposure_purchase', 'days_since_last_funnel_action'
    ],
    '2. Pre-Exposure Funnel Engagement': [
        'store_content_visitor_count', 'store_page_visitor_count',
        'order_cart_visitor_count', 'checkout_visitor_count', 'purchaser_count',
        'has_store_content_visit', 'has_store_page_visit', 'has_order_cart_visit',
        'has_checkout_visit', 'has_purchase', 'num_event_days'
    ],
    '3. Merchant Affinity & Diversity': [
        'merchant_total_orders_ytd', 'unique_merchants_ytd', 'merchant_total_spend_ytd',
        'unique_verticals_ytd', 'num_favorite_merchants', 'top_merchant_orders',
        'top_merchant_pct_orders', 'max_merchant_order_concentration',
        'avg_merchant_order_share'
    ],
    '4. Order Frequency & Patterns': [
        'frequency_total_orders_ytd', 'distinct_order_days', 'unique_stores',
        'frequency_total_spend_ytd', 'ytd_period_days', 'ytd_period_weeks',
        'avg_orders_per_week', 'avg_orders_per_month', 'avg_days_between_orders',
        'avg_days_between_orders_actual', 'median_days_between_orders',
        'min_gap_between_orders', 'max_gap_between_orders', 'stddev_gap_between_orders',
        'longest_streak_weeks', 'orders_in_longest_streak', 'number_of_streaks',
        'active_weeks', 'pct_weeks_active'
    ],
    '5. Order Timing Distribution': [
        'orders_0_7d', 'orders_8_14d', 'orders_15_30d', 'orders_31_60d',
        'orders_61_90d', 'orders_91_180d', 'orders_180plus_d',
        'orders_early_period', 'orders_late_period'
    ],
    '6. Binary Flags': [
        'is_dashpass', 'is_consistent_orderer'
    ]
}

print("\n" + "="*80)
for domain, features in continuous_by_domain.items():
    existing_features = [f for f in features if f in df.columns]
    if len(existing_features) > 0:
        print(f"\n{domain} ({len(existing_features)} features):")
        for feat in existing_features:
            missing = df[feat].isnull().sum() / len(df) * 100
            print(f"  • {feat:50s} (missing: {missing:.1f}%)")

print(f"\n\n📂 CATEGORICAL FEATURES ({len(categorical_features)} total):")
print("-" * 80)

categorical_by_domain = {
    'User Characteristics': [
        'lifestage', 'lifestage_bucket', 'experience', 'business_vertical_line'
    ],
    'Geography': [
        'country_id', 'region_name', 'submarket_id', 'urban_type'
    ],
    'Behavior Patterns': [
        'user_ordering_type', 'frequency_bucket', 'merchant_diversity',
        'regularity_pattern', 'activity_trend', 'pre_exposure_engagement_level'
    ],
    'Device & Platform': [
        'platform', 'app_version'
    ],
    'Merchant Details': [
        'top_merchant_id', 'top_merchant_name', 'top_merchant_vertical'
    ]
}

for domain, features in categorical_by_domain.items():
    existing_features = [f for f in features if f in df.columns]
    if len(existing_features) > 0:
        print(f"\n{domain}:")
        for feat in existing_features:
            nunique = df[feat].nunique()
            missing = df[feat].isnull().sum() / len(df) * 100
            print(f"  • {feat:50s} ({nunique:4d} categories, missing: {missing:.1f}%)")

# =======================
# 9. SAVE FEATURE LISTS
# =======================
print("\n[9] Saving Feature Lists for Modeling")
print("-" * 80)

# Save feature lists (only if they don't exist)
if not (OUTPUT_DIR / '01_continuous_features.txt').exists():
    with open(OUTPUT_DIR / '01_continuous_features.txt', 'w') as f:
        f.write('\n'.join(continuous_features))

if not (OUTPUT_DIR / '01_categorical_features.txt').exists():
    with open(OUTPUT_DIR / '01_categorical_features.txt', 'w') as f:
        f.write('\n'.join(categorical_features))

if not (OUTPUT_DIR / '01_target_variables.txt').exists():
    with open(OUTPUT_DIR / '01_target_variables.txt', 'w') as f:
        f.write('\n'.join(TARGET_VARIABLES.keys()))

print("✓ Saved feature lists:")
print("  - outputs/01_continuous_features.txt")
print("  - outputs/01_categorical_features.txt")
print("  - outputs/01_target_variables.txt")

# =======================
# 10. BASIC DATA QUALITY
# =======================
print("\n[10] Data Quality Summary")
print("-" * 80)

print(f"\nDuplicates:")
device_dupes = df.duplicated(subset=['dd_device_id'], keep=False).sum()
print(f"  Device-level duplicates: {device_dupes:,}")

print(f"\nData completeness:")
print(f"  Rows: {len(df):,}")
print(f"  Features: {len(feature_columns)}")
print(f"  Total cells: {len(df) * len(feature_columns):,}")
total_missing = df[feature_columns].isnull().sum().sum()
total_cells = len(df) * len(feature_columns)
print(f"  Missing cells: {total_missing:,} ({total_missing/total_cells*100:.2f}%)")

# =======================
# 11. FINAL SUMMARY
# =======================
print("\n" + "="*80)
print("PHASE 1 COMPLETE: FEATURE IDENTIFICATION")
print("="*80)

summary = f"""
📊 DATASET SUMMARY:
  • Total rows (30% sample): {len(df):,}
  • Unique devices: {df['dd_device_id'].nunique():,}
  • Unique users: {df['user_id'].nunique():,}
  • Date range: {df['exposure_day'].min()} to {df['exposure_day'].max()}

🎯 TARGET VARIABLES: {len(existing_targets)}
  • Primary: has_order_post_exposure
  • Compliance: did_reonboarding
  • Opt-ins: push_system_optin, sms_marketing_optin
  • Volume: total_orders_post_exposure, total_gov_post_exposure

📈 FEATURES FOR MODELING: {len(feature_columns)}
  • Continuous: {len(continuous_features)}
    - Tenure & Timing: {len([f for f in continuous_features if 'tenure' in f or 'days_since' in f or 'days_before' in f])}
    - Order Volume: {len([f for f in continuous_features if 'total_orders' in f or ('orders_' in f and 'ytd' in f)])}
    - Order Frequency: {len([f for f in continuous_features if 'avg_orders' in f or 'gap' in f or 'streak' in f])}
    - Merchant Behavior: {len([f for f in continuous_features if 'merchant' in f or 'store' in f])}
    - Funnel Metrics: {len([f for f in continuous_features if 'visitor' in f or 'purchaser' in f])}
  
  • Categorical: {len(categorical_features)}
    - User characteristics (lifestage, experience)
    - Behavior patterns (ordering type, frequency bucket)
    - Geography (region, submarket)

🔬 TREATMENT ASSIGNMENT:
  • Control: {control_count:,} ({control_count/len(df):.1%})
  • Treatment: {treatment_count:,} ({treatment_count/len(df):.1%})
"""

if 'did_reonboarding' in df.columns:
    compliance_rate = treatment_df['did_reonboarding'].mean()
    summary += f"  • Compliance (treatment): {compliance_rate:.2%}\n"

if 'has_order_post_exposure' in df.columns:
    control_order = df[df['is_treatment']==0]['has_order_post_exposure'].mean()
    treatment_order = df[df['is_treatment']==1]['has_order_post_exposure'].mean()
    order_lift = ((treatment_order - control_order) / control_order * 100) if control_order > 0 else 0
    summary += f"\n📦 ORDER RATE:\n"
    summary += f"  • Control: {control_order:.3%}\n"
    summary += f"  • Treatment: {treatment_order:.3%}\n"
    summary += f"  • Lift: {order_lift:+.2f}%\n"

print(summary)

# Save summary
with open(OUTPUT_DIR / '01_phase1_summary.txt', 'w') as f:
    f.write(summary)

print(f"\n✅ Phase 1 Complete!")
print(f"   Outputs saved to: user-analysis/cx_ios_reonboarding/outputs/")
print(f"   Plots saved to: user-analysis/cx_ios_reonboarding/plots/")
print(f"\n📁 Key Files Generated:")
print(f"   • 01_feature_inventory.csv - Complete feature list")
print(f"   • 01_target_feature_correlations.csv - Correlation matrix")
print(f"   • 01_target_feature_correlation_heatmap.png - Visualization")
print(f"   • 01_target_variable_summary.csv - Outcome analysis")

print(f"\n➡️  Next: Run Phase 2 - Data Cleaning")
print(f"   python user-analysis/cx_ios_reonboarding/python/02_data_cleaning.py")
