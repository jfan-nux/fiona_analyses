# Databricks notebook source
# MAGIC %md
# MAGIC # Session Conversion Prediction - Logistic Regression Models (PySpark)
# MAGIC
# MAGIC Builds logistic regression models to predict session conversion using PySpark.
# MAGIC Excludes data leakage features and generic features.
# MAGIC
# MAGIC **Key Steps**:
# MAGIC 1. Load session features from Snowflake
# MAGIC 2. Exclude data leakage features (DoubleDash, payment actions, etc.)
# MAGIC 3. Train Pre-Funnel Model (browsing behavior only)
# MAGIC 4. Train Funnel-Inclusive Model (with funnel progression)
# MAGIC 5. Analyze and save model coefficients to Snowflake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# Install compatible versions of packages for sentence-transformers
# Using newer versions that work together
#%pip install --upgrade torch transformers sentence-transformers scikit-learn huggingface-hub

# COMMAND ----------

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from typing import List, Tuple
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Connection Setup

# COMMAND ----------

# Snowflake connection setup
your_scope = "fionafan-scope"
user = dbutils.secrets.get(scope=your_scope, key="snowflake-user")
password = dbutils.secrets.get(scope=your_scope, key="snowflake-password")
default_schema = "fionafan"

OPTIONS = dict(
    sfurl="doordash.snowflakecomputing.com/",
    sfuser=user,
    sfpassword=password,
    sfaccount="DOORDASH",
    sfdatabase="PRODDB",
    sfrole=default_schema,
    sfwarehouse="TEAM_DATA_ANALYTICS_ETL",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load session features from Snowflake (including first_session for similarity calculation)
# Sample 2000 users and include ALL their sessions for faster testing
query = """
WITH sampled_users AS (
    SELECT DISTINCT user_id
    FROM proddb.fionafan.all_user_sessions_with_events_features_gen
    WHERE funnel_converted_bool IS NOT NULL
        AND user_id IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 2000
)
SELECT s.*
FROM proddb.fionafan.all_user_sessions_with_events_features_gen s
INNER JOIN sampled_users u ON s.user_id = u.user_id
WHERE s.funnel_converted_bool IS NOT NULL
"""

df_spark = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

session_count = df_spark.count()
conversion_rate = df_spark.agg(F.avg("funnel_converted_bool")).collect()[0][0]

print(f"Loaded {session_count:,} sessions")
print(f"Conversion rate: {conversion_rate:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Store Embedding Similarity

# COMMAND ----------

def create_store_embeddings(df_pd):
    """
    Create embeddings for THREE types of store features and calculate session-to-session similarity.
    1. Impression embeddings (what stores users saw)
    2. Attribution embeddings (what stores users clicked)
    3. Funnel store page embeddings (what stores users visited)
    
    Returns DataFrame with added similarity features (3 similarity scores per session).
    """
    print("\n🔄 Creating store embeddings for 3 event types...")
    
    # Load sentence transformer model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Define the three types of store features
    store_types = [
        ('impression', 'STORE_TAGS_CONCAT_IMPRESSION', 'STORE_CATEGORIES_CONCAT_IMPRESSION', 'STORE_NAMES_CONCAT_IMPRESSION'),
        ('attribution', 'STORE_TAGS_CONCAT_ATTRIBUTION', 'STORE_CATEGORIES_CONCAT_ATTRIBUTION', 'STORE_NAMES_CONCAT_ATTRIBUTION'),
        ('funnel_store', 'STORE_TAGS_CONCAT_FUNNEL_STORE', 'STORE_CATEGORIES_CONCAT_FUNNEL_STORE', 'STORE_NAMES_CONCAT_FUNNEL_STORE'),
    ]
    
    # Sort by user, device, date, session_num to ensure temporal ordering
    df_pd = df_pd.sort_values(['USER_ID', 'DD_DEVICE_ID', 'EVENT_DATE', 'SESSION_NUM']).reset_index(drop=True)
    
    # Process each store type
    for store_type, tags_col, categories_col, names_col in store_types:
        print(f"\n  📊 Processing {store_type.upper()} embeddings...")
        
        # Combine store text features into a single text representation
        df_pd[f'STORE_TEXT_{store_type.upper()}'] = (
            df_pd[tags_col].fillna('') + ' | ' +
            df_pd[categories_col].fillna('') + ' | ' +
            df_pd[names_col].fillna('')
        )
        
        # Replace empty strings with a placeholder
        df_pd[f'STORE_TEXT_{store_type.upper()}'] = df_pd[f'STORE_TEXT_{store_type.upper()}'].replace('', 'no_store_info')
        df_pd[f'STORE_TEXT_{store_type.upper()}'] = df_pd[f'STORE_TEXT_{store_type.upper()}'].replace(' |  | ', 'no_store_info')
        
        print(f"    Generating embeddings for {len(df_pd):,} sessions...")
        
        # Generate embeddings in batches
        batch_size = 1000
        embeddings_list = []
        
        for i in range(0, len(df_pd), batch_size):
            batch = df_pd[f'STORE_TEXT_{store_type.upper()}'].iloc[i:i+batch_size].tolist()
            batch_embeddings = model.encode(batch, show_progress_bar=False)
            embeddings_list.append(batch_embeddings)
            
            if (i // batch_size) % 10 == 0:
                print(f"      Processed {i:,} / {len(df_pd):,} sessions...")
        
        embeddings = np.vstack(embeddings_list)
        print(f"    ✅ Generated embeddings with shape: {embeddings.shape}")
        
        # Calculate similarity to previous session
        print(f"    🔄 Calculating session-to-session similarity...")
        
        # Initialize similarity feature (keep as NaN for missing)
        df_pd[f'STORE_SIMILARITY_{store_type.upper()}_PREV'] = np.nan
        
        # Calculate similarity within each user-device combination
        processed_groups = 0
        for (user_id, device_id), group_df in df_pd.groupby(['USER_ID', 'DD_DEVICE_ID']):
            indices = group_df.index.tolist()
            
            if len(indices) > 1:
                for i in range(1, len(indices)):
                    curr_idx = indices[i]
                    prev_idx = indices[i-1]
                    
                    # Calculate cosine similarity between current and previous embedding
                    similarity = cosine_similarity(
                        embeddings[curr_idx].reshape(1, -1),
                        embeddings[prev_idx].reshape(1, -1)
                    )[0, 0]
                    
                    df_pd.loc[curr_idx, f'STORE_SIMILARITY_{store_type.upper()}_PREV'] = similarity
            
            processed_groups += 1
            if processed_groups % 10000 == 0:
                print(f"      Processed {processed_groups:,} user-device groups...")
        
        # Count non-null similarities
        non_null_count = df_pd[f'STORE_SIMILARITY_{store_type.upper()}_PREV'].notna().sum()
        mean_sim = df_pd[f'STORE_SIMILARITY_{store_type.upper()}_PREV'].mean()
        
        print(f"    ✅ Calculated similarity for {processed_groups:,} user-device groups")
        print(f"      - Sessions with previous session: {non_null_count:,}")
        print(f"      - Mean similarity (when prev exists): {mean_sim:.3f}")
    
    # Add flag for has previous session
    df_pd['HAS_PREVIOUS_SESSION'] = df_pd['STORE_SIMILARITY_IMPRESSION_PREV'].notna().astype(int)
    
    print(f"\n  ✅ Created 3 similarity features for each session")
    
    return df_pd

# Convert Spark DataFrame to Pandas for embedding calculation
print("Converting to Pandas for embedding calculation...")
df_pd = df_spark.toPandas()

# Calculate embeddings and similarity
df_pd = create_store_embeddings(df_pd)

# Convert back to Spark DataFrame
print("\nConverting back to Spark DataFrame...")
df_spark_with_embeddings = spark.createDataFrame(df_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save DataFrame with Embeddings to Snowflake

# COMMAND ----------

# Persist df_spark_with_embeddings to Snowflake
embeddings_table = "session_features_with_store_embeddings"

print(f"\n💾 Saving DataFrame with embeddings to Snowflake...")
print(f"   Table: proddb.fionafan.{embeddings_table}")
print(f"   Rows: {df_spark_with_embeddings.count():,}")

# Handle NullType columns (Snowflake can't save them)
# Either drop them or cast to StringType
print(f"   🔍 Checking for NullType columns...")
df_to_save = df_spark_with_embeddings
null_type_cols = []

for field in df_to_save.schema.fields:
    if isinstance(field.dataType, T.NullType):
        null_type_cols.append(field.name)

if null_type_cols:
    print(f"   ⚠️  Found {len(null_type_cols)} NullType columns: {null_type_cols}")
    print(f"   🔄 Casting to StringType...")
    
    for col_name in null_type_cols:
        df_to_save = df_to_save.withColumn(col_name, F.col(col_name).cast(T.StringType()))
    
    print(f"   ✅ Fixed NullType columns")

df_to_save.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{embeddings_table}") \
    .mode("overwrite") \
    .save()

print(f"   ✅ Successfully saved to proddb.fionafan.{embeddings_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter Out First Sessions for Modeling

# COMMAND ----------

# Now filter out first_session type (they won't have previous session similarity anyway)
print("\nFiltering out first_session type (no previous session for comparison)...")
df_spark = df_spark_with_embeddings.filter(F.col("session_type") != "first_session")

session_count_filtered = df_spark.count()
conversion_rate_filtered = df_spark.agg(F.avg("funnel_converted_bool")).collect()[0][0]

print(f"  ✅ Filtered sessions: {session_count_filtered:,}")
print(f"  ✅ Conversion rate: {conversion_rate_filtered:.2%}")
print(f"  ✅ Removed {session_count - session_count_filtered:,} first sessions")

# Cache for reuse
df_spark.cache()

# COMMAND ----------

df_spark.select('funnel_converted_bool')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Features

# COMMAND ----------

# Define specific feature list (curated, no data leakage)
all_covariates = [
    # Temporal features
    'days_since_onboarding',
    'days_since_onboarding_mod7',
    'session_day_of_week',
    'onboarding_day_of_week',
    
    # Session duration
    'session_duration_seconds',
    
    # Impression features
    'impression_unique_stores',
    'impression_unique_features',
    'impression_to_attribution_rate',
    
    # Action features
    'action_num_backgrounds',
    'action_num_foregrounds',
    'action_num_tab_switches',
    'action_num_address_actions',
    'action_num_auth_actions',
    'action_had_background',
    'action_had_foreground',
    'action_had_tab_switch',
    'action_had_address',
    'action_had_auth',
    
    # Attribution features
    'attribution_event_count',
    'attribution_num_card_clicks',
    'attribution_unique_stores_clicked',
    'attribution_to_add_rate',
    'attribution_cnt_traditional_carousel',
    'attribution_cnt_core_search',
    'attribution_cnt_pill_filter',
    'attribution_cnt_cuisine_filter',
    'attribution_cnt_home_feed',
    'attribution_cnt_banner',
    'attribution_cnt_offers',
    'attribution_cnt_tab_explore',
    'attribution_cnt_tab_me',
    
    # Error features
    'error_event_count',
    
    # Funnel features (progression only, no conversion outcomes)
    'funnel_num_adds',
    'funnel_num_store_page',
    'funnel_num_cart',
    'funnel_num_checkout',
    'funnel_add_per_attribution_click',
    
    # Timing features
    'timing_mean_inter_event_seconds',
    'timing_median_inter_event_seconds',
    'timing_std_inter_event_seconds',
    'timing_min_inter_event_seconds',
    'timing_max_inter_event_seconds',
    'funnel_seconds_to_first_add',
    'funnel_seconds_to_store_page',
    'funnel_seconds_to_cart',
    'funnel_seconds_to_checkout',
    'attribution_seconds_to_first_card_click',
    'attribution_first_click_from_search',
    'action_seconds_to_first_tab_switch',
    'action_seconds_to_first_foreground',
    'timing_events_first_30s',
    'timing_events_first_2min',
    
    # Sequence features
    'sequence_impression_to_attribution',
    'sequence_attribution_to_funnel_store',
    'sequence_funnel_store_to_action_add',
    
    # Store features
    'store_nv_impression_occurred',
    'store_nv_impression_position',
    
    # Store embedding similarity features (NEW - 3 types)
    'store_similarity_impression_prev',      # Similarity for stores user SAW
    'store_similarity_attribution_prev',     # Similarity for stores user CLICKED
    'store_similarity_funnel_store_prev',    # Similarity for stores user VISITED
]

print(f"\n📊 Feature Selection:")
print(f"  - ✅ Total features defined: {len(all_covariates)}")

# Check which features are available in the dataset (case-insensitive)
# Snowflake returns column names in uppercase, so we need to match case-insensitively
df_columns_lower = [c.lower() for c in df_spark.columns]
column_name_map = {c.lower(): c for c in df_spark.columns}  # Map lowercase to actual column name

available_features = []
missing_features = []

for feat in all_covariates:
    feat_lower = feat.lower()
    if feat_lower in df_columns_lower:
        # Use the actual column name from Snowflake (uppercase)
        available_features.append(column_name_map[feat_lower])
    else:
        missing_features.append(feat)

if missing_features:
    print(f"\n  ⚠️  Warning: {len(missing_features)} features not found in data:")
    for feat in missing_features:
        print(f"      - {feat}")

print(f"\n  ✅ Available for modeling: {len(available_features)} features")

# Use only available features
all_covariates = available_features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Feature Sets

# COMMAND ----------

# Pre-funnel features: exclude all funnel_* features (case-insensitive)
pre_funnel_covariates = [c for c in all_covariates if not c.lower().startswith('funnel_')]

# Funnel-inclusive features: use all features (already curated to exclude target-related)
funnel_inclusive_covariates = all_covariates.copy()

print(f"\n📋 Feature Sets Defined:")
print(f"  - Pre-Funnel Model: {len(pre_funnel_covariates)} features")
print(f"  - Funnel-Inclusive Model: {len(funnel_inclusive_covariates)} features")

# Show breakdown by category (case-insensitive)
print(f"\n🔍 Pre-Funnel Feature Categories:")
temporal = [f for f in pre_funnel_covariates if any(x in f.lower() for x in ['days', 'day_of_week'])]
impression = [f for f in pre_funnel_covariates if 'impression' in f.lower()]
action = [f for f in pre_funnel_covariates if 'action' in f.lower()]
attribution = [f for f in pre_funnel_covariates if 'attribution' in f.lower()]
timing = [f for f in pre_funnel_covariates if 'timing' in f.lower() or 'seconds' in f.lower()]
sequence = [f for f in pre_funnel_covariates if 'sequence' in f.lower()]
store = [f for f in pre_funnel_covariates if 'store' in f.lower()]
other = [f for f in pre_funnel_covariates if not any(x in f.lower() for x in ['days', 'day_of_week', 'impression', 'action', 'attribution', 'timing', 'seconds', 'sequence', 'store'])]

print(f"  - Temporal: {len(temporal)}")
print(f"  - Impression: {len(impression)}")
print(f"  - Action: {len(action)}")
print(f"  - Attribution: {len(attribution)}")
print(f"  - Timing: {len(timing)}")
print(f"  - Sequence: {len(sequence)}")
print(f"  - Store: {len(store)}")
if other:
    print(f"  - Other: {len(other)}")

print(f"\n🔍 Funnel-Inclusive Additional Features:")
funnel_features = [f for f in funnel_inclusive_covariates if f.lower().startswith('funnel_')]
print(f"  - Funnel progression: {len(funnel_features)}")
for i, feat in enumerate(funnel_features, 1):
    print(f"    {i}. {feat}")

# COMMAND ----------

# MAGIC %md
# MAGIC - ## Feature Correlation Analysis

# COMMAND ----------

df_spark.select('funnel_converted_bool').show()

# COMMAND ----------


# Calculate correlation matrix for features and target
print("\n📊 Calculating feature correlations...")

# Find target column case-insensitively directly from df_spark
target_search_lower = 'funnel_converted_bool'
target_actual = None
for col in df_spark.columns:
    if col.lower() == target_search_lower:
        target_actual = col
        break

if target_actual is None:
    print(f"  ❌ Warning: Target column '{target_search_lower}' not found in data")
    print(f"     Available columns: {df_spark.columns[:10]}...")
    target_corr = None
else:
    print(f"  ✅ Found target column: '{target_actual}'")
    target_col = target_actual  # Update target_col for consistency
    correlation_features = funnel_inclusive_covariates + [target_col]
    
    # Convert to Pandas for correlation calculation (use sample if too large)
    # Cast all features to double first to ensure they're numeric
    correlation_features_typed = []
    for feat in correlation_features:
        correlation_features_typed.append(F.col(feat).cast(T.DoubleType()).alias(feat))
    
    if df_spark.count() > 50000:
        df_corr_sample = df_spark.select(*correlation_features_typed).sample(fraction=50000/df_spark.count(), seed=42).toPandas()
        print(f"  Using sample of {len(df_corr_sample):,} sessions for correlation analysis")
    else:
        df_corr_sample = df_spark.select(*correlation_features_typed).toPandas()
        print(f"  Using all {len(df_corr_sample):,} sessions for correlation analysis")
    
    print(f"  🔍 Pandas columns in correlation sample: {len(df_corr_sample.columns)}")
    print(f"  🔍 First 10 columns: {list(df_corr_sample.columns)[:10]}")
    
    # Calculate correlation matrix
    corr_matrix = df_corr_sample.corr()
    
    # Get correlations with target (check if target is in correlation matrix)
    if target_col in corr_matrix.columns:
        target_corr = corr_matrix[target_col].sort_values(ascending=False)
    else:
        # Try to find it case-insensitively
        corr_cols_lower = {c.lower(): c for c in corr_matrix.columns}
        target_col_in_corr = corr_cols_lower.get('funnel_converted_bool', None)
        if target_col_in_corr:
            target_corr = corr_matrix[target_col_in_corr].sort_values(ascending=False)
            target_col = target_col_in_corr  # Update to use the correct name
        else:
            print(f"  ❌ Target not found in correlation matrix")
            print(f"     Correlation columns: {list(corr_matrix.columns)[:10]}...")
            target_corr = None
    
    if target_corr is not None:
        print(f"\n🔝 Top 20 Features Correlated with Target ({target_col}):")
        print(f"{'Rank':<6} {'Feature':<50} {'Correlation':<12}")
        print("-" * 68)
        
        for rank, (feat, corr_val) in enumerate(target_corr.head(21).items(), 0):
            if feat != target_col:  # Skip target itself
                print(f"{rank:<6} {feat:<50} {corr_val:>11.4f}")

# COMMAND ----------

corr_matrix.to_csv('correlation.csv')

# COMMAND ----------


if target_corr is not None:
    # Plot 1: Full Correlation Heatmap (top 30 features)
    print("\n📈 Generating correlation heatmap...")
    
    # Select top 30 features by absolute correlation with target
    top_features_by_corr = target_corr.abs().sort_values(ascending=False).head(31).index.tolist()
    if target_col in top_features_by_corr:
        top_features_by_corr.remove(target_col)
    top_features_by_corr = top_features_by_corr[:30]
    
    # Get correlation submatrix
    corr_subset = corr_matrix.loc[top_features_by_corr, top_features_by_corr]
    
    # Plot
    fig, ax = plt.subplots(figsize=(16, 14))
    
    sns.heatmap(
        corr_subset,
        annot=False,
        cmap='RdBu_r',
        center=0,
        vmin=-1,
        vmax=1,
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.8, "label": "Correlation"},
        ax=ax
    )
    
    ax.set_title('Feature Correlation Heatmap (Top 30 Features by Target Correlation)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    plt.xticks(rotation=90, ha='right', fontsize=8)
    plt.yticks(rotation=0, fontsize=8)
    plt.tight_layout()
    display(fig)


# COMMAND ----------

# Plot 2: Feature-Target Correlation Bar Chart
print("\n📊 Feature-Target Correlations...")

fig, ax = plt.subplots(figsize=(14, 12))

# Get top 30 features by absolute correlation
top_30_corr = target_corr.drop(target_col).abs().sort_values(ascending=False).head(30)
top_30_features = top_30_corr.index.tolist()
top_30_values = target_corr[top_30_features].values

# Create colors based on sign
colors = ['#d62728' if x < 0 else '#2ca02c' for x in top_30_values]

# Horizontal bar plot
y_pos = np.arange(len(top_30_features))
ax.barh(y_pos, top_30_values, color=colors, alpha=0.7)

# Customize
ax.set_yticks(y_pos)
ax.set_yticklabels(top_30_features, fontsize=9)
ax.invert_yaxis()
ax.set_xlabel('Correlation with Target (funnel_converted_bool)', fontsize=11, fontweight='bold')
ax.set_title('Top 30 Features by Correlation with Conversion', fontsize=13, fontweight='bold')
ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
ax.grid(axis='x', alpha=0.3)

# Add value labels
for i, value in enumerate(top_30_values):
    x_pos = value + (0.01 if value > 0 else -0.01)
    ha = 'left' if value > 0 else 'right'
    ax.text(x_pos, i, f'{value:+.3f}', va='center', ha=ha, fontsize=8)

plt.tight_layout()
display(fig)

print(f"\n✅ Correlation analysis complete")
print(f"  - Highest positive correlation: {target_corr.drop(target_col).max():.4f}")
print(f"  - Highest negative correlation: {target_corr.drop(target_col).min():.4f}")

# COMMAND ----------

# Plot 3: High Multicollinearity Detection
print("\n⚠️  Detecting high multicollinearity (|correlation| > 0.8)...")

# Find pairs with high correlation (excluding diagonal)
high_corr_pairs = []
for i in range(len(corr_subset.columns)):
    for j in range(i+1, len(corr_subset.columns)):
        corr_val = corr_subset.iloc[i, j]
        if abs(corr_val) > 0.8:
            high_corr_pairs.append({
                'feature_1': corr_subset.columns[i],
                'feature_2': corr_subset.columns[j],
                'correlation': corr_val
            })

if high_corr_pairs:
    high_corr_df = pd.DataFrame(high_corr_pairs).sort_values('correlation', ascending=False, key=abs)
    print(f"\n  ⚠️  Found {len(high_corr_df)} feature pairs with |correlation| > 0.8:")
    print(f"\n  {'Feature 1':<50} {'Feature 2':<50} {'Correlation':<12}")
    print("  " + "-" * 112)
    for _, row in high_corr_df.head(20).iterrows():
        print(f"  {row['feature_1']:<50} {row['feature_2']:<50} {row['correlation']:>11.4f}")
    
    if len(high_corr_df) > 20:
        print(f"\n  ... and {len(high_corr_df) - 20} more pairs")
else:
    print(f"\n  ✅ No severe multicollinearity detected (all |correlations| < 0.8)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Model DataFrame

# COMMAND ----------

def prepare_model_dataframe(
    spark: SparkSession, 
    sdf_all: SparkDataFrame, 
    covariates: List[str], 
    target: str
) -> Tuple[SparkDataFrame, List[str]]:
    """Prepare dataframe for modeling with feature engineering and validation"""
    cols = list(set(covariates + [target]))
    sdf = sdf_all.select(*cols)
    
    # Determine zero-variance columns via variance aggregation
    if covariates:
        agg_exprs = [F.variance(F.col(c)).alias(c) for c in covariates]
        var_row = sdf.agg(*agg_exprs).collect()[0]
    else:
        var_row = {}
    
    usable_covars: List[str] = []
    zero_var_features = []
    
    for c in covariates:
        v = var_row[c] if isinstance(var_row, dict) else getattr(var_row, c)
        try:
            if v is not None and float(v) > 0.0:
                usable_covars.append(c)
            else:
                zero_var_features.append(c)
        except Exception:
            continue
    
    if zero_var_features:
        print(f"  ⚠️  Excluding {len(zero_var_features)} zero-variance features")
    
    if not usable_covars:
        raise ValueError("No usable covariates after preprocessing (all zero-variance or invalid).")
    
    # Cast covariates to double and impute nulls with 0.0
    for c in usable_covars:
        field = next((f for f in sdf.schema.fields if f.name == c), None)
        if isinstance(field.dataType, T.BooleanType):
            col_double = F.when(F.col(c), F.lit(1.0)).otherwise(F.lit(0.0))
        else:
            col_double = F.col(c).cast(T.DoubleType())
        sdf = sdf.withColumn(c, F.coalesce(col_double, F.lit(0.0)))
    
    # Prepare label column (double, nulls to 0.0)
    label_double = F.coalesce(F.col(target).cast(T.DoubleType()), F.lit(0.0))
    sdf = sdf.withColumn("label", label_double)
    
    print(f"  ✅ Prepared {len(usable_covars)} features for modeling")
    
    return sdf.select(*(usable_covars + ["label"])), usable_covars

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Logistic Regression Models

# COMMAND ----------

def fit_logistic_model_spark(
    spark: SparkSession,
    sdf_all: SparkDataFrame,
    covariates: List[str],
    target: str,
    max_iter: int,
    reg_param: float,
    elastic_net: float,
    model_name: str,
) -> pd.DataFrame:
    """Fit a logistic regression model using PySpark"""
    print(f"\n{'='*80}")
    print(f"Training {model_name}")
    print(f"{'='*80}")
    
    sdf, usable_covars = prepare_model_dataframe(spark, sdf_all, covariates, target)
    
    # Drop rows where label is null
    sdf = sdf.filter(F.col("label").isNotNull())
    n_obs = sdf.count()
    
    if n_obs == 0:
        print(f"  ⚠️  No observations for {model_name}")
        return pd.DataFrame({
            "model_name": [model_name],
            "covariate": ["<no_data>"],
            "coefficient": [np.nan],
            "odds_ratio_1sd": [np.nan],
            "pct_impact_1sd": [np.nan],
            "n_obs": [0],
            "converged": [False],
            "target": [target],
        })
    
    print(f"  📊 Dataset: {n_obs:,} observations")
    print(f"  📊 Features: {len(usable_covars)}")
    
    # Split data for train/test
    train_df, test_df = sdf.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    
    print(f"  📊 Train: {train_count:,} | Test: {test_count:,}")
    
    # Assemble features
    assembler = VectorAssembler(inputCols=usable_covars, outputCol="features", handleInvalid="keep")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=max_iter,
        regParam=reg_param,
        elasticNetParam=elastic_net,
        standardization=True,
        fitIntercept=True,
    )
    pipeline = Pipeline(stages=[assembler, lr])
    
    try:
        # Fit model
        print(f"  🔄 Training model...")
        model = pipeline.fit(train_df)
        lr_model = model.stages[-1]
        coeffs = lr_model.coefficients.toArray().tolist()
        intercept = float(lr_model.intercept)
        
        # Evaluate on test set
        predictions = model.transform(test_df)
        
        # Calculate metrics
        evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
        test_auc = evaluator.evaluate(predictions)
        
        # Convergence check
        try:
            total_iter = int(lr_model.summary.totalIterations)
        except Exception:
            total_iter = max_iter
        converged = total_iter < max_iter
        
        print(f"  ✅ Test AUC-ROC: {test_auc:.4f}")
        print(f"  ✅ Converged: {converged} (iterations: {total_iter}/{max_iter})")
        
        # Build output rows
        rows = []
        rows.append({
            "model_name": model_name,
            "covariate": "const",
            "coefficient": intercept,
            "odds_ratio_1sd": np.exp(intercept),
            "pct_impact_1sd": (np.exp(intercept) - 1) * 100,
            "n_obs": n_obs,
            "test_count": test_count,
            "test_auc": test_auc,
            "converged": converged,
            "iterations": total_iter,
            "target": target,
        })
        
        for name, beta in zip(usable_covars, coeffs):
            odds_ratio = np.exp(beta)
            pct_impact = (odds_ratio - 1) * 100
            
            rows.append({
                "model_name": model_name,
                "covariate": name,
                "coefficient": float(beta),
                "odds_ratio_1sd": odds_ratio,
                "pct_impact_1sd": pct_impact,
                "n_obs": n_obs,
                "test_count": test_count,
                "test_auc": test_auc,
                "converged": converged,
                "iterations": total_iter,
                "target": target,
            })
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"  ❌ Failed to fit {model_name}: {e}")
        return pd.DataFrame({
            "model_name": [model_name],
            "covariate": ["<model_failed>"],
            "coefficient": [np.nan],
            "odds_ratio_1sd": [np.nan],
            "pct_impact_1sd": [np.nan],
            "n_obs": [n_obs],
            "test_count": [0],
            "test_auc": [np.nan],
            "converged": [False],
            "iterations": [0],
            "target": [target],
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Pre-Funnel Model

# COMMAND ----------

target = "funnel_converted_bool"
max_iter = 1000
reg_param = 1.0
elastic_net = 0.0

# Model 1: Pre-Funnel Features
res_pre = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    covariates=pre_funnel_covariates,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    elastic_net=elastic_net,
    model_name="Pre-Funnel Model",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Funnel-Inclusive Model

# COMMAND ----------

# Model 2: Funnel-Inclusive Features
res_full = fit_logistic_model_spark(
    spark=spark,
    sdf_all=df_spark,
    covariates=funnel_inclusive_covariates,
    target=target,
    max_iter=max_iter,
    reg_param=reg_param,
    elastic_net=elastic_net,
    model_name="Funnel-Inclusive Model",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Analyze Results

# COMMAND ----------

# Combine results
results_df = pd.concat([res_pre, res_full], ignore_index=True)

# Reorder columns
col_order = [
    "model_name",
    "target",
    "covariate",
    "coefficient",
    "odds_ratio_1sd",
    "pct_impact_1sd",
    "n_obs",
    "test_count",
    "test_auc",
    "converged",
    "iterations",
]
for c in col_order:
    if c not in results_df.columns:
        results_df[c] = np.nan
results_df = results_df[col_order]

# Add timestamp
results_df['created_at'] = datetime.now()

print(f"\n📊 Total results: {len(results_df)} rows")
print(f"   - Pre-Funnel Model: {len(res_pre)} coefficients")
print(f"   - Funnel-Inclusive Model: {len(res_full)} coefficients")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Features by Model

# COMMAND ----------

# Print top features for each model
for model_name in ["Pre-Funnel Model", "Funnel-Inclusive Model"]:
    model_results = results_df[results_df['model_name'] == model_name].copy()
    model_results = model_results[model_results['covariate'] != 'const']
    model_results['abs_pct_impact'] = model_results['pct_impact_1sd'].abs()
    model_results = model_results.sort_values('abs_pct_impact', ascending=False)
    
    print(f"\n{'='*100}")
    print(f"{model_name} - Top 20 Features by Impact")
    print(f"{'='*100}")
    print(f"{'Rank':<6} {'Feature':<50} {'Coef':<10} {'Odds Ratio':<12} {'% Impact':<12}")
    print("-" * 100)
    
    for rank, (idx, row) in enumerate(model_results.head(20).iterrows(), 1):
        print(f"{rank:<6} {row['covariate']:<50} {row['coefficient']:>9.4f} "
              f"{row['odds_ratio_1sd']:>11.4f} {row['pct_impact_1sd']:>11.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Performance Comparison

# COMMAND ----------

# Compare model performance
print(f"\n{'='*80}")
print("MODEL PERFORMANCE COMPARISON")
print(f"{'='*80}")
print(f"\n{'Metric':<30} {'Pre-Funnel':<15} {'Funnel-Inclusive':<15}")
print("-" * 60)

pre_metrics = res_pre[res_pre['covariate'] == 'const'].iloc[0]
full_metrics = res_full[res_full['covariate'] == 'const'].iloc[0]

print(f"{'Test AUC-ROC':<30} {pre_metrics['test_auc']:<15.4f} {full_metrics['test_auc']:<15.4f}")
print(f"{'Converged':<30} {str(pre_metrics['converged']):<15} {str(full_metrics['converged']):<15}")
print(f"{'Iterations':<30} {pre_metrics['iterations']:<15.0f} {full_metrics['iterations']:<15.0f}")
print(f"{'N Features':<30} {len(res_pre)-1:<15.0f} {len(res_full)-1:<15.0f}")
print(f"{'N Observations':<30} {pre_metrics['n_obs']:<15,.0f} {full_metrics['n_obs']:<15,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Top Features

# COMMAND ----------

# Plot top 15 features for each model
fig, axes = plt.subplots(1, 2, figsize=(20, 10))

for idx, model_name in enumerate(["Pre-Funnel Model", "Funnel-Inclusive Model"]):
    ax = axes[idx]
    
    model_results = results_df[results_df['model_name'] == model_name].copy()
    model_results = model_results[model_results['covariate'] != 'const']
    model_results['abs_pct_impact'] = model_results['pct_impact_1sd'].abs()
    model_results = model_results.sort_values('abs_pct_impact', ascending=False).head(15)
    
    # Create color based on sign
    colors = ['#d62728' if x < 0 else '#2ca02c' for x in model_results['pct_impact_1sd']]
    
    # Horizontal bar plot
    y_pos = np.arange(len(model_results))
    ax.barh(y_pos, model_results['pct_impact_1sd'], color=colors, alpha=0.7)
    
    # Customize
    ax.set_yticks(y_pos)
    ax.set_yticklabels(model_results['covariate'], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('% Change in Odds (for 1 SD increase)', fontsize=11, fontweight='bold')
    ax.set_title(f'{model_name}\nTop 15 Features by Impact', 
                fontsize=12, fontweight='bold')
    ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (idx_val, row) in enumerate(model_results.iterrows()):
        value = row['pct_impact_1sd']
        x_pos = value + (5 if value > 0 else -5)
        ha = 'left' if value > 0 else 'right'
        ax.text(x_pos, i, f'{value:+.1f}%', va='center', ha=ha, fontsize=8)

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

# Convert to Spark DataFrame
results_spark = spark.createDataFrame(results_df)

# Save to Snowflake
output_table = "session_conversion_model_results_databricks"

results_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_table}") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {len(results_df)} rows to proddb.fionafan.{output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Complete

# COMMAND ----------

print("\n" + "="*80)
print("ANALYSIS COMPLETE!")
print("="*80)
print(f"\n📊 Results Summary:")
print(f"   - Total sessions analyzed: {session_count:,}")
print(f"   - Conversion rate: {conversion_rate:.2%}")
print(f"   - Pre-Funnel Model AUC: {pre_metrics['test_auc']:.4f}")
print(f"   - Funnel-Inclusive Model AUC: {full_metrics['test_auc']:.4f}")
print(f"\n💾 Results saved to:")
print(f"   - Table: proddb.fionafan.{output_table}")
print(f"\n🎯 Next Steps:")
print(f"   1. Review top features to understand conversion drivers")
print(f"   2. Compare with previous model results")
print(f"   3. Validate no data leakage in top features")

# COMMAND ----------


