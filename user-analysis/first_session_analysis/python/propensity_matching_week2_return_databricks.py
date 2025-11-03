# Databricks notebook source
# MAGIC %md
# MAGIC # Propensity Score Matching: Week 2 Return Impact Analysis
# MAGIC
# MAGIC **Goal:** Find "identical twins" based on first session behavior - one who returned in week 2, one who didn't.
# MAGIC Then compare their outcomes in terms of:
# MAGIC - 1111 cohort membership
# MAGIC - Total order volume
# MAGIC - Order conversion rates
# MAGIC
# MAGIC **Method:**
# MAGIC 1. Build propensity score model to predict week 2 return based on first session features
# MAGIC 2. Match treatment (returned week 2) with control (didn't return) based on similar propensity scores
# MAGIC 3. Calculate week 1 order metrics from july_cohort_deliveries_28d
# MAGIC 4. Compare outcomes between matched groups

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

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
# MAGIC ## Define First Session Features for Matching

# COMMAND ----------

MATCHING_FEATURES = [
    # First session features
    'ONBOARDING_DAY_OF_WEEK',
    'NUM_UNIQUE_STORES',
    'IMPRESSION_UNIQUE_STORES',
    'ACTION_HAD_FOREGROUND',
    'ACTION_HAD_ADDRESS',
    'ACTION_HAD_AUTH',
    'ATTRIBUTION_CNT_TRADITIONAL_CAROUSEL',
    'ATTRIBUTION_CNT_CORE_SEARCH',
    'ATTRIBUTION_CNT_PILL_FILTER',
    'ATTRIBUTION_CNT_CUISINE_FILTER',
    'ATTRIBUTION_CNT_DOUBLEDASH',
    'ATTRIBUTION_CNT_HOME_FEED',
    'ATTRIBUTION_CNT_BANNER',
    'ATTRIBUTION_CNT_OFFERS',
    'ATTRIBUTION_CNT_TAB_EXPLORE',
    'ATTRIBUTION_CNT_TAB_ME',
    'ATTRIBUTION_ENTROPY',
    'ERROR_HAD_ANY',
    'FUNNEL_HAD_ADD',
    'FUNNEL_REACHED_STORE_BOOL',
    'FUNNEL_REACHED_CART_BOOL',
    'FUNNEL_REACHED_CHECKOUT_BOOL',
    'FUNNEL_CONVERTED_BOOL',
    'FUNNEL_SECONDS_TO_FIRST_ADD',
    'FUNNEL_SECONDS_TO_FIRST_ORDER',
    'FUNNEL_SECONDS_TO_STORE_PAGE',
    'FUNNEL_SECONDS_TO_CART',
    'FUNNEL_SECONDS_TO_CHECKOUT',
    'FUNNEL_SECONDS_TO_SUCCESS',
    'STORE_NV_IMPRESSION_OCCURRED',
    
    # Week 1 order performance features (NEW)
    'WEEK1_HAD_ORDER',
    'WEEK1_ORDER_COUNT',
    'WEEK1_DAYS_TO_FIRST_ORDER',
]

print(f"📊 Using {len(MATCHING_FEATURES)} features for propensity matching")
print(f"   - First session features: 30")
print(f"   - Week 1 order features: 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load First Session Data with Week 2 Return and 1111 Cohort

# COMMAND ----------


# Build feature list for query (exclude week 1 features which are calculated separately)
first_session_features = [f for f in MATCHING_FEATURES 
                          if not f.startswith('WEEK1_')]
feature_cols = ", ".join([f"fs.{feat}" for feat in first_session_features])

query = f"""
WITH cohort_1111 AS (
    SELECT DISTINCT consumer_id
    FROM proddb.fionafan.users_activity_1111
),
week1_orders AS (
    SELECT 
        consumer_id,
        COUNT(DISTINCT delivery_id) as week1_order_count,
        MIN(days_since_onboarding) as week1_days_to_first_order
    FROM proddb.fionafan.july_cohort_deliveries_28d
    WHERE days_since_onboarding BETWEEN 0 AND 6
    GROUP BY consumer_id
)
SELECT 
    fs.user_id,
    fs.dd_device_id,
    fs.cohort_type,
    fs.onboarding_day,
    {feature_cols},
    CASE WHEN e.sessions_day_8_14 > 0 THEN 1 ELSE 0 END as returned_week_2,
    CASE WHEN c.consumer_id IS NOT NULL THEN 1 ELSE 0 END as is_1111_cohort,
    CASE WHEN w.week1_order_count > 0 THEN 1 ELSE 0 END as week1_had_order,
    COALESCE(w.week1_order_count, 0) as week1_order_count,
    COALESCE(w.week1_days_to_first_order, 7) as week1_days_to_first_order
FROM proddb.fionafan.all_user_sessions_with_events_features_gen fs
LEFT JOIN proddb.fionafan.all_user_sessions_enriched e 
    ON fs.user_id = e.consumer_id
LEFT JOIN cohort_1111 c 
    ON fs.user_id = c.consumer_id
LEFT JOIN week1_orders w
    ON fs.user_id = w.consumer_id
WHERE fs.session_type = 'first_session'
    AND fs.cohort_type IS NOT NULL
    AND fs.user_id IS NOT NULL
"""

print("📊 Loading first session data with week 1 order features...")

df_first_session = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", query)
    .load()
)

# Cache for reuse
df_first_session.cache()

total_count = df_first_session.count()
returned_week_2 = df_first_session.filter(F.col("returned_week_2") == 1).count()
not_returned = df_first_session.filter(F.col("returned_week_2") == 0).count()
week1_ordered = df_first_session.filter(F.col("week1_had_order") == 1).count()

print(f"\n✅ Loaded {total_count:,} first sessions")
print(f"   - Returned week 2: {returned_week_2:,} ({100*returned_week_2/total_count:.1f}%)")
print(f"   - Did not return week 2: {not_returned:,} ({100*not_returned/total_count:.1f}%)")
print(f"   - Ordered in week 1: {week1_ordered:,} ({100*week1_ordered/total_count:.1f}%)")

# Show week 1 order stats by cohort
print(f"\n📊 Week 1 Order Performance by Cohort:")
week1_stats = df_first_session.groupBy("cohort_type").agg(
    F.count("*").alias("n_users"),
    F.sum("week1_had_order").alias("n_ordered_week1"),
    F.avg("week1_order_count").alias("avg_orders_week1")
).collect()

for row in week1_stats:
    cohort = row['cohort_type']
    n_users = row['n_users']
    n_ordered = row['n_ordered_week1']
    avg_orders = row['avg_orders_week1']
    pct_ordered = 100 * n_ordered / n_users if n_users > 0 else 0
    print(f"   {cohort:20s}: {pct_ordered:5.1f}% ordered | {avg_orders:.2f} avg orders")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Propensity Score Model

# COMMAND ----------

print("🔄 Building propensity score model...")

# Prepare features - convert to uppercase for matching with Spark DataFrame columns
df_columns_upper = [c.upper() for c in df_first_session.columns]
column_name_map = {c.upper(): c for c in df_first_session.columns}

available_features = []
missing_features = []
for feat in MATCHING_FEATURES:
    feat_upper = feat.upper()
    if feat_upper in df_columns_upper:
        available_features.append(column_name_map[feat_upper])
    else:
        missing_features.append(feat)

print(f"  📊 Using {len(available_features)} available features")
if missing_features:
    print(f"  ⚠️  Missing {len(missing_features)} features: {missing_features[:5]}")

# Count feature types
first_session_count = sum(1 for f in available_features if not f.upper().startswith('WEEK1_'))
week1_count = sum(1 for f in available_features if f.upper().startswith('WEEK1_'))
print(f"      - First session features: {first_session_count}")
print(f"      - Week 1 order features: {week1_count}")

# Prepare DataFrame - fill nulls and cast to double
df_model = df_first_session.select(
    "user_id", 
    "cohort_type", 
    "onboarding_day",
    "returned_week_2", 
    "is_1111_cohort",
    *available_features
)

for feat in available_features:
    df_model = df_model.withColumn(
        feat, 
        F.coalesce(F.col(feat).cast(T.DoubleType()), F.lit(0.0))
    )

# Build propensity score model pipeline
assembler = VectorAssembler(
    inputCols=available_features, 
    outputCol="features_raw", 
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="features_raw", 
    outputCol="features", 
    withMean=True, 
    withStd=True
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="returned_week_2",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.0,
)

pipeline = Pipeline(stages=[assembler, scaler, lr])

# Train model
print("  🔄 Training propensity score model...")
propensity_model = pipeline.fit(df_model)

# Get propensity scores (probability of returning in week 2)
df_with_propensity = propensity_model.transform(df_model)

# Extract propensity score (probability of class 1)
# Need to use a UDF because probability is a Vector type
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

@udf(returnType=T.DoubleType())
def extract_prob_class1(probability):
    """Extract probability of class 1 from probability vector"""
    if probability is not None:
        return float(probability[1])
    return 0.0

df_with_propensity = df_with_propensity.withColumn(
    "propensity_score",
    extract_prob_class1(F.col("probability"))
)

df_with_propensity = df_with_propensity.select(
    "user_id",
    "cohort_type",
    "onboarding_day",
    "returned_week_2",
    "is_1111_cohort",
    "propensity_score"
)

print("  ✅ Propensity scores calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Propensity Score Matching

# COMMAND ----------

print("🔄 Performing propensity score matching (Nearest Neighbor 1:1)...")
print("   📌 Matching WITHIN each cohort type separately")
print("   📌 Sampling strategy:")
print("      - Sample 200K from TREATMENT group")
print("      - Use full CONTROL pool (up to 500K) for better match quality")

# Show distribution by cohort
cohort_dist = df_with_propensity.groupBy("cohort_type", "returned_week_2").count().collect()

print(f"\n  📊 Full sample sizes by cohort:")
for row in sorted(cohort_dist, key=lambda x: (x['cohort_type'], -x['returned_week_2'])):
    status = "Treatment" if row['returned_week_2'] == 1 else "Control"
    print(f"     {row['cohort_type']:20s} - {status:10s}: {row['count']:,}")

# Perform matching separately for each cohort
all_matched_pairs = []
SAMPLE_SIZE_PER_COHORT = 20000

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n  🔄 Matching {cohort} cohort...")
    
    # Filter for this cohort
    cohort_data_full = df_with_propensity.filter(F.col("cohort_type") == cohort)
    
    # Separate treatment and control FIRST
    treatment_full = cohort_data_full.filter(F.col("returned_week_2") == 1)
    control_full = cohort_data_full.filter(F.col("returned_week_2") == 0)
    
    treatment_full_count = treatment_full.count()
    control_full_count = control_full.count()
    
    print(f"     Full sizes - Treatment: {treatment_full_count:,} | Control: {control_full_count:,}")
    
    # Sample 200K from TREATMENT
    if treatment_full_count <= SAMPLE_SIZE_PER_COHORT:
        treatment = treatment_full
        print(f"     Using all {treatment_full_count:,} treatment users")
    else:
        treatment_fraction = SAMPLE_SIZE_PER_COHORT / treatment_full_count
        treatment = treatment_full.sample(withReplacement=False, fraction=treatment_fraction, seed=42)
        treatment_sampled = treatment.count()
        print(f"     Sampled {treatment_sampled:,} treatment users from {treatment_full_count:,}")
    
    # For CONTROL: use full pool (or sample if extremely large)
    # Cap control at 500K to keep cross join manageable
    MAX_CONTROL_POOL = 50000
    
    if control_full_count <= MAX_CONTROL_POOL:
        control = control_full
        print(f"     Using all {control_full_count:,} control users")
    else:
        control_fraction = MAX_CONTROL_POOL / control_full_count
        control = control_full.sample(withReplacement=False, fraction=control_fraction, seed=42)
        control_sampled = control.count()
        print(f"     Sampled {control_sampled:,} control users from {control_full_count:,}")
    
    treatment_count = treatment.count()
    control_count = control.count()
    
    print(f"     Treatment: {treatment_count:,} | Control pool: {control_count:,}")
    
    if treatment_count == 0 or control_count == 0:
        print(f"     ⚠️  Skipping {cohort} - insufficient data")
        continue
    
    # Use bucketing to reduce cross join size
    # Create 100 propensity score buckets (0.00-0.01, 0.01-0.02, etc.)
    NUM_BUCKETS = 100
    
    print(f"     🔄 Using bucketed matching ({NUM_BUCKETS} propensity score buckets)...")
    
    treatment = treatment.withColumn(
        "ps_bucket",
        F.floor(F.col("propensity_score") * NUM_BUCKETS)
    )
    
    control = control.withColumn(
        "ps_bucket",
        F.floor(F.col("propensity_score") * NUM_BUCKETS)
    )
    
    # Add unique IDs
    treatment = treatment.withColumn("treatment_id", F.monotonically_increasing_id())
    control = control.withColumn("control_id", F.monotonically_increasing_id())
    
    # Alias for join
    control_alias = control.alias("c")
    treatment_alias = treatment.alias("t")
    
    # Join only within same bucket or adjacent buckets (± 1)
    # This dramatically reduces cross join size
    matches = treatment_alias.join(
        control_alias,
        (F.col("t.ps_bucket") >= F.col("c.ps_bucket") - 1) &
        (F.col("t.ps_bucket") <= F.col("c.ps_bucket") + 1),
        "inner"
    )
    
    matches = matches.withColumn(
        "propensity_distance",
        F.abs(F.col("t.propensity_score") - F.col("c.propensity_score"))
    )
    
    # For each treatment, find the closest control
    window = Window.partitionBy("t.treatment_id").orderBy("propensity_distance")
    
    matches = matches.withColumn("rank", F.row_number().over(window))
    
    # Keep only the best match for each treatment
    best_matches = matches.filter(F.col("rank") == 1)
    
    print(f"     ✅ Bucketing complete - reduced join size significantly")
    
    # Select relevant columns and rename
    cohort_matched = best_matches.select(
        F.col("t.user_id").alias("treatment_user_id"),
        F.col("c.user_id").alias("control_user_id"),
        F.col("t.cohort_type").alias("cohort_type"),
        F.col("t.onboarding_day").alias("onboarding_day"),
        F.col("t.propensity_score").alias("treatment_propensity"),
        F.col("c.propensity_score").alias("control_propensity"),
        F.col("propensity_distance"),
        F.col("t.is_1111_cohort").alias("treatment_is_1111"),
        F.col("c.is_1111_cohort").alias("control_is_1111"),
    )
    
    n_cohort_matches = cohort_matched.count()
    avg_distance = cohort_matched.agg(F.avg('propensity_distance')).collect()[0][0]
    
    print(f"     ✅ Matched {n_cohort_matches:,} pairs | Avg distance: {avg_distance:.4f}")
    
    all_matched_pairs.append(cohort_matched)

# Combine all cohort matches
if all_matched_pairs:
    matched_pairs = all_matched_pairs[0]
    for df in all_matched_pairs[1:]:
        matched_pairs = matched_pairs.union(df)
    
    matched_pairs.cache()
    n_matches = matched_pairs.count()
    
    print(f"\n  ✅ Total matched pairs across all cohorts: {n_matches:,}")
    print(f"  📊 Overall average propensity distance: {matched_pairs.agg(F.avg('propensity_distance').alias('avg')).collect()[0]['avg']:.4f}")
else:
    print(f"\n  ❌ No matches created")
    matched_pairs = None

# COMMAND ----------

def calculate_smd(treatment_df, control_df, feature_cols):
    """
    Calculate Standardized Mean Difference for covariate balance check
    
    Args:
        treatment_df: Treatment group DataFrame
        control_df: Control group DataFrame
        feature_cols: List of feature column names
        
    Returns:
        DataFrame with SMD for each feature
    """
    smd_results = []
    
    for feat in feature_cols:
        # Get statistics for treatment
        t_stats = treatment_df.agg(
            F.avg(feat).alias('t_mean'),
            F.variance(feat).alias('t_var')
        ).collect()[0]
        
        # Get statistics for control
        c_stats = control_df.agg(
            F.avg(feat).alias('c_mean'),
            F.variance(feat).alias('c_var')
        ).collect()[0]
        
        # Convert to float (handles Decimal types from Spark)
        t_mean = float(t_stats['t_mean']) if t_stats['t_mean'] is not None else 0.0
        c_mean = float(c_stats['c_mean']) if c_stats['c_mean'] is not None else 0.0
        t_var = float(t_stats['t_var']) if t_stats['t_var'] is not None else 0.0
        c_var = float(c_stats['c_var']) if c_stats['c_var'] is not None else 0.0
        
        # Calculate pooled standard deviation
        pooled_sd = np.sqrt((t_var + c_var) / 2)
        
        # Calculate SMD
        if pooled_sd > 0:
            smd = (t_mean - c_mean) / pooled_sd
        else:
            smd = 0.0
        
        smd_results.append({
            'feature': feat,
            'treatment_mean': t_mean,
            'control_mean': c_mean,
            'smd': smd,
            'abs_smd': abs(smd)
        })
    
    return pd.DataFrame(smd_results)

# COMMAND ----------

print("🔄 Calculating balance before and after matching...")

# Get feature columns (uppercase from Snowflake)
feature_cols_actual = [column_name_map[f.upper()] for f in MATCHING_FEATURES 
                       if f.upper() in column_name_map]

# We need to use df_model which has all the features
# Join df_with_propensity back to df_model to get user_id -> returned_week_2 mapping
user_treatment_status = df_with_propensity.select("user_id", "returned_week_2", "cohort_type")

# Calculate SMD for each cohort separately
all_smd_results = []

for cohort in ['new', 'active', 'post_onboarding']:
    print(f"\n  📊 SMD for {cohort} cohort...")
    
    # Before matching - full treatment and control groups for this cohort
    # Use df_first_session which has all features including returned_week_2
    cohort_data_full = df_first_session.filter(F.col("cohort_type") == cohort)
    
    treatment_before = cohort_data_full.filter(F.col("returned_week_2") == 1)
    control_before = cohort_data_full.filter(F.col("returned_week_2") == 0)
    
    # After matching - only matched users for this cohort
    cohort_matched = matched_pairs.filter(F.col("cohort_type") == cohort)
    
    if cohort_matched.count() == 0:
        print(f"     ⚠️  No matches for {cohort}")
        continue
    
    treatment_matched_ids = cohort_matched.select("treatment_user_id").distinct()
    control_matched_ids = cohort_matched.select("control_user_id").distinct()
    
    treatment_after = df_first_session.filter(F.col("cohort_type") == cohort).join(
        treatment_matched_ids,
        F.col("user_id") == F.col("treatment_user_id"),
        "inner"
    )
    
    control_after = df_first_session.filter(F.col("cohort_type") == cohort).join(
        control_matched_ids,
        F.col("user_id") == F.col("control_user_id"),
        "inner"
    )
    
    # Calculate SMD before and after
    smd_before_cohort = calculate_smd(treatment_before, control_before, feature_cols_actual)
    smd_before_cohort['timing'] = 'before'
    smd_before_cohort['cohort_type'] = cohort
    
    smd_after_cohort = calculate_smd(treatment_after, control_after, feature_cols_actual)
    smd_after_cohort['timing'] = 'after'
    smd_after_cohort['cohort_type'] = cohort
    
    all_smd_results.extend([smd_before_cohort, smd_after_cohort])
    
    # Show improvement
    before_poor = (smd_before_cohort['abs_smd'] >= 0.25).sum()
    after_poor = (smd_after_cohort['abs_smd'] >= 0.25).sum()
    after_excellent = (smd_after_cohort['abs_smd'] < 0.1).sum()
    
    print(f"     Before: {before_poor} features with |SMD| ≥ 0.25")
    print(f"     After:  {after_poor} features with |SMD| ≥ 0.25, {after_excellent} with |SMD| < 0.1")

# Combine all results
smd_combined = pd.concat(all_smd_results, ignore_index=True)

# Also calculate overall SMD (aggregated across cohorts)
print(f"\n  📊 Overall SMD (all cohorts combined)...")

# Use df_first_session which has all features
treatment_before_all = df_first_session.filter(F.col("returned_week_2") == 1)
control_before_all = df_first_session.filter(F.col("returned_week_2") == 0)

treatment_matched_ids_all = matched_pairs.select("treatment_user_id").distinct()
control_matched_ids_all = matched_pairs.select("control_user_id").distinct()

treatment_after_all = df_first_session.join(
    treatment_matched_ids_all,
    F.col("user_id") == F.col("treatment_user_id"),
    "inner"
)

control_after_all = df_first_session.join(
    control_matched_ids_all,
    F.col("user_id") == F.col("control_user_id"),
    "inner"
)

smd_before = calculate_smd(treatment_before_all, control_before_all, feature_cols_actual)
smd_before['timing'] = 'before'
smd_before['cohort_type'] = 'overall'

smd_after = calculate_smd(treatment_after_all, control_after_all, feature_cols_actual)
smd_after['timing'] = 'after'
smd_after['cohort_type'] = 'overall'

# Add overall to combined results
smd_combined = pd.concat([smd_combined, smd_before, smd_after], ignore_index=True)

print(f"\n✅ Balance check complete for all cohorts and overall")

# COMMAND ----------


# Display balance summary
print("\n" + "="*80)
print("COVARIATE BALANCE SUMMARY BY COHORT")
print("="*80)

for cohort in ['new', 'active', 'post_onboarding', 'overall']:
    smd_cohort_before = smd_combined[
        (smd_combined['cohort_type'] == cohort) & 
        (smd_combined['timing'] == 'before')
    ]
    smd_cohort_after = smd_combined[
        (smd_combined['cohort_type'] == cohort) & 
        (smd_combined['timing'] == 'after')
    ]
    
    if len(smd_cohort_before) == 0:
        continue
    
    before_excellent = (smd_cohort_before['abs_smd'] < 0.1).sum()
    before_acceptable = ((smd_cohort_before['abs_smd'] >= 0.1) & (smd_cohort_before['abs_smd'] < 0.25)).sum()
    before_poor = (smd_cohort_before['abs_smd'] >= 0.25).sum()
    
    after_excellent = (smd_cohort_after['abs_smd'] < 0.1).sum()
    after_acceptable = ((smd_cohort_after['abs_smd'] >= 0.1) & (smd_cohort_after['abs_smd'] < 0.25)).sum()
    after_poor = (smd_cohort_after['abs_smd'] >= 0.25).sum()
    
    print(f"\n{cohort.upper()}:")
    print(f"  Before: |SMD| < 0.1: {before_excellent:2d} | < 0.25: {before_acceptable:2d} | ≥ 0.25: {before_poor:2d}")
    print(f"  After:  |SMD| < 0.1: {after_excellent:2d} | < 0.25: {after_acceptable:2d} | ≥ 0.25: {after_poor:2d}")

# Show worst balanced features after matching (overall)
print(f"\n⚠️  Top 10 Features with Largest |SMD| AFTER Matching (Overall):")
print("-" * 80)
smd_after_overall = smd_combined[
    (smd_combined['cohort_type'] == 'overall') & 
    (smd_combined['timing'] == 'after')
]
worst_balanced = smd_after_overall.nlargest(10, 'abs_smd')
for idx, row in worst_balanced.iterrows():
    status = "✅" if row['abs_smd'] < 0.1 else "⚠️" if row['abs_smd'] < 0.25 else "❌"
    print(f"{status} {row['feature']:<50} SMD: {row['smd']:>7.3f}")


# COMMAND ----------


# Visualize balance improvement (use overall SMD)
fig, axes = plt.subplots(1, 2, figsize=(16, 8))

# Plot 1: Love plot (SMD before vs after) - OVERALL
ax1 = axes[0]

# Get overall SMD data
smd_before_overall = smd_combined[
    (smd_combined['cohort_type'] == 'overall') & 
    (smd_combined['timing'] == 'before')
].copy()

smd_after_overall = smd_combined[
    (smd_combined['cohort_type'] == 'overall') & 
    (smd_combined['timing'] == 'after')
].copy()

# Sort by before SMD for better visualization
smd_plot = smd_before_overall.merge(
    smd_after_overall[['feature', 'smd', 'abs_smd']], 
    on='feature', 
    suffixes=('_before', '_after')
)
smd_plot = smd_plot.sort_values('abs_smd_before', ascending=False).head(20)

y_pos = np.arange(len(smd_plot))

ax1.scatter(smd_plot['smd_before'], y_pos, alpha=0.7, s=80, 
           color='#d62728', marker='o', label='Before Matching')
ax1.scatter(smd_plot['smd_after'], y_pos, alpha=0.7, s=80, 
           color='#2ca02c', marker='s', label='After Matching')

# Add reference lines
ax1.axvline(x=-0.1, color='gray', linestyle='--', linewidth=1, alpha=0.5)
ax1.axvline(x=0.1, color='gray', linestyle='--', linewidth=1, alpha=0.5)
ax1.axvline(x=0, color='black', linestyle='-', linewidth=1, alpha=0.3)

ax1.set_yticks(y_pos)
ax1.set_yticklabels(smd_plot['feature'], fontsize=8)
ax1.set_xlabel('Standardized Mean Difference', fontsize=11, fontweight='bold')
ax1.set_title('Covariate Balance: Before vs After Matching\n(Top 20 Features by Initial Imbalance)', 
             fontsize=12, fontweight='bold')
ax1.legend(loc='best', fontsize=10)
ax1.grid(axis='x', alpha=0.3)
ax1.set_xlim(-1, 1)

# Add balance threshold annotations
ax1.text(0.1, len(smd_plot) + 0.5, 'Excellent\nbalance', 
        ha='center', va='bottom', fontsize=8, color='gray')

# Plot 2: Distribution of absolute SMD
ax2 = axes[1]

bins = np.linspace(0, 1, 20)
ax2.hist(smd_before_overall['abs_smd'], bins=bins, alpha=0.6, color='#d62728', 
        label='Before Matching', edgecolor='black')
ax2.hist(smd_after_overall['abs_smd'], bins=bins, alpha=0.6, color='#2ca02c', 
        label='After Matching', edgecolor='black')

ax2.axvline(x=0.1, color='blue', linestyle='--', linewidth=2, 
           label='Excellent (0.1)', alpha=0.7)
ax2.axvline(x=0.25, color='orange', linestyle='--', linewidth=2, 
           label='Acceptable (0.25)', alpha=0.7)

ax2.set_xlabel('Absolute Standardized Mean Difference', fontsize=11, fontweight='bold')
ax2.set_ylabel('Number of Features', fontsize=11, fontweight='bold')
ax2.set_title('Distribution of Covariate Balance', fontsize=12, fontweight='bold')
ax2.legend(loc='best', fontsize=10)
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
display(fig)

# COMMAND ----------


# Plot balance improvement for each cohort separately
fig, axes = plt.subplots(1, 3, figsize=(20, 8))

for idx, cohort in enumerate(['new', 'active', 'post_onboarding']):
    ax = axes[idx]
    
    smd_cohort_before = smd_combined[
        (smd_combined['cohort_type'] == cohort) & 
        (smd_combined['timing'] == 'before')
    ]
    smd_cohort_after = smd_combined[
        (smd_combined['cohort_type'] == cohort) & 
        (smd_combined['timing'] == 'after')
    ]
    
    if len(smd_cohort_before) == 0:
        continue
    
    # Merge before and after
    smd_cohort_plot = smd_cohort_before.merge(
        smd_cohort_after[['feature', 'smd', 'abs_smd']], 
        on='feature', 
        suffixes=('_before', '_after')
    )
    smd_cohort_plot = smd_cohort_plot.sort_values('abs_smd_before', ascending=False).head(15)
    
    y_pos = np.arange(len(smd_cohort_plot))
    
    ax.scatter(smd_cohort_plot['smd_before'], y_pos, alpha=0.7, s=60, 
              color='#d62728', marker='o', label='Before')
    ax.scatter(smd_cohort_plot['smd_after'], y_pos, alpha=0.7, s=60, 
              color='#2ca02c', marker='s', label='After')
    
    # Add reference lines
    ax.axvline(x=-0.1, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.axvline(x=0.1, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.axvline(x=0, color='black', linestyle='-', linewidth=1, alpha=0.3)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(smd_cohort_plot['feature'], fontsize=7)
    ax.set_xlabel('SMD', fontsize=10, fontweight='bold')
    ax.set_title(f'{cohort.replace("_", " ").title()} Cohort\nTop 15 Features by Initial Imbalance', 
                fontsize=11, fontweight='bold')
    ax.legend(loc='best', fontsize=9)
    ax.grid(axis='x', alpha=0.3)
    ax.set_xlim(-0.5, 0.5)

plt.suptitle('Covariate Balance by Cohort Type', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
display(fig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Week 1 Order Performance

# COMMAND ----------

print("🔄 Loading week 1 order performance...")

# Load week 1 orders (days 0-6)
week1_orders_query = """
SELECT 
    consumer_id,
    COUNT(DISTINCT delivery_id) as week1_order_count,
    MIN(CASE WHEN delivery_id IS NOT NULL 
        THEN days_since_onboarding END) as days_to_first_order_week1
FROM proddb.fionafan.july_cohort_deliveries_28d
WHERE days_since_onboarding BETWEEN 0 AND 6
GROUP BY consumer_id
"""

df_week1_orders = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", week1_orders_query)
    .load()
)

print(f"  ✅ Loaded week 1 order data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get All Orders (28 Days) for Matched Users

# COMMAND ----------

# Get all user IDs from matched pairs
matched_user_ids = (
    matched_pairs.select(F.col("treatment_user_id").alias("user_id"))
    .union(matched_pairs.select(F.col("control_user_id").alias("user_id")))
    .distinct()
)

# Create temp view for Snowflake query
matched_user_ids.createOrReplaceTempView("matched_users_temp")

# Load all orders for matched users
all_orders_query = """
SELECT 
    consumer_id,
    COUNT(DISTINCT delivery_id) as total_orders_28d,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 0 AND 6 
        THEN delivery_id END) as orders_week1,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 7 AND 13 
        THEN delivery_id END) as orders_week2,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 14 AND 20 
        THEN delivery_id END) as orders_week3,
    COUNT(DISTINCT CASE WHEN days_since_onboarding BETWEEN 21 AND 27 
        THEN delivery_id END) as orders_week4,
    MIN(CASE WHEN delivery_id IS NOT NULL 
        THEN days_since_onboarding END) as days_to_first_order
FROM proddb.fionafan.july_cohort_deliveries_28d
GROUP BY consumer_id
"""

df_all_orders = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", all_orders_query)
    .load()
)

print(f"  ✅ Loaded 28-day order data for matched users")

# COMMAND ----------

# Load session retention data (week 3 and week 4)
session_retention_query = """
SELECT 
    consumer_id,
    CASE WHEN sessions_day_14_20 > 0 THEN 1 ELSE 0 END as had_session_week3,
    CASE WHEN sessions_day_21_27 > 0 THEN 1 ELSE 0 END as had_session_week4
FROM proddb.fionafan.all_user_sessions_enriched
"""

df_session_retention = (
    spark.read.format("snowflake")
    .options(**OPTIONS)
    .option("query", session_retention_query)
    .load()
)

print(f"  ✅ Loaded session retention data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Orders and Sessions to Matched Pairs

# COMMAND ----------

# Join treatment orders and GMV
matched_with_orders = matched_pairs.join(
    df_all_orders.alias("t_orders"),
    F.col("treatment_user_id") == F.col("t_orders.consumer_id"),
    "left"
).select(
    "treatment_user_id",
    "control_user_id",
    "cohort_type",
    "treatment_propensity",
    "control_propensity",
    "propensity_distance",
    "treatment_is_1111",
    "control_is_1111",
    F.col("t_orders.total_orders_28d").alias("treatment_total_orders"),
    F.col("t_orders.orders_week1").alias("treatment_orders_week1"),
    F.col("t_orders.orders_week2").alias("treatment_orders_week2"),
    F.col("t_orders.orders_week3").alias("treatment_orders_week3"),
    F.col("t_orders.orders_week4").alias("treatment_orders_week4"),
    F.col("t_orders.days_to_first_order").alias("treatment_days_to_first_order"),
)

# Join control orders and GMV
matched_with_orders = matched_with_orders.join(
    df_all_orders.alias("c_orders"),
    F.col("control_user_id") == F.col("c_orders.consumer_id"),
    "left"
).select(
    "*",
    F.col("c_orders.total_orders_28d").alias("control_total_orders"),
    F.col("c_orders.orders_week1").alias("control_orders_week1"),
    F.col("c_orders.orders_week2").alias("control_orders_week2"),
    F.col("c_orders.orders_week3").alias("control_orders_week3"),
    F.col("c_orders.orders_week4").alias("control_orders_week4"),
    F.col("c_orders.days_to_first_order").alias("control_days_to_first_order"),
)

# Join treatment session retention
matched_with_orders = matched_with_orders.join(
    df_session_retention.alias("t_sessions"),
    F.col("treatment_user_id") == F.col("t_sessions.consumer_id"),
    "left"
).select(
    "*",
    F.col("t_sessions.had_session_week3").alias("treatment_had_session_week3"),
    F.col("t_sessions.had_session_week4").alias("treatment_had_session_week4"),
)

# Join control session retention
matched_with_orders = matched_with_orders.join(
    df_session_retention.alias("c_sessions"),
    F.col("control_user_id") == F.col("c_sessions.consumer_id"),
    "left"
).select(
    "*",
    F.col("c_sessions.had_session_week3").alias("control_had_session_week3"),
    F.col("c_sessions.had_session_week4").alias("control_had_session_week4"),
)

# Fill nulls with 0
null_fill_cols = [
    "treatment_total_orders", "treatment_orders_week1", "treatment_orders_week2",
    "treatment_orders_week3", "treatment_orders_week4",
    "control_total_orders", "control_orders_week1", "control_orders_week2",
    "control_orders_week3", "control_orders_week4",
    "treatment_had_session_week3", "treatment_had_session_week4",
    "control_had_session_week3", "control_had_session_week4",
]

for col in null_fill_cols:
    matched_with_orders = matched_with_orders.withColumn(
        col, F.coalesce(F.col(col), F.lit(0))
    )

matched_with_orders.cache()

print(f"  ✅ Joined order data to {matched_with_orders.count():,} matched pairs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Outcome Differences

# COMMAND ----------

print("\n" + "="*80)
print("PROPENSITY MATCHING RESULTS")
print("="*80)

# Overall statistics
overall_stats = matched_with_orders.agg(
    F.count("*").alias("n_pairs"),
    F.avg("treatment_is_1111").alias("treatment_1111_rate"),
    F.avg("control_is_1111").alias("control_1111_rate"),
    F.avg("treatment_total_orders").alias("treatment_avg_orders"),
    F.avg("control_total_orders").alias("control_avg_orders"),
    F.sum(F.when(F.col("treatment_total_orders") > 0, 1).otherwise(0)).alias("treatment_ordered_count"),
    F.sum(F.when(F.col("control_total_orders") > 0, 1).otherwise(0)).alias("control_ordered_count"),
    F.avg("treatment_orders_week1").alias("treatment_avg_orders_week1"),
    F.avg("control_orders_week1").alias("control_avg_orders_week1"),
).collect()[0]

n_pairs = overall_stats['n_pairs']
treatment_1111_rate = overall_stats['treatment_1111_rate']
control_1111_rate = overall_stats['control_1111_rate']
treatment_avg_orders = overall_stats['treatment_avg_orders']
control_avg_orders = overall_stats['control_avg_orders']
treatment_ordered_count = overall_stats['treatment_ordered_count']
control_ordered_count = overall_stats['control_ordered_count']
treatment_avg_orders_week1 = overall_stats['treatment_avg_orders_week1']
control_avg_orders_week1 = overall_stats['control_avg_orders_week1']

# Calculate differences
pct_diff_1111 = ((treatment_1111_rate - control_1111_rate) / control_1111_rate * 100) if control_1111_rate > 0 else 0
pct_diff_orders = ((treatment_avg_orders - control_avg_orders) / control_avg_orders * 100) if control_avg_orders > 0 else 0
treatment_ordered_pct = treatment_ordered_count / n_pairs * 100
control_ordered_pct = control_ordered_count / n_pairs * 100
pct_diff_ordered = treatment_ordered_pct - control_ordered_pct

print(f"\n📊 OVERALL RESULTS ({n_pairs:,} matched pairs):")
print("-" * 80)
print(f"\n1111 Cohort Membership:")
print(f"  Treatment (returned week 2):  {treatment_1111_rate:.2%}")
print(f"  Control (did not return):     {control_1111_rate:.2%}")
print(f"  → Difference:                 {pct_diff_1111:+.1f}%")

print(f"\nTotal Orders (28 days):")
print(f"  Treatment:  {treatment_avg_orders:.2f} orders/user")
print(f"  Control:    {control_avg_orders:.2f} orders/user")
print(f"  → Difference: {pct_diff_orders:+.1f}%")

print(f"\n% Who Ordered (28 days):")
print(f"  Treatment:  {treatment_ordered_pct:.2f}%")
print(f"  Control:    {control_ordered_pct:.2f}%")
print(f"  → Difference: {pct_diff_ordered:+.2f} pp")

print(f"\nWeek 1 Orders:")
print(f"  Treatment:  {treatment_avg_orders_week1:.2f} orders/user")
print(f"  Control:    {control_avg_orders_week1:.2f} orders/user")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results by Cohort Type

# COMMAND ----------

print("\n" + "="*80)
print("RESULTS BY COHORT TYPE")
print("="*80)

cohort_stats = matched_with_orders.groupBy("cohort_type").agg(
    F.count("*").alias("n_pairs"),
    F.avg("treatment_is_1111").alias("treatment_1111_rate"),
    F.avg("control_is_1111").alias("control_1111_rate"),
    F.avg("treatment_total_orders").alias("treatment_avg_orders"),
    F.avg("control_total_orders").alias("control_avg_orders"),
    F.sum(F.when(F.col("treatment_total_orders") > 0, 1).otherwise(0)).alias("treatment_ordered_count"),
    F.sum(F.when(F.col("control_total_orders") > 0, 1).otherwise(0)).alias("control_ordered_count"),
).collect()

for row in cohort_stats:
    cohort = row['cohort_type']
    n_pairs = row['n_pairs']
    t_1111 = row['treatment_1111_rate']
    c_1111 = row['control_1111_rate']
    t_orders = row['treatment_avg_orders']
    c_orders = row['control_avg_orders']
    t_ordered = row['treatment_ordered_count'] / n_pairs * 100
    c_ordered = row['control_ordered_count'] / n_pairs * 100
    
    pct_diff_1111 = ((t_1111 - c_1111) / c_1111 * 100) if c_1111 > 0 else 0
    pct_diff_orders = ((t_orders - c_orders) / c_orders * 100) if c_orders > 0 else 0
    pct_diff_ordered = t_ordered - c_ordered
    
    print(f"\n{cohort.upper()} ({n_pairs:,} pairs):")
    print("-" * 60)
    print(f"  1111 Rate:    {t_1111:.2%} vs {c_1111:.2%}  ({pct_diff_1111:+.1f}%)")
    print(f"  Avg Orders:   {t_orders:.2f} vs {c_orders:.2f}  ({pct_diff_orders:+.1f}%)")
    print(f"  % Ordered:    {t_ordered:.1f}% vs {c_ordered:.1f}%  ({pct_diff_ordered:+.2f} pp)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Week 3 and Week 4 Outcomes

# COMMAND ----------

print("\n" + "="*80)
print("WEEK 3 & WEEK 4 RETENTION AND ORDER VOLUME")
print("="*80)

week34_stats = matched_with_orders.groupBy("cohort_type").agg(
    F.count("*").alias("n_pairs"),
    # Week 3
    F.sum("treatment_had_session_week3").alias("treatment_session_week3"),
    F.sum("control_had_session_week3").alias("control_session_week3"),
    F.avg("treatment_orders_week3").alias("treatment_avg_orders_week3"),
    F.avg("control_orders_week3").alias("control_avg_orders_week3"),
    # Week 4
    F.sum("treatment_had_session_week4").alias("treatment_session_week4"),
    F.sum("control_had_session_week4").alias("control_session_week4"),
    F.avg("treatment_orders_week4").alias("treatment_avg_orders_week4"),
    F.avg("control_orders_week4").alias("control_avg_orders_week4"),
).collect()

for row in week34_stats:
    cohort = row['cohort_type']
    n_pairs = row['n_pairs']
    
    # Week 3
    t_sess_w3 = row['treatment_session_week3'] / n_pairs * 100
    c_sess_w3 = row['control_session_week3'] / n_pairs * 100
    t_orders_w3 = row['treatment_avg_orders_week3']
    c_orders_w3 = row['control_avg_orders_week3']
    
    # Week 4
    t_sess_w4 = row['treatment_session_week4'] / n_pairs * 100
    c_sess_w4 = row['control_session_week4'] / n_pairs * 100
    t_orders_w4 = row['treatment_avg_orders_week4']
    c_orders_w4 = row['control_avg_orders_week4']
    
    # Calculate % differences
    pct_diff_sess_w3 = t_sess_w3 - c_sess_w3
    pct_diff_orders_w3 = ((t_orders_w3 - c_orders_w3) / c_orders_w3 * 100) if c_orders_w3 > 0 else 0
    pct_diff_sess_w4 = t_sess_w4 - c_sess_w4
    pct_diff_orders_w4 = ((t_orders_w4 - c_orders_w4) / c_orders_w4 * 100) if c_orders_w4 > 0 else 0
    
    print(f"\n{cohort.upper()} ({n_pairs:,} pairs):")
    print("-" * 70)
    print(f"\nWeek 3 (Days 14-20):")
    print(f"  Session Retention: {t_sess_w3:.1f}% vs {c_sess_w3:.1f}%  (Δ {pct_diff_sess_w3:+.1f} pp)")
    print(f"  Avg Orders/User:   {t_orders_w3:.2f} vs {c_orders_w3:.2f}  (Δ {pct_diff_orders_w3:+.1f}%)")
    
    print(f"\nWeek 4 (Days 21-27):")
    print(f"  Session Retention: {t_sess_w4:.1f}% vs {c_sess_w4:.1f}%  (Δ {pct_diff_sess_w4:+.1f} pp)")
    print(f"  Avg Orders/User:   {t_orders_w4:.2f} vs {c_orders_w4:.2f}  (Δ {pct_diff_orders_w4:+.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Convert to pandas for plotting
plot_data = matched_with_orders.select(
    "cohort_type",
    "treatment_is_1111",
    "control_is_1111",
    "treatment_total_orders",
    "control_total_orders",
    "treatment_had_session_week3",
    "control_had_session_week3",
    "treatment_had_session_week4",
    "control_had_session_week4",
    "treatment_orders_week3",
    "control_orders_week3",
    "treatment_orders_week4",
    "control_orders_week4",
).toPandas()

# COMMAND ----------

# Plot 1: 1111 Rate Comparison
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

metrics = [
    ('1111 Cohort Rate', 'treatment_is_1111', 'control_is_1111', '%'),
    ('Avg Total Orders', 'treatment_total_orders', 'control_total_orders', 'orders'),
    ('Ordered (Binary)', 
     (plot_data['treatment_total_orders'] > 0).astype(float), 
     (plot_data['control_total_orders'] > 0).astype(float), '%'),
]

for idx, (title, treatment_col, control_col, unit) in enumerate(metrics):
    ax = axes[idx]
    
    if isinstance(treatment_col, str):
        treatment_vals = plot_data.groupby('cohort_type')[treatment_col].mean()
        control_vals = plot_data.groupby('cohort_type')[control_col].mean()
    else:
        treatment_vals = plot_data.groupby('cohort_type').apply(lambda x: treatment_col[x.index].mean())
        control_vals = plot_data.groupby('cohort_type').apply(lambda x: control_col[x.index].mean())
    
    cohorts = treatment_vals.index.tolist()
    x = np.arange(len(cohorts))
    width = 0.35
    
    if unit == '%':
        treatment_display = treatment_vals * 100
        control_display = control_vals * 100
    else:
        treatment_display = treatment_vals
        control_display = control_vals
    
    bars1 = ax.bar(x - width/2, treatment_display, width, label='Returned Week 2', color='#2ca02c', alpha=0.8)
    bars2 = ax.bar(x + width/2, control_display, width, label='Did Not Return', color='#d62728', alpha=0.8)
    
    ax.set_ylabel(title + (f' ({unit})' if unit else ''), fontsize=11)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([c.replace('_', ' ').title() for c in cohorts], fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}',
                   ha='center', va='bottom', fontsize=8)

plt.suptitle('Propensity Matched Comparison: Week 2 Return Impact', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
display(fig)

# COMMAND ----------

# Plot 2: Week 3 and Week 4 Outcomes
fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Week 3 Session Retention
ax1 = axes[0, 0]
treatment_vals = plot_data.groupby('cohort_type')['treatment_had_session_week3'].mean() * 100
control_vals = plot_data.groupby('cohort_type')['control_had_session_week3'].mean() * 100

cohorts = treatment_vals.index.tolist()
x = np.arange(len(cohorts))
width = 0.35

bars1 = ax1.bar(x - width/2, treatment_vals, width, label='Returned Week 2', color='#2ca02c', alpha=0.8)
bars2 = ax1.bar(x + width/2, control_vals, width, label='Did Not Return', color='#d62728', alpha=0.8)

ax1.set_ylabel('Session Retention (%)', fontsize=11)
ax1.set_title('Week 3 Session Retention (Days 14-20)', fontsize=12, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels([c.replace('_', ' ').title() for c in cohorts], fontsize=10)
ax1.legend(fontsize=9)
ax1.grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}%', ha='center', va='bottom', fontsize=8)

# Week 4 Session Retention
ax2 = axes[0, 1]
treatment_vals = plot_data.groupby('cohort_type')['treatment_had_session_week4'].mean() * 100
control_vals = plot_data.groupby('cohort_type')['control_had_session_week4'].mean() * 100

bars1 = ax2.bar(x - width/2, treatment_vals, width, label='Returned Week 2', color='#2ca02c', alpha=0.8)
bars2 = ax2.bar(x + width/2, control_vals, width, label='Did Not Return', color='#d62728', alpha=0.8)

ax2.set_ylabel('Session Retention (%)', fontsize=11)
ax2.set_title('Week 4 Session Retention (Days 21-27)', fontsize=12, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels([c.replace('_', ' ').title() for c in cohorts], fontsize=10)
ax2.legend(fontsize=9)
ax2.grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.1f}%', ha='center', va='bottom', fontsize=8)

# Week 3 Orders
ax3 = axes[1, 0]
treatment_vals = plot_data.groupby('cohort_type')['treatment_orders_week3'].mean()
control_vals = plot_data.groupby('cohort_type')['control_orders_week3'].mean()

bars1 = ax3.bar(x - width/2, treatment_vals, width, label='Returned Week 2', color='#2ca02c', alpha=0.8)
bars2 = ax3.bar(x + width/2, control_vals, width, label='Did Not Return', color='#d62728', alpha=0.8)

ax3.set_ylabel('Avg Orders per User', fontsize=11)
ax3.set_title('Week 3 Average Orders (Days 14-20)', fontsize=12, fontweight='bold')
ax3.set_xticks(x)
ax3.set_xticklabels([c.replace('_', ' ').title() for c in cohorts], fontsize=10)
ax3.legend(fontsize=9)
ax3.grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.2f}', ha='center', va='bottom', fontsize=8)

# Week 4 Orders
ax4 = axes[1, 1]
treatment_vals = plot_data.groupby('cohort_type')['treatment_orders_week4'].mean()
control_vals = plot_data.groupby('cohort_type')['control_orders_week4'].mean()

bars1 = ax4.bar(x - width/2, treatment_vals, width, label='Returned Week 2', color='#2ca02c', alpha=0.8)
bars2 = ax4.bar(x + width/2, control_vals, width, label='Did Not Return', color='#d62728', alpha=0.8)

ax4.set_ylabel('Avg Orders per User', fontsize=11)
ax4.set_title('Week 4 Average Orders (Days 21-27)', fontsize=12, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels([c.replace('_', ' ').title() for c in cohorts], fontsize=10)
ax4.legend(fontsize=9)
ax4.grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.2f}', ha='center', va='bottom', fontsize=8)

plt.suptitle('Week 3 & 4 Outcomes: Session Retention and Order Volume', 
            fontsize=14, fontweight='bold', y=1.0)
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

# Save matched pairs with outcomes
output_table = "propensity_matched_week2_return_results"

matched_with_orders.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_table}") \
    .mode("overwrite") \
    .save()

print(f"\n✅ Saved {matched_with_orders.count():,} matched pairs to proddb.fionafan.{output_table}")

# COMMAND ----------

# Create summary table
summary_data = []

# Overall summary
summary_data.append({
    'cohort_type': 'overall',
    'n_pairs': n_pairs,
    'treatment_1111_rate': treatment_1111_rate,
    'control_1111_rate': control_1111_rate,
    'pct_diff_1111': pct_diff_1111,
    'treatment_avg_orders': treatment_avg_orders,
    'control_avg_orders': control_avg_orders,
    'pct_diff_orders': pct_diff_orders,
    'treatment_ordered_pct': treatment_ordered_pct,
    'control_ordered_pct': control_ordered_pct,
    'pct_diff_ordered': pct_diff_ordered,
    'created_at': datetime.now(),
})

# By cohort
for row in cohort_stats:
    cohort = row['cohort_type']
    n_pairs = row['n_pairs']
    t_1111 = row['treatment_1111_rate']
    c_1111 = row['control_1111_rate']
    t_orders = row['treatment_avg_orders']
    c_orders = row['control_avg_orders']
    t_ordered = row['treatment_ordered_count'] / n_pairs * 100
    c_ordered = row['control_ordered_count'] / n_pairs * 100
    
    pct_diff_1111 = ((t_1111 - c_1111) / c_1111 * 100) if c_1111 > 0 else 0
    pct_diff_orders = ((t_orders - c_orders) / c_orders * 100) if c_orders > 0 else 0
    pct_diff_ordered = t_ordered - c_ordered
    
    summary_data.append({
        'cohort_type': cohort,
        'n_pairs': n_pairs,
        'treatment_1111_rate': t_1111,
        'control_1111_rate': c_1111,
        'pct_diff_1111': pct_diff_1111,
        'treatment_avg_orders': t_orders,
        'control_avg_orders': c_orders,
        'pct_diff_orders': pct_diff_orders,
        'treatment_ordered_pct': t_ordered,
        'control_ordered_pct': c_ordered,
        'pct_diff_ordered': pct_diff_ordered,
        'created_at': datetime.now(),
    })

summary_df = spark.createDataFrame(summary_data)

output_summary_table = "propensity_matched_week2_return_summary"

summary_df.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", f"proddb.fionafan.{output_summary_table}") \
    .mode("overwrite") \
    .save()

print(f"✅ Saved summary to proddb.fionafan.{output_summary_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis Complete

# COMMAND ----------

print("\n" + "="*80)
print("PROPENSITY MATCHING ANALYSIS COMPLETE!")
print("="*80)
print(f"\n📊 Summary:")
print(f"   - Matched {n_pairs:,} pairs of users")
print(f"   - Treatment: Users who returned in week 2")
print(f"   - Control: Similar users (by first session) who didn't return")
print(f"\n💡 Key Findings:")
print(f"   - 1111 cohort rate difference: {pct_diff_1111:+.1f}%")
print(f"   - Total orders difference: {pct_diff_orders:+.1f}%")
print(f"   - % who ordered difference: {pct_diff_ordered:+.2f} pp")
print(f"\n💾 Results saved to:")
print(f"   - Matched pairs: proddb.fionafan.{output_table}")
print(f"   - Summary stats: proddb.fionafan.{output_summary_table}")
print(f"\n🎯 Interpretation:")
print(f"   This analysis shows the causal impact of returning in week 2")
print(f"   by comparing identical twins based on first session behavior")

# COMMAND ----------

print("\n" + "="*80)
print("CONDITIONAL ANALYSIS: WEEK 1 ORDERING BEHAVIOR")
print("="*80)

# Analysis for matched pairs conditional on week 1 ordering
week1_conditional = matched_with_orders.groupBy("cohort_type").agg(
    # Overall
    F.count("*").alias("n_pairs_total"),
    F.avg("treatment_is_1111").alias("treatment_1111_overall"),
    F.avg("control_is_1111").alias("control_1111_overall"),
    F.sum(F.when(F.col("treatment_total_orders") > 0, 1).otherwise(0)).alias("treatment_ordered_overall"),
    F.sum(F.when(F.col("control_total_orders") > 0, 1).otherwise(0)).alias("control_ordered_overall"),
    
    # Ordered in Week 1
    F.sum(F.when(F.col("treatment_orders_week1") > 0, 1).otherwise(0)).alias("n_ordered_week1"),
    F.sum(F.when((F.col("treatment_orders_week1") > 0) & (F.col("treatment_is_1111") == 1), 1).otherwise(0)).alias("treatment_1111_ordered_week1"),
    F.sum(F.when((F.col("control_orders_week1") > 0) & (F.col("control_is_1111") == 1), 1).otherwise(0)).alias("control_1111_ordered_week1"),
    F.sum(F.when((F.col("treatment_orders_week1") > 0) & (F.col("treatment_total_orders") > 0), 1).otherwise(0)).alias("treatment_ordered_28d_given_week1"),
    F.sum(F.when((F.col("control_orders_week1") > 0) & (F.col("control_total_orders") > 0), 1).otherwise(0)).alias("control_ordered_28d_given_week1"),
    F.sum(F.when(F.col("treatment_orders_week1") > 0, F.col("treatment_total_orders")).otherwise(0)).alias("treatment_total_orders_given_week1"),
    F.sum(F.when(F.col("control_orders_week1") > 0, F.col("control_total_orders")).otherwise(0)).alias("control_total_orders_given_week1"),
    
    # Did NOT order in Week 1
    F.sum(F.when(F.col("treatment_orders_week1") == 0, 1).otherwise(0)).alias("n_no_order_week1"),
    F.sum(F.when((F.col("treatment_orders_week1") == 0) & (F.col("treatment_is_1111") == 1), 1).otherwise(0)).alias("treatment_1111_no_order_week1"),
    F.sum(F.when((F.col("control_orders_week1") == 0) & (F.col("control_is_1111") == 1), 1).otherwise(0)).alias("control_1111_no_order_week1"),
    F.sum(F.when((F.col("treatment_orders_week1") == 0) & (F.col("treatment_total_orders") > 0), 1).otherwise(0)).alias("treatment_ordered_28d_no_week1"),
    F.sum(F.when((F.col("control_orders_week1") == 0) & (F.col("control_total_orders") > 0), 1).otherwise(0)).alias("control_ordered_28d_no_week1"),
    F.sum(F.when(F.col("treatment_orders_week1") == 0, F.col("treatment_total_orders")).otherwise(0)).alias("treatment_total_orders_no_week1"),
    F.sum(F.when(F.col("control_orders_week1") == 0, F.col("control_total_orders")).otherwise(0)).alias("control_total_orders_no_week1"),
).collect()

for row in week1_conditional:
    cohort = row['cohort_type']
    n_pairs = row['n_pairs_total']
    
    # Week 1 orderers
    n_ordered_w1 = row['n_ordered_week1']
    t_1111_w1 = row['treatment_1111_ordered_week1'] / n_ordered_w1 * 100 if n_ordered_w1 > 0 else 0
    c_1111_w1 = row['control_1111_ordered_week1'] / n_ordered_w1 * 100 if n_ordered_w1 > 0 else 0
    t_ord_w1 = row['treatment_ordered_28d_given_week1'] / n_ordered_w1 * 100 if n_ordered_w1 > 0 else 0
    c_ord_w1 = row['control_ordered_28d_given_week1'] / n_ordered_w1 * 100 if n_ordered_w1 > 0 else 0
    t_avg_orders_w1 = row['treatment_total_orders_given_week1'] / n_ordered_w1 if n_ordered_w1 > 0 else 0
    c_avg_orders_w1 = row['control_total_orders_given_week1'] / n_ordered_w1 if n_ordered_w1 > 0 else 0
    
    # Non-orderers in week 1
    n_no_order_w1 = row['n_no_order_week1']
    t_1111_no_w1 = row['treatment_1111_no_order_week1'] / n_no_order_w1 * 100 if n_no_order_w1 > 0 else 0
    c_1111_no_w1 = row['control_1111_no_order_week1'] / n_no_order_w1 * 100 if n_no_order_w1 > 0 else 0
    t_ord_no_w1 = row['treatment_ordered_28d_no_week1'] / n_no_order_w1 * 100 if n_no_order_w1 > 0 else 0
    c_ord_no_w1 = row['control_ordered_28d_no_week1'] / n_no_order_w1 * 100 if n_no_order_w1 > 0 else 0
    t_avg_orders_no_w1 = row['treatment_total_orders_no_week1'] / n_no_order_w1 if n_no_order_w1 > 0 else 0
    c_avg_orders_no_w1 = row['control_total_orders_no_week1'] / n_no_order_w1 if n_no_order_w1 > 0 else 0
    
    # Calculate percentage differences
    pct_diff_orders_w1 = ((t_avg_orders_w1 - c_avg_orders_w1) / c_avg_orders_w1 * 100) if c_avg_orders_w1 > 0 else 0
    pct_diff_orders_no_w1 = ((t_avg_orders_no_w1 - c_avg_orders_no_w1) / c_avg_orders_no_w1 * 100) if c_avg_orders_no_w1 > 0 else 0
    
    print(f"\n{cohort.upper()} COHORT ({n_pairs:,} pairs):")
    print("-" * 80)
    
    print(f"\nOrdered in Week 1 ({n_ordered_w1:,} pairs):")
    print(f"  1111 Rate:      Treatment {t_1111_w1:.1f}% vs Control {c_1111_w1:.1f}%  (Δ {t_1111_w1-c_1111_w1:+.1f} pp)")
    print(f"  Order Rate:     Treatment {t_ord_w1:.1f}% vs Control {c_ord_w1:.1f}%  (Δ {t_ord_w1-c_ord_w1:+.1f} pp)")
    print(f"  Avg Orders/User: Treatment {t_avg_orders_w1:.2f} vs Control {c_avg_orders_w1:.2f}  (Δ {pct_diff_orders_w1:+.1f}%)")
    
    print(f"\nDid NOT Order in Week 1 ({n_no_order_w1:,} pairs):")
    print(f"  1111 Rate:      Treatment {t_1111_no_w1:.1f}% vs Control {c_1111_no_w1:.1f}%  (Δ {t_1111_no_w1-c_1111_no_w1:+.1f} pp)")
    print(f"  Order Rate:     Treatment {t_ord_no_w1:.1f}% vs Control {c_ord_no_w1:.1f}%  (Δ {t_ord_no_w1-c_ord_no_w1:+.1f} pp)")
    print(f"  Avg Orders/User: Treatment {t_avg_orders_no_w1:.2f} vs Control {c_avg_orders_no_w1:.2f}  (Δ {pct_diff_orders_no_w1:+.1f}%)")
